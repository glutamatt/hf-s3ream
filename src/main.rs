//! hf-s3ream — stream S3 prefixes into HuggingFace Buckets.
//!
//! Architecture: object_store streams S3 GETs into xet-data's CAS upload pipeline
//! via SingleFileCleaner.add_data(); on completion of all files, a single batched
//! commit is sent to /api/buckets/{id}/batch.
//!
//! No disk staging in the hot path. Memory bounded by the xorb formation window
//! (~64-128 MiB per active file) plus object_store's stream buffers.

use anyhow::{Context, Result};
use clap::Parser;
use std::collections::BTreeMap;
use std::time::Duration;
use tracing::info;

mod bucket_client;
mod cas_uploader;
mod jobs_client;
mod progress;
mod sync;

#[derive(Parser, Debug)]
#[command(name = "hf-s3ream", version, about, long_about = None)]
struct Cli {
    /// Source S3 URL: s3://bucket/prefix/
    source: String,

    /// Destination HF bucket: hf://buckets/org/name (org/name also accepted)
    dest: String,

    /// HF Hub endpoint
    #[arg(long, default_value = "https://huggingface.co", env = "HF_ENDPOINT")]
    hub_endpoint: String,

    /// HF token. Defaults to HF_TOKEN env var or ~/.cache/huggingface/token
    #[arg(long, env = "HF_TOKEN")]
    hf_token: Option<String>,

    /// AWS region of the SOURCE bucket. If unset, it is auto-detected from the
    /// bucket via S3 GetBucketLocation (falling back to us-east-1).
    #[arg(long, env = "AWS_REGION")]
    aws_region: Option<String>,

    /// Number of files uploaded concurrently. 32 saturates a typical 25 Gbps
    /// cloud VM NIC; 64-128 are within 5% of optimal. See README for the sweep.
    #[arg(long, default_value_t = 32)]
    parallel_files: usize,

    /// Number of parallel ranged S3 GETs per file (multipart download).
    /// 1 = single GET (one TCP connection per file). Higher saturates the NIC
    /// faster on a single file, at the cost of more memory.
    #[arg(long, default_value_t = 8)]
    s3_part_concurrency: usize,

    /// Part size in MiB for multipart S3 reads. Files smaller than this are
    /// downloaded with a single GET regardless of --s3-part-concurrency.
    #[arg(long, default_value_t = 16)]
    s3_part_size_mib: usize,

    /// XOR every byte of the S3 stream with this constant before feeding it
    /// to the xet pipeline. 0 = passthrough (default, real sync). Any non-zero
    /// value defeats CAS dedup against existing data — use only for benchmarking
    /// upload-side throughput.
    #[arg(long, default_value_t = 0, value_parser = parse_u8_hex_or_dec)]
    xor_byte: u8,

    /// Stop after this many GiB of source bytes have been queued for upload.
    /// 0 = unlimited (default). Use for shorter benchmark runs.
    #[arg(long, default_value_t = 0)]
    limit_gib: u64,

    /// Exclude S3 keys matching the given glob (matched against the FULL key,
    /// so `*decay*` matches anywhere in the path). Repeat for multiple patterns;
    /// any match excludes the object. Filtering is client-side after listing
    /// (S3 ListObjectsV2 has no native exclude).
    #[arg(long = "exclude", value_name = "GLOB")]
    exclude: Vec<String>,

    /// Worker range LOWER bound (exclusive): only copy keys strictly greater
    /// than this. Maps to S3 ListObjectsV2 `start-after`. Set by the lister when
    /// it spawns a per-range copier; unset = start from the beginning of the
    /// prefix. Adjacent ranges share a boundary (this == the previous range's
    /// --stop-at) so the partition is gap-free and overlap-free.
    #[arg(long)]
    start_after: Option<String>,

    /// Worker range UPPER bound (inclusive): stop once a listed key sorts after
    /// this. Since S3 returns keys in ascending order, listing halts entirely at
    /// that point. Unset = list to the end of the prefix.
    #[arg(long)]
    stop_at: Option<String>,

    /// Files committed per bucket batch (the "minibatch"). Lower = more frequent
    /// commits + lower peak memory; higher = fewer, larger commits. The listing
    /// streams, so this bounds memory regardless of total object count.
    ///
    /// Default 1000 (was 20000): each finalize registers every file in the
    /// session with the destination, so a fleet finalizing 20k-file sessions
    /// at once write-throttled it (2026-07-13) — the server then holds shard
    /// POSTs open for minutes and copiers eat FINALIZE_TIMEOUT. Smaller
    /// sessions spread the same writes along the run and let each shard POST
    /// finish fast. Official clients commit ≤256–1000 files
    /// (upload_large_folder UPLOAD_BATCH_SIZE_XET=256).
    #[arg(long, default_value_t = 1_000, env = "COMMIT_CHUNK")]
    commit_chunk: usize,

    /// ALSO commit once the current session reaches this many GiB of source
    /// bytes, whichever of --commit-chunk / --commit-gib trips first. Spreads
    /// commits along the run for big-file ranges where the file count never
    /// reaches --commit-chunk (otherwise one giant finalize+commit lands at the
    /// very end — and a whole fleet doing that simultaneously stampedes the
    /// CAS). 0 disables the byte trigger.
    #[arg(long, default_value_t = 16, env = "COMMIT_GIB")]
    commit_gib: u64,

    /// Dry run: list source and destination, plan diff, but don't transfer
    #[arg(long)]
    dry_run: bool,

    // ── Planner mode (`--plan`) ────────────────────────────────────────────
    /// Planner mode: list the source ONCE, cut the sorted keyspace into ranges,
    /// and spawn a copier Job per range (each runs a normal --start-after/
    /// --stop-at worker). This is how the web Space drives a full copy.
    #[arg(long)]
    plan: bool,

    /// Cut a new range once it reaches this many GiB (0 = no byte limit).
    #[arg(long, default_value_t = 250)]
    range_gib: u64,

    /// ...or this many keys, whichever comes first (0 = no key limit).
    #[arg(long, default_value_t = 2_000_000)]
    range_keys: u64,

    /// Container image the spawned copiers run (should match this binary's build).
    #[arg(long, default_value = "ghcr.io/glutamatt/hf-s3ream:v0.3.1")]
    copier_image: String,

    /// HF Jobs flavor for spawned copiers.
    #[arg(long, default_value = "cpu-upgrade")]
    copier_flavor: String,

    /// Namespace to spawn copier Jobs under. Unset → resolved via whoami.
    #[arg(long)]
    jobs_namespace: Option<String>,

    /// Cap on concurrently-active (spawned, non-terminal) copiers.
    #[arg(long, default_value_t = 32)]
    max_inflight: usize,

    /// Minimum delay between consecutive copier launches (spreads image pulls).
    #[arg(long, default_value_t = 750)]
    launch_stagger_ms: u64,

    /// Value of the `hf-s3ream-run` label stamped on every copier, so the Space
    /// can re-attach to a run's copiers after a reload.
    #[arg(long, default_value = "hf-s3ream")]
    run_label: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Default to a quiet log: our own start/progress/done lines (hf_s3ream=info)
    // plus only WARN+ from the xet stack, which otherwise emits an INFO line per
    // CAS request (retry-wrapper, http-client config, adaptive-concurrency,
    // per-request success). Override with RUST_LOG=… for the full firehose,
    // e.g. RUST_LOG=hf_s3ream=debug,xet_client=info,xet_data=info.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "hf_s3ream=info,xet_data=warn,xet_client=warn".into()),
        )
        .init();

    let cli = Cli::parse();
    let token = resolve_token(cli.hf_token.clone()).context("resolve HF token")?;

    info!(
        source = %cli.source,
        dest = %cli.dest,
        endpoint = %cli.hub_endpoint,
        parallel = cli.parallel_files,
        dry_run = cli.dry_run,
        plan = cli.plan,
        "starting clone",
    );

    if cli.plan {
        // Validate the dest parses (copiers re-parse it), then run the planner.
        let _ = parse_dest(&cli.dest)?;
        let copier_secrets = copier_secrets(&token);
        let copier_env = copier_env();
        return sync::plan(sync::PlanConfig {
            source_s3_url: cli.source,
            dest: cli.dest,
            hub_endpoint: cli.hub_endpoint,
            hf_token: token,
            aws_region: cli.aws_region,
            exclude_globs: cli.exclude,
            limit_bytes: cli.limit_gib.saturating_mul(1024 * 1024 * 1024),
            range_bytes: cli.range_gib.saturating_mul(1024 * 1024 * 1024),
            range_keys: cli.range_keys,
            copier_image: cli.copier_image,
            copier_flavor: cli.copier_flavor,
            jobs_namespace: cli.jobs_namespace,
            max_inflight: cli.max_inflight,
            launch_stagger: Duration::from_millis(cli.launch_stagger_ms),
            run_label: cli.run_label,
            commit_chunk: cli.commit_chunk,
            commit_gib: cli.commit_gib,
            s3_part_concurrency: cli.s3_part_concurrency,
            s3_part_size_mib: cli.s3_part_size_mib,
            xor_byte: cli.xor_byte,
            copier_secrets,
            copier_env,
        })
        .await;
    }

    sync::run(sync::Config {
        source_s3_url: cli.source,
        dest_bucket: parse_dest(&cli.dest)?,
        hub_endpoint: cli.hub_endpoint,
        hf_token: token,
        aws_region: cli.aws_region,
        parallel_files: cli.parallel_files,
        s3_part_concurrency: cli.s3_part_concurrency,
        s3_part_size: (cli.s3_part_size_mib as u64) * 1024 * 1024,
        xor_byte: cli.xor_byte,
        limit_bytes: cli.limit_gib.saturating_mul(1024 * 1024 * 1024),
        exclude_globs: cli.exclude,
        start_after: cli.start_after,
        stop_at: cli.stop_at,
        commit_chunk: cli.commit_chunk,
        commit_bytes: cli.commit_gib.saturating_mul(1024 * 1024 * 1024),
        dry_run: cli.dry_run,
    })
    .await
}

fn resolve_token(cli_token: Option<String>) -> Result<String> {
    if let Some(t) = cli_token {
        return Ok(t);
    }
    let home = std::env::var("HOME").context("HOME not set")?;
    let path = format!("{home}/.cache/huggingface/token");
    std::fs::read_to_string(&path)
        .map(|s| s.trim().to_string())
        .with_context(|| format!("no --hf-token / HF_TOKEN set and {path} not readable"))
}

/// Secrets forwarded to every copier: the AWS creds the planner itself received
/// (SSO temp creds also carry a session token), plus the HF token so the copier
/// can mint a CAS write token. Sent via the encrypted `secrets` channel — never
/// on argv.
fn copier_secrets(hf_token: &str) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    for k in [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
    ] {
        if let Ok(v) = std::env::var(k) {
            if !v.is_empty() {
                m.insert(k.to_string(), v);
            }
        }
    }
    m.insert("HF_TOKEN".to_string(), hf_token.to_string());
    m
}

/// Non-secret env forwarded to every copier. Forward RUST_LOG if set, else the
/// same quiet default this binary uses so copier logs aren't a xet firehose.
fn copier_env() -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    let rust_log = std::env::var("RUST_LOG")
        .unwrap_or_else(|_| "hf_s3ream=info,xet_data=warn,xet_client=warn".to_string());
    m.insert("RUST_LOG".to_string(), rust_log);
    m
}

fn parse_dest(s: &str) -> Result<BucketRef> {
    let s = s.strip_prefix("hf://buckets/").unwrap_or(s);
    let (org, name) = s.split_once('/').with_context(|| {
        format!("bucket dest must be org/name or hf://buckets/org/name, got: {s}")
    })?;
    Ok(BucketRef {
        org: org.to_string(),
        name: name.to_string(),
    })
}

#[derive(Debug, Clone)]
pub struct BucketRef {
    pub org: String,
    pub name: String,
}

impl BucketRef {
    pub fn id(&self) -> String {
        format!("{}/{}", self.org, self.name)
    }
}

fn parse_u8_hex_or_dec(s: &str) -> std::result::Result<u8, String> {
    let s = s.trim();
    let parsed = if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        u8::from_str_radix(hex, 16)
    } else {
        s.parse::<u8>()
    };
    parsed.map_err(|e| format!("invalid u8 (0..=255 or 0xNN): {e}"))
}
