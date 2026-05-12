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
use tracing::info;

mod bucket_client;
mod cas_uploader;
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

    /// AWS region (otherwise uses default credential chain)
    #[arg(long, env = "AWS_REGION", default_value = "us-east-1")]
    aws_region: String,

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

    /// This task's shard index (0-based). Used with --shard-count for slurm
    /// array-based sharded clones. Each task processes the subset of source
    /// files where `fnv1a64(key) % shard_count == shard_id`. Default 0.
    #[arg(long, default_value_t = 0, requires = "shard_count")]
    shard_id: u64,

    /// Total number of shards (slurm array size). 1 = no sharding (default).
    /// File assignment is stable across re-runs (same hash modulo), so
    /// retries of failed array indices reprocess the same file subset.
    #[arg(long, default_value_t = 1)]
    shard_count: u64,

    /// Dry run: list source and destination, plan diff, but don't transfer
    #[arg(long)]
    dry_run: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "hf_s3ream=info,xet_data=info,xet_client=info".into()),
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
        "starting clone",
    );

    if cli.shard_count == 0 {
        anyhow::bail!("--shard-count must be >= 1");
    }
    if cli.shard_id >= cli.shard_count {
        anyhow::bail!(
            "--shard-id ({}) must be < --shard-count ({})",
            cli.shard_id,
            cli.shard_count
        );
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
        shard_id: cli.shard_id,
        shard_count: cli.shard_count,
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
