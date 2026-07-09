//! Top-level sync orchestrator.
//!
//! Phase 2: object_store streams S3 → CasUploader (xet-data CAS pipeline) →
//! collects XetFileInfo per file → single batch commit on /api/buckets/{id}/batch.

use anyhow::{bail, Context, Result};
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ClientOptions, ObjectStore, ObjectStoreExt};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use url::Url;

use crate::bucket_client::{BatchOp, BucketClient};
use crate::cas_uploader::CasUploader;
use crate::BucketRef;

pub struct Config {
    pub source_s3_url: String,
    pub dest_bucket: BucketRef,
    pub hub_endpoint: String,
    pub hf_token: String,
    /// Explicit source-bucket region. `None` → auto-detect via GetBucketLocation.
    pub aws_region: Option<String>,
    pub parallel_files: usize,
    /// Parallel ranged GETs per file (1 = single GET).
    pub s3_part_concurrency: usize,
    /// Part size in bytes for multipart reads.
    pub s3_part_size: u64,
    /// XOR every byte from S3 with this constant before chunking. 0 = no-op.
    /// Used to defeat CAS dedup for upload-bandwidth benchmarks.
    pub xor_byte: u8,
    /// Stop adding files to the work-list once total bytes reaches this. 0 = unlimited.
    pub limit_bytes: u64,
    /// Glob patterns to exclude (matched against the full S3 key). Multiple
    /// patterns OR'd; any match excludes the object. Empty = no exclusion.
    pub exclude_globs: Vec<String>,
    /// This task's shard index (0-based) for slurm-array sharded clones.
    pub shard_id: u64,
    /// Total number of shards. 1 = no sharding.
    pub shard_count: u64,
    pub dry_run: bool,
    /// Files committed per bucket batch (the "minibatch"). Lower = more frequent
    /// commits + lower peak memory; higher = fewer, larger commits. Bounds memory
    /// (ops Vec + in-flight files) so this scales to hundreds of millions of
    /// objects without holding the whole listing in RAM or POSTing one giant batch.
    pub commit_chunk: usize,
}

pub async fn run(cfg: Config) -> Result<()> {
    // Resolve the source-bucket region BEFORE building the S3 clients: an
    // explicit --aws-region/$AWS_REGION wins, else auto-detect from the bucket
    // (GetBucketLocation). Using the wrong region makes list/get fail, so this
    // removes the #1 "S3 access failed" footgun for non-us-east-1 buckets.
    let (bucket_hint, _) = parse_s3_url(&cfg.source_s3_url)?;
    let region = resolve_region(&bucket_hint, cfg.aws_region.as_deref()).await;

    let (store, bucket_name, prefix) = build_s3_store(&cfg.source_s3_url, &region)?;
    info!(bucket = %bucket_name, prefix = %prefix, region = %region, "scanning S3 source (streaming)");

    let bucket_http = Arc::new(BucketClient::new(
        cfg.hub_endpoint.clone(),
        cfg.hf_token.clone(),
    ));

    // Build the --exclude globset once.
    let exclude = if cfg.exclude_globs.is_empty() {
        None
    } else {
        let mut builder = globset::GlobSetBuilder::new();
        for pat in &cfg.exclude_globs {
            builder.add(
                globset::Glob::new(pat)
                    .with_context(|| format!("invalid --exclude glob: {pat}"))?,
            );
        }
        Some(builder.build().context("build globset")?)
    };

    // aws-sdk-s3 paginator for the listing (raw String keys, no Path validation).
    let sdk = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new(region.clone()))
        .load()
        .await;
    let client = aws_sdk_s3::Client::new(&sdk);
    let mut paginator = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .prefix(&prefix)
        .into_paginator()
        .send();

    let started = Instant::now();
    let parallel = cfg.parallel_files.max(1);
    let part_concurrency = cfg.s3_part_concurrency.max(1);
    let part_size = cfg.s3_part_size.max(1);
    let xor_byte = cfg.xor_byte;
    if xor_byte != 0 {
        warn!(
            xor_byte = format!("0x{xor_byte:02x}"),
            "XOR transform active: data uploaded will NOT match source. Benchmark mode."
        );
    }

    // Shared live counters (real copy): files/bytes committed, and kept-so-far
    // (the moving "total", since we don't know it until listing completes).
    let files_done = Arc::new(AtomicU64::new(0));
    let bytes_done = Arc::new(AtomicU64::new(0));
    let kept_counter = Arc::new(AtomicU64::new(0));

    let stats_handle = if cfg.dry_run {
        None
    } else {
        let files_done = files_done.clone();
        let bytes_done = bytes_done.clone();
        let kept_counter = kept_counter.clone();
        Some(tokio::spawn(async move {
            let mut last_t = Instant::now();
            let mut last_bytes = 0u64;
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            tick.tick().await; // skip the immediate first tick
            loop {
                tick.tick().await;
                let now = Instant::now();
                let f = files_done.load(Ordering::Relaxed);
                let b = bytes_done.load(Ordering::Relaxed);
                let dt = now.duration_since(last_t).as_secs_f64();
                let mibps_5s = if dt > 0.0 {
                    (b.saturating_sub(last_bytes)) as f64 / dt / (1024.0 * 1024.0)
                } else {
                    0.0
                };
                let mibps_avg =
                    b as f64 / started.elapsed().as_secs_f64().max(0.001) / (1024.0 * 1024.0);
                let total = kept_counter.load(Ordering::Relaxed);
                info!(
                    files = f,
                    kept = total,
                    gib_done = b as f64 / 1024.0_f64.powi(3),
                    last_5s_mibps = format!("{mibps_5s:.0}"),
                    avg_mibps = format!("{mibps_avg:.0}"),
                    elapsed_s = format!("{:.0}", started.elapsed().as_secs_f64()),
                    "progress",
                );
                println!(
                    "PROGRESS {}",
                    serde_json::json!({
                        "files": f,
                        "total": total,
                        "bytes_done": b,
                        "mibps_5s": mibps_5s.round(),
                        "mibps_avg": mibps_avg.round(),
                        "elapsed_s": started.elapsed().as_secs(),
                    })
                );
                last_t = now;
                last_bytes = b;
            }
        }))
    };

    // Streaming state.
    let part16 = 16u64 * 1024 * 1024; // matches default --s3-part-size-mib
    let mut listed = 0u64;
    let mut kept = 0u64;
    let mut skipped_invalid = 0u64;
    let mut acc_bytes = 0u64; // for --limit-gib
    let mut chunk: Vec<S3Object> = Vec::with_capacity(cfg.commit_chunk.clamp(1, 65_536));
    // dry-run stat accumulators (streaming — no per-object retention).
    let (mut d_count, mut d_total, mut d_min, mut d_max, mut d_le16) =
        (0u64, 0u64, u64::MAX, 0u64, 0u64);
    let mut hist = [0u64; 64]; // size buckets by bit-length → approximate median
    let mut limit_hit = false;

    'outer: while let Some(page) = paginator.next().await {
        let page = page.context("aws-sdk-s3 list_objects_v2")?;
        for obj in page.contents() {
            listed += 1;
            let raw_key = match obj.key() {
                Some(k) => k.to_string(),
                None => continue,
            };
            if !key_belongs_to_prefix(&raw_key, &prefix) {
                continue;
            }
            // Keys with empty `//` segments are valid in S3 but not representable
            // as object_store Paths. We can't pre-scan in a stream, so skip+count
            // (was: refuse the whole clone).
            let parts: Vec<&str> = raw_key.split('/').collect();
            if parts.iter().any(|p| p.is_empty()) && parts != [""] {
                skipped_invalid += 1;
                if skipped_invalid <= 10 {
                    warn!(key = %raw_key, "skipping S3 key with empty path segment (invalid for object_store)");
                }
                continue;
            }
            let size = obj.size().unwrap_or(0).max(0) as u64;

            if let Some(set) = &exclude {
                if set.is_match(&raw_key) {
                    continue;
                }
            }
            if cfg.shard_count > 1 && fnv1a64(raw_key.as_bytes()) % cfg.shard_count != cfg.shard_id
            {
                continue;
            }
            if cfg.limit_bytes > 0 && acc_bytes >= cfg.limit_bytes {
                limit_hit = true;
                break 'outer;
            }
            acc_bytes = acc_bytes.saturating_add(size);
            kept += 1;
            kept_counter.store(kept, Ordering::Relaxed);

            if cfg.dry_run {
                d_count += 1;
                d_total = d_total.saturating_add(size);
                d_min = d_min.min(size);
                d_max = d_max.max(size);
                if size <= part16 {
                    d_le16 += 1;
                }
                let b = (64 - size.max(1).leading_zeros()) as usize;
                hist[b.min(63)] += 1;
            } else {
                let path = Path::from_iter(parts.iter().copied());
                chunk.push(S3Object {
                    key: raw_key,
                    path,
                    size,
                });
                if chunk.len() >= cfg.commit_chunk.max(1) {
                    let batch = std::mem::take(&mut chunk);
                    let n = process_chunk(
                        batch,
                        &store,
                        &bucket_http,
                        &cfg.dest_bucket,
                        &prefix,
                        parallel,
                        part_concurrency,
                        part_size,
                        xor_byte,
                        &files_done,
                        &bytes_done,
                    )
                    .await?;
                    info!(committed = n, kept, listed, "committed chunk");
                }
            }

            if listed.is_multiple_of(100_000) {
                info!(listed, kept, skipped_invalid, "listing…");
                println!(
                    "LISTING {}",
                    serde_json::json!({"listed": listed, "kept": kept})
                );
            }
        }
    }
    println!(
        "LISTING {}",
        serde_json::json!({"listed": listed, "kept": kept, "done": true})
    );
    info!(listed, kept, skipped_invalid, limit_hit, "listing complete");

    if cfg.dry_run {
        let pct_le_16mib = if d_count == 0 {
            0.0
        } else {
            (d_le16 as f64 * 1000.0 / d_count as f64).round() / 10.0
        };
        // Approximate median from the bit-length histogram (order-of-magnitude).
        let mut cum = 0u64;
        let mut median = 0u64;
        for (b, c) in hist.iter().enumerate() {
            cum += c;
            if d_count > 0 && cum * 2 >= d_count {
                median = if b == 0 { 0 } else { 1u64 << (b - 1) };
                break;
            }
        }
        let stats = serde_json::json!({
            "count": d_count,
            "total_bytes": d_total,
            "min": if d_count > 0 { d_min } else { 0 },
            "median": median,
            "max": d_max,
            "pct_le_16mib": pct_le_16mib,
            "region": region,
            "skipped_invalid": skipped_invalid,
        });
        println!("DRYRUN_STATS {stats}");
        // Access smoke test for the HF side: can this token mint a CAS write
        // token for the destination bucket? Non-fatal (CLI dry-run without a
        // bucket still exits 0).
        match bucket_http.get_cas_write_token(&cfg.dest_bucket).await {
            Ok(_) => {
                info!("dry run: destination bucket write-token OK");
                println!("DRYRUN_BUCKET ok");
            }
            Err(e) => {
                warn!("dry run: destination bucket write-token FAILED (bucket not created yet, or no write access?): {e:#}");
                println!("DRYRUN_BUCKET error");
            }
        }
        return Ok(());
    }

    // Flush the final partial chunk.
    if !chunk.is_empty() {
        let n = process_chunk(
            chunk,
            &store,
            &bucket_http,
            &cfg.dest_bucket,
            &prefix,
            parallel,
            part_concurrency,
            part_size,
            xor_byte,
            &files_done,
            &bytes_done,
        )
        .await?;
        info!(committed = n, kept, "committed final chunk");
    }
    if let Some(h) = stats_handle {
        h.abort();
    }

    let files = files_done.load(Ordering::Relaxed);
    let bytes = bytes_done.load(Ordering::Relaxed);
    let elapsed = started.elapsed().as_secs_f64();
    let throughput_mibps = (bytes as f64 / (1024.0 * 1024.0)) / elapsed.max(0.001);
    info!(
        files,
        kept,
        elapsed_s = elapsed,
        bytes,
        throughput_mibps,
        "done"
    );
    println!(
        "DONE {}",
        serde_json::json!({
            "files": files,
            "bytes": bytes,
            "elapsed_s": elapsed,
            "throughput_mibps": throughput_mibps,
        })
    );
    Ok(())
}

/// Upload one chunk of files through a fresh CAS session, then commit them as a
/// single bucket batch. A new session per chunk keeps `finalize()` (which flushes
/// the shard the commit needs) correct and bounds memory. Returns files committed.
#[allow(clippy::too_many_arguments)]
async fn process_chunk(
    chunk: Vec<S3Object>,
    store: &Arc<dyn ObjectStore>,
    bucket_http: &Arc<BucketClient>,
    dest: &BucketRef,
    key_prefix: &str,
    parallel: usize,
    part_concurrency: usize,
    part_size: u64,
    xor_byte: u8,
    files_done: &Arc<AtomicU64>,
    bytes_done: &Arc<AtomicU64>,
) -> Result<usize> {
    let uploader = Arc::new(
        CasUploader::new(bucket_http.clone(), dest.clone())
            .await
            .context("init CAS uploader")?,
    );
    let ops_collector: Arc<Mutex<Vec<BatchOp>>> =
        Arc::new(Mutex::new(Vec::with_capacity(chunk.len())));

    let results: Vec<Result<UploadOutcome>> = futures::stream::iter(chunk.into_iter().map(|obj| {
        let store = store.clone();
        let uploader = uploader.clone();
        let ops_collector = ops_collector.clone();
        let key_prefix = key_prefix.to_string();
        let files_done = files_done.clone();
        let bytes_done = bytes_done.clone();
        async move {
            upload_one(
                store,
                uploader,
                ops_collector,
                key_prefix,
                obj,
                part_concurrency,
                part_size,
                xor_byte,
                files_done,
                bytes_done,
            )
            .await
        }
    }))
    .buffer_unordered(parallel)
    .collect()
    .await;

    let mut hard_errors = 0usize;
    let mut skipped = 0usize;
    for r in results {
        match r {
            Ok(UploadOutcome::Uploaded) => {}
            Ok(UploadOutcome::Skipped { key, reason }) => {
                skipped += 1;
                warn!(key = %key, reason = %reason, "skipped");
            }
            Err(e) => {
                hard_errors += 1;
                warn!("file failed: {:#}", e);
            }
        }
    }
    if hard_errors > 0 {
        bail!(
            "{hard_errors} files failed in chunk (non-recoverable); aborting ({skipped} skipped)"
        );
    }

    let ops = ops_collector.lock().await.split_off(0);
    uploader.finalize().await.context("CAS session finalize")?;
    let n = ops.len();
    bucket_http
        .batch(dest, &ops)
        .await
        .context("bucket batch commit")?;
    Ok(n)
}

/// Result of trying to upload one file.
pub enum UploadOutcome {
    Uploaded,
    /// Source object was 404 / vanished / phantom — skip without failing the batch.
    Skipped {
        key: String,
        reason: String,
    },
}

#[allow(clippy::too_many_arguments)]
async fn upload_one(
    store: Arc<dyn ObjectStore>,
    uploader: Arc<CasUploader>,
    ops_collector: Arc<Mutex<Vec<BatchOp>>>,
    key_prefix: String,
    obj: S3Object,
    part_concurrency: usize,
    part_size: u64,
    xor_byte: u8,
    files_done: Arc<AtomicU64>,
    bytes_done: Arc<AtomicU64>,
) -> Result<UploadOutcome> {
    // Use the Path captured at list time — never reconstruct from a string,
    // because Path::from(s) treats `s` as raw and re-encodes special chars.
    let path = &obj.path;

    // Choose between single-GET stream and multipart parallel ranged reads.
    // For files smaller than part_size or when part_concurrency=1, single GET
    // is simpler and avoids extra overhead.
    let xet_info = if part_concurrency <= 1 || obj.size <= part_size {
        let result = match store.get(path).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => {
                return Ok(UploadOutcome::Skipped {
                    key: obj.key.clone(),
                    reason: "S3 GET returned 404 (likely a phantom listing entry)".into(),
                });
            }
            Err(e) => {
                return Err(anyhow::Error::from(e)).with_context(|| format!("S3 get {}", obj.key))
            }
        };
        let stream = result.into_stream().map(move |r| {
            r.map_err(map_object_store_error)
                .map(|c| xor_chunk(c, xor_byte))
        });
        uploader.upload_stream(stream).await
    } else {
        // Multipart: split into ranges, issue ranged GETs in parallel via
        // futures::Stream::buffered(N). buffered() spawns N futures concurrently
        // but yields results IN INPUT ORDER — so ranges arrive at the cleaner
        // in offset order even if completed out of order. This is exactly the
        // s5cmd cat / orderedwriter pattern.
        let ranges = split_ranges(obj.size, part_size);
        let path_arc = Arc::new(path.clone());
        let store_for_parts = store.clone();
        let stream = futures::stream::iter(ranges.into_iter().map(move |(start, end)| {
            let store = store_for_parts.clone();
            let path = path_arc.clone();
            async move {
                store
                    .get_range(&path, start..end)
                    .await
                    .map_err(map_object_store_error)
            }
        }))
        .buffered(part_concurrency)
        .map(move |r| r.map(|c| xor_chunk(c, xor_byte)));
        uploader.upload_stream(stream).await
    };
    let xet_info = match xet_info {
        Ok(info) => info,
        Err(e) if e.downcast_ref::<SourceNotFound>().is_some() => {
            return Ok(UploadOutcome::Skipped {
                key: obj.key.clone(),
                reason: "S3 GET returned 404 (likely a phantom listing entry)".into(),
            });
        }
        Err(e) => return Err(e).with_context(|| format!("upload {}", obj.key)),
    };

    let rel_path = relative_key_path(&obj.key, &key_prefix);

    let mtime_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    debug!(
        path = %rel_path,
        size = obj.size,
        xet_hash = %xet_info.hash,
        "uploaded"
    );

    ops_collector.lock().await.push(BatchOp::AddFile {
        path: rel_path,
        xet_hash: xet_info.hash,
        mtime: mtime_ms,
        content_type: None,
    });

    files_done.fetch_add(1, Ordering::Relaxed);
    bytes_done.fetch_add(obj.size, Ordering::Relaxed);

    Ok(UploadOutcome::Uploaded)
}

#[derive(Debug, Clone)]
pub struct S3Object {
    /// Decoded key: original raw characters as they appear in S3, used for
    /// FNV sharding, dest-bucket file path, and human-readable logs.
    pub key: String,
    /// object_store Path holding the *encoded* representation. Used directly
    /// in store.get / get_range so we never re-encode (which would turn
    /// `Batch_%23128` into `Batch_%2523128` → S3 NoSuchKey).
    pub path: Path,
    pub size: u64,
}

/// Stable, dependency-free 64-bit hash for shard assignment.
/// FNV-1a is fine for this — we just need uniform distribution and
/// reproducibility across cloner versions.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Parse `s3://bucket/prefix` into `(bucket, prefix)`.
fn parse_s3_url(url: &str) -> Result<(String, String)> {
    let parsed = Url::parse(url).with_context(|| format!("parse {url}"))?;
    if parsed.scheme() != "s3" {
        bail!("source must be s3:// URL, got {url}");
    }
    let bucket = parsed
        .host_str()
        .with_context(|| format!("S3 URL missing bucket: {url}"))?
        .to_string();
    let prefix = parsed.path().trim_start_matches('/').to_string();
    Ok((bucket, prefix))
}

/// Resolve the region to use: explicit value if given (and non-empty), else
/// auto-detect from the bucket, else fall back to us-east-1.
async fn resolve_region(bucket: &str, explicit: Option<&str>) -> String {
    if let Some(r) = explicit {
        if !r.trim().is_empty() {
            return r.trim().to_string();
        }
    }
    match detect_bucket_region(bucket).await {
        Some(r) => {
            info!(bucket = %bucket, region = %r, "auto-detected S3 bucket region");
            r
        }
        None => {
            warn!(
                bucket = %bucket,
                "could not auto-detect region (GetBucketLocation failed / denied); \
                 defaulting to us-east-1 — pass --aws-region if this is wrong"
            );
            "us-east-1".to_string()
        }
    }
}

/// Detect a bucket's region via S3 GetBucketLocation (a global operation: a
/// us-east-1 client resolves buckets in any region). Maps the legacy empty/
/// `EU` constraints to `us-east-1`/`eu-west-1`.
async fn detect_bucket_region(bucket: &str) -> Option<String> {
    let sdk = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .load()
        .await;
    let client = aws_sdk_s3::Client::new(&sdk);
    let out = client
        .get_bucket_location()
        .bucket(bucket)
        .send()
        .await
        .ok()?;
    Some(match out.location_constraint() {
        None => "us-east-1".to_string(),
        Some(lc) => match lc.as_str() {
            "" => "us-east-1".to_string(),
            "EU" => "eu-west-1".to_string(),
            s => s.to_string(),
        },
    })
}

fn build_s3_store(url: &str, region: &str) -> Result<(Arc<dyn ObjectStore>, String, String)> {
    let (bucket, prefix) = parse_s3_url(url)?;

    // Default reqwest timeout is 30s — far too short for multi-GB GETs over a
    // single connection. Set generous timeouts so a slow stream doesn't get
    // killed mid-file.
    let client_opts = ClientOptions::new()
        .with_timeout(Duration::from_secs(3600))
        .with_connect_timeout(Duration::from_secs(30));

    let store = AmazonS3Builder::from_env()
        .with_bucket_name(&bucket)
        .with_region(region)
        .with_client_options(client_opts)
        .build()
        .with_context(|| format!("build S3 client for bucket {bucket}"))?;

    Ok((Arc::new(store), bucket, prefix))
}

/// XOR every byte of `chunk` with `byte`. Passthrough when `byte == 0`.
/// Used for upload-bandwidth benchmarks: defeats CAS dedup while preserving
/// chunk length and offsets, so CDC behaves identically to the un-XOR'd path.
fn xor_chunk(chunk: bytes::Bytes, byte: u8) -> bytes::Bytes {
    if byte == 0 {
        return chunk;
    }
    let mut buf = bytes::BytesMut::with_capacity(chunk.len());
    buf.extend(chunk.iter().map(|b| b ^ byte));
    buf.freeze()
}

/// Split [0, total_size) into half-open `(start, end)` ranges of `part_size` bytes each.
fn split_ranges(total_size: u64, part_size: u64) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    let mut start = 0u64;
    while start < total_size {
        let end = (start + part_size).min(total_size);
        out.push((start, end));
        start = end;
    }
    out
}

#[derive(Debug)]
struct SourceNotFound;

impl fmt::Display for SourceNotFound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("source object not found")
    }
}

impl std::error::Error for SourceNotFound {}

fn map_object_store_error(e: object_store::Error) -> anyhow::Error {
    match e {
        object_store::Error::NotFound { .. } => anyhow::Error::new(SourceNotFound),
        e => anyhow::Error::from(e),
    }
}

fn key_belongs_to_prefix(key: &str, prefix: &str) -> bool {
    if prefix.is_empty() {
        return true;
    }
    if prefix.ends_with('/') {
        key.starts_with(prefix)
    } else {
        key == prefix
            || key
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.starts_with('/'))
    }
}

fn relative_key_path(key: &str, prefix: &str) -> String {
    if prefix.is_empty() {
        return key.to_string();
    }
    if prefix.ends_with('/') {
        return key.strip_prefix(prefix).unwrap_or(key).to_string();
    }
    match key.strip_prefix(prefix) {
        Some("") => key.to_string(),
        Some(rest) if rest.starts_with('/') => rest.trim_start_matches('/').to_string(),
        _ => key.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_with_trailing_slash_does_not_match_siblings() {
        assert!(key_belongs_to_prefix("foo/a", "foo/"));
        assert!(key_belongs_to_prefix("foo/bar/a", "foo/"));
        assert!(!key_belongs_to_prefix("foobar/a", "foo/"));
        assert!(!key_belongs_to_prefix("foo", "foo/"));
    }

    #[test]
    fn prefix_without_trailing_slash_matches_exact_or_child_only() {
        assert!(key_belongs_to_prefix("foo", "foo"));
        assert!(key_belongs_to_prefix("foo/a", "foo"));
        assert!(!key_belongs_to_prefix("foobar/a", "foo"));
    }

    #[test]
    fn relative_paths_respect_prefix_boundaries() {
        assert_eq!(relative_key_path("foo/a", "foo/"), "a");
        assert_eq!(relative_key_path("foo/bar/a", "foo/"), "bar/a");
        assert_eq!(relative_key_path("foo/a", "foo"), "a");
        assert_eq!(relative_key_path("foo", "foo"), "foo");
        assert_eq!(relative_key_path("foobar/a", "foo"), "foobar/a");
        assert_eq!(relative_key_path("foo/a", ""), "foo/a");
    }
}
