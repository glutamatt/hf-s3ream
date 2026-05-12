//! Top-level sync orchestrator.
//!
//! Phase 2: object_store streams S3 → CasUploader (xet-data CAS pipeline) →
//! collects XetFileInfo per file → single batch commit on /api/buckets/{id}/batch.

use anyhow::{bail, Context, Result};
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ClientOptions, ObjectStore, ObjectStoreExt};
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
    pub aws_region: String,
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
}

pub async fn run(cfg: Config) -> Result<()> {
    let (store, bucket_name, prefix) = build_s3_store(&cfg.source_s3_url, &cfg.aws_region)?;
    info!(bucket = %bucket_name, prefix = %prefix, "scanning S3 source");

    let mut objects = list_s3(&bucket_name, &prefix, &cfg.aws_region).await?;

    // Apply --exclude globs BEFORE sharding so each shard's FNV partition is
    // computed over the same post-exclude set (deterministic across shard
    // count changes and re-runs).
    if !cfg.exclude_globs.is_empty() {
        let mut builder = globset::GlobSetBuilder::new();
        for pat in &cfg.exclude_globs {
            let g = globset::Glob::new(pat)
                .with_context(|| format!("invalid --exclude glob: {pat}"))?;
            builder.add(g);
        }
        let set = builder.build().context("build globset")?;
        let before = objects.len();
        objects.retain(|o| !set.is_match(&o.key));
        info!(
            patterns = ?cfg.exclude_globs,
            excluded = before - objects.len(),
            kept = objects.len(),
            "applied --exclude filter",
        );
    }

    if cfg.shard_count > 1 {
        let total_listed = objects.len();
        objects.retain(|o| fnv1a64(o.key.as_bytes()) % cfg.shard_count == cfg.shard_id);
        info!(
            shard_id = cfg.shard_id,
            shard_count = cfg.shard_count,
            kept = objects.len(),
            of_total = total_listed,
            "applied --shard-id/--shard-count filter (FNV-1a64(key) % shard_count == shard_id)"
        );
    }
    if cfg.limit_bytes > 0 {
        let mut acc = 0u64;
        let mut keep = 0usize;
        for (i, o) in objects.iter().enumerate() {
            if acc >= cfg.limit_bytes {
                break;
            }
            acc = acc.saturating_add(o.size);
            keep = i + 1;
        }
        let dropped = objects.len() - keep;
        objects.truncate(keep);
        info!(
            kept = objects.len(),
            dropped,
            limit_gib = cfg.limit_bytes as f64 / 1024.0_f64.powi(3),
            "applied --limit-gib (truncated work-list to the prefix that fits)"
        );
    }
    let total_bytes: u64 = objects.iter().map(|o| o.size).sum();
    info!(
        count = objects.len(),
        total_gib = total_bytes as f64 / 1024.0_f64.powi(3),
        "source listed"
    );

    let bucket_http = Arc::new(BucketClient::new(
        cfg.hub_endpoint.clone(),
        cfg.hf_token.clone(),
    ));

    if cfg.dry_run {
        info!("dry run: skipping CAS upload + bucket batch");
        for o in objects.iter().take(10) {
            info!(key = %o.key, size = o.size, "  would transfer");
        }
        if objects.len() > 10 {
            info!("  ... and {} more", objects.len() - 10);
        }
        return Ok(());
    }

    let uploader = Arc::new(
        CasUploader::new(bucket_http.clone(), cfg.dest_bucket.clone())
            .await
            .context("init CAS uploader")?,
    );

    let started = Instant::now();
    let key_prefix = prefix.trim_end_matches('/').to_string();
    let parallel = cfg.parallel_files.max(1);
    let ops_collector: Arc<Mutex<Vec<BatchOp>>> =
        Arc::new(Mutex::new(Vec::with_capacity(objects.len())));

    // Live progress counters incremented by upload_one as files finish.
    let files_done = Arc::new(AtomicU64::new(0));
    let bytes_done = Arc::new(AtomicU64::new(0));
    let total_files = objects.len() as u64;

    // Print a one-line progress sample every 5s. Cancelled after the upload
    // phase via the abort handle.
    let stats_handle = {
        let files_done = files_done.clone();
        let bytes_done = bytes_done.clone();
        tokio::spawn(async move {
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
                let avg_mibps = if dt > 0.0 {
                    (b.saturating_sub(last_bytes)) as f64 / dt / (1024.0 * 1024.0)
                } else {
                    0.0
                };
                let total_mibps =
                    b as f64 / started.elapsed().as_secs_f64().max(0.001) / (1024.0 * 1024.0);
                info!(
                    files = f,
                    of = total_files,
                    gib_done = b as f64 / 1024.0_f64.powi(3),
                    last_5s_mibps = format!("{avg_mibps:.0}"),
                    avg_mibps = format!("{total_mibps:.0}"),
                    elapsed_s = format!("{:.0}", started.elapsed().as_secs_f64()),
                    "progress",
                );
                last_t = now;
                last_bytes = b;
            }
        })
    };

    info!(parallel, "uploading...");

    let part_concurrency = cfg.s3_part_concurrency.max(1);
    let part_size = cfg.s3_part_size.max(1);
    let xor_byte = cfg.xor_byte;
    if xor_byte != 0 {
        warn!(
            xor_byte = format!("0x{xor_byte:02x}"),
            "XOR transform active: data uploaded will NOT match source. Benchmark mode."
        );
    }

    let stream = futures::stream::iter(objects.into_iter().map(|obj| {
        let store = store.clone();
        let uploader = uploader.clone();
        let ops_collector = ops_collector.clone();
        let key_prefix = key_prefix.clone();
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
    .buffer_unordered(parallel);

    let results: Vec<Result<UploadOutcome>> = stream.collect().await;
    stats_handle.abort();
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
                // {:#} prints the full anyhow chain (top-level + all .context())
                warn!("file failed: {:#}", e);
            }
        }
    }

    let ops = ops_collector.lock().await.split_off(0);
    if hard_errors > 0 {
        bail!("{hard_errors} files failed (non-recoverable); not committing batch ({skipped} skipped)");
    }
    if skipped > 0 {
        info!(
            skipped,
            "some files were skipped (e.g. 404 / phantom listings); committing the rest"
        );
    }

    info!(
        files = ops.len(),
        "finalizing CAS session (flushing pending xorbs/shards)"
    );
    uploader.finalize().await.context("CAS session finalize")?;

    info!(ops = ops.len(), "committing bucket batch");
    bucket_http
        .batch(&cfg.dest_bucket, &ops)
        .await
        .context("bucket batch commit")?;

    info!(
        files = ops.len(),
        elapsed_s = started.elapsed().as_secs_f64(),
        bytes = total_bytes,
        throughput_mibps =
            (total_bytes as f64 / (1024.0 * 1024.0)) / started.elapsed().as_secs_f64().max(0.001),
        "done"
    );
    Ok(())
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
            r.map_err(anyhow::Error::from)
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
                    .map_err(anyhow::Error::from)
            }
        }))
        .buffered(part_concurrency)
        .map(move |r| r.map(|c| xor_chunk(c, xor_byte)));
        uploader.upload_stream(stream).await
    }
    .with_context(|| format!("upload {}", obj.key))?;

    let rel_path = obj
        .key
        .strip_prefix(&key_prefix)
        .map(|s| s.trim_start_matches('/'))
        .unwrap_or(&obj.key)
        .to_string();

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

fn build_s3_store(url: &str, region: &str) -> Result<(Arc<dyn ObjectStore>, String, String)> {
    let parsed = Url::parse(url).with_context(|| format!("parse {url}"))?;
    if parsed.scheme() != "s3" {
        bail!("source must be s3:// URL, got {url}");
    }
    let bucket = parsed
        .host_str()
        .with_context(|| format!("S3 URL missing bucket: {url}"))?
        .to_string();
    let prefix = parsed.path().trim_start_matches('/').to_string();

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

/// List S3 objects via aws-sdk-s3 (raw String keys, no Path validation).
///
/// We tried object_store::list earlier and hit a hard wall: when the
/// underlying S3 ListObjectsV2 response contained a key with an empty
/// path segment (e.g. `"foo//1000/file"`), object_store's stream errored
/// on that one item AND stopped yielding ALL subsequent items, including
/// from later pages. Result: a small fraction of the bucket got listed
/// before the stream went silent.
///
/// aws-sdk-s3 returns raw `String` keys with no parsing. We then attempt
/// to construct an `object_store::Path` for each key (so the existing
/// upload-via-`store.get(&path)` path keeps working). Keys whose Path
/// construction fails are warn-and-skipped, leaving the rest of the
/// listing untouched.
async fn list_s3(bucket: &str, prefix: &str, region: &str) -> Result<Vec<S3Object>> {
    let region_provider = aws_sdk_s3::config::Region::new(region.to_string());
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;
    let client = aws_sdk_s3::Client::new(&sdk_config);

    let mut paginator = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix.trim_end_matches('/'))
        .into_paginator()
        .send();

    let mut out = Vec::new();
    let mut skipped_invalid = 0u64;
    while let Some(page) = paginator.next().await {
        let page = page.context("aws-sdk-s3 list_objects_v2")?;
        for obj in page.contents() {
            let raw_key = match obj.key() {
                Some(k) => k.to_string(),
                None => continue,
            };
            let size = obj.size().unwrap_or(0).max(0) as u64;

            // Try to build an object_store::Path from the raw key. Path::from
            // panics on some inputs in older versions, so go through `parse`
            // which is fallible. `parse` interprets its input as URL-encoded;
            // raw S3 keys aren't URL-encoded, so percent-encode the special
            // characters first via Path::from_iter on split parts.
            let parts: Vec<&str> = raw_key.split('/').collect();
            // Skip keys with empty segments (`//`) — they're valid in S3 but
            // not in object_store, and the user can't usefully access them
            // via this clone anyway.
            if parts.iter().any(|p| p.is_empty()) && parts != [""] {
                skipped_invalid += 1;
                if skipped_invalid <= 10 {
                    warn!(key = %raw_key, "skipping S3 key with empty path segment (invalid for object_store)");
                }
                continue;
            }
            let path = Path::from_iter(parts.iter().copied());

            out.push(S3Object {
                key: raw_key,
                path,
                size,
            });
        }
    }
    if skipped_invalid > 0 {
        warn!(
            count = skipped_invalid,
            "skipped S3 listing entries with paths object_store cannot represent \
             (e.g. keys containing `//` empty segments). These objects will not be cloned."
        );
    }
    out.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(out)
}
