//! Top-level sync orchestrator.
//!
//! Phase 2: object_store streams S3 → CasUploader (xet-data CAS pipeline) →
//! collects XetFileInfo per file → single batch commit on /api/buckets/{id}/batch.

use anyhow::{bail, Context, Result};
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ClientOptions, ObjectStore, ObjectStoreExt};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, info, warn};
use url::Url;

use crate::bucket_client::{BatchOp, BucketClient};
use crate::cas_uploader::CasUploader;
use crate::jobs_client::{JobInfo, JobSpec, JobsClient};
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
    /// Worker range lower bound (exclusive): S3 `start-after`. None = from the
    /// start of the prefix. Set by the lister when spawning a per-range copier.
    pub start_after: Option<String>,
    /// Worker range upper bound (inclusive): stop listing once a key sorts past
    /// it. None = to the end of the prefix. Adjacent ranges share a boundary
    /// (range i's stop_at == range i+1's start_after) → gap-free, overlap-free.
    pub stop_at: Option<String>,
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
    let exclude = build_globset(&cfg.exclude_globs)?;

    // aws-sdk-s3 client for the listing (raw String keys, no Path validation).
    // When many range copiers list the bucket at once (disjoint slices, but the
    // same bucket), S3 connections can saturate and the SDK's ~3.1s default
    // connect timeout then fails list pages. Give connections more headroom +
    // enable SDK retries; we ALSO retry each page ourselves (list_page_with_retry)
    // so a transient list failure never kills the copier.
    let client = build_list_client(&region).await;

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

    // Real copy: spawn the uploader as a SEPARATE task fed by a bounded channel, so
    // uploading OVERLAPS listing. The listing loop below just filters + sends; the
    // consumer uploads/commits concurrently and back-pressures listing (bounded
    // channel) if uploads fall behind, keeping memory flat. Uploads start on the
    // first object listed — no waiting to buffer a whole commit_chunk first.
    let (tx, consumer) = if cfg.dry_run {
        (None, None)
    } else {
        let cap = cfg.commit_chunk.clamp(256, 100_000);
        let (tx, rx) = mpsc::channel::<S3Object>(cap);
        let handle = tokio::spawn(upload_consumer(
            rx,
            store.clone(),
            bucket_http.clone(),
            cfg.dest_bucket.clone(),
            prefix.clone(),
            parallel,
            part_concurrency,
            part_size,
            xor_byte,
            cfg.commit_chunk,
            files_done.clone(),
            bytes_done.clone(),
        ));
        (Some(tx), Some(handle))
    };

    // Streaming state.
    let part16 = 16u64 * 1024 * 1024; // matches default --s3-part-size-mib
    let mut listed = 0u64;
    let mut kept = 0u64;
    let mut skipped_invalid = 0u64;
    let mut acc_bytes = 0u64; // for --limit-gib; also the "bytes so far" in LISTING
    let mut kept_le16 = 0u64; // kept files ≤16 MiB so far (small-file share for tuning)
    // dry-run stat accumulators (streaming — no per-object retention).
    let (mut d_count, mut d_total, mut d_min, mut d_max) = (0u64, 0u64, u64::MAX, 0u64);
    let mut hist = [0u64; 64]; // size buckets by bit-length → approximate median
    let mut limit_hit = false;

    // Manual pagination with per-page retry (instead of the auto-paginator, which
    // ends the stream on the first error). A transient list failure — connect
    // timeout / throttling when many copiers list the same bucket at once — must
    // NOT kill the copier, so we retry each page with backoff.
    let mut continuation: Option<String> = None;
    'outer: loop {
        let page = list_page_with_retry(
            &client,
            &bucket_name,
            &prefix,
            continuation.as_deref(),
            // `start-after` is honored only on the first request; once we have a
            // continuation token S3 pages from there. It's our range lower bound.
            cfg.start_after.as_deref(),
        )
        .await?;
        for obj in page.contents() {
            let raw_key = match obj.key() {
                Some(k) => k.to_string(),
                None => continue,
            };
            // Range upper bound (inclusive). S3 lists ascending, so the first key
            // past stop_at means this range is fully consumed — stop everything.
            if let Some(stop) = cfg.stop_at.as_deref() {
                if raw_key.as_str() > stop {
                    break 'outer;
                }
            }
            listed += 1;
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
            if cfg.limit_bytes > 0 && acc_bytes >= cfg.limit_bytes {
                limit_hit = true;
                break 'outer;
            }
            acc_bytes = acc_bytes.saturating_add(size);
            kept += 1;
            kept_counter.store(kept, Ordering::Relaxed);
            if size <= part16 {
                kept_le16 += 1;
            }

            if cfg.dry_run {
                d_count += 1;
                d_total = d_total.saturating_add(size);
                d_min = d_min.min(size);
                d_max = d_max.max(size);
                let b = (64 - size.max(1).leading_zeros()) as usize;
                hist[b.min(63)] += 1;
            } else {
                let path = Path::from_iter(parts.iter().copied());
                let obj = S3Object {
                    key: raw_key,
                    path,
                    size,
                };
                // Hand off to the uploader. send().await blocks (back-pressure) only
                // if the channel is full — i.e. uploads are behind — bounding memory.
                // An Err means the consumer died (a hard upload/commit error); stop
                // listing and let the error surface when we await the consumer below.
                if tx.as_ref().expect("tx set for real copy").send(obj).await.is_err() {
                    break 'outer;
                }
            }

            if listed.is_multiple_of(100_000) {
                info!(listed, kept, skipped_invalid, "listing…");
                println!(
                    "LISTING {}",
                    serde_json::json!({
                        "listed": listed, "kept": kept,
                        "bytes": acc_bytes, "le16": kept_le16,
                    })
                );
            }
        }
        // Advance to the next page, or stop when S3 says there are no more.
        match page.next_continuation_token() {
            Some(t) => continuation = Some(t.to_string()),
            None => break 'outer,
        }
    }
    println!(
        "LISTING {}",
        serde_json::json!({
            "listed": listed, "kept": kept,
            "bytes": acc_bytes, "le16": kept_le16, "done": true,
        })
    );
    info!(listed, kept, skipped_invalid, limit_hit, "listing complete");

    if cfg.dry_run {
        let pct_le_16mib = if d_count == 0 {
            0.0
        } else {
            (kept_le16 as f64 * 1000.0 / d_count as f64).round() / 10.0
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

    // Listing done: close the channel so the consumer drains its last partial
    // chunk and finishes, then surface any upload/commit error it hit.
    drop(tx);
    if let Some(h) = consumer {
        h.await.context("upload consumer task panicked")??;
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

/// Configuration for the planner (`--plan`): list the source ONCE, cut the
/// sorted keyspace into byte/key-balanced contiguous ranges, spawn a copier job
/// per range (pipelined — as each range closes), then monitor and re-spawn any
/// that fail. All orchestration lives here; the web Space only observes.
pub struct PlanConfig {
    pub source_s3_url: String,
    /// Raw destination string ("org/name" or "hf://buckets/org/name"), forwarded
    /// verbatim to each copier's argv.
    pub dest: String,
    pub hub_endpoint: String,
    /// Token used to spawn copiers AND injected as each copier's HF_TOKEN secret.
    /// In the Space flow this is the user's OAuth token (carries `jobs` scope).
    pub hf_token: String,
    pub aws_region: Option<String>,
    pub exclude_globs: Vec<String>,
    /// Stop planning after this many source bytes (0 = unlimited). Testing knob.
    pub limit_bytes: u64,
    /// Cut a new range once it reaches this many bytes (0 = no byte limit)...
    pub range_bytes: u64,
    /// ...or this many keys (0 = no key limit), whichever comes first. Both 0 =
    /// one range = one copier (degenerate, == unsharded).
    pub range_keys: u64,
    pub copier_image: String,
    pub copier_flavor: String,
    /// Namespace to POST jobs under. None/empty → resolve via whoami.
    pub jobs_namespace: Option<String>,
    /// Cap on concurrently-active (spawned, non-terminal) copiers.
    pub max_inflight: usize,
    /// Minimum delay between consecutive copier launches (spreads image pulls).
    pub launch_stagger: Duration,
    /// Value of the `hf-s3ream-run` label stamped on every copier (tab re-attach).
    pub run_label: String,
    pub commit_chunk: usize,
    pub s3_part_concurrency: usize,
    pub s3_part_size_mib: usize,
    /// Secrets (AWS_*, HF_TOKEN) read from the planner's own env, re-injected
    /// into every copier via the encrypted `secrets` channel.
    pub copier_secrets: BTreeMap<String, String>,
    /// Non-secret env (e.g. RUST_LOG) forwarded to every copier.
    pub copier_env: BTreeMap<String, String>,
}

/// One planned range and the copier job that owns it.
struct Copier {
    idx: u64,
    /// Range lower bound (exclusive); None for the first range.
    start_after: Option<String>,
    /// Range upper bound (inclusive) = the last key that fell into this range.
    stop_at: String,
    files: u64,
    bytes: u64,
    /// --parallel-files chosen for this range from its own small-file share.
    pf: usize,
    job_id: Option<String>,
    attempts: u32,
    /// Last observed job stage ("" before the first spawn/poll).
    stage: String,
}

const MAX_COPIER_ATTEMPTS: u32 = 3;

/// Streaming range accumulator + copier fleet. Fields prefixed `cur_` describe
/// the range currently being filled by the listing loop in [`plan`].
struct Planner {
    cfg: PlanConfig,
    jobs: JobsClient,
    namespace: String,
    region: String,
    copiers: Vec<Copier>,
    range_idx: u64,
    cur_start_after: Option<String>,
    cur_files: u64,
    cur_bytes: u64,
    cur_le16: u64,
}

impl Planner {
    /// Fold one listed object into the current range; cut+spawn if it's now full.
    async fn add(&mut self, key: &str, size: u64) -> Result<()> {
        self.cur_files += 1;
        self.cur_bytes = self.cur_bytes.saturating_add(size);
        if size <= 16 * 1024 * 1024 {
            self.cur_le16 += 1;
        }
        let by_bytes = self.cfg.range_bytes > 0 && self.cur_bytes >= self.cfg.range_bytes;
        let by_keys = self.cfg.range_keys > 0 && self.cur_files >= self.cfg.range_keys;
        if by_bytes || by_keys {
            self.cut(key.to_string()).await?;
        }
        Ok(())
    }

    /// Close the current range at `stop_at`, spawn its copier, advance to the next.
    async fn cut(&mut self, stop_at: String) -> Result<()> {
        // Small-file majority → higher --parallel-files (per-file-overhead-bound);
        // big-file majority → lower (multipart RAM). Matches run()'s guidance.
        let pf = if self.cur_le16 * 2 >= self.cur_files.max(1) {
            128
        } else {
            32
        };
        let mut c = Copier {
            idx: self.range_idx,
            start_after: self.cur_start_after.clone(),
            stop_at: stop_at.clone(),
            files: self.cur_files,
            bytes: self.cur_bytes,
            pf,
            job_id: None,
            attempts: 0,
            stage: String::new(),
        };
        println!(
            "RANGE {}",
            serde_json::json!({
                "idx": c.idx, "start_after": c.start_after, "stop_at": c.stop_at,
                "files": c.files, "bytes": c.bytes, "pf": c.pf,
            })
        );
        // Back-pressure: never exceed max_inflight active copiers.
        self.wait_for_slot().await;
        c.attempts += 1;
        let (job_id, stage) = self.spawn(&c).await?;
        println!(
            "COPIER {}",
            serde_json::json!({
                "idx": c.idx, "job_id": job_id, "start_after": c.start_after,
                "stop_at": c.stop_at, "attempt": c.attempts,
            })
        );
        c.job_id = Some(job_id);
        c.stage = stage;
        self.copiers.push(c);
        // Spread image pulls: pause before the next launch.
        if !self.cfg.launch_stagger.is_zero() {
            tokio::time::sleep(self.cfg.launch_stagger).await;
        }
        self.range_idx += 1;
        self.cur_start_after = Some(stop_at);
        self.cur_files = 0;
        self.cur_bytes = 0;
        self.cur_le16 = 0;
        Ok(())
    }

    /// POST one copier job. Returns (job_id, initial stage). Does not mutate `c`.
    async fn spawn(&self, c: &Copier) -> Result<(String, String)> {
        let spec = build_copier_spec(&self.cfg, &self.region, c);
        let info = self
            .jobs
            .run_job(&self.namespace, &spec)
            .await
            .with_context(|| format!("spawn copier for range {}", c.idx))?;
        let stage = info
            .status
            .map(|s| s.stage)
            .unwrap_or_else(|| "RUNNING".to_string());
        Ok((info.id, stage))
    }

    /// Number of spawned copiers not yet in a terminal stage.
    fn active(&self) -> usize {
        self.copiers
            .iter()
            .filter(|c| c.job_id.is_some() && !JobInfo::is_terminal(&c.stage))
            .count()
    }

    /// Poll the current stage of every spawned, non-terminal copier.
    async fn refresh(&mut self) {
        for c in self.copiers.iter_mut() {
            let Some(id) = c.job_id.clone() else { continue };
            if JobInfo::is_terminal(&c.stage) {
                continue;
            }
            match self.jobs.job_status(&self.namespace, &id).await {
                Ok(Some(st)) => {
                    if st.stage == "ERROR" && c.stage != "ERROR" {
                        warn!(job = %id, range = c.idx, message = ?st.message, "copier entered ERROR");
                    }
                    c.stage = st.stage;
                }
                Ok(None) => {}
                Err(e) => warn!(job = %id, "status poll failed: {e:#}"),
            }
        }
    }

    /// Block until fewer than max_inflight copiers are active. Only frees slots
    /// as jobs go terminal (COMPLETED or ERROR); ERROR'd ones are re-spawned
    /// later in [`Planner::monitor`], not here (that would consume the slot we're
    /// waiting for, stalling the listing).
    async fn wait_for_slot(&mut self) {
        let cap = self.cfg.max_inflight.max(1);
        while self.active() >= cap {
            self.refresh().await;
            if self.active() < cap {
                break;
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    /// Re-spawn every ERROR'd copier that still has attempts left (idempotent —
    /// xet CAS dedups anything an earlier attempt already committed).
    async fn respawn_failed(&mut self) -> Result<()> {
        let to_respawn: Vec<usize> = self
            .copiers
            .iter()
            .enumerate()
            .filter(|(_, c)| c.stage == "ERROR" && c.attempts < MAX_COPIER_ATTEMPTS)
            .map(|(i, _)| i)
            .collect();
        for i in to_respawn {
            self.copiers[i].attempts += 1;
            let attempt = self.copiers[i].attempts;
            let (job_id, stage) = self.spawn(&self.copiers[i]).await?;
            println!(
                "COPIER {}",
                serde_json::json!({
                    "idx": self.copiers[i].idx, "job_id": job_id,
                    "start_after": self.copiers[i].start_after,
                    "stop_at": self.copiers[i].stop_at, "attempt": attempt,
                })
            );
            self.copiers[i].job_id = Some(job_id);
            self.copiers[i].stage = stage;
        }
        Ok(())
    }

    /// After the plan is complete: poll + re-spawn failures until every copier is
    /// terminal (COMPLETED / CANCELED / DELETED, or ERROR with attempts exhausted).
    async fn monitor(&mut self) -> Result<()> {
        loop {
            self.refresh().await;
            self.respawn_failed().await?;
            // "pending" = still running, or ERROR'd but re-spawnable next cycle.
            let pending = self
                .copiers
                .iter()
                .filter(|c| {
                    !JobInfo::is_terminal(&c.stage)
                        || (c.stage == "ERROR" && c.attempts < MAX_COPIER_ATTEMPTS)
                })
                .count();
            if pending == 0 {
                break;
            }
            info!(active = self.active(), pending, "monitoring copiers");
            tokio::time::sleep(Duration::from_secs(15)).await;
        }
        let completed = self.copiers.iter().filter(|c| c.stage == "COMPLETED").count();
        let failed = self.copiers.iter().filter(|c| c.stage == "ERROR").count();
        let canceled = self
            .copiers
            .iter()
            .filter(|c| c.stage == "CANCELED" || c.stage == "DELETED")
            .count();
        println!(
            "PLAN_RESULT {}",
            serde_json::json!({
                "ranges": self.copiers.len(), "completed": completed,
                "failed": failed, "canceled": canceled,
            })
        );
        if failed > 0 {
            bail!(
                "{failed}/{} copier(s) failed after {MAX_COPIER_ATTEMPTS} attempts",
                self.copiers.len()
            );
        }
        Ok(())
    }
}

pub async fn plan(cfg: PlanConfig) -> Result<()> {
    let (bucket_name, prefix) = parse_s3_url(&cfg.source_s3_url)?;
    let region = resolve_region(&bucket_name, cfg.aws_region.as_deref()).await;
    let client = build_list_client(&region).await;
    let jobs = JobsClient::new(cfg.hub_endpoint.clone(), cfg.hf_token.clone());
    let namespace = match &cfg.jobs_namespace {
        Some(ns) if !ns.trim().is_empty() => ns.trim().to_string(),
        _ => jobs
            .whoami()
            .await
            .context("resolve jobs namespace via whoami (pass --jobs-namespace to skip)")?,
    };
    let exclude = build_globset(&cfg.exclude_globs)?;
    info!(bucket = %bucket_name, prefix = %prefix, region = %region, namespace = %namespace, "planning: streaming list → ranges → copiers");

    let mut p = Planner {
        cfg,
        jobs,
        namespace,
        region,
        copiers: Vec::new(),
        range_idx: 0,
        cur_start_after: None,
        cur_files: 0,
        cur_bytes: 0,
        cur_le16: 0,
    };

    let mut listed = 0u64;
    let mut kept = 0u64;
    let mut kept_bytes = 0u64;
    let mut skipped_invalid = 0u64;
    let mut limit_hit = false;
    let mut last_key: Option<String> = None;

    let mut continuation: Option<String> = None;
    'outer: loop {
        let page =
            list_page_with_retry(&client, &bucket_name, &prefix, continuation.as_deref(), None)
                .await?;
        for obj in page.contents() {
            let raw_key = match obj.key() {
                Some(k) => k.to_string(),
                None => continue,
            };
            listed += 1;
            if !key_belongs_to_prefix(&raw_key, &prefix) {
                continue;
            }
            let parts: Vec<&str> = raw_key.split('/').collect();
            if parts.iter().any(|s| s.is_empty()) && parts != [""] {
                skipped_invalid += 1;
                continue;
            }
            if let Some(set) = &exclude {
                if set.is_match(&raw_key) {
                    continue;
                }
            }
            if p.cfg.limit_bytes > 0 && kept_bytes >= p.cfg.limit_bytes {
                limit_hit = true;
                break 'outer;
            }
            let size = obj.size().unwrap_or(0).max(0) as u64;
            kept += 1;
            kept_bytes = kept_bytes.saturating_add(size);
            last_key = Some(raw_key.clone());
            p.add(&raw_key, size).await?;

            if listed.is_multiple_of(100_000) {
                println!(
                    "PLANNING {}",
                    serde_json::json!({
                        "listed": listed, "kept": kept, "bytes": kept_bytes, "ranges": p.range_idx,
                    })
                );
            }
        }
        match page.next_continuation_token() {
            Some(t) => continuation = Some(t.to_string()),
            None => break 'outer,
        }
    }

    // Close the final (partial) range.
    if p.cur_files > 0 {
        if let Some(stop) = last_key {
            p.cut(stop).await?;
        }
    }

    let total_bytes: u64 = p.copiers.iter().map(|c| c.bytes).sum();
    let total_files: u64 = p.copiers.iter().map(|c| c.files).sum();
    println!(
        "PLAN_DONE {}",
        serde_json::json!({
            "ranges": p.copiers.len(), "files": total_files, "bytes": total_bytes,
            "skipped_invalid": skipped_invalid, "limit_hit": limit_hit, "region": p.region,
        })
    );
    info!(
        ranges = p.copiers.len(),
        files = total_files,
        bytes = total_bytes,
        skipped_invalid,
        "plan complete; monitoring copiers"
    );

    p.monitor().await
}

/// Build the argv + env + secrets + timeout for one copier job.
fn build_copier_spec(cfg: &PlanConfig, region: &str, c: &Copier) -> JobSpec {
    let mut command = vec![
        "hf-s3ream".to_string(),
        cfg.source_s3_url.clone(),
        cfg.dest.clone(),
        "--hub-endpoint".to_string(),
        cfg.hub_endpoint.clone(),
        "--aws-region".to_string(),
        region.to_string(),
        "--stop-at".to_string(),
        c.stop_at.clone(),
        "--parallel-files".to_string(),
        c.pf.to_string(),
        "--s3-part-concurrency".to_string(),
        cfg.s3_part_concurrency.to_string(),
        "--s3-part-size-mib".to_string(),
        cfg.s3_part_size_mib.to_string(),
        "--commit-chunk".to_string(),
        cfg.commit_chunk.to_string(),
    ];
    if let Some(sa) = &c.start_after {
        command.push("--start-after".to_string());
        command.push(sa.clone());
    }
    for g in &cfg.exclude_globs {
        command.push("--exclude".to_string());
        command.push(g.clone());
    }
    let mut labels = BTreeMap::new();
    labels.insert("hf-s3ream-run".to_string(), cfg.run_label.clone());
    JobSpec {
        command,
        arguments: vec![],
        environment: cfg.copier_env.clone(),
        flavor: cfg.copier_flavor.clone(),
        docker_image: cfg.copier_image.clone(),
        secrets: cfg.copier_secrets.clone(),
        timeout_seconds: Some(copier_timeout_s(c.bytes, c.files)),
        labels,
    }
}

/// A generous per-copier timeout. HF Jobs bill per second and kill only AT the
/// cap, so over-provisioning is free while under-provisioning kills a copy
/// mid-commit. Big-file ranges are bandwidth-bound (~80 MiB/s floor); small-file
/// ranges are per-file-overhead-bound (~300 files/s pessimistic). Take the
/// binding constraint + fixed overhead (image pull + finalize tail).
fn copier_timeout_s(bytes: u64, files: u64) -> u64 {
    let by_bytes = bytes / (80 * 1024 * 1024);
    let by_files = files / 300;
    (600 + by_bytes.max(by_files)).max(900)
}

/// Build the aws-sdk-s3 client used for listing (shared by run + plan): generous
/// connect timeout + SDK retries so many concurrent listers don't trip on a
/// transient connect failure.
async fn build_list_client(region: &str) -> aws_sdk_s3::Client {
    let sdk = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new(region.to_string()))
        .retry_config(aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(5))
        .timeout_config(
            aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                .connect_timeout(Duration::from_secs(15))
                .build(),
        )
        .load()
        .await;
    aws_sdk_s3::Client::new(&sdk)
}

/// Compile the --exclude globs into a GlobSet (None when empty). Any match
/// excludes the object.
fn build_globset(patterns: &[String]) -> Result<Option<globset::GlobSet>> {
    if patterns.is_empty() {
        return Ok(None);
    }
    let mut builder = globset::GlobSetBuilder::new();
    for pat in patterns {
        builder.add(globset::Glob::new(pat).with_context(|| format!("invalid --exclude glob: {pat}"))?);
    }
    Ok(Some(builder.build().context("build globset")?))
}

/// Drains listed objects from `rx` and uploads them through the CAS pipeline,
/// committing one bucket batch per `commit_chunk` files. Runs concurrently with
/// the listing loop that feeds `rx`, so uploading OVERLAPS listing: the first
/// byte moves as soon as the first object is listed, and listing never blocks
/// behind a commit. A fresh CasUploader session per commit keeps `finalize()`
/// (which flushes the shard the commit needs) correct and bounds memory.
#[allow(clippy::too_many_arguments)]
async fn upload_consumer(
    rx: mpsc::Receiver<S3Object>,
    store: Arc<dyn ObjectStore>,
    bucket_http: Arc<BucketClient>,
    dest: BucketRef,
    key_prefix: String,
    parallel: usize,
    part_concurrency: usize,
    part_size: u64,
    xor_byte: u8,
    commit_chunk: usize,
    files_done: Arc<AtomicU64>,
    bytes_done: Arc<AtomicU64>,
) -> Result<()> {
    let chunk = commit_chunk.max(1);
    // mpsc::Receiver → Stream (no extra dependency). `&mut stream` stays usable
    // across sessions because Pin<Box<_>> is Unpin. `.fuse()` is REQUIRED: `take`
    // drains the unfold to None at the end of a session, and the outer `while let`
    // polls it once more — a bare unfold panics if polled after None; Fuse keeps
    // returning None so the loop exits cleanly.
    let mut stream = Box::pin(
        futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|obj| (obj, rx))
        })
        .fuse(),
    );

    let mut committed_total = 0usize;
    // Each iteration is one commit session. `first` is that session's first object;
    // None means the channel is closed and fully drained → we're done. Waiting here
    // only blocks if listing is behind.
    while let Some(first) = stream.next().await {
        let uploader = Arc::new(
            CasUploader::new(bucket_http.clone(), dest.clone())
                .await
                .context("init CAS uploader")?,
        );
        let ops_collector: Arc<Mutex<Vec<BatchOp>>> = Arc::new(Mutex::new(Vec::new()));

        // Upload `first` plus up to `chunk - 1` more, streaming from the channel and
        // uploading `parallel` at a time — no buffering the whole chunk up front.
        let results: Vec<Result<UploadOutcome>> = futures::stream::once(async { first })
            .chain((&mut stream).take(chunk - 1))
            .map(|obj| {
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
            })
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
            .batch(&dest, &ops)
            .await
            .context("bucket batch commit")?;
        committed_total += n;
        info!(committed = n, committed_total, "committed chunk");
    }
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
        let bd = bytes_done.clone();
        let stream = result
            .into_stream()
            .map(move |r| r.map_err(map_object_store_error).map(|c| xor_chunk(c, xor_byte)))
            .inspect(move |r| {
                // Credit source bytes AS they stream (not once at file completion),
                // so the throughput graph is a smooth, honest real-time rate even
                // with GB-scale files.
                if let Ok(c) = r {
                    bd.fetch_add(c.len() as u64, Ordering::Relaxed);
                }
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
        let bd = bytes_done.clone();
        let stream = stream.inspect(move |r| {
            if let Ok(c) = r {
                bd.fetch_add(c.len() as u64, Ordering::Relaxed);
            }
        });
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

    // Bytes were credited per-chunk while streaming (above); here we only mark
    // the file itself complete.
    files_done.fetch_add(1, Ordering::Relaxed);

    Ok(UploadOutcome::Uploaded)
}

#[derive(Debug, Clone)]
pub struct S3Object {
    /// Decoded key: original raw characters as they appear in S3, used for the
    /// dest-bucket file path and human-readable logs.
    pub key: String,
    /// object_store Path holding the *encoded* representation. Used directly
    /// in store.get / get_range so we never re-encode (which would turn
    /// `Batch_%23128` into `Batch_%2523128` → S3 NoSuchKey).
    pub path: Path,
    pub size: u64,
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

/// Fetch one ListObjectsV2 page, retrying transient failures with backoff. Many
/// concurrent copiers listing (disjoint slices of) the same bucket can saturate
/// S3 and trip connect timeouts / throttling; a single such hiccup must not kill
/// the copier (the old auto-paginator ended the stream on the first error), so we
/// retry the page — the SDK also retries per attempt — before giving up.
async fn list_page_with_retry(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    continuation: Option<&str>,
    start_after: Option<&str>,
) -> Result<aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output> {
    const MAX_ATTEMPTS: u32 = 8;
    let mut attempt = 0u32;
    loop {
        let mut req = client.list_objects_v2().bucket(bucket).prefix(prefix);
        // A continuation token pages from a prior response; `start-after` seeds the
        // FIRST request only (the range lower bound). They're mutually exclusive —
        // once we're paging, the token carries the position.
        if let Some(t) = continuation {
            req = req.continuation_token(t);
        } else if let Some(sa) = start_after {
            req = req.start_after(sa);
        }
        match req.send().await {
            Ok(out) => return Ok(out),
            Err(e) => {
                attempt += 1;
                if attempt >= MAX_ATTEMPTS {
                    return Err(e).context("aws-sdk-s3 list_objects_v2 (exhausted retries)");
                }
                // 500ms, 1s, 2s, 4s, 8s, 10s, 10s … (capped).
                let backoff = Duration::from_millis((250u64 << attempt.min(6)).min(10_000));
                warn!(attempt, ?backoff, "list page failed (transient?), retrying: {e}");
                tokio::time::sleep(backoff).await;
            }
        }
    }
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
