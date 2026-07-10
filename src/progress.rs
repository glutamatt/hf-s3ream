//! Live transfer metrics — distinct S3-read vs CAS-ingest counters, per-file
//! in-flight tracking, and a stall watchdog.
//!
//! Two byte counters answer "which side is stuck?" when throughput drops:
//! `s3_bytes` counts bytes received from S3 (credited per chunk, including
//! bytes re-read by retries — it measures transport work, not progress);
//! `hf_bytes` counts bytes accepted by the xet CAS pipeline (credited per
//! `add_data`). They normally advance in lockstep, one chunk apart per file;
//! a single-sided stall points at the S3 read side vs the CAS upload side.
//!
//! The watchdog: every stats tick where BOTH counters are flat while work is
//! in flight, a counter increments; past a threshold it warn-logs the oldest
//! stalled in-flight files (key, bytes moved, seconds since last progress) and
//! the consumer phase — so a silent zero-bandwidth window names its culprit
//! instead of printing only `progress` lines.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// Coarse phase of the upload consumer, for logs + the stall watchdog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Phase {
    /// Waiting for the listing to hand over the next object.
    Idle = 0,
    /// Uploading a chunk's files (S3 → CAS).
    Uploading = 1,
    /// `CasUploader::finalize()` — flushing pending xorbs + the shard to CAS.
    Finalizing = 2,
    /// POSTing the bucket batch commit to the Hub.
    Committing = 3,
}

impl Phase {
    fn from_u8(v: u8) -> Phase {
        match v {
            1 => Phase::Uploading,
            2 => Phase::Finalizing,
            3 => Phase::Committing,
            _ => Phase::Idle,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Phase::Idle => "idle",
            Phase::Uploading => "uploading",
            Phase::Finalizing => "finalizing",
            Phase::Committing => "committing",
        }
    }
}

/// One file currently being transferred (a single attempt). Registered by
/// [`Metrics::track`], unregistered when the guard drops (success OR failure,
/// so a retried file re-registers fresh).
pub struct InflightFile {
    pub key: String,
    pub size: u64,
    /// Bytes received from S3 for this attempt.
    read: AtomicU64,
    /// Bytes accepted by the CAS pipeline (`add_data`) for this attempt.
    ingested: AtomicU64,
    /// Millis since `Metrics::started` of the last byte moved (either side).
    last_progress_ms: AtomicU64,
    started_ms: u64,
}

/// Snapshot of a stalled in-flight file, for the watchdog log.
pub struct StalledFile {
    pub key: String,
    pub size: u64,
    pub read: u64,
    pub ingested: u64,
    /// Seconds since the last byte moved for this file.
    pub idle_s: u64,
    /// Seconds since this attempt started.
    pub age_s: u64,
}

pub struct Metrics {
    started: Instant,
    /// Bytes received from S3 (transport bytes: retried reads count again).
    pub s3_bytes: AtomicU64,
    /// Bytes accepted by the xet CAS pipeline via `add_data`.
    pub hf_bytes: AtomicU64,
    /// Files fully uploaded (their ops queued for commit).
    pub files_done: AtomicU64,
    /// Files kept by the listing so far (the moving "total").
    pub kept_total: AtomicU64,
    /// Ranged-GET attempts that failed and were retried (multipart path).
    pub s3_part_retries: AtomicU64,
    /// Whole-file transfer attempts that failed and were retried.
    pub file_retries: AtomicU64,
    phase: AtomicU8,
    phase_since_ms: AtomicU64,
    inflight: Mutex<HashMap<u64, Arc<InflightFile>>>,
    next_id: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            started: Instant::now(),
            s3_bytes: AtomicU64::new(0),
            hf_bytes: AtomicU64::new(0),
            files_done: AtomicU64::new(0),
            kept_total: AtomicU64::new(0),
            s3_part_retries: AtomicU64::new(0),
            file_retries: AtomicU64::new(0),
            phase: AtomicU8::new(Phase::Idle as u8),
            phase_since_ms: AtomicU64::new(0),
            inflight: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
        })
    }

    fn now_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    pub fn set_phase(&self, p: Phase) {
        self.phase.store(p as u8, Ordering::Relaxed);
        self.phase_since_ms.store(self.now_ms(), Ordering::Relaxed);
    }

    pub fn phase(&self) -> Phase {
        Phase::from_u8(self.phase.load(Ordering::Relaxed))
    }

    /// Seconds spent in the current phase.
    pub fn phase_age_s(&self) -> u64 {
        (self.now_ms().saturating_sub(self.phase_since_ms.load(Ordering::Relaxed))) / 1000
    }

    /// Register one file-transfer attempt. Dropping the guard unregisters it.
    pub fn track(self: &Arc<Self>, key: &str, size: u64) -> InflightGuard {
        let now = self.now_ms();
        let file = Arc::new(InflightFile {
            key: key.to_string(),
            size,
            read: AtomicU64::new(0),
            ingested: AtomicU64::new(0),
            last_progress_ms: AtomicU64::new(now),
            started_ms: now,
        });
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.inflight.lock().unwrap().insert(id, file.clone());
        InflightGuard {
            metrics: self.clone(),
            id,
            file,
        }
    }

    /// Credit `len` bytes received from S3 for `file`.
    pub fn on_s3_chunk(&self, file: &InflightFile, len: u64) {
        self.s3_bytes.fetch_add(len, Ordering::Relaxed);
        file.read.fetch_add(len, Ordering::Relaxed);
        file.last_progress_ms.store(self.now_ms(), Ordering::Relaxed);
    }

    /// Credit `len` bytes accepted by the CAS pipeline for `file`.
    pub fn on_ingest(&self, file: &InflightFile, len: u64) {
        self.hf_bytes.fetch_add(len, Ordering::Relaxed);
        file.ingested.fetch_add(len, Ordering::Relaxed);
        file.last_progress_ms.store(self.now_ms(), Ordering::Relaxed);
    }

    pub fn inflight_len(&self) -> usize {
        self.inflight.lock().unwrap().len()
    }

    /// The `max` in-flight files that have gone longest without moving a byte,
    /// most-stalled first.
    pub fn stalled_files(&self, max: usize) -> Vec<StalledFile> {
        let now = self.now_ms();
        let mut v: Vec<StalledFile> = self
            .inflight
            .lock()
            .unwrap()
            .values()
            .map(|f| StalledFile {
                key: f.key.clone(),
                size: f.size,
                read: f.read.load(Ordering::Relaxed),
                ingested: f.ingested.load(Ordering::Relaxed),
                idle_s: now.saturating_sub(f.last_progress_ms.load(Ordering::Relaxed)) / 1000,
                age_s: now.saturating_sub(f.started_ms) / 1000,
            })
            .collect();
        v.sort_by(|a, b| b.idle_s.cmp(&a.idle_s));
        v.truncate(max);
        v
    }
}

/// RAII registration of one in-flight file attempt.
pub struct InflightGuard {
    metrics: Arc<Metrics>,
    id: u64,
    file: Arc<InflightFile>,
}

impl InflightGuard {
    pub fn file(&self) -> Arc<InflightFile> {
        self.file.clone()
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.metrics.inflight.lock().unwrap().remove(&self.id);
    }
}

/// Consecutive flat ticks (5s each) before the watchdog first fires: 20s.
const STALL_TICKS: u32 = 4;
/// Re-log the stall every this many ticks after the first fire: 30s.
const STALL_RELOG_TICKS: u32 = 6;

/// Spawn the 5s stats/progress loop. Prints the `progress` info line and the
/// machine-readable `PROGRESS {json}` line (the Space graphs `bytes_done` /
/// `mibps_5s` — those keys keep their original S3-side meaning), and runs the
/// stall watchdog described in the module docs.
pub fn spawn_stats_loop(m: Arc<Metrics>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        const MIB: f64 = 1024.0 * 1024.0;
        let mut last_t = Instant::now();
        let mut last_s3 = 0u64;
        let mut last_hf = 0u64;
        let mut flat_ticks = 0u32;
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        tick.tick().await; // skip the immediate first tick
        loop {
            tick.tick().await;
            let now = Instant::now();
            let dt = now.duration_since(last_t).as_secs_f64().max(0.001);
            let s3 = m.s3_bytes.load(Ordering::Relaxed);
            let hf = m.hf_bytes.load(Ordering::Relaxed);
            let files = m.files_done.load(Ordering::Relaxed);
            let total = m.kept_total.load(Ordering::Relaxed);
            let s3_5s = s3.saturating_sub(last_s3) as f64 / dt / MIB;
            let hf_5s = hf.saturating_sub(last_hf) as f64 / dt / MIB;
            let elapsed = m.elapsed().as_secs_f64().max(0.001);
            let avg = s3 as f64 / elapsed / MIB;
            let phase = m.phase();
            let inflight = m.inflight_len();
            info!(
                files,
                kept = total,
                gib_done = s3 as f64 / 1024.0_f64.powi(3),
                last_5s_mibps = format!("{s3_5s:.0}"),
                hf_5s_mibps = format!("{hf_5s:.0}"),
                avg_mibps = format!("{avg:.0}"),
                inflight,
                phase = phase.name(),
                elapsed_s = format!("{elapsed:.0}"),
                "progress",
            );
            println!(
                "PROGRESS {}",
                serde_json::json!({
                    // Original keys (the Space parses these; S3-side semantics).
                    "files": files,
                    "total": total,
                    "bytes_done": s3,
                    "mibps_5s": s3_5s.round(),
                    "mibps_avg": avg.round(),
                    "elapsed_s": m.elapsed().as_secs(),
                    // Split metrics + pipeline state.
                    "hf_bytes": hf,
                    "hf_mibps_5s": hf_5s.round(),
                    "inflight": inflight,
                    "phase": phase.name(),
                    "s3_part_retries": m.s3_part_retries.load(Ordering::Relaxed),
                    "file_retries": m.file_retries.load(Ordering::Relaxed),
                })
            );

            // Watchdog: both sides flat while there is work in flight.
            let working = inflight > 0 || phase != Phase::Idle;
            if s3 == last_s3 && hf == last_hf && working {
                flat_ticks += 1;
            } else {
                flat_ticks = 0;
            }
            if flat_ticks >= STALL_TICKS
                && (flat_ticks - STALL_TICKS).is_multiple_of(STALL_RELOG_TICKS)
            {
                let stalled_for_s = u64::from(flat_ticks) * 5;
                match phase {
                    Phase::Finalizing => warn!(
                        stalled_for_s,
                        phase_s = m.phase_age_s(),
                        "zero throughput: CAS finalize still running (xorb/shard flush)"
                    ),
                    Phase::Committing => warn!(
                        stalled_for_s,
                        phase_s = m.phase_age_s(),
                        "zero throughput: bucket batch commit still running"
                    ),
                    Phase::Uploading | Phase::Idle => {
                        for f in m.stalled_files(5) {
                            warn!(
                                key = %f.key,
                                size = f.size,
                                read = f.read,
                                ingested = f.ingested,
                                no_progress_s = f.idle_s,
                                in_flight_s = f.age_s,
                                "stalled file (no bytes moved)"
                            );
                        }
                        warn!(
                            inflight,
                            stalled_for_s,
                            "zero throughput with work in flight — S3 reads AND CAS ingest both flat"
                        );
                    }
                }
            }
            last_t = now;
            last_s3 = s3;
            last_hf = hf;
        }
    })
}
