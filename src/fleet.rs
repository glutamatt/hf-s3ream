//! Live view of the copier fleet, printed by the planner as `PLAN_PROGRESS`.
//!
//! The planner follows the log of each copier it starts (the same SSE stream
//! the web Space reads), keeps the latest `PROGRESS` / `DONE` / `LISTING`
//! figures of each range, and prints the sum every few seconds. So a client
//! that follows only the planner's log (`hf jobs logs -f`, the `hf s3ream`
//! CLI) sees the live throughput of the whole fleet.
//!
//! Best effort by design: a log stream that fails only makes the figures of
//! one range older. It never slows the listing, the launches or the watch of
//! the copiers, and it never fails the copy.

use serde::Serialize;
use std::collections::HashMap;

use crate::brake::{median, round3, Latency, Op};
use crate::growth::{Growth, View};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tracing::debug;

/// Where the fleet stands from the copiers' Job stages (counted by the
/// planner). Files and bytes here count the ranges whose copier COMPLETED.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Stages {
    /// Ranges cut so far (the total once `listing_done`).
    pub ranges: usize,
    pub done: usize,
    /// Spawned and not ended yet (SCHEDULING or RUNNING).
    pub active: usize,
    /// Of `active`: RUNNING, the copier's container has started.
    pub running: usize,
    /// Of `active`: not RUNNING yet (SCHEDULING), waiting for a machine.
    /// A fresh copier can stay here a minute or more: its rate is not in
    /// the sums yet.
    pub scheduling: usize,
    /// Ended ERROR: re-spawned while attempts remain.
    pub error: usize,
    pub canceled: usize,
    pub files_done: u64,
    pub bytes_done: u64,
    pub files_planned: u64,
    pub bytes_planned: u64,
}

/// The latest figures of one range, from its current copier's log.
#[derive(Debug, Clone, Default, PartialEq)]
struct Sample {
    /// The copier these figures come from: a re-spawned range starts over.
    job: String,
    files: u64,
    bytes: u64,
    committed: u64,
    skipped: u64,
    skipped_bytes: u64,
    mibps_5s: f64,
    hf_mibps_5s: f64,
    committed_fps_5s: f64,
    /// `elapsed_s` of the PROGRESS line these figures come from. A stream
    /// that reconnects replays the log from the start: older lines are
    /// ignored.
    elapsed_s: u64,
    /// DONE seen: the figures are final, the rates are 0.
    done: bool,
    /// The copier's brake (`brake.rs`) is raising its rate.
    ramping: bool,
    /// When the copier's brake last cut its rate.
    cut_at: Option<Instant>,
    /// The latency of each upload call the copier timed lately.
    latency: Latency,
    /// When a line last moved these figures. Rates older than
    /// [`STALE_AFTER`] count as 0 (a stream that stalls must not freeze a
    /// high rate on screen).
    at: Option<Instant>,
}

const STALE_AFTER: Duration = Duration::from_secs(20);

/// A copier whose brake cut its rate less than this ago counts as braking (in
/// PLAN_PROGRESS, for the eye: the planner itself reads the power,
/// `growth.rs`).
pub const BRAKE_QUIET: Duration = Duration::from_secs(60);

impl Sample {
    /// Fold one log line of the copier into the figures.
    fn apply(&mut self, line: &str, now: Instant) {
        let (name, rest) = match line.split_once(' ') {
            Some(x) => x,
            None => return,
        };
        let v: serde_json::Value = match name {
            "PROGRESS" | "DONE" | "LISTING" => match serde_json::from_str(rest) {
                Ok(v) => v,
                Err(_) => return,
            },
            _ => return,
        };
        let u = |k: &str| v.get(k).and_then(|x| x.as_u64());
        let f = |k: &str| v.get(k).and_then(|x| x.as_f64()).unwrap_or(0.0);
        match name {
            "PROGRESS" => {
                let elapsed = u("elapsed_s").unwrap_or(0);
                if self.done || elapsed < self.elapsed_s {
                    return;
                }
                self.elapsed_s = elapsed;
                self.files = u("files").unwrap_or(self.files);
                self.bytes = u("bytes_done").unwrap_or(self.bytes);
                self.committed = u("committed").unwrap_or(self.committed);
                self.mibps_5s = f("mibps_5s");
                self.hf_mibps_5s = f("hf_mibps_5s");
                self.committed_fps_5s = f("committed_fps_5s");
                // The brake: `action` is "fast" / "probe" / "hold" / "free",
                // or {"cut": cause}; `cut_age_s` the seconds since its last
                // cut. A copier without a brake has neither.
                let brake = v.get("brake");
                let action = brake.and_then(|b| b.get("action")).and_then(|a| a.as_str());
                // Only the fast start counts (shown in PLAN_PROGRESS): a copier
                // probing above it can probe for its whole life.
                self.ramping = action == Some("fast");
                self.cut_at = brake
                    .and_then(|b| b.get("cut_age_s"))
                    .and_then(|a| a.as_u64())
                    .and_then(|s| now.checked_sub(Duration::from_secs(s)));
                self.latency = brake
                    .and_then(|b| b.get("latency"))
                    .and_then(|l| serde_json::from_value(l.clone()).ok())
                    .unwrap_or_default();
            }
            "DONE" => {
                self.done = true;
                self.files = u("files").unwrap_or(self.files);
                self.bytes = u("bytes").unwrap_or(self.bytes);
                self.committed = u("committed").unwrap_or(self.committed);
                self.skipped = u("skipped_existing").unwrap_or(self.skipped);
                self.skipped_bytes = u("skipped_existing_bytes").unwrap_or(self.skipped_bytes);
                self.mibps_5s = 0.0;
                self.hf_mibps_5s = 0.0;
                self.committed_fps_5s = 0.0;
                self.ramping = false;
                self.latency = Latency::default();
            }
            _ => {
                // LISTING: running counts of files already at the destination.
                self.skipped = self.skipped.max(u("skipped_existing").unwrap_or(0));
                self.skipped_bytes = self
                    .skipped_bytes
                    .max(u("skipped_existing_bytes").unwrap_or(0));
            }
        }
        self.at = Some(now);
    }

    /// The rates, unless the copier is done, ended, or silent too long.
    fn live(&self, now: Instant) -> bool {
        !self.done
            && self
                .at
                .is_some_and(|at| now.duration_since(at) < STALE_AFTER)
    }
}

#[derive(Default)]
struct State {
    stages: Stages,
    listing_done: bool,
    /// Range idx → the figures of its current copier.
    ranges: HashMap<u64, Sample>,
}

/// One `PLAN_PROGRESS` line: the stages, plus the live sums of the copiers'
/// own figures.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Snapshot {
    #[serde(flatten)]
    pub stages: Stages,
    pub listing_done: bool,
    /// Files and bytes read so far by every copier (finished or not).
    pub files_copied: u64,
    pub bytes_copied: u64,
    /// Files landed in the bucket.
    pub committed: u64,
    /// Already at the destination (--skip-existing): done, but not read.
    pub skipped: u64,
    pub skipped_bytes: u64,
    /// Sum of the copiers' 5-second rates (they run at the same time, so
    /// their rates add up).
    pub mibps_5s: f64,
    pub hf_mibps_5s: f64,
    pub committed_fps_5s: f64,
    /// Live copiers whose brake is raising their rate...
    pub ramping: usize,
    /// ...and those whose brake cut it less than [`BRAKE_QUIET`] ago.
    pub braking: usize,
    /// The latency of each upload call: the median over the live copiers
    /// that timed it lately (`brake.rs`). The planner sizes the fleet toward
    /// the most throughput per latency (`growth.rs`).
    pub latency: Latency,
    /// The copiers the fleet size allows now (`growth.rs`); None when it is
    /// off.
    pub target: Option<usize>,
}

impl State {
    fn snapshot(&self, now: Instant) -> Snapshot {
        let mut s = Snapshot {
            stages: self.stages.clone(),
            listing_done: self.listing_done,
            files_copied: 0,
            bytes_copied: 0,
            committed: 0,
            skipped: 0,
            skipped_bytes: 0,
            mibps_5s: 0.0,
            hf_mibps_5s: 0.0,
            committed_fps_5s: 0.0,
            ramping: 0,
            braking: 0,
            latency: Latency::default(),
            target: None,
        };
        let mut times: [Vec<f64>; 4] = Default::default();
        for r in self.ranges.values() {
            s.files_copied += r.files;
            s.bytes_copied += r.bytes;
            s.committed += r.committed;
            s.skipped += r.skipped;
            s.skipped_bytes += r.skipped_bytes;
            if r.live(now) {
                s.mibps_5s += r.mibps_5s;
                s.hf_mibps_5s += r.hf_mibps_5s;
                s.committed_fps_5s += r.committed_fps_5s;
                s.ramping += usize::from(r.ramping);
                s.braking += usize::from(
                    r.cut_at
                        .is_some_and(|t| now.duration_since(t) < BRAKE_QUIET),
                );
                for op in Op::ALL {
                    if let Some(t) = r.latency.get(op) {
                        times[op.index()].push(t);
                    }
                }
            }
        }
        for op in Op::ALL {
            let xs = &times[op.index()];
            if !xs.is_empty() {
                s.latency.set(op, Some(round3(median(xs))));
            }
        }
        let round1 = |x: f64| (x * 10.0).round() / 10.0;
        s.mibps_5s = round1(s.mibps_5s);
        s.hf_mibps_5s = round1(s.hf_mibps_5s);
        s.committed_fps_5s = s.committed_fps_5s.round();
        s
    }
}

/// How often `PLAN_PROGRESS` goes out while the figures move.
const REPORT_EVERY: Duration = Duration::from_secs(5);
/// ...and at least this often while they do not (a sign of life).
const HEARTBEAT: Duration = Duration::from_secs(30);

/// The live fleet view. Cheap to clone: the clones share one state.
#[derive(Clone)]
pub struct Fleet {
    state: Arc<Mutex<State>>,
    /// None: copier logs are not followed (`--no-live-progress`); the lines
    /// carry the stages only.
    logs: Option<Arc<LogSource>>,
    last: Arc<Mutex<Option<(Snapshot, Instant)>>>,
    /// The fleet size (`growth.rs`), fed with each snapshot.
    growth: Option<Arc<Mutex<Growth>>>,
}

struct LogSource {
    http: reqwest::Client,
    endpoint: String,
    token: String,
    namespace: String,
}

impl Fleet {
    pub fn new(endpoint: &str, token: &str, namespace: &str, follow_logs: bool) -> Self {
        let logs = follow_logs.then(|| {
            // A log stream stays open for the whole life of a copier: no
            // total timeout. The server sends a keep-alive every ~30 s, so a
            // read silent for 90 s means a dead connection.
            let http = reqwest::Client::builder()
                .user_agent(concat!("hf-s3ream/", env!("CARGO_PKG_VERSION")))
                .connect_timeout(Duration::from_secs(10))
                .read_timeout(Duration::from_secs(90))
                .tcp_keepalive(Duration::from_secs(30))
                .build()
                .expect("reqwest client");
            Arc::new(LogSource {
                http,
                endpoint: endpoint.to_string(),
                token: token.to_string(),
                namespace: namespace.to_string(),
            })
        });
        Self {
            state: Arc::new(Mutex::new(State::default())),
            logs,
            last: Arc::new(Mutex::new(None)),
            growth: None,
        }
    }

    /// Size the fleet toward the most power, throughput / CAS latency
    /// (`growth.rs`). It needs the copiers' logs: without them, it stays off
    /// (every copier up to `max` at once).
    pub fn with_growth(mut self, max: usize) -> Self {
        if self.logs.is_some() {
            self.growth = Some(Arc::new(Mutex::new(Growth::new(max, Instant::now()))));
        }
        self
    }

    /// The copiers the fleet size allows now; None when it is off.
    pub fn target(&self) -> Option<usize> {
        self.growth.as_ref().map(|g| g.lock().unwrap().target())
    }

    pub fn set_stages(&self, stages: Stages) {
        self.state.lock().unwrap().stages = stages;
    }

    pub fn set_listing_done(&self) {
        self.state.lock().unwrap().listing_done = true;
    }

    /// Follow the log of copier `job` for range `idx`, in the background.
    /// A re-spawned range starts over from its new copier.
    pub fn follow(&self, idx: u64, job: &str) {
        self.state.lock().unwrap().ranges.insert(
            idx,
            Sample {
                job: job.to_string(),
                ..Sample::default()
            },
        );
        let Some(logs) = self.logs.clone() else {
            return;
        };
        let state = self.state.clone();
        let job = job.to_string();
        tokio::spawn(async move { follow_copier(logs, state, idx, job).await });
    }

    /// Print `PLAN_PROGRESS` now if the figures changed since the last line,
    /// or the heartbeat is due; `force` prints in any case.
    pub fn report(&self, force: bool) {
        let now = Instant::now();
        let mut snap = self.state.lock().unwrap().snapshot(now);
        if let Some(g) = &self.growth {
            let mut g = g.lock().unwrap();
            g.observe(
                now,
                View {
                    running: snap.stages.running,
                    active: snap.stages.active,
                    bytes: snap.bytes_copied,
                    latency: snap.latency,
                    more_to_launch: !snap.listing_done
                        || snap.stages.ranges
                            > snap.stages.done
                                + snap.stages.active
                                + snap.stages.error
                                + snap.stages.canceled,
                },
            );
            snap.target = Some(g.target());
        }
        let mut last = self.last.lock().unwrap();
        let due = force
            || match &*last {
                Some((prev, at)) => *prev != snap || now.duration_since(*at) >= HEARTBEAT,
                None => true,
            };
        if due {
            println!("PLAN_PROGRESS {}", serde_json::json!(snap));
            *last = Some((snap, now));
        }
    }

    /// Print `PLAN_PROGRESS` every [`REPORT_EVERY`] while the planner runs.
    /// The caller aborts the task when it is done.
    pub fn spawn_reporter(&self) -> JoinHandle<()> {
        let fleet = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(REPORT_EVERY);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                fleet.report(false);
            }
        })
    }
}

/// Read one copier's log until the copier ends, and fold its lines into
/// `state`. Reconnects with a growing delay: a stream can close before the
/// container starts, or drop. The delay grows after every closed stream,
/// even one that carried lines: a reconnect replays the log from the start,
/// so lines prove nothing, and a fleet of streams that keep closing must not
/// hammer the Hub with the token the planner needs (at worst, a range's live
/// figures lag a minute; the next lines catch up).
async fn follow_copier(logs: Arc<LogSource>, state: Arc<Mutex<State>>, idx: u64, job: String) {
    let logs_url = format!("{}/api/jobs/{}/{}/logs", logs.endpoint, logs.namespace, job);
    let job_url = format!("{}/api/jobs/{}/{}", logs.endpoint, logs.namespace, job);
    let mut delay = Duration::from_secs(5);
    loop {
        if let Err(e) = stream_once(&logs, &logs_url, &state, idx, &job).await {
            debug!(job, "copier log stream: {e:#}");
        }
        // Still this range's copier, and not ended?
        let current = state
            .lock()
            .unwrap()
            .ranges
            .get(&idx)
            .is_some_and(|s| s.job == job && !s.done);
        if !current || job_ended(&logs, &job_url).await {
            if let Some(s) = state.lock().unwrap().ranges.get_mut(&idx) {
                if s.job == job {
                    s.mibps_5s = 0.0;
                    s.hf_mibps_5s = 0.0;
                    s.committed_fps_5s = 0.0;
                }
            }
            return;
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(60));
    }
}

/// One SSE connection, until it closes or the range gets a new copier.
async fn stream_once(
    logs: &LogSource,
    url: &str,
    state: &Mutex<State>,
    idx: u64,
    job: &str,
) -> anyhow::Result<()> {
    use futures::StreamExt;
    let resp = logs
        .http
        .get(url)
        .bearer_auth(&logs.token)
        .header("Accept", "text/event-stream")
        .send()
        .await?
        .error_for_status()?;
    let mut body = resp.bytes_stream();
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = body.next().await {
        buf.extend_from_slice(&chunk?);
        while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
            let raw: Vec<u8> = buf.drain(..=pos).collect();
            let Some(line) = sse_log_line(&raw) else {
                continue;
            };
            let mut st = state.lock().unwrap();
            match st.ranges.get_mut(&idx) {
                Some(s) if s.job == job => s.apply(&line, Instant::now()),
                // The range was re-spawned: this copier no longer counts.
                _ => return Ok(()),
            }
        }
    }
    Ok(())
}

/// The log line carried by one SSE line: `data: {"data": "<line>", …}`.
/// None for keep-alives, other fields, and the synthetic "Job started" banner.
fn sse_log_line(raw: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(raw).ok()?.trim_end();
    let payload = text.strip_prefix("data:")?.trim_start();
    let v: serde_json::Value = serde_json::from_str(payload).ok()?;
    let line = v.get("data")?.as_str()?;
    if line.is_empty() || line.starts_with("===== Job started") {
        return None;
    }
    Some(line.to_string())
}

async fn job_ended(logs: &LogSource, url: &str) -> bool {
    let stage = async {
        let v: serde_json::Value = logs
            .http
            .get(url)
            .bearer_auth(&logs.token)
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .ok()?
            .json()
            .await
            .ok()?;
        v.pointer("/status/stage")?.as_str().map(str::to_string)
    }
    .await;
    // Unknown: keep following (the planner's own watch has the last word).
    stage.is_some_and(|s| crate::jobs_client::JobInfo::is_terminal(&s))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn progress(elapsed: u64, files: u64, bytes: u64, mibps: f64) -> String {
        format!(
            r#"PROGRESS {{"files":{files},"bytes_done":{bytes},"committed":{files},"mibps_5s":{mibps},"hf_mibps_5s":{mibps},"committed_fps_5s":10,"elapsed_s":{elapsed}}}"#
        )
    }

    #[test]
    fn a_replayed_log_does_not_move_the_figures_back() {
        let now = Instant::now();
        let mut s = Sample::default();
        s.apply(&progress(10, 5, 500, 100.0), now);
        s.apply(&progress(20, 9, 900, 120.0), now);
        // Reconnect: the server replays from the start.
        s.apply(&progress(10, 5, 500, 100.0), now);
        assert_eq!((s.files, s.bytes, s.mibps_5s), (9, 900, 120.0));
    }

    #[test]
    fn done_is_final_and_stops_the_rates() {
        let now = Instant::now();
        let mut s = Sample::default();
        s.apply(&progress(10, 5, 500, 100.0), now);
        s.apply(
            r#"DONE {"files":12,"committed":12,"bytes":1200,"skipped_existing":3,"skipped_existing_bytes":30}"#,
            now,
        );
        s.apply(&progress(30, 1, 1, 1.0), now);
        assert_eq!(
            (s.files, s.bytes, s.committed, s.skipped),
            (12, 1200, 12, 3)
        );
        assert!(!s.live(now));
    }

    #[test]
    fn the_brake_of_each_copier_is_counted() {
        let now = Instant::now();
        let mut st = State::default();
        let lines = [
            // Doubling its rate, never cut.
            r#"PROGRESS {"elapsed_s":10,"hf_mibps_5s":100.0,"brake":{"mode":"on","rate_mibps":128.0,"action":"fast","ratio":null,"cuts":0,"cut_age_s":null,"errors":{}}}"#,
            // Cut 12 s ago, holding.
            r#"PROGRESS {"elapsed_s":10,"hf_mibps_5s":50.0,"brake":{"mode":"on","rate_mibps":512.0,"action":"hold","ratio":1.4,"cuts":1,"cut_age_s":12,"errors":{"throttled":3}}}"#,
            // Just cut.
            r#"PROGRESS {"elapsed_s":10,"hf_mibps_5s":50.0,"brake":{"mode":"on","rate_mibps":256.0,"action":{"cut":{"error":"server_error"}},"ratio":null,"cuts":2,"cut_age_s":0,"errors":{"server_error":1}}}"#,
            // Cut long ago, free.
            r#"PROGRESS {"elapsed_s":10,"hf_mibps_5s":50.0,"brake":{"mode":"on","rate_mibps":900.0,"action":"free","ratio":1.0,"cuts":1,"cut_age_s":300,"errors":{}}}"#,
            // An image without the brake.
            r#"PROGRESS {"elapsed_s":10,"hf_mibps_5s":50.0}"#,
        ];
        for (idx, line) in lines.into_iter().enumerate() {
            let mut s = Sample::default();
            s.apply(line, now);
            st.ranges.insert(idx as u64, s);
        }
        let snap = st.snapshot(now);
        assert_eq!((snap.ramping, snap.braking), (1, 2));
        assert_eq!(snap.hf_mibps_5s, 300.0);
        // A copier that went silent counts for nothing.
        let later = now + STALE_AFTER;
        assert_eq!(st.snapshot(later).braking, 0);
    }

    #[test]
    fn rates_of_a_silent_copier_count_as_zero() {
        let then = Instant::now();
        let mut s = Sample::default();
        s.apply(&progress(10, 5, 500, 100.0), then);
        assert!(s.live(then));
        assert!(!s.live(then + STALE_AFTER));
    }

    #[test]
    fn the_snapshot_adds_up_the_ranges() {
        let now = Instant::now();
        let mut st = State {
            stages: Stages {
                ranges: 2,
                active: 2,
                ..Stages::default()
            },
            ..State::default()
        };
        for (idx, mibps) in [(0, 100.0), (1, 50.5)] {
            let mut s = Sample {
                job: format!("j{idx}"),
                ..Sample::default()
            };
            s.apply(&progress(10, 4, 400, mibps), now);
            st.ranges.insert(idx, s);
        }
        let snap = st.snapshot(now);
        assert_eq!(
            (snap.files_copied, snap.bytes_copied, snap.committed),
            (8, 800, 8)
        );
        assert_eq!(snap.mibps_5s, 150.5);
        assert_eq!(snap.committed_fps_5s, 20.0);
        let line = serde_json::json!(snap);
        // Flattened: one flat object for the clients.
        assert_eq!(line["ranges"], 2);
        assert_eq!(line["listing_done"], false);
    }

    #[test]
    fn sse_lines() {
        assert_eq!(
            sse_log_line(br#"data: {"data":"PROGRESS {}","timestamp":"t"}"#).as_deref(),
            Some("PROGRESS {}")
        );
        assert_eq!(sse_log_line(b": keep-alive\n"), None);
        assert_eq!(
            sse_log_line(br#"data: {"data":"===== Job started at x"}"#),
            None
        );
        assert_eq!(sse_log_line(br#"data: {"data":""}"#), None);
    }

    /// A local server that answers like the Hub: the log stream of a copier
    /// (SSE, then closed), then its stage. The follower reads the stream,
    /// sees the Job ended, and stops.
    #[tokio::test]
    async fn follows_a_copier_log_over_http_until_it_ends() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut req = vec![0u8; 4096];
                let n = sock.read(&mut req).await.unwrap();
                let head = String::from_utf8_lossy(&req[..n]).to_string();
                assert!(
                    head.contains("authorization: Bearer tok")
                        || head.contains("Authorization: Bearer tok")
                );
                let (ctype, body) = if head.starts_with("GET /api/jobs/ns/j0/logs") {
                    let lines = [
                        r#"data: {"data":"===== Job started at 2026-11-03","timestamp":"t"}"#
                            .to_string(),
                        ": keep-alive".to_string(),
                        format!(
                            r#"data: {{"data":{},"timestamp":"t"}}"#,
                            serde_json::json!(progress(5, 3, 300, 40.0))
                        ),
                        format!(
                            r#"data: {{"data":{},"timestamp":"t"}}"#,
                            serde_json::json!(r#"DONE {"files":7,"committed":7,"bytes":700}"#)
                        ),
                    ];
                    ("text/event-stream", lines.join("\n\n") + "\n\n")
                } else {
                    (
                        "application/json",
                        r#"{"id":"j0","status":{"stage":"COMPLETED"}}"#.to_string(),
                    )
                };
                let resp = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: {ctype}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                sock.write_all(resp.as_bytes()).await.unwrap();
            }
        });
        let fleet = Fleet::new(&format!("http://{addr}"), "tok", "ns", true);
        fleet.follow(0, "j0");
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let snap = fleet.state.lock().unwrap().snapshot(Instant::now());
            if snap.files_copied == 7 {
                assert_eq!(
                    (snap.bytes_copied, snap.committed, snap.mibps_5s),
                    (700, 7, 0.0)
                );
                break;
            }
            assert!(Instant::now() < deadline, "no DONE read: {snap:?}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
