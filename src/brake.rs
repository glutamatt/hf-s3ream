//! Adaptive backpressure: each copier adapts the bytes per second it feeds to
//! the xet pipeline to the health of CAS.
//!
//! Why: CAS spends CPU on every byte we commit (it reads each xorb back from
//! S3 and validates it). When a fleet pushes faster than CAS can scale, its
//! CPU saturates and other users' downloads get 5xx before our own uploads
//! see many errors. So the brake listens to an early signal (how long CAS
//! takes to commit a xorb) as well as to errors (429, 5xx, timeouts, dropped
//! connections).
//!
//! The parts:
//! - [`SignalLayer`] reads xet-core's own tracing events: the start and end
//!   of each xorb commit (to time it) and the retry lines of its retry wrapper
//!   (to see congestion errors). xet-core keeps both inside, so its events are
//!   the only window we have. They are matched by target and message: xet-core
//!   is pinned by tag, and [`tests`] run its real retry wrapper to catch a
//!   change of wording.
//! - [`Controller`]: AIMD with a fast start, the way TCP does it. Doubles every
//!   tick up to a known-safe level, then grows by a small step (the pace CAS
//!   adds pods). A congestion error halves the rate and holds it 30 s; slow
//!   commits (2x their base time) cut it by 20 %. It climbs back fast to 70 %
//!   of the rate that broke, then slowly.
//! - [`Limiter`]: a token bucket in front of `add_data`, set by the controller.
//!
//! The fleet needs no coordination: all copiers see the same CAS, so they
//! brake together. The fleet figures (safe level, ramp) are split between the
//! copiers by `--brake-share`, which the planner sets.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use tracing::field::{Field, Visit};
use tracing::{info, Event, Subscriber};
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

const MIB: f64 = 1024.0 * 1024.0;
const GIB: f64 = 1024.0 * MIB;

/// How often the controller decides.
pub const TICK: Duration = Duration::from_secs(2);
/// First rate of a copier.
const START_RATE: f64 = 64.0 * MIB;
/// The rate never goes below this: a copier always keeps moving, so it keeps
/// measuring.
const FLOOR_RATE: f64 = 16.0 * MIB;
/// A congestion error: the rate is halved, and the fast climb stops at 70 %
/// of the rate that broke.
const HARD_CUT: f64 = 0.5;
const HARD_THRESHOLD: f64 = 0.7;
const HARD_HOLD: Duration = Duration::from_secs(30);
/// Slow commits: a gentler cut.
const SOFT_CUT: f64 = 0.8;
const SOFT_THRESHOLD: f64 = 0.9;
const SOFT_HOLD: Duration = Duration::from_secs(10);
/// Holds are spread by ±20 % so the copiers that braked together do not all
/// climb back at the same second.
const HOLD_JITTER: f64 = 0.2;
/// At most one cut in this time: a burst of errors is one congestion event
/// (as TCP cuts once per window), and the effect of a cut needs a few commit
/// times to show.
const MIN_CUT_GAP: Duration = Duration::from_secs(5);
/// Commit time over its base above which the rate is cut...
const SLOW_RATIO: f64 = 2.0;
/// ...and above which it no longer grows.
const HOLD_RATIO: f64 = 1.3;
/// The base commit time is the lowest tick median of this window (like the
/// min RTT of BBR): no need to know it in advance, and it follows a CAS that
/// gets faster.
const BASE_WINDOW: Duration = Duration::from_secs(10 * 60);
/// Fewer commits than this in a tick: no latency signal for that tick.
const MIN_SAMPLES: usize = 3;
/// Smaller xorbs are left out of the timing: their fixed costs dominate.
pub const MIN_TIMED_XORB_BYTES: u64 = 8 * 1024 * 1024;
/// The rate only grows while the copier uses most of it (RFC 7661): a copier
/// held back by its source must not build a rate it never used, which a cut
/// would then not bring down.
const BUSY_SHARE: f64 = 0.7;
/// Entries of an upload or a commit that never ended are dropped after this.
const FORGET_AFTER: Duration = Duration::from_secs(10 * 60);

/// What the brake does in this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Adapt the rate.
    On,
    /// Compute and log the rate, but do not apply it (to calibrate).
    Shadow,
    /// No brake.
    Off,
}

impl Mode {
    /// The spelling of `--brake`.
    pub fn name(self) -> &'static str {
        match self {
            Mode::On => "on",
            Mode::Shadow => "shadow",
            Mode::Off => "off",
        }
    }
}

/// The fleet figures, and how many copiers share them.
#[derive(Debug, Clone, Copy)]
pub struct Params {
    /// Fast climb up to this rate, bytes/s.
    pub fast_until: f64,
    /// Then grow by this much per tick, bytes/s.
    pub step: f64,
}

impl Params {
    /// `fast_gibps` and `ramp_gibps` (per minute) are fleet figures, split
    /// evenly between `share` copiers.
    pub fn for_share(fast_gibps: f64, ramp_gibps: f64, share: usize) -> Self {
        let share = share.max(1) as f64;
        let per_tick = TICK.as_secs_f64() / 60.0;
        Self {
            fast_until: (fast_gibps * GIB / share).max(FLOOR_RATE),
            step: (ramp_gibps * GIB / share * per_tick).max(0.0),
        }
    }
}

/// An error that says CAS (or the storage behind it) is overloaded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Congestion {
    /// 429: CAS upload slots are full.
    Throttled,
    /// 5xx: CAS (or S3 under it) failed.
    ServerError,
    /// 408, or a request that timed out.
    Timeout,
    /// The connection failed or was cut.
    Connection,
}

impl Congestion {
    /// The worst first: the cause a tick with several errors reports.
    fn rank(self) -> u8 {
        match self {
            Congestion::ServerError => 3,
            Congestion::Throttled => 2,
            Congestion::Timeout => 1,
            Congestion::Connection => 0,
        }
    }
}

/// Why the controller cut the rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Cause {
    Error(Congestion),
    SlowCommits,
}

/// What the controller did at a tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    /// Doubling, below the threshold.
    Fast,
    /// Growing by the small step, above the threshold.
    Probe,
    /// Keeping the rate: in a hold after a cut, or commits a bit slow.
    Hold,
    /// The copier does not use its rate (its source or its own pipeline is
    /// the limit): the rate stays as it is.
    Free,
    /// The rate was cut.
    Cut(Cause),
}

/// What a tick saw.
#[derive(Debug, Default, Clone)]
pub struct Observation {
    /// The worst congestion error since the last tick, if any.
    pub congestion: Option<Congestion>,
    /// Commit times since the last tick, seconds per MiB.
    pub commit_s_per_mib: Vec<f64>,
    /// Bytes fed to xet since the last tick.
    pub bytes: u64,
}

/// The rate controller of one copier. Pure: time comes in, so tests drive it.
pub struct Controller {
    params: Params,
    rate: f64,
    threshold: f64,
    last_tick: Option<Instant>,
    last_cut: Option<Instant>,
    hold_until: Option<Instant>,
    /// (tick, median commit time) for the base.
    medians: VecDeque<(Instant, f64)>,
    rng: u64,
}

/// The result of a tick.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Decision {
    pub action: Action,
    /// The new rate, bytes/s.
    pub rate: f64,
    /// Median commit time of the tick over the base, when known.
    pub ratio: Option<f64>,
}

impl Controller {
    pub fn new(params: Params, seed: u64) -> Self {
        Self {
            params,
            rate: START_RATE,
            threshold: params.fast_until,
            last_tick: None,
            last_cut: None,
            hold_until: None,
            medians: VecDeque::new(),
            rng: splitmix64(seed),
        }
    }

    #[cfg(test)]
    pub fn rate(&self) -> f64 {
        self.rate
    }

    /// A number in [-1, 1) for the jitter (xorshift64: plenty for spreading
    /// holds, and no dependency).
    fn noise(&mut self) -> f64 {
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        (x >> 11) as f64 / (1u64 << 53) as f64 * 2.0 - 1.0
    }

    fn hold_for(&mut self, d: Duration) -> Duration {
        d.mul_f64(1.0 + HOLD_JITTER * self.noise())
    }

    /// Median commit time of this tick over the base, and update the base.
    fn latency_ratio(&mut self, now: Instant, samples: &[f64]) -> Option<f64> {
        while self
            .medians
            .front()
            .is_some_and(|(t, _)| now.duration_since(*t) > BASE_WINDOW)
        {
            self.medians.pop_front();
        }
        if samples.len() < MIN_SAMPLES {
            return None;
        }
        let median = median(samples);
        self.medians.push_back((now, median));
        let base = self
            .medians
            .iter()
            .map(|(_, m)| *m)
            .fold(f64::INFINITY, f64::min);
        (base > 0.0).then(|| median / base)
    }

    fn cut(&mut self, now: Instant, factor: f64, threshold: f64, hold: Duration) {
        self.threshold = (self.rate * threshold).max(FLOOR_RATE);
        self.rate = (self.rate * factor).max(FLOOR_RATE);
        self.last_cut = Some(now);
        let hold = self.hold_for(hold);
        self.hold_until = Some(now + hold);
    }

    /// One tick: read what happened since the last one, set the new rate.
    pub fn tick(&mut self, now: Instant, obs: &Observation) -> Decision {
        let dt = self
            .last_tick
            .map(|t| now.duration_since(t).as_secs_f64())
            .unwrap_or(TICK.as_secs_f64())
            .max(1e-3);
        self.last_tick = Some(now);
        // Bytes/s fed to xet during this tick, under the rate set at the
        // last one.
        let used = obs.bytes as f64 / dt;

        let ratio = self.latency_ratio(now, &obs.commit_s_per_mib);
        let may_cut = self
            .last_cut
            .is_none_or(|t| now.duration_since(t) >= MIN_CUT_GAP);
        let held = self.hold_until.is_some_and(|t| now < t);

        let action = if let (Some(c), true) = (obs.congestion, may_cut) {
            self.cut(now, HARD_CUT, HARD_THRESHOLD, HARD_HOLD);
            Action::Cut(Cause::Error(c))
        } else if ratio.is_some_and(|r| r > SLOW_RATIO) && may_cut {
            self.cut(now, SOFT_CUT, SOFT_THRESHOLD, SOFT_HOLD);
            Action::Cut(Cause::SlowCommits)
        } else if held || obs.congestion.is_some() || ratio.is_some_and(|r| r > HOLD_RATIO) {
            Action::Hold
        } else if used < BUSY_SHARE * self.rate {
            Action::Free
        } else if self.rate < self.threshold {
            self.rate = (self.rate * 2.0).min(self.threshold);
            Action::Fast
        } else {
            self.rate += self.params.step;
            Action::Probe
        };
        Decision {
            action,
            rate: self.rate,
            ratio,
        }
    }
}

/// Spreads a seed over all bits (close seeds give unrelated streams), and is
/// never 0, which xorshift needs.
fn splitmix64(seed: u64) -> u64 {
    let mut z = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    (z ^ (z >> 31)) | 1
}

fn median(xs: &[f64]) -> f64 {
    let mut v = xs.to_vec();
    v.sort_by(|a, b| a.total_cmp(b));
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

/// A token bucket: callers ask for bytes and wait while the bucket is in
/// debt. Debt lets one request be larger than the bucket (a 16 MiB S3 part at
/// a low rate) while the average stays at the rate.
pub struct Limiter {
    enforce: bool,
    /// Bytes per second; 0 = no limit.
    rate: AtomicU64,
    bucket: Mutex<(f64, Instant)>,
    /// Bytes that went through, for the controller.
    passed: AtomicU64,
}

/// Tokens a bucket holds at most, in seconds of its rate.
const BURST_S: f64 = 0.5;

impl Limiter {
    pub fn new(enforce: bool) -> Self {
        Self {
            enforce,
            rate: AtomicU64::new(0),
            bucket: Mutex::new((0.0, Instant::now())),
            passed: AtomicU64::new(0),
        }
    }

    pub fn set_rate(&self, bytes_per_s: f64) {
        self.rate
            .store(bytes_per_s.max(1.0) as u64, Ordering::Relaxed);
    }

    pub fn passed(&self) -> u64 {
        self.passed.load(Ordering::Relaxed)
    }

    /// How long a request for `n` bytes must wait now. Takes the tokens.
    fn reserve(&self, n: u64, now: Instant) -> Duration {
        self.passed.fetch_add(n, Ordering::Relaxed);
        let rate = self.rate.load(Ordering::Relaxed) as f64;
        if !self.enforce || rate <= 0.0 {
            return Duration::ZERO;
        }
        let mut b = self.bucket.lock().unwrap();
        let (tokens, at) = *b;
        let refill = now.saturating_duration_since(at).as_secs_f64() * rate;
        let tokens = (tokens + refill).min(rate * BURST_S) - n as f64;
        *b = (tokens, now);
        if tokens >= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(-tokens / rate)
        }
    }

    /// Wait until `n` more bytes may go to xet.
    pub async fn acquire(&self, n: u64) {
        let wait = self.reserve(n, Instant::now());
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }
}

/// What xet-core's events told us since the last tick.
#[derive(Default)]
struct SignalState {
    /// Xorb hash → (size, seen at), from the start of its upload.
    xorb_sizes: HashMap<String, (u64, Instant)>,
    /// Commit call id → (started at, xorb size if known).
    commits: HashMap<u64, (Instant, Option<u64>)>,
    samples: Vec<f64>,
    congestion: Option<Congestion>,
    /// Congestion errors seen, by cause, since the start.
    errors: HashMap<Congestion, u64>,
}

/// Shared by the tracing layer (writer) and the brake task (reader).
#[derive(Default)]
pub struct Signals {
    state: Mutex<SignalState>,
}

impl Signals {
    fn xorb_upload_started(&self, hash: String, size: u64, now: Instant) {
        self.state
            .lock()
            .unwrap()
            .xorb_sizes
            .insert(hash, (size, now));
    }

    fn commit_started(&self, call_id: u64, hash: &str, now: Instant) {
        let mut s = self.state.lock().unwrap();
        let size = s.xorb_sizes.remove(hash).map(|(size, _)| size);
        s.commits.insert(call_id, (now, size));
    }

    fn commit_ended(&self, call_id: u64, committed: bool, now: Instant) {
        let mut s = self.state.lock().unwrap();
        let Some((started, size)) = s.commits.remove(&call_id) else {
            return;
        };
        // Only a real commit of a xorb big enough to time: a grant that was
        // not found did no validation work.
        if let (true, Some(size)) = (committed, size) {
            if size >= MIN_TIMED_XORB_BYTES {
                let secs = now.duration_since(started).as_secs_f64();
                s.samples.push(secs / (size as f64 / MIB));
            }
        }
    }

    fn congestion(&self, c: Congestion) {
        let mut s = self.state.lock().unwrap();
        *s.errors.entry(c).or_default() += 1;
        if s.congestion.is_none_or(|old| c.rank() > old.rank()) {
            s.congestion = Some(c);
        }
    }

    /// What happened since the last call, and forget uploads or commits that
    /// never ended (a commit that failed for good has no end event).
    pub fn take(&self, now: Instant) -> (Option<Congestion>, Vec<f64>) {
        let mut s = self.state.lock().unwrap();
        s.xorb_sizes
            .retain(|_, (_, at)| now.duration_since(*at) < FORGET_AFTER);
        s.commits
            .retain(|_, (at, _)| now.duration_since(*at) < FORGET_AFTER);
        (s.congestion.take(), std::mem::take(&mut s.samples))
    }

    /// Congestion errors since the start, by cause.
    pub fn errors(&self) -> HashMap<Congestion, u64> {
        self.state.lock().unwrap().errors.clone()
    }
}

const XET_REMOTE_CLIENT: &str = "xet_client::cas_client::remote_client";
const XET_RETRY_WRAPPER: &str = "xet_client::cas_client::retry_wrapper";

/// A tracing layer that turns xet-core's events into [`Signals`].
pub struct SignalLayer {
    signals: Arc<Signals>,
}

impl SignalLayer {
    pub fn new(signals: Arc<Signals>) -> Self {
        Self { signals }
    }

    /// The events this layer needs, whatever the log filter shows: the
    /// commit events are at DEBUG, the retry lines at INFO.
    pub fn filter() -> Targets {
        Targets::new()
            .with_target(XET_REMOTE_CLIENT, tracing::Level::DEBUG)
            .with_target(XET_RETRY_WRAPPER, tracing::Level::INFO)
    }
}

/// The fields of an xet-core event that the layer reads.
#[derive(Default)]
struct Fields {
    message: String,
    call_id: Option<u64>,
    hash: Option<String>,
    size: Option<u64>,
    result: Option<String>,
}

impl Visit for Fields {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "call_id" => self.call_id = Some(value),
            "size" => self.size = Some(value),
            _ => {}
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if let Ok(v) = u64::try_from(value) {
            self.record_u64(field, v);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_debug(field, &format_args!("{value}"));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "message" => self.message = format!("{value:?}"),
            "hash" => self.hash = Some(format!("{value:?}")),
            "result" => self.result = Some(format!("{value:?}")),
            _ => {}
        }
    }
}

impl<S: Subscriber> Layer<S> for SignalLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let target = event.metadata().target();
        if target != XET_REMOTE_CLIENT && target != XET_RETRY_WRAPPER {
            return;
        }
        let mut f = Fields::default();
        event.record(&mut f);
        let now = Instant::now();
        if target == XET_RETRY_WRAPPER {
            if let Some(c) = classify_retry(&f.message) {
                self.signals.congestion(c);
            }
            return;
        }
        match f.message.as_str() {
            "Starting upload_xorb API call" => {
                if let (Some(hash), Some(size)) = (f.hash, f.size) {
                    self.signals.xorb_upload_started(hash, size, now);
                }
            }
            "Starting commit_xorb_upload API call" => {
                if let (Some(id), Some(hash)) = (f.call_id, f.hash) {
                    self.signals.commit_started(id, &hash, now);
                }
            }
            "Completed commit_xorb_upload API call" => {
                if let Some(id) = f.call_id {
                    let committed = f.result.as_deref() == Some("Committed");
                    self.signals.commit_ended(id, committed, now);
                }
            }
            m if m.starts_with("Upload grant URL unreachable") => {
                self.signals.congestion(Congestion::Connection);
            }
            _ => {}
        }
    }
}

/// A retry line of xet-core's retry wrapper, as a congestion error. The two
/// forms (`process_ok_response` and `process_error_response`):
///
/// `Retryable Error: "cas::commit_xorb_upload" api call failed (request id X, retry 1): HTTP status server error (503 Service Unavailable) for url (...)`
/// `Retryable Client Error: cas::upload_shard api call failed (retry 2): error sending request for url (...)`
///
/// Only CAS calls count (`cas::…`, including the PUT to the S3 grant URL).
/// Other failures (403, 404, 409…) are not retried as "Retryable" lines.
fn classify_retry(msg: &str) -> Option<Congestion> {
    let (context, rest) = msg.split_once(": ")?;
    if context != "Retryable Error" && context != "Retryable Client Error" {
        return None;
    }
    let (api, err) = rest.split_once(" api call failed")?;
    if !api.trim_matches('"').starts_with("cas::") {
        return None;
    }
    match http_status(err) {
        Some(429) => Some(Congestion::Throttled),
        Some(408) => Some(Congestion::Timeout),
        Some(s) if (500..600).contains(&s) => Some(Congestion::ServerError),
        Some(_) => None,
        None if err.contains("timed out") || err.contains("timeout") => Some(Congestion::Timeout),
        None => Some(Congestion::Connection),
    }
}

/// The status in reqwest's `HTTP status … error (503 Service Unavailable)`.
fn http_status(err: &str) -> Option<u16> {
    let at = err.find(" error (")? + " error (".len();
    err.get(at..at + 3)?.parse().ok()
}

/// What the brake shows in the copier's `PROGRESS` line.
#[derive(Debug, Clone, Serialize)]
pub struct Status {
    pub mode: Mode,
    /// The rate the brake allows (in shadow mode: would allow), MiB/s.
    pub rate_mibps: f64,
    pub action: Action,
    /// Median commit time over its base, at the last tick that had commits.
    pub ratio: Option<f64>,
    /// Cuts since the start.
    pub cuts: u64,
    /// Seconds since the last cut.
    pub cut_age_s: Option<u64>,
    /// Congestion errors seen since the start, by cause.
    pub errors: HashMap<Congestion, u64>,
}

/// The brake of one copier: the signals, the limiter, and the task that
/// ties them.
pub struct Brake {
    mode: Mode,
    params: Params,
    pub signals: Arc<Signals>,
    pub limiter: Arc<Limiter>,
    status: Mutex<Option<(Decision, u64, Option<Instant>)>>,
}

impl Brake {
    pub fn new(mode: Mode, params: Params) -> Arc<Self> {
        Arc::new(Self {
            mode,
            params,
            signals: Arc::new(Signals::default()),
            limiter: Arc::new(Limiter::new(mode == Mode::On)),
            status: Mutex::new(None),
        })
    }

    /// The layer to add to the tracing subscriber (None when off).
    pub fn layer<S>(&self) -> Option<impl Layer<S>>
    where
        S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        (self.mode != Mode::Off)
            .then(|| SignalLayer::new(self.signals.clone()).with_filter(SignalLayer::filter()))
    }

    /// Start the controller (a copy that moves data; not for a dry run).
    pub fn spawn(self: &Arc<Self>) -> Option<tokio::task::JoinHandle<()>> {
        if self.mode == Mode::Off {
            return None;
        }
        self.limiter.set_rate(START_RATE);
        let brake = self.clone();
        info!(
            mode = ?self.mode,
            fast_until_mibps = format!("{:.0}", self.params.fast_until / MIB),
            step_mibps = format!("{:.1}", self.params.step / MIB),
            "brake: adaptive rate toward CAS"
        );
        Some(tokio::spawn(async move { brake.run().await }))
    }

    async fn run(self: Arc<Self>) {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(1)
            ^ u64::from(std::process::id());
        let mut ctl = Controller::new(self.params, seed);
        let mut tick = tokio::time::interval(TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        let mut passed = self.limiter.passed();
        let mut cuts = 0u64;
        let mut last_cut = None;
        loop {
            tick.tick().await;
            let now = Instant::now();
            let (congestion, samples) = self.signals.take(now);
            let total = self.limiter.passed();
            let obs = Observation {
                congestion,
                commit_s_per_mib: samples,
                bytes: total.saturating_sub(passed),
            };
            passed = total;
            let d = ctl.tick(now, &obs);
            self.limiter.set_rate(d.rate);
            if let Action::Cut(cause) = d.action {
                cuts += 1;
                last_cut = Some(now);
                info!(
                    mode = ?self.mode,
                    ?cause,
                    rate_mibps = format!("{:.0}", d.rate / MIB),
                    ratio = d.ratio.map(|r| format!("{r:.2}")),
                    "brake: rate cut"
                );
            }
            *self.status.lock().unwrap() = Some((d, cuts, last_cut));
        }
    }

    /// For the `PROGRESS` line; None before the first tick.
    pub fn status(&self) -> Option<Status> {
        let (d, cuts, last_cut) = (*self.status.lock().unwrap())?;
        Some(Status {
            mode: self.mode,
            rate_mibps: (d.rate / MIB * 10.0).round() / 10.0,
            action: d.action,
            ratio: d.ratio.map(|r| (r * 100.0).round() / 100.0),
            cuts,
            cut_age_s: last_cut.map(|t| t.elapsed().as_secs()),
            errors: self.signals.errors(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: Duration = TICK;

    fn params() -> Params {
        // 8 GiB/s and +3 GiB/s/min over 8 copiers: 1 GiB/s fast, +12.8 MiB/s
        // per tick.
        Params::for_share(8.0, 3.0, 8)
    }

    /// A copier that uses all of its rate.
    fn busy(c: &Controller) -> Observation {
        Observation {
            bytes: (c.rate() * S.as_secs_f64()) as u64,
            ..Observation::default()
        }
    }

    fn commits(c: &Controller, s_per_mib: f64) -> Observation {
        Observation {
            commit_s_per_mib: vec![s_per_mib; 10],
            ..busy(c)
        }
    }

    #[test]
    fn shares_split_the_fleet_figures() {
        let p = params();
        assert_eq!(p.fast_until, GIB);
        assert!((p.step - 3.0 * GIB / 8.0 / 30.0).abs() < 1.0);
        // A share of 0 is one copier.
        assert_eq!(Params::for_share(8.0, 3.0, 0).fast_until, 8.0 * GIB);
    }

    #[test]
    fn a_clean_start_doubles_up_to_the_safe_level_then_probes() {
        let t0 = Instant::now();
        let mut c = Controller::new(params(), 7);
        let mut rates = vec![];
        for i in 1..=6 {
            let obs = busy(&c);
            let d = c.tick(t0 + S * i, &obs);
            rates.push((d.action, (d.rate / MIB).round()));
        }
        assert_eq!(
            rates,
            vec![
                (Action::Fast, 128.0),
                (Action::Fast, 256.0),
                (Action::Fast, 512.0),
                (Action::Fast, 1024.0),
                (Action::Probe, 1037.0),
                (Action::Probe, 1050.0),
            ]
        );
    }

    #[test]
    fn an_error_halves_the_rate_holds_then_climbs_back_fast_to_70_percent() {
        let t0 = Instant::now();
        let mut c = Controller::new(params(), 7);
        for i in 1..=4 {
            let obs = busy(&c);
            c.tick(t0 + S * i, &obs);
        }
        assert_eq!(c.rate(), GIB);
        let mut obs = busy(&c);
        obs.congestion = Some(Congestion::Throttled);
        let d = c.tick(t0 + S * 5, &obs);
        assert_eq!(d.action, Action::Cut(Cause::Error(Congestion::Throttled)));
        assert_eq!(d.rate, 0.5 * GIB);
        // Held for 30 s ± 20 %: no growth for at least 24 s.
        let mut t = t0 + S * 5;
        let mut held = 0;
        loop {
            t += S;
            let obs = busy(&c);
            let d = c.tick(t, &obs);
            if d.action != Action::Hold {
                // Out of the hold: a fast step, capped at 70 % of 1 GiB/s.
                assert_eq!(d.action, Action::Fast);
                assert_eq!(d.rate, 0.7 * GIB);
                break;
            }
            held += 1;
        }
        let held_s = held as u64 * S.as_secs();
        assert!((24..=36).contains(&held_s), "held {held_s} s");
        let obs = busy(&c);
        assert_eq!(c.tick(t + S, &obs).action, Action::Probe);
    }

    #[test]
    fn a_burst_of_errors_is_one_cut_and_errors_keep_the_rate_held() {
        let t0 = Instant::now();
        let mut c = Controller::new(params(), 7);
        for i in 1..=4 {
            let obs = busy(&c);
            c.tick(t0 + S * i, &obs);
        }
        let mut cuts = 0;
        for i in 5..=6 {
            let mut obs = busy(&c);
            obs.congestion = Some(Congestion::ServerError);
            if let Action::Cut(_) = c.tick(t0 + S * i, &obs).action {
                cuts += 1;
            }
        }
        // Two ticks 2 s apart: one cut (5 s minimum between cuts).
        assert_eq!(cuts, 1);
        assert_eq!(c.rate(), 0.5 * GIB);
        // Errors that go on: one more cut every 5 s at most, down to the floor.
        for i in 7..=60 {
            let mut obs = busy(&c);
            obs.congestion = Some(Congestion::ServerError);
            c.tick(t0 + S * i, &obs);
        }
        assert_eq!(c.rate(), FLOOR_RATE);
    }

    #[test]
    fn slow_commits_cut_gently_and_a_bit_slow_holds() {
        let t0 = Instant::now();
        let mut c = Controller::new(params(), 7);
        for i in 1..=4 {
            let obs = commits(&c, 0.01);
            c.tick(t0 + S * i, &obs);
        }
        assert_eq!(c.rate(), GIB);
        // 1.5x the base: no cut, no growth.
        let obs = commits(&c, 0.015);
        let d = c.tick(t0 + S * 5, &obs);
        assert_eq!(d.action, Action::Hold);
        assert_eq!(d.ratio, Some(1.5));
        assert_eq!(d.rate, GIB);
        // 3x the base: -20 %.
        let obs = commits(&c, 0.03);
        let d = c.tick(t0 + S * 6, &obs);
        assert_eq!(d.action, Action::Cut(Cause::SlowCommits));
        assert_eq!(d.rate, 0.8 * GIB);
        // Still slow 2 s later: held (5 s between cuts), then cut again.
        let obs = commits(&c, 0.03);
        assert_eq!(c.tick(t0 + S * 7, &obs).action, Action::Hold);
        let obs = commits(&c, 0.03);
        assert_eq!(c.tick(t0 + S * 8, &obs).action, Action::Hold);
        let obs = commits(&c, 0.03);
        assert_eq!(
            c.tick(t0 + S * 9, &obs).action,
            Action::Cut(Cause::SlowCommits)
        );
        assert!((c.rate() - 0.64 * GIB).abs() < 1.0);
    }

    #[test]
    fn too_few_commits_give_no_latency_signal() {
        let t0 = Instant::now();
        let mut c = Controller::new(params(), 7);
        let mut obs = busy(&c);
        obs.commit_s_per_mib = vec![0.01, 0.01];
        let d = c.tick(t0 + S, &obs);
        assert_eq!(d.ratio, None);
        assert_eq!(d.action, Action::Fast);
    }

    #[test]
    fn the_base_forgets_after_ten_minutes() {
        let t0 = Instant::now();
        let mut c = Controller::new(params(), 7);
        let obs = commits(&c, 0.01);
        c.tick(t0 + S, &obs);
        // 11 minutes later, commits are 1.5x slower for good: after the old
        // base expires, they are the new base.
        let t1 = t0 + Duration::from_secs(11 * 60);
        let obs = commits(&c, 0.015);
        assert_eq!(c.tick(t1, &obs).ratio, Some(1.0));
    }

    #[test]
    fn a_copier_that_does_not_use_its_rate_does_not_grow_it() {
        let t0 = Instant::now();
        let mut c = Controller::new(params(), 7);
        // The source gives 10 MiB/s: far below the 64 MiB/s start.
        for i in 1..=10 {
            let obs = Observation {
                bytes: (10.0 * MIB * S.as_secs_f64()) as u64,
                ..Observation::default()
            };
            assert_eq!(c.tick(t0 + S * i, &obs).action, Action::Free);
        }
        assert_eq!(c.rate(), START_RATE);
    }

    #[test]
    fn the_rate_never_goes_below_the_floor() {
        let t0 = Instant::now();
        let mut c = Controller::new(params(), 7);
        for i in 1..=100 {
            let mut obs = busy(&c);
            obs.congestion = Some(Congestion::Connection);
            c.tick(t0 + Duration::from_secs(6 * i), &obs);
        }
        assert_eq!(c.rate(), FLOOR_RATE);
    }

    #[test]
    fn holds_are_spread_between_copiers() {
        let holds: Vec<Duration> = (1..=20)
            .map(|seed| Controller::new(params(), seed).hold_for(HARD_HOLD))
            .collect();
        assert!(holds
            .iter()
            .all(|h| (24.0..=36.0).contains(&h.as_secs_f64())));
        let distinct: std::collections::HashSet<u128> =
            holds.iter().map(|h| h.as_millis()).collect();
        assert!(distinct.len() > 10);
    }

    #[test]
    fn the_limiter_keeps_the_average_rate_with_parts_bigger_than_its_bucket() {
        let l = Limiter::new(true);
        l.set_rate(10.0 * MIB);
        let t0 = Instant::now();
        // A 16 MiB part at 10 MiB/s: in debt, waits 1.6 s (minus nothing:
        // the bucket starts empty).
        let w = l.reserve(16 * 1024 * 1024, t0);
        assert!((w.as_secs_f64() - 1.6).abs() < 1e-6, "{w:?}");
        // The next one, asked at once, waits for both.
        let w = l.reserve(16 * 1024 * 1024, t0);
        assert!((w.as_secs_f64() - 3.2).abs() < 1e-6, "{w:?}");
        // Much later, the debt is paid and the bucket holds 0.5 s of rate.
        let w = l.reserve(5 * 1024 * 1024, t0 + Duration::from_secs(60));
        assert_eq!(w, Duration::ZERO);
        assert_eq!(l.passed(), 37 * 1024 * 1024);
    }

    #[test]
    fn a_shadow_limiter_counts_but_never_waits() {
        let l = Limiter::new(false);
        l.set_rate(1.0);
        assert_eq!(l.reserve(1 << 30, Instant::now()), Duration::ZERO);
        assert_eq!(l.passed(), 1 << 30);
    }

    #[test]
    fn retry_lines_are_classified() {
        let cases = [
            (
                r#"Retryable Error: "cas::commit_xorb_upload" api call failed (request id abc, retry 1): HTTP status server error (503 Service Unavailable) for url (https://cas/v1/xorb-commits/x)"#,
                Some(Congestion::ServerError),
            ),
            (
                r#"Retryable Error: "cas::request_xorb_upload_grant" api call failed (request id abc): HTTP status client error (429 Too Many Requests) for url (https://cas/v1/xorb-grants/x)"#,
                Some(Congestion::Throttled),
            ),
            (
                r#"Retryable Error: "cas::upload_shard_v2" api call failed (request id abc): HTTP status client error (408 Request Timeout) for url (https://cas/v2/shards)"#,
                Some(Congestion::Timeout),
            ),
            (
                "Retryable Client Error: cas::upload_xorb_to_grant api call failed (retry 2): error sending request for url (https://s3/x): operation timed out",
                Some(Congestion::Timeout),
            ),
            (
                "Retryable Client Error: cas::query_dedup api call failed: error sending request for url (https://cas/v1/chunks/x): connection closed before message completed",
                Some(Congestion::Connection),
            ),
            // Not CAS: a download range from S3.
            (
                r#"Retryable Error: "s3::get_range" api call failed (request id abc): HTTP status server error (503 Service Unavailable) for url (https://s3/x)"#,
                None,
            ),
            // Not a retry: a fatal 403, an expected 404.
            (
                r#"Fatal Error: "cas::commit_xorb_upload" api call failed (request id abc): HTTP status client error (403 Forbidden) for url (https://cas/x)"#,
                None,
            ),
            (
                r#"Not Found (cache miss): "cas::query_dedup" api call failed (request id abc): HTTP status client error (404 Not Found) for url (https://cas/x)"#,
                None,
            ),
            ("Request Success: cas::upload_shard api call succeeded (request id abc).", None),
        ];
        for (line, want) in cases {
            assert_eq!(classify_retry(line), want, "{line}");
        }
    }

    /// The events of an xet commit, emitted the way xet-core emits them
    /// (target, level, fields), go through a real subscriber to the layer.
    #[test]
    fn the_layer_times_commits_from_xet_events() {
        use tracing_subscriber::prelude::*;
        // xet-core logs its `XorbCommitResult` with `?`: an enum, no quotes.
        #[derive(Debug)]
        enum XorbCommitResult {
            Committed,
        }
        let signals = Arc::new(Signals::default());
        let subscriber = tracing_subscriber::registry()
            .with(SignalLayer::new(signals.clone()).with_filter(SignalLayer::filter()));
        tracing::subscriber::with_default(subscriber, || {
            let hash = "a1b2";
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 1u64, prefix = "default", hash = %hash, size = 64u64 * 1024 * 1024,
                num_chunks = 1000u64, "Starting upload_xorb API call");
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 2u64, prefix = "default", hash = %hash, grant_id = "g",
                "Starting commit_xorb_upload API call");
            std::thread::sleep(Duration::from_millis(20));
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 2u64, prefix = "default", hash = %hash, grant_id = "g",
                result = ?XorbCommitResult::Committed, "Completed commit_xorb_upload API call");
            // A small xorb is not timed.
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 3u64, prefix = "default", hash = %"c3", size = 1024u64,
                num_chunks = 1u64, "Starting upload_xorb API call");
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 4u64, prefix = "default", hash = %"c3", grant_id = "g",
                "Starting commit_xorb_upload API call");
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 4u64, prefix = "default", hash = %"c3", grant_id = "g",
                result = ?XorbCommitResult::Committed, "Completed commit_xorb_upload API call");
        });
        let (congestion, samples) = signals.take(Instant::now());
        assert_eq!(congestion, None);
        assert_eq!(samples.len(), 1);
        // ≥ 20 ms for 64 MiB.
        assert!(samples[0] >= 0.02 / 64.0, "{samples:?}");
    }

    /// The contract with xet-core: its real retry wrapper, on a 503 then a
    /// 429 from a local server, emits retry lines the layer reads as
    /// congestion. A new wording in xet-core fails this test.
    #[tokio::test]
    async fn xet_retry_wrapper_lines_reach_the_layer() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tracing_subscriber::prelude::*;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for status in ["503 Service Unavailable", "429 Too Many Requests", "200 OK"] {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut buf = vec![0u8; 4096];
                let _ = sock.read(&mut buf).await.unwrap();
                let reply =
                    format!("HTTP/1.1 {status}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
                sock.write_all(reply.as_bytes()).await.unwrap();
            }
        });

        let signals = Arc::new(Signals::default());
        let subscriber = tracing_subscriber::registry()
            .with(SignalLayer::new(signals.clone()).with_filter(SignalLayer::filter()));
        let _guard = tracing::subscriber::set_default(subscriber);

        let ctx = xet_runtime::core::XetContext::from_external(
            tokio::runtime::Handle::current(),
            xet_runtime::config::XetConfig::new(),
        );
        let client = xet_client::cas_client::build_http_client(&ctx, "test", None, None).unwrap();
        let url = format!("http://{addr}/v1/xorb-commits/default/h/grant/g");
        let resp = xet_client::cas_client::retry_wrapper::RetryWrapper::new(
            ctx,
            "cas::commit_xorb_upload",
        )
        .with_base_delay(Duration::from_millis(10))
        .run(move || client.post(url.clone()).send())
        .await;
        assert!(resp.is_ok());
        let (congestion, _) = signals.take(Instant::now());
        assert_eq!(congestion, Some(Congestion::ServerError));
        let errors = signals.errors();
        assert_eq!(errors.get(&Congestion::ServerError), Some(&1));
        assert_eq!(errors.get(&Congestion::Throttled), Some(&1));
    }
}
