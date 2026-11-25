//! Adaptive backpressure: each copier adapts the bytes per second it feeds to
//! the xet pipeline to the health of CAS.
//!
//! Why: CAS is a shared service, and it checks every xorb and every shard we
//! send. A fleet of copiers can push faster than it can grow. The brake of a
//! copier reacts to the errors that say so (429, 5xx, timeouts, dropped
//! connections). It also times each upload call ([`Op`]: the upload grant,
//! the xorb commit, the shard, the PUT to the storage) and reports the
//! latencies ([`Latency`]) in its `PROGRESS` line: the planner sizes the fleet
//! toward the most throughput per latency (`growth.rs`). One copier lives a
//! few minutes, too short to tell a change from noise; the fleet sees all of
//! them.
//!
//! The parts:
//! - [`SignalLayer`] reads xet-core's own tracing events: the start and end
//!   of each upload call (to time it) and the retry lines of its retry wrapper
//!   (to see congestion errors). xet-core keeps both inside, so its events are
//!   the only window we have. They are matched by target and message: xet-core
//!   is pinned by tag, and [`tests`] run its real retry wrapper to catch a
//!   change of wording.
//! - [`Meter`]: the recent 90th percentile time of each call. Not the median:
//!   when a service queues, the slow tail grows first. On a real run, our
//!   median commit time stayed flat up to a wave of 500s.
//! - [`Controller`]: TCP's slow start, then CUBIC's gentle cut. The rate
//!   doubles every tick while the copier uses it, with no ceiling until the
//!   first error: a copier reaches its full speed in ~10 s. A congestion error
//!   cuts the rate actually used to 70 % and holds it 10 s; it climbs back
//!   fast to 90 % of it, then by small steps. (Halving and holding 30 s, as
//!   Reno does, took a fleet of 64 from ~70 to ~5 GiB/s on a real run, and it
//!   came back slowly: every copier cut at once.)
//! - [`Limiter`]: a token bucket in front of `add_data`, set by the controller.
//!
//! The fleet needs no coordination: all copiers see the same CAS, so they
//! brake together. How much the fleet pushes in all is the planner's job: it
//! sets the number of copiers (`growth.rs`). (An earlier fleet budget, a safe
//! level split between the copiers, capped each copier at 16 GiB/s ÷ the
//! fleet size: at 61 copiers, 269 MiB/s for a copier that does ~1.1 GiB/s,
//! and the fleet stuck near 20 GiB/s however many copiers ran.)

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

/// How often the controller decides.
pub const TICK: Duration = Duration::from_secs(2);
/// First rate of a copier. It doubles every tick while used: 1 GiB/s in 8 s.
const START_RATE: f64 = 64.0 * MIB;
/// The rate never goes below this: a copier always keeps moving, so it keeps
/// measuring.
const FLOOR_RATE: f64 = 16.0 * MIB;
/// A congestion error: the rate actually used is cut to 70 % (CUBIC's β), and
/// the fast climb back stops at 90 % of it.
const HARD_CUT: f64 = 0.7;
const HARD_THRESHOLD: f64 = 0.9;
const HARD_HOLD: Duration = Duration::from_secs(10);
/// Holds are spread by ±20 % so the copiers that braked together do not all
/// climb back at the same second.
const HOLD_JITTER: f64 = 0.2;
/// At most one cut in this time: a burst of errors is one congestion event
/// (as TCP cuts once per window). The bytes a copier fed before a cut keep
/// reaching CAS for some seconds (xorbs being formed, uploaded, committed):
/// the errors of that time come from the load before the cut, and a second
/// cut for them would brake twice for one event (seen on the bench).
const MIN_CUT_GAP: Duration = Duration::from_secs(15);
/// The time of a call is the median of the calls of its recent window
/// ([`Op::window`]), at most the last [`RECENT_MAX`] of them: one tick alone
/// has too few.
const RECENT_MAX: usize = 32;
/// The percentile of the recent call times the copier reports.
const TAIL: f64 = 0.9;
/// Fewer calls than this in the window: no latency signal for that call.
const MIN_SAMPLES: usize = 3;
/// Smaller xorbs are left out of the timing: their fixed costs dominate.
const MIN_TIMED_XORB_BYTES: u64 = 8 * 1024 * 1024;
/// The same for shards (a shard lists the chunks of ~1000 files: a few MiB).
const MIN_TIMED_SHARD_BYTES: u64 = 256 * 1024;
/// The rate only grows while the copier uses most of it (RFC 7661): a copier
/// held back by its source must not build a rate it never used, which a cut
/// would then not bring down.
const BUSY_SHARE: f64 = 0.7;
/// Entries of an upload or a commit that never ended are dropped after this.
const FORGET_AFTER: Duration = Duration::from_secs(10 * 60);
/// Past the fast climb after a cut, the rate grows by this share of the rate
/// that broke, per tick: from 70 % back to 100 % in ~20 s, then on.
const PROBE_SHARE: f64 = 1.0 / 32.0;
/// The rate used, smoothed over ~5 ticks: a cut takes 70 % of what the copier really
/// pushed, not a rate far above it that it never used (then the cut would not
/// bite), nor one stalled tick (then it would cut to the floor).
const USED_SMOOTH: f64 = 0.2;
/// Cuts closer than this to each other are one crisis: the rate before its
/// first cut is the one to climb back to.
const CRISIS: Duration = Duration::from_secs(60);

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

/// An error that says CAS (or the storage behind it) is overloaded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Congestion {
    /// 429: CAS asks us to slow down.
    Throttled,
    /// 5xx: CAS, or the storage it hands an upload to, failed.
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

/// An upload call a copier times.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Op {
    /// Ask CAS where to upload a xorb (time per call).
    Grant,
    /// Ask CAS to check and keep a xorb uploaded to its grant (time per MiB
    /// of xorb).
    Commit,
    /// Send a shard to CAS, which checks it and registers its files (time
    /// per MiB of shard).
    Shard,
    /// Upload a xorb to its grant URL: the storage behind CAS, not CAS
    /// itself (time per MiB). Timed to tell where a slow upload comes from,
    /// not a sign of CAS health.
    Put,
}

impl Op {
    pub const ALL: [Op; 4] = [Op::Grant, Op::Commit, Op::Shard, Op::Put];

    pub fn index(self) -> usize {
        self as usize
    }

    /// Smallest payload timed (per MiB), or None: timed per call.
    fn min_bytes(self) -> Option<u64> {
        match self {
            Op::Grant => None,
            Op::Commit | Op::Put => Some(MIN_TIMED_XORB_BYTES),
            Op::Shard => Some(MIN_TIMED_SHARD_BYTES),
        }
    }

    /// The recent window of the median: a copier makes several xorb calls
    /// per second, but sends a shard only when a session ends (every 10 to
    /// 30 s).
    fn window(self) -> Duration {
        match self {
            Op::Shard => Duration::from_secs(60),
            _ => Duration::from_secs(10),
        }
    }
}

/// A latency per call, in ms (ms per MiB for the calls timed per MiB). None:
/// too few calls lately.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, serde::Deserialize)]
pub struct Latency {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_ms_per_mib: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_ms_per_mib: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub put_ms_per_mib: Option<f64>,
}

impl Latency {
    pub fn get(&self, op: Op) -> Option<f64> {
        match op {
            Op::Grant => self.grant_ms,
            Op::Commit => self.commit_ms_per_mib,
            Op::Shard => self.shard_ms_per_mib,
            Op::Put => self.put_ms_per_mib,
        }
    }

    pub fn set(&mut self, op: Op, v: Option<f64>) {
        let slot = match op {
            Op::Grant => &mut self.grant_ms,
            Op::Commit => &mut self.commit_ms_per_mib,
            Op::Shard => &mut self.shard_ms_per_mib,
            Op::Put => &mut self.put_ms_per_mib,
        };
        *slot = v;
    }

    /// Rounded for a log line: 3 significant digits.
    pub fn rounded(&self) -> Self {
        let mut r = *self;
        for op in Op::ALL {
            r.set(op, self.get(op).map(round3));
        }
        r
    }
}

/// `x` to 3 significant digits.
pub fn round3(x: f64) -> f64 {
    if x == 0.0 || !x.is_finite() {
        return x;
    }
    let scale = 10f64.powi(2 - x.abs().log10().floor() as i32);
    (x * scale).round() / scale
}

/// What the controller did at a tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    /// Doubling: no error yet, or climbing back to 70 % of the rate that
    /// broke.
    Fast,
    /// Growing by the small step, past that.
    Probe,
    /// Keeping the rate: in a hold after a cut, or errors go on.
    Hold,
    /// The copier does not use its rate (its source or its own pipeline is
    /// the limit): the rate stays as it is.
    Free,
    /// The rate was cut, for this error.
    Cut(Congestion),
}

/// What a tick saw.
#[derive(Debug, Default, Clone)]
pub struct Observation {
    /// The worst congestion error since the last tick, if any.
    pub congestion: Option<Congestion>,
    /// Bytes fed to xet since the last tick.
    pub bytes: u64,
}

/// The recent time of each call ([`Op`]): its 90th percentile ([`TAIL`]).
#[derive(Default)]
pub struct Meter {
    /// One per [`Op`], by [`Op::index`]: (when, time).
    recent: [VecDeque<(Instant, f64)>; 4],
}

impl Meter {
    /// Add the call times of a tick (in the units of [`Latency`]). Returns
    /// the [`TAIL`] percentile time of each call over its recent window.
    pub fn tick(&mut self, now: Instant, calls: &[(Op, f64)]) -> Latency {
        for (op, t) in calls {
            self.recent[op.index()].push_back((now, *t));
        }
        let mut latency = Latency::default();
        for op in Op::ALL {
            let r = &mut self.recent[op.index()];
            while r.len() > RECENT_MAX
                || r.front()
                    .is_some_and(|(t, _)| now.duration_since(*t) > op.window())
            {
                r.pop_front();
            }
            if r.len() >= MIN_SAMPLES {
                let times: Vec<f64> = r.iter().map(|(_, t)| *t).collect();
                latency.set(op, Some(percentile(&times, TAIL)));
            }
        }
        latency
    }
}

/// The rate controller of one copier. Pure: time comes in, so tests drive it.
pub struct Controller {
    rate: f64,
    /// Doubling stops here: none before the first error.
    threshold: f64,
    /// The rate used, smoothed ([`USED_SMOOTH`]); None before the first tick.
    used: Option<f64>,
    last_tick: Option<Instant>,
    last_cut: Option<Instant>,
    hold_until: Option<Instant>,
    /// The rate used before the cuts of the last crisis (0 before any cut).
    before_cut: f64,
    rng: u64,
}

/// The result of a tick.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Decision {
    pub action: Action,
    /// The new rate, bytes/s.
    pub rate: f64,
}

impl Controller {
    pub fn new(seed: u64) -> Self {
        Self {
            rate: START_RATE,
            threshold: f64::INFINITY,
            used: None,
            last_tick: None,
            last_cut: None,
            hold_until: None,
            before_cut: 0.0,
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

    fn cut(&mut self, now: Instant) {
        // What the copier really pushed: its rate can be far above that (the
        // rate stops growing, not shrinking, once unused).
        let base = self
            .rate
            .min(self.used.unwrap_or(self.rate))
            .max(FLOOR_RATE);
        let same_crisis = self
            .last_cut
            .is_some_and(|t| now.duration_since(t) < CRISIS);
        self.before_cut = if same_crisis {
            self.before_cut.max(base)
        } else {
            base
        };
        self.threshold = (base * HARD_THRESHOLD).max(FLOOR_RATE);
        self.rate = (base * HARD_CUT).max(FLOOR_RATE);
        self.last_cut = Some(now);
        let hold = self.hold_for(HARD_HOLD);
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
        self.used = Some(self.used.map_or(used, |u| u + USED_SMOOTH * (used - u)));

        let may_cut = self
            .last_cut
            .is_none_or(|t| now.duration_since(t) >= MIN_CUT_GAP);
        let held = self.hold_until.is_some_and(|t| now < t);

        let action = if let (Some(c), true) = (obs.congestion, may_cut) {
            self.cut(now);
            Action::Cut(c)
        } else if held || obs.congestion.is_some() {
            Action::Hold
        } else if used < BUSY_SHARE * self.rate {
            Action::Free
        } else if self.rate < self.threshold {
            self.rate = (self.rate * 2.0).min(self.threshold);
            Action::Fast
        } else {
            self.rate += self.before_cut * PROBE_SHARE;
            Action::Probe
        };
        Decision {
            action,
            rate: self.rate,
        }
    }
}

/// Spreads a seed over all bits (close seeds give unrelated streams), and is
/// never 0, which xorshift needs.
pub fn splitmix64(seed: u64) -> u64 {
    let mut z = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    (z ^ (z >> 31)) | 1
}

/// The `p` quantile of `xs` (nearest rank).
pub fn percentile(xs: &[f64], p: f64) -> f64 {
    let mut v = xs.to_vec();
    v.sort_by(|a, b| a.total_cmp(b));
    let rank = ((v.len() as f64 * p).ceil() as usize).clamp(1, v.len());
    v[rank - 1]
}

pub fn median(xs: &[f64]) -> f64 {
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
    /// Xorb hash → (size, seen at), from the start of its upload: the commit
    /// call does not carry the size.
    xorb_sizes: HashMap<String, (u64, Instant)>,
    /// Call id → (call, started at, payload bytes if known). xet-core numbers
    /// all its calls from one counter.
    calls: HashMap<u64, (Op, Instant, Option<u64>)>,
    samples: Vec<(Op, f64)>,
    congestion: Option<Congestion>,
    /// Congestion errors seen, by cause, since the start.
    errors: HashMap<Congestion, u64>,
    /// When the latest connection errors came (see [`CONNECTION_BURST`]).
    connection_errors: VecDeque<Instant>,
}

/// A dropped connection counts as congestion only when [`CONNECTION_BURST`]
/// of them come within [`CONNECTION_WINDOW`]. One alone is common on a busy
/// pool (S3 closing a reused connection: `IncompleteMessage` on the staging
/// PUT, seen on a real run) and cut the rate for nothing. 429 and 5xx count
/// at once.
const CONNECTION_BURST: usize = 3;
const CONNECTION_WINDOW: Duration = Duration::from_secs(10);

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

    fn call_started(&self, op: Op, call_id: u64, size: Option<u64>, now: Instant) {
        self.state
            .lock()
            .unwrap()
            .calls
            .insert(call_id, (op, now, size));
    }

    fn commit_started(&self, call_id: u64, hash: &str, now: Instant) {
        let mut s = self.state.lock().unwrap();
        let size = s.xorb_sizes.remove(hash).map(|(size, _)| size);
        s.calls.insert(call_id, (Op::Commit, now, size));
    }

    /// `ok`: the call did its full work (a commit that found no grant, or an
    /// upload the storage already had, stored nothing: not timed).
    fn call_ended(&self, call_id: u64, ok: bool, now: Instant) {
        let mut s = self.state.lock().unwrap();
        let Some((op, started, size)) = s.calls.remove(&call_id) else {
            return;
        };
        if !ok {
            return;
        }
        let ms = now.duration_since(started).as_secs_f64() * 1000.0;
        let time = match op.min_bytes() {
            None => Some(ms),
            Some(min) => size.filter(|b| *b >= min).map(|b| ms / (b as f64 / MIB)),
        };
        if let Some(t) = time {
            s.samples.push((op, t));
        }
    }

    fn congestion(&self, c: Congestion, now: Instant) {
        let mut s = self.state.lock().unwrap();
        *s.errors.entry(c).or_default() += 1;
        if c == Congestion::Connection {
            s.connection_errors.push_back(now);
            while s
                .connection_errors
                .front()
                .is_some_and(|t| now.duration_since(*t) > CONNECTION_WINDOW)
            {
                s.connection_errors.pop_front();
            }
            if s.connection_errors.len() < CONNECTION_BURST {
                return;
            }
        }
        if s.congestion.is_none_or(|old| c.rank() > old.rank()) {
            s.congestion = Some(c);
        }
    }

    /// What happened since the last call, and forget calls that never
    /// ended (a call that failed for good has no end event).
    pub fn take(&self, now: Instant) -> (Option<Congestion>, Vec<(Op, f64)>) {
        let mut s = self.state.lock().unwrap();
        s.xorb_sizes
            .retain(|_, (_, at)| now.duration_since(*at) < FORGET_AFTER);
        s.calls
            .retain(|_, (_, at, _)| now.duration_since(*at) < FORGET_AFTER);
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
    /// call events are at DEBUG, the retry lines at INFO.
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
                self.signals.congestion(c, now);
            }
            return;
        }
        let ended = |ok: bool| {
            if let Some(id) = f.call_id {
                self.signals.call_ended(id, ok, now);
            }
        };
        match f.message.as_str() {
            "Starting upload_xorb API call" => {
                if let (Some(hash), Some(size)) = (f.hash.clone(), f.size) {
                    self.signals.xorb_upload_started(hash, size, now);
                }
            }
            "Starting request_xorb_upload_grant API call" => {
                if let Some(id) = f.call_id {
                    self.signals.call_started(Op::Grant, id, None, now);
                }
            }
            // Granted, or the xorb is already there: CAS answered either way.
            "Completed request_xorb_upload_grant API call" => ended(true),
            "Starting upload_xorb_to_grant call" => {
                if let Some(id) = f.call_id {
                    self.signals.call_started(Op::Put, id, f.size, now);
                }
            }
            // "already_uploaded": a retry that found the data there.
            "Completed upload_xorb_to_grant call" => {
                ended(f.result.as_deref() == Some("uploaded"));
            }
            "Starting commit_xorb_upload API call" => {
                if let (Some(id), Some(hash)) = (f.call_id, f.hash.as_deref()) {
                    self.signals.commit_started(id, hash, now);
                }
            }
            // "GrantNotFound": nothing was stored.
            "Completed commit_xorb_upload API call" => {
                ended(f.result.as_deref() == Some("Committed"));
            }
            "Starting upload_shard API call" => {
                if let Some(id) = f.call_id {
                    self.signals.call_started(Op::Shard, id, f.size, now);
                }
            }
            "Completed upload_shard API call" => ended(true),
            m if m.starts_with("Upload grant URL unreachable") => {
                self.signals.congestion(Congestion::Connection, now);
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
    /// The recent time of each call: the planner sizes the fleet from it
    /// (`growth.rs`).
    pub latency: Latency,
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
    pub signals: Arc<Signals>,
    pub limiter: Arc<Limiter>,
    status: Mutex<Option<Last>>,
}

/// The state of the last tick, for [`Brake::status`].
#[derive(Clone, Copy)]
struct Last {
    decision: Decision,
    latency: Latency,
    cuts: u64,
    last_cut: Option<Instant>,
}

impl Brake {
    pub fn new(mode: Mode) -> Arc<Self> {
        Arc::new(Self {
            mode,
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
        info!(mode = ?self.mode, "brake: slow start, cut to 70 % on congestion errors");
        Some(tokio::spawn(async move { brake.run().await }))
    }

    async fn run(self: Arc<Self>) {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(1)
            ^ u64::from(std::process::id());
        let mut ctl = Controller::new(seed);
        let mut meter = Meter::default();
        let mut tick = tokio::time::interval(TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        let mut passed = self.limiter.passed();
        let mut cuts = 0u64;
        let mut last_cut = None;
        loop {
            tick.tick().await;
            let now = Instant::now();
            let (congestion, calls) = self.signals.take(now);
            let total = self.limiter.passed();
            let obs = Observation {
                congestion,
                bytes: total.saturating_sub(passed),
            };
            passed = total;
            let latency = meter.tick(now, &calls);
            let d = ctl.tick(now, &obs);
            self.limiter.set_rate(d.rate);
            if let Action::Cut(cause) = d.action {
                cuts += 1;
                last_cut = Some(now);
                info!(
                    mode = ?self.mode,
                    ?cause,
                    rate_mibps = format!("{:.0}", d.rate / MIB),
                    latency = ?latency.rounded(),
                    "brake: rate cut"
                );
            }
            *self.status.lock().unwrap() = Some(Last {
                decision: d,
                latency,
                cuts,
                last_cut,
            });
        }
    }

    /// For the `PROGRESS` line; None before the first tick.
    pub fn status(&self) -> Option<Status> {
        let last = (*self.status.lock().unwrap())?;
        Some(Status {
            mode: self.mode,
            rate_mibps: (last.decision.rate / MIB * 10.0).round() / 10.0,
            action: last.decision.action,
            latency: last.latency.rounded(),
            cuts: last.cuts,
            cut_age_s: last.last_cut.map(|t| t.elapsed().as_secs()),
            errors: self.signals.errors(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: Duration = TICK;

    fn params_free() -> Controller {
        Controller::new(7)
    }

    /// A copier that uses all of its rate.
    fn busy(c: &Controller) -> Observation {
        Observation {
            bytes: (c.rate() * S.as_secs_f64()) as u64,
            ..Observation::default()
        }
    }

    /// A copier that can do `cap` bytes/s at most.
    fn capped(c: &Controller, cap: f64) -> Observation {
        Observation {
            bytes: (c.rate().min(cap) * S.as_secs_f64()) as u64,
            ..Observation::default()
        }
    }

    const CAP: f64 = 1.1 * 1024.0 * MIB;

    #[test]
    fn slow_start_reaches_full_speed_in_seconds_with_no_ceiling() {
        let t0 = Instant::now();
        let mut c = params_free();
        let mut rates = vec![];
        for i in 1..=8 {
            let obs = capped(&c, CAP);
            let d = c.tick(t0 + S * i, &obs);
            rates.push((d.action, (d.rate / MIB).round()));
        }
        // Doubles while used, past the copier's 1.1 GiB/s; then the copier no
        // longer uses its rate, which stays where it is.
        assert_eq!(
            rates,
            vec![
                (Action::Fast, 128.0),
                (Action::Fast, 256.0),
                (Action::Fast, 512.0),
                (Action::Fast, 1024.0),
                (Action::Fast, 2048.0),
                (Action::Free, 2048.0),
                (Action::Free, 2048.0),
                (Action::Free, 2048.0),
            ]
        );
    }

    #[test]
    fn an_error_cuts_the_rate_used_not_a_rate_never_used() {
        let t0 = Instant::now();
        let mut c = params_free();
        for i in 1..=30 {
            let obs = capped(&c, CAP);
            c.tick(t0 + S * i, &obs);
        }
        assert_eq!(c.rate(), 2048.0 * MIB);
        // Cutting 2 GiB/s to 70 % would leave the copier at its full
        // 1.1 GiB/s: the cut is taken on the ~1.1 GiB/s it really pushed.
        let mut obs = capped(&c, CAP);
        obs.congestion = Some(Congestion::Throttled);
        let d = c.tick(t0 + S * 31, &obs);
        assert_eq!(d.action, Action::Cut(Congestion::Throttled));
        assert!(
            (d.rate - CAP * HARD_CUT).abs() < 0.01 * CAP,
            "{}",
            d.rate / MIB
        );
    }

    #[test]
    fn after_a_cut_it_holds_climbs_back_fast_to_90_percent_then_by_steps() {
        let t0 = Instant::now();
        let mut c = params_free();
        for i in 1..=4 {
            let obs = busy(&c);
            c.tick(t0 + S * i, &obs);
        }
        assert_eq!(c.rate(), 1024.0 * MIB);
        // The rate used is still catching up with the doubling: smoothed, it
        // is below 1 GiB/s.
        let used = c.used.unwrap();
        let mut obs = busy(&c);
        obs.congestion = Some(Congestion::ServerError);
        let d = c.tick(t0 + S * 5, &obs);
        let base = c.before_cut;
        assert!(base <= 1024.0 * MIB && base > used, "{base} {used}");
        assert_eq!(d.rate, base * HARD_CUT);
        // Held for 10 s ± 20 %: no growth for at least 8 s.
        let mut t = t0 + S * 5;
        let mut held = 0;
        loop {
            t += S;
            let obs = busy(&c);
            let d = c.tick(t, &obs);
            if d.action != Action::Hold {
                // Out of the hold: a fast step, capped at 90 % of the base.
                assert_eq!(d.action, Action::Fast);
                assert_eq!(d.rate, base * HARD_THRESHOLD);
                break;
            }
            held += 1;
        }
        let held_s = held as u64 * S.as_secs();
        assert!((8..=12).contains(&held_s), "held {held_s} s");
        // Then +1/32 of the base per tick, past the base too.
        for _ in 0..12 {
            t += S;
            let before = c.rate();
            let obs = busy(&c);
            assert_eq!(c.tick(t, &obs).action, Action::Probe);
            assert!((c.rate() - before - base * PROBE_SHARE).abs() < 1.0);
        }
        assert!(c.rate() > base);
    }

    #[test]
    fn a_burst_of_errors_is_one_cut_and_errors_keep_the_rate_held() {
        let t0 = Instant::now();
        let mut c = params_free();
        for i in 1..=10 {
            let obs = capped(&c, CAP);
            c.tick(t0 + S * i, &obs);
        }
        let mut cuts = 0;
        for i in 11..=12 {
            let mut obs = capped(&c, CAP);
            obs.congestion = Some(Congestion::ServerError);
            if let Action::Cut(_) = c.tick(t0 + S * i, &obs).action {
                cuts += 1;
            }
        }
        // Two ticks 2 s apart: one cut (15 s minimum between cuts).
        assert_eq!(cuts, 1);
        let after = c.rate();
        // Errors that go on: one more cut every 15 s at most, down to the floor.
        for i in 13..=300 {
            let mut obs = capped(&c, CAP);
            obs.congestion = Some(Congestion::ServerError);
            c.tick(t0 + S * i, &obs);
        }
        assert!(after > FLOOR_RATE);
        assert_eq!(c.rate(), FLOOR_RATE);
    }

    #[test]
    fn a_copier_that_does_not_use_its_rate_does_not_grow_it() {
        let t0 = Instant::now();
        let mut c = params_free();
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
        let mut c = params_free();
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
            .map(|seed| Controller::new(seed).hold_for(HARD_HOLD))
            .collect();
        assert!(holds
            .iter()
            .all(|h| (8.0..=12.0).contains(&h.as_secs_f64())));
        let distinct: std::collections::HashSet<u128> =
            holds.iter().map(|h| h.as_millis()).collect();
        assert!(distinct.len() > 10);
    }

    #[test]
    fn the_meter_gives_the_recent_tail_of_each_call() {
        let t0 = Instant::now();
        let mut m = Meter::default();
        // Two commits: too few for a median.
        let l = m.tick(t0, &[(Op::Commit, 10.0), (Op::Commit, 12.0)]);
        assert_eq!(l, Latency::default());
        // A third: the 90th percentile of the three (the slowest). Grants are
        // kept apart.
        let l = m.tick(
            t0 + S,
            &[
                (Op::Commit, 50.0),
                (Op::Grant, 3.0),
                (Op::Grant, 4.0),
                (Op::Grant, 5.0),
            ],
        );
        assert_eq!(l.commit_ms_per_mib, Some(50.0));
        assert_eq!(l.grant_ms, Some(5.0));
        assert_eq!((l.shard_ms_per_mib, l.put_ms_per_mib), (None, None));
        // 11 s after the last ones, the xorb calls are out of their 10 s
        // window...
        let l = m.tick(t0 + S + Duration::from_secs(11), &[]);
        assert_eq!((l.commit_ms_per_mib, l.grant_ms), (None, None));
        // ...but shards, rare, are kept for 60 s.
        m.tick(t0, &[(Op::Shard, 1.0), (Op::Shard, 2.0), (Op::Shard, 3.0)]);
        let l = m.tick(t0 + Duration::from_secs(50), &[]);
        assert_eq!(l.shard_ms_per_mib, Some(3.0));
        let l = m.tick(t0 + Duration::from_secs(61), &[]);
        assert_eq!(l.shard_ms_per_mib, None);
    }

    #[test]
    fn latency_rounds_to_three_digits_and_skips_unknown_calls() {
        assert_eq!(round3(1234.5), 1230.0);
        assert_eq!(round3(0.012345), 0.0123);
        let l = Latency {
            grant_ms: Some(12.345),
            ..Latency::default()
        };
        assert_eq!(
            serde_json::to_string(&l.rounded()).unwrap(),
            r#"{"grant_ms":12.3}"#
        );
        let back: Latency = serde_json::from_str(r#"{"grant_ms":12.3}"#).unwrap();
        assert_eq!(back.grant_ms, Some(12.3));
    }

    #[test]
    fn the_limiter_keeps_the_average_rate_with_parts_bigger_than_its_bucket() {
        let l = Limiter::new(true);
        l.set_rate(10.0 * MIB);
        let t0 = Instant::now();
        // A 16 MiB part at 10 MiB/s: in debt, waits 1.6 s (the bucket starts
        // empty when the limiter is made, a few µs before t0: on a loaded
        // machine those µs of refill showed, hence the 1 ms margin).
        let w = l.reserve(16 * 1024 * 1024, t0);
        assert!((w.as_secs_f64() - 1.6).abs() < 1e-3, "{w:?}");
        // The next one, asked at once, waits for both.
        let w = l.reserve(16 * 1024 * 1024, t0);
        assert!((w.as_secs_f64() - 3.2).abs() < 1e-3, "{w:?}");
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
    fn one_dropped_connection_is_not_congestion_three_are() {
        let s = Signals::default();
        let t0 = Instant::now();
        s.congestion(Congestion::Connection, t0);
        s.congestion(Congestion::Connection, t0 + Duration::from_secs(4));
        assert_eq!(s.take(t0 + Duration::from_secs(5)).0, None);
        // Two more, but the first is now out of the window: 3 in 10 s.
        s.congestion(Congestion::Connection, t0 + Duration::from_secs(12));
        assert_eq!(s.take(t0 + Duration::from_secs(12)).0, None);
        s.congestion(Congestion::Connection, t0 + Duration::from_secs(13));
        assert_eq!(
            s.take(t0 + Duration::from_secs(13)).0,
            Some(Congestion::Connection)
        );
        // All are counted for the PROGRESS line.
        assert_eq!(s.errors().get(&Congestion::Connection), Some(&4));
        // A 5xx counts at once.
        s.congestion(Congestion::ServerError, t0 + Duration::from_secs(60));
        assert_eq!(
            s.take(t0 + Duration::from_secs(60)).0,
            Some(Congestion::ServerError)
        );
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

    /// The events of each upload call, emitted the way xet-core emits them
    /// (target, level, fields), go through a real subscriber to the layer.
    #[test]
    fn the_layer_times_each_call_from_xet_events() {
        use tracing_subscriber::prelude::*;
        // xet-core logs its `XorbCommitResult` with `?`: an enum, no quotes.
        #[derive(Debug)]
        enum XorbCommitResult {
            Committed,
            GrantNotFound,
        }
        const MIB_U64: u64 = 1024 * 1024;
        let signals = Arc::new(Signals::default());
        let subscriber = tracing_subscriber::registry()
            .with(SignalLayer::new(signals.clone()).with_filter(SignalLayer::filter()));
        tracing::subscriber::with_default(subscriber, || {
            let hash = "a1b2";
            let grant_id = String::from("g1");
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 1u64, prefix = "default", hash = %hash, size = 64 * MIB_U64,
                num_chunks = 1000u64, "Starting upload_xorb API call");
            // The grant.
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 2u64, prefix = "default", hash = %hash, length = 64 * MIB_U64,
                "Starting request_xorb_upload_grant API call");
            std::thread::sleep(Duration::from_millis(5));
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 2u64, prefix = "default", hash = %hash, result = "granted",
                "Completed request_xorb_upload_grant API call");
            // The PUT to the grant URL.
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 3u64, grant_id = grant_id, size = 64 * MIB_U64,
                "Starting upload_xorb_to_grant call");
            std::thread::sleep(Duration::from_millis(5));
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 3u64, grant_id = grant_id, result = "uploaded",
                "Completed upload_xorb_to_grant call");
            // The commit.
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 4u64, prefix = "default", hash = %hash, grant_id = "g1",
                "Starting commit_xorb_upload API call");
            std::thread::sleep(Duration::from_millis(20));
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 4u64, prefix = "default", hash = %hash, grant_id = "g1",
                result = ?XorbCommitResult::Committed, "Completed commit_xorb_upload API call");
            // The shard (its size is a usize in xet-core).
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 5u64, size = 2 * MIB_U64 as usize, api_version = "v2",
                "Starting upload_shard API call");
            std::thread::sleep(Duration::from_millis(10));
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 5u64, size = 2 * MIB_U64 as usize, api_version = "v2",
                result = "sync performed", "Completed upload_shard API call");

            // Not timed: a small xorb, a commit that found no grant, a PUT the
            // storage already had, a small shard.
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 6u64, prefix = "default", hash = %"c3", size = 1024u64,
                num_chunks = 1u64, "Starting upload_xorb API call");
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 7u64, prefix = "default", hash = %"c3", grant_id = "g",
                "Starting commit_xorb_upload API call");
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 7u64, prefix = "default", hash = %"c3", grant_id = "g",
                result = ?XorbCommitResult::Committed, "Completed commit_xorb_upload API call");
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 8u64, prefix = "default", hash = %"d4", size = 64 * MIB_U64,
                num_chunks = 1000u64, "Starting upload_xorb API call");
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 9u64, prefix = "default", hash = %"d4", grant_id = "g",
                "Starting commit_xorb_upload API call");
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 9u64, prefix = "default", hash = %"d4", grant_id = "g",
                result = ?XorbCommitResult::GrantNotFound, "Completed commit_xorb_upload API call");
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 10u64, grant_id = grant_id, size = 64 * MIB_U64,
                "Starting upload_xorb_to_grant call");
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 10u64, grant_id = grant_id, result = "already_uploaded",
                "Completed upload_xorb_to_grant call");
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 11u64, size = 1024usize, "Starting upload_shard API call");
            tracing::event!(target: XET_REMOTE_CLIENT, tracing::Level::DEBUG,
                call_id = 11u64, size = 1024usize, result = "exists",
                "Completed upload_shard API call");
        });
        let (congestion, calls) = signals.take(Instant::now());
        assert_eq!(congestion, None);
        let ops: Vec<Op> = calls.iter().map(|(op, _)| *op).collect();
        assert_eq!(
            ops,
            vec![Op::Grant, Op::Put, Op::Commit, Op::Shard],
            "{calls:?}"
        );
        let time = |op: Op| calls.iter().find(|(o, _)| *o == op).unwrap().1;
        // A grant per call; the others per MiB of payload.
        assert!(time(Op::Grant) >= 5.0, "{calls:?}");
        assert!(time(Op::Put) >= 5.0 / 64.0, "{calls:?}");
        assert!(time(Op::Commit) >= 20.0 / 64.0, "{calls:?}");
        assert!(time(Op::Shard) >= 10.0 / 2.0, "{calls:?}");
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
