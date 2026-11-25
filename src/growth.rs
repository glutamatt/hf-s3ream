//! The size of the fleet: AIMD, as TCP, on the power `P = throughput /
//! latency`.
//!
//! `P` is Kleinrock's power: it grows while copiers add throughput faster
//! than they add latency, and is highest at the knee, where CAS starts to
//! queue (the point BBR aims at too). It needs no base level and no
//! threshold: only the shape of the curve.
//!
//! The planner measures its fleet over [`MEASURE_FOR`] (the first measure once
//! the copiers launched are running, [`SETTLE_FOR`] after that; then one
//! measure after the other):
//! - `X`: the bytes fed to xet, per second;
//! - `L`: the latency of CAS, the geometric mean of its calls (grant, commit,
//!   shard: `brake.rs`), each the 90th percentile over the recent calls of a
//!   copier, then the median over the copiers. A geometric mean weighs each
//!   call the same; one call twice slower moves `L` by ×1.26.
//! - `P = X / L`, on each of [`SLICES`] slices of the measure. The spread of
//!   the slices, pooled over the measures of the last [`BEST_FOR`], gives the
//!   noise `σ`. Pooled, not per measure: 4 slices tell their noise badly, and
//!   one agitated measure (copiers ending and starting together) gave σ = 14 %
//!   on a real run, which hid a real loss of 25 % of the power.
//!
//! Each measure is compared with the best one of the last [`BEST_FOR`] (the
//! one with the most power; each measure leaves the window on its own, so the
//! best ages smoothly: when a single best expired at once, the next measure
//! took its place, even one taken past the knee, and the fleet lost its
//! bearings and climbed in the tests):
//!
//! ```text
//! less power than the best (beyond 2σ), with more copiers:  N × 0.7, then one measure without growth
//! less power at the best size (CAS changed):                N × 0.7, the old measures forgotten
//! no clear gain, with 25 % more copiers than the best:      N × 0.7, then one measure without growth
//! otherwise (fewer copiers than the best included):         +4 copiers a minute
//! ```
//!
//! - latency stable, throughput up: `P` up, the fleet keeps growing;
//! - CAS adds capacity: `P` up more, the fleet keeps growing;
//! - latency up faster than throughput: `P` down, the fleet shrinks;
//! - copiers that add nothing (a slow source, our own pipeline): the fleet
//!   grows 25 % past its best size, shrinks, grows again: it saw-tooths
//!   around the best size, as a TCP flow does around the capacity of its
//!   path.
//!
//! **Additive increase** ([`RAMP_COPIERS`] every [`RAMP_EVERY`], ~4 GiB/s more
//! per minute), not doublings: doubling from 32 to 64 copiers added ~35 GiB/s
//! in a minute on a real run, and CAS answered with a wave of 500s; and
//! doubling from 8 was slow at the start (now 16). **Multiplicative decrease**
//! by [`DECREASE`] (CUBIC's β, gentler than Reno's ½), then one measure of the
//! smaller fleet before it grows again. (An earlier version climbed with
//! Rprop's steps, probed every 10 minutes and shrank on any tie: its rules
//! piled up, and the fleet swung from 4 to 41 copiers in the tests.)
//!
//! `N` is the copiers launched (running, or waiting for a machine): a fleet
//! that cannot fill its target (a slow listing, the commit budget) does not
//! grow it further.
//!
//! Errors are the copiers' own business: a copier cuts its rate on a 429 or
//! a 5xx within seconds (`brake.rs`), as each TCP flow reacts to its own
//! losses. The planner does not watch errors: when copiers brake, the fleet
//! throughput drops (and latency often rises), so the power drops, and the
//! next measure shrinks the fleet. The reaction is in proportion: one 500 on
//! one copier of 60 is noise, an overload that brakes many is a clear loss.

use crate::brake::{median, Latency, Op};
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tracing::info;

/// The first size: a fleet CAS took at once on every real run so far.
pub const INITIAL_COPIERS: usize = 16;
/// Additive increase: this many more copiers...
const RAMP_COPIERS: usize = 4;
/// ...this often.
const RAMP_EVERY: Duration = Duration::from_secs(60);
/// Multiplicative decrease.
const DECREASE: f64 = 0.7;
/// No clear gain with this share more copiers than the best size: they did
/// not pay.
const UNPAID: f64 = 0.25;
/// A change of power counts when it is over this many σ.
const SIGNIFICANT: f64 = 2.0;
/// The measures the best is taken from: CAS and its other users change.
const BEST_FOR: Duration = Duration::from_secs(10 * 60);
/// Measure the fleet over this time. Copiers send a shard every 10 to 30 s,
/// and stop and go every ~40 s (they wait for their shards).
const MEASURE_FOR: Duration = Duration::from_secs(120);
/// The measure is cut in this many slices, for its noise.
const SLICES: usize = 4;
/// A slice needs a time of a call in at least this many observations.
const MIN_LATENCY_OBS: usize = 2;
/// The fleet is ready for its first measure once its copiers are launched and
/// running, but for the share of them that waits for a machine in a fleet at
/// its size (a range ends, the next copier starts: ~5 s each)...
const WAITING_SHARE: f64 = 0.1;
/// ...and the measure starts after this much of it: the copiers just started
/// reach their full speed in ~10 s.
const SETTLE_FOR: Duration = Duration::from_secs(30);
/// A fleet that cannot fill its target after this long is measured anyway.
const SETTLE_AT_MOST: Duration = Duration::from_secs(5 * 60);
const MIB: f64 = 1024.0 * 1024.0;

/// What the planner sees of its fleet, from the copiers' logs.
#[derive(Debug, Clone, Copy, Default)]
pub struct View {
    pub running: usize,
    /// Copiers launched and not ended (running, or waiting for a machine).
    pub active: usize,
    /// Bytes fed to xet by the fleet since the start (finished copiers
    /// included).
    pub bytes: u64,
    /// The latency of each call now: the median over the copiers that have
    /// one.
    pub latency: Latency,
    /// Ranges are left to launch (listed and not started yet, or the listing
    /// goes on): a fleet below its target is still filling up.
    pub more_to_launch: bool,
}

/// One slice of a measure.
#[derive(Debug, Clone, Default)]
struct Slice {
    secs: f64,
    moved: u64,
    /// The fleet latency of each call at each observation, by [`Op`].
    latency: [Vec<f64>; 4],
}

/// A measure in progress.
#[derive(Debug, Clone)]
struct Measure {
    since: Instant,
    copiers: f64,
    n: u32,
    last_at: Instant,
    last_bytes: u64,
    slices: [Slice; SLICES],
}

#[derive(Debug, Clone)]
enum State {
    Settle {
        since: Instant,
        /// Since when the fleet is ready (launched, and running but for
        /// [`WAITING_SHARE`]).
        ready_since: Option<Instant>,
    },
    Measure(Box<Measure>),
}

/// A measured fleet: copiers launched (average), and per slice its MiB/s and
/// the latency of each call.
#[derive(Debug, Clone, PartialEq)]
struct Point {
    copiers: f64,
    slices: Vec<(f64, Latency)>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Verdict {
    Better,
    Worse,
    Same,
}

/// How the power changed between two measures.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Change {
    verdict: Verdict,
    /// Δ ln P, and its noise.
    delta: f64,
    sigma: f64,
}

pub struct Growth {
    max: usize,
    target: usize,
    state: State,
    /// A measure is in: the next ones follow each other.
    measured_once: bool,
    /// The measures of the last [`BEST_FOR`], and when they were taken: the
    /// best is the one with the most power.
    recent: VecDeque<(Instant, Point)>,
    /// The spread of ln P over the slices of each recent measure: (when,
    /// variance, degrees of freedom).
    noise: VecDeque<(Instant, f64, f64)>,
    /// When the ramp last added copiers.
    last_ramp: Instant,
    /// The fleet filled its target at some point since the last ramp.
    filled: bool,
    /// After a decrease, no growth until the next measure is in.
    hold: bool,
}

impl Growth {
    pub fn new(max: usize, now: Instant) -> Self {
        let max = max.max(1);
        Self {
            max,
            target: INITIAL_COPIERS.min(max),
            state: State::Settle {
                since: now,
                ready_since: None,
            },
            measured_once: false,
            recent: VecDeque::new(),
            noise: VecDeque::new(),
            last_ramp: now,
            filled: false,
            hold: false,
        }
    }

    /// How many copiers may run now.
    pub fn target(&self) -> usize {
        self.target
    }

    /// Read the fleet (every few seconds).
    pub fn observe(&mut self, now: Instant, v: View) {
        // Additive increase. Only a fleet that filled its target grows it (a
        // slow listing, the commit budget hold it below). Filled at some point
        // of the minute, not at the instant of the ramp: ranges end all the
        // time in a big fleet, and the next copier takes a moment to start.
        // On a real run (44 copiers, ranges of ~80 s), one copier was missing
        // at almost every ramp, and the fleet stayed at 44 for 5 minutes.
        if v.active >= self.target && v.more_to_launch {
            self.filled = true;
        }
        if !self.hold && now.duration_since(self.last_ramp) >= RAMP_EVERY {
            self.last_ramp = now;
            if self.filled && self.target < self.max {
                self.target = (self.target + RAMP_COPIERS).min(self.max);
            }
            self.filled = false;
        }
        // The first measure needs the fleet at work: the copiers launched,
        // and running. Launched is not enough: at the start of a real run,
        // 7 copiers of 8 still waited for a machine, the first measure went
        // from 1 to 10 GiB/s, its noise (σ 52 %) hid a doubling of the power,
        // and the fleet shrank. "No copier waiting" is too much: in a big
        // fleet some copier always is (ranges end, the next copiers start),
        // and at 41 and 61 copiers each measure waited the full
        // [`SETTLE_AT_MOST`]. After the first, the measures follow each
        // other: a growing fleet never sits still.
        let launched = v.active >= self.target || !v.more_to_launch;
        let waiting = v.active.saturating_sub(v.running);
        let ready =
            launched && v.running > 0 && waiting as f64 <= WAITING_SHARE * self.target as f64;
        let state = std::mem::replace(
            &mut self.state,
            State::Settle {
                since: now,
                ready_since: None,
            },
        );
        self.state = match state {
            State::Settle { since, ready_since } => {
                let ready_since = if ready {
                    ready_since.or(Some(now))
                } else {
                    None
                };
                let settled = ready_since.is_some_and(|t| now.duration_since(t) >= SETTLE_FOR)
                    || (self.measured_once && v.running > 0);
                // A fleet with no copier running has nothing to measure.
                let late = now.duration_since(since) >= SETTLE_AT_MOST && v.running > 0;
                if settled || late {
                    let mut m = Measure {
                        since: now,
                        copiers: 0.0,
                        n: 0,
                        last_at: now,
                        last_bytes: v.bytes,
                        slices: Default::default(),
                    };
                    m.add(now, &v);
                    State::Measure(Box::new(m))
                } else {
                    State::Settle { since, ready_since }
                }
            }
            State::Measure(mut m) => {
                m.add(now, &v);
                if now.duration_since(m.since) < MEASURE_FOR {
                    State::Measure(m)
                } else {
                    self.measured(now, m.point());
                    State::Settle {
                        since: now,
                        ready_since: None,
                    }
                }
            }
        };
    }

    fn measured(&mut self, now: Instant, p: Point) {
        self.measured_once = true;
        let n = p.copiers;
        while self
            .recent
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) >= BEST_FOR)
        {
            self.recent.pop_front();
        }
        let noise = self.pooled_noise(now, &p);
        let vs_best = self
            .best(noise)
            .and_then(|b| compare(b, &p, noise).map(|c| (c, b.copiers)));
        // Less power than the best says which side of the top the fleet is
        // on only with its size: more copiers than the best, it went past the
        // knee; fewer, it is below the top and grows. (Read as a loss either
        // way, the smaller fleet after a decrease looked worse than the best,
        // and the fleet shrank again and again, down to 1 copier in the
        // tests.) The same size with less power: CAS changed, not us.
        let decrease = match vs_best {
            Some((c, best_n)) if c.verdict == Verdict::Worse && (n - best_n).abs() < 1.0 => {
                self.recent.clear();
                Some("less power at the best size: CAS changed")
            }
            Some((c, best_n)) if c.verdict == Verdict::Worse && n > best_n => {
                Some("less power with more copiers than the best: past the knee")
            }
            Some((c, best_n)) if c.verdict == Verdict::Same && n >= best_n * (1.0 + UNPAID) => {
                Some("no gain from 25 % more copiers than the best")
            }
            _ => None,
        };
        self.recent.push_back((now, p.clone()));

        let from = self.target;
        let why = match decrease {
            Some(why) => {
                self.target = ((n * DECREASE).round() as usize).clamp(1, self.max);
                self.hold = true;
                why
            }
            None => {
                self.hold = false;
                "grow"
            }
        };
        let shown = vs_best.map(|(c, _)| c);
        info!(
            from,
            to = self.target,
            copiers = format!("{n:.1}"),
            why,
            power_change = shown.map(|c| format!("{:+.1}%", (c.delta.exp() - 1.0) * 100.0)),
            sigma = shown.map(|c| format!("{:.1}%", c.sigma * 100.0)),
            best_copiers = self.best(noise).map(|b| format!("{:.1}", b.copiers)),
            mibps = format!("{:.0}", p.mibps()),
            "growth: measure"
        );
    }

    /// The best recent measure: of those with the most power (not clearly
    /// below the top one), the one with the fewest copiers. Fewest: on a
    /// plateau every measure has the same power, the top one is the luckiest,
    /// at any size, and the fleet climbed the plateau after it in the tests;
    /// and the same power with fewer copiers costs less.
    fn best(&self, noise: Option<f64>) -> Option<&Point> {
        let top = self
            .recent
            .iter()
            .map(|(_, q)| (q, mean_var(&q.ln_power(&q.calls())).0))
            .filter(|(_, m)| m.is_finite())
            .max_by(|a, b| a.1.total_cmp(&b.1))
            .map(|(q, _)| q)?;
        self.recent
            .iter()
            .map(|(_, q)| q)
            .filter(|q| compare(top, q, noise).is_none_or(|c| c.verdict != Verdict::Worse))
            .min_by(|a, b| a.copiers.total_cmp(&b.copiers))
    }

    /// Add the spread of `p` to the recent ones, and pool them: the variance
    /// of ln P within a measure around its trend, weighted by degrees of
    /// freedom. Around its trend: a ramping fleet grows during a measure
    /// (+8 copiers in 2 min), and the spread around the mean counted that
    /// growth as noise; σ swelled, real losses past the knee hid in it, and
    /// the fleet climbed 70 % past its best size in the tests. None before any
    /// measure with three slices.
    fn pooled_noise(&mut self, now: Instant, p: &Point) -> Option<f64> {
        let lp = p.ln_power(&p.calls());
        if let Some((var, df)) = detrended_var(&lp) {
            self.noise.push_back((now, var, df));
        }
        while self
            .noise
            .front()
            .is_some_and(|(at, _, _)| now.duration_since(*at) >= BEST_FOR)
        {
            self.noise.pop_front();
        }
        let df: f64 = self.noise.iter().map(|(_, _, d)| d).sum();
        (df > 0.0).then(|| self.noise.iter().map(|(_, v, d)| v * d).sum::<f64>() / df)
    }
}

impl Measure {
    fn add(&mut self, now: Instant, v: &View) {
        self.copiers += v.active as f64;
        self.n += 1;
        let slice_len = MEASURE_FOR.as_secs_f64() / SLICES as f64;
        let i =
            ((now.duration_since(self.since).as_secs_f64() / slice_len) as usize).min(SLICES - 1);
        let s = &mut self.slices[i];
        s.secs += now.duration_since(self.last_at).as_secs_f64();
        // A re-spawned range starts its count over: only increases count.
        s.moved += v.bytes.saturating_sub(self.last_bytes);
        self.last_at = now;
        self.last_bytes = v.bytes;
        for op in Op::ALL {
            if let Some(t) = v.latency.get(op) {
                s.latency[op.index()].push(t);
            }
        }
    }

    fn point(&self) -> Point {
        let slices = self
            .slices
            .iter()
            .filter(|s| s.secs > 0.0)
            .map(|s| {
                let mut latency = Latency::default();
                for op in Op::ALL {
                    let xs = &s.latency[op.index()];
                    if xs.len() >= MIN_LATENCY_OBS {
                        latency.set(op, Some(median(xs)));
                    }
                }
                (s.moved as f64 / MIB / s.secs, latency)
            })
            .collect();
        Point {
            copiers: self.copiers / f64::from(self.n.max(1)),
            slices,
        }
    }
}

impl Point {
    fn mibps(&self) -> f64 {
        let n = self.slices.len().max(1) as f64;
        self.slices.iter().map(|(x, _)| x).sum::<f64>() / n
    }

    /// ln P of each slice, with `L` the geometric mean of the calls in `ops`.
    fn ln_power(&self, ops: &[Op]) -> Vec<f64> {
        self.slices
            .iter()
            .filter(|(x, _)| *x > 0.0)
            .map(|(x, lat)| {
                let ln_l: f64 = ops
                    .iter()
                    .map(|op| lat.get(*op).unwrap_or(1.0).ln())
                    .sum::<f64>()
                    / ops.len().max(1) as f64;
                x.ln() - ln_l
            })
            .collect()
    }

    /// The CAS calls timed in every slice.
    fn calls(&self) -> Vec<Op> {
        Op::CAS
            .into_iter()
            .filter(|op| {
                self.slices
                    .iter()
                    .all(|(_, lat)| lat.get(*op).is_some_and(|t| t > 0.0))
            })
            .collect()
    }
}

/// How the power changed from `a` to `b`, on the CAS calls both timed, with
/// `noise` the pooled variance of ln P per slice (None: the spread of these
/// two measures). None when a measure has fewer than two slices.
fn compare(a: &Point, b: &Point, noise: Option<f64>) -> Option<Change> {
    let ops: Vec<Op> = a
        .calls()
        .into_iter()
        .filter(|op| b.calls().contains(op))
        .collect();
    let (pa, pb) = (a.ln_power(&ops), b.ln_power(&ops));
    if pa.len() < 2 || pb.len() < 2 {
        return None;
    }
    let (ma, va) = mean_var(&pa);
    let (mb, vb) = mean_var(&pb);
    let delta = mb - ma;
    let (va, vb) = noise.map_or((va, vb), |v| (v, v));
    let sigma = (va / pa.len() as f64 + vb / pb.len() as f64).sqrt();
    let verdict = if delta > SIGNIFICANT * sigma {
        Verdict::Better
    } else if delta < -SIGNIFICANT * sigma {
        Verdict::Worse
    } else {
        Verdict::Same
    };
    Some(Change {
        verdict,
        delta,
        sigma,
    })
}

/// The variance of `xs` around the straight line that fits them best (by
/// index), and its degrees of freedom; None with fewer than 3 values.
fn detrended_var(xs: &[f64]) -> Option<(f64, f64)> {
    let n = xs.len();
    if n < 3 {
        return None;
    }
    let nf = n as f64;
    let mx = (nf - 1.0) / 2.0;
    let (my, _) = mean_var(xs);
    let sxx: f64 = (0..n).map(|i| (i as f64 - mx).powi(2)).sum();
    let sxy: f64 = xs
        .iter()
        .enumerate()
        .map(|(i, y)| (i as f64 - mx) * (y - my))
        .sum();
    let slope = sxy / sxx;
    let ss: f64 = xs
        .iter()
        .enumerate()
        .map(|(i, y)| (y - my - slope * (i as f64 - mx)).powi(2))
        .sum();
    Some((ss / (nf - 2.0), nf - 2.0))
}

/// Mean and sample variance.
fn mean_var(xs: &[f64]) -> (f64, f64) {
    let n = xs.len() as f64;
    let m = xs.iter().sum::<f64>() / n;
    let v = xs.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (n - 1.0).max(1.0);
    (m, v)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TICK: Duration = Duration::from_secs(5);

    /// A fleet whose copiers all start at once: `mibps(copiers)` and
    /// `latency(copiers)` (ms) give its throughput and the time of every CAS
    /// call. `noise` (0 to 1) shakes both, the same way at each run.
    struct Sim {
        t: Instant,
        bytes: f64,
        sizes: Vec<usize>,
        noise: f64,
        rng: u64,
    }

    impl Sim {
        fn new(t: Instant, noise: f64) -> Self {
            Self {
                t,
                bytes: 0.0,
                sizes: vec![INITIAL_COPIERS],
                noise,
                rng: 0x9e37_79b9_7f4a_7c15,
            }
        }

        /// A number in [-1, 1).
        fn shake(&mut self) -> f64 {
            let mut x = self.rng;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.rng = x;
            (x >> 11) as f64 / (1u64 << 53) as f64 * 2.0 - 1.0
        }

        fn run(
            &mut self,
            g: &mut Growth,
            for_: Duration,
            mibps: impl Fn(usize) -> f64,
            latency: impl Fn(usize) -> f64,
        ) {
            let end = self.t + for_;
            while self.t < end {
                self.t += TICK;
                let c = g.target();
                let x = mibps(c) * (1.0 + self.noise * self.shake());
                let l = latency(c) * (1.0 + self.noise * self.shake());
                self.bytes += x * MIB * TICK.as_secs_f64();
                g.observe(
                    self.t,
                    View {
                        running: c,
                        active: c,
                        bytes: self.bytes as u64,
                        latency: Latency {
                            grant_ms: Some(l),
                            commit_ms_per_mib: Some(l),
                            shard_ms_per_mib: Some(l),
                            put_ms_per_mib: Some(1.0),
                        },
                        more_to_launch: true,
                    },
                );
                if self.sizes.last() != Some(&g.target()) {
                    self.sizes.push(g.target());
                }
            }
        }

        /// The median of the last `n` sizes.
        fn median_of_last(&self, n: usize) -> usize {
            let mut tail = self.sizes[self.sizes.len().saturating_sub(n)..].to_vec();
            tail.sort();
            tail[tail.len() / 2]
        }
    }

    fn linear(c: usize) -> f64 {
        1000.0 * c as f64
    }

    fn flat(_: usize) -> f64 {
        10.0
    }

    /// A CAS that queues: its latency grows as `1 / (1 − ρ)` with
    /// `ρ = c / (2 · best)`, so `P = X / L` is highest at `best` copiers.
    fn queue(best: f64) -> impl Fn(usize) -> f64 {
        move |c| {
            let rho = (c as f64 / (2.0 * best)).min(0.98);
            10.0 / (1.0 - rho)
        }
    }

    #[test]
    fn a_fleet_that_scales_grows_by_four_copiers_a_minute_up_to_the_cap() {
        let t0 = Instant::now();
        let mut g = Growth::new(64, t0);
        let mut sim = Sim::new(t0, 0.0);
        sim.run(&mut g, Duration::from_secs(15 * 60), linear, flat);
        assert_eq!(&sim.sizes[..5], &[16, 20, 24, 28, 32], "{:?}", sim.sizes);
        assert_eq!(g.target(), 64, "{:?}", sim.sizes);
    }

    #[test]
    fn a_queueing_cas_holds_the_fleet_around_its_best_size() {
        for noise in [0.0, 0.05] {
            let t0 = Instant::now();
            let mut g = Growth::new(128, t0);
            let mut sim = Sim::new(t0, noise);
            sim.run(&mut g, Duration::from_secs(90 * 60), linear, queue(30.0));
            // A saw-tooth around the best size (30), never far above it.
            let median = sim.median_of_last(20);
            assert!(
                (22..=38).contains(&median),
                "noise {noise}: {median} {:?}",
                sim.sizes
            );
            assert!(
                sim.sizes.iter().all(|s| *s <= 52),
                "noise {noise}: {:?}",
                sim.sizes
            );
        }
    }

    #[test]
    fn copiers_that_add_nothing_are_given_back() {
        // The fleet tops out at 22 GiB/s (not CAS: latency stays flat); 22
        // copiers fill it.
        let capped = |c: usize| (1000.0 * c as f64).min(22_528.0);
        let t0 = Instant::now();
        let mut g = Growth::new(64, t0);
        let mut sim = Sim::new(t0, 0.05);
        sim.run(&mut g, Duration::from_secs(90 * 60), capped, flat);
        // A saw-tooth from ~the best size up to 25 % past it, plus the copiers
        // the ramp adds during one measure: no runaway, but up to ~2× the 22
        // copiers needed at its peaks (the price of measuring while growing).
        let median = sim.median_of_last(20);
        assert!((18..=34).contains(&median), "{median}: {:?}", sim.sizes);
        assert!(sim.sizes.iter().all(|s| *s <= 48), "{:?}", sim.sizes);
    }

    #[test]
    fn the_fleet_follows_a_cas_that_adds_capacity() {
        let t0 = Instant::now();
        let mut g = Growth::new(128, t0);
        let mut sim = Sim::new(t0, 0.0);
        sim.run(&mut g, Duration::from_secs(40 * 60), linear, queue(20.0));
        let before = sim.median_of_last(10);
        // CAS doubles: the best size moves to 40.
        sim.run(&mut g, Duration::from_secs(60 * 60), linear, queue(40.0));
        let after = sim.median_of_last(10);
        assert!(
            (12..=28).contains(&before) && (28..=52).contains(&after),
            "{before} then {after}: {:?}",
            sim.sizes
        );
    }

    #[test]
    fn braking_copiers_shrink_the_fleet_through_the_power() {
        // No error signal reaches the planner: past 28 copiers, CAS overloads
        // and every copier halves its rate; the power drops, the fleet
        // shrinks.
        let braked = |c: usize| {
            if c >= 28 {
                500.0 * c as f64
            } else {
                1000.0 * c as f64
            }
        };
        let t0 = Instant::now();
        let mut g = Growth::new(64, t0);
        let mut sim = Sim::new(t0, 0.0);
        sim.run(&mut g, Duration::from_secs(30 * 60), braked, flat);
        // Past 28 the power falls; the ramp overshoots by a measure (~+8).
        assert!(sim.sizes.iter().all(|s| *s <= 40), "{:?}", sim.sizes);
        let median = sim.median_of_last(10);
        assert!((18..=32).contains(&median), "{median}: {:?}", sim.sizes);
    }

    #[test]
    fn a_decrease_holds_growth_for_one_measure() {
        let t0 = Instant::now();
        let mut g = Growth::new(64, t0);
        let mut sim = Sim::new(t0, 0.0);
        // Grows, then the power falls at 32: × 0.7.
        let cliff = |c: usize| if c >= 32 { 3000.0 } else { 1000.0 * c as f64 };
        sim.run(&mut g, Duration::from_secs(20 * 60), cliff, flat);
        // After each decrease, the next size change is the next decrease or
        // a +4 step taken after a measure of the smaller fleet: never a
        // decrease followed at once by +4.
        let i = sim.sizes.iter().position(|s| *s >= 32).unwrap();
        assert!(sim.sizes[i + 1] < sim.sizes[i], "{:?}", sim.sizes);
    }

    #[test]
    fn copiers_waiting_for_a_machine_count_in_the_fleet() {
        let t0 = Instant::now();
        let mut g = Growth::new(64, t0);
        g.target = 20;
        // 20 copiers launched; half the time one of them waits for a machine
        // (a range ended, the next copier starts).
        let mut bytes = 0u64;
        for k in 1..=40u32 {
            let waiting = (k % 2 == 0) as usize;
            bytes += 20 * 1000 * 1024 * 1024 * 5;
            g.observe(
                t0 + TICK * k,
                View {
                    running: 20 - waiting,
                    active: 20,
                    bytes,
                    more_to_launch: true,
                    ..View::default()
                },
            );
        }
        // The first measure counts 20 copiers, not 19.5.
        assert_eq!(g.best(None).unwrap().copiers, 20.0);
    }

    #[test]
    fn a_fleet_that_cannot_fill_its_target_does_not_grow_it_further() {
        let t0 = Instant::now();
        let mut g = Growth::new(64, t0);
        let mut t = t0;
        let mut bytes = 0u64;
        // Only 4 copiers ever run (the listing is slow).
        for _ in 0..200 {
            t += TICK;
            bytes += 4 * 1000 * 1024 * 1024 * 5;
            g.observe(
                t,
                View {
                    running: 4,
                    active: 4,
                    bytes,
                    more_to_launch: true,
                    ..View::default()
                },
            );
        }
        assert_eq!(g.target(), INITIAL_COPIERS);
    }

    #[test]
    fn a_fleet_that_fills_its_target_between_ramps_grows_it() {
        let t0 = Instant::now();
        let mut g = Growth::new(64, t0);
        let mut bytes = 0u64;
        // Ranges end all the time: a copier is missing at every ramp (each
        // minute), the fleet is full the rest of the time.
        for k in 1..=48u32 {
            let t = t0 + TICK * k;
            let missing = (k % 12 == 0) as usize;
            let active = g.target() - missing;
            bytes += active as u64 * 1000 * 1024 * 1024 * 5;
            g.observe(
                t,
                View {
                    running: active,
                    active,
                    bytes,
                    more_to_launch: true,
                    ..View::default()
                },
            );
        }
        assert_eq!(g.target(), INITIAL_COPIERS + 4 * RAMP_COPIERS);
    }

    #[test]
    fn the_first_measure_waits_for_the_copiers_to_run_the_next_ones_do_not() {
        let t0 = Instant::now();
        let mut g = Growth::new(64, t0);
        // 16 copiers launched, 14 still waiting for a machine: not measured.
        let starting = View {
            running: 2,
            active: 16,
            more_to_launch: false,
            ..View::default()
        };
        for k in 1..=24 {
            g.observe(t0 + TICK * k, starting);
        }
        assert!(matches!(g.state, State::Settle { .. }) && !g.measured_once);
        // All running: 30 s, then a 2 min measure.
        let running = View {
            running: 16,
            ..starting
        };
        for k in 25..=56 {
            g.observe(t0 + TICK * k, running);
        }
        assert!(g.measured_once);
        // The next measure starts at once, copiers waiting or not.
        g.observe(t0 + TICK * 57, starting);
        assert!(matches!(g.state, State::Measure(_)), "{:?}", g.state);
    }

    #[test]
    fn a_fleet_below_its_target_is_measured_after_five_minutes() {
        let t0 = Instant::now();
        let mut g = Growth::new(64, t0);
        // 5 of 16 launched, more to launch (the listing is slow).
        let fleet = View {
            running: 5,
            active: 5,
            more_to_launch: true,
            ..View::default()
        };
        for k in 1..=59 {
            g.observe(t0 + TICK * k, fleet);
        }
        assert!(!g.measured_once);
        for k in 60..=90 {
            g.observe(t0 + TICK * k, fleet);
        }
        assert!(g.measured_once);
    }

    #[test]
    fn power_compares_on_the_calls_both_measures_timed() {
        let lat = |grant, shard| Latency {
            grant_ms: Some(grant),
            commit_ms_per_mib: Some(10.0),
            shard_ms_per_mib: shard,
            put_ms_per_mib: Some(99.0),
        };
        let point = |x: f64, l: Latency| Point {
            copiers: 8.0,
            slices: vec![(x, l), (x * 1.01, l), (x * 0.99, l), (x, l)],
        };
        // Same throughput, grant 4× slower: L (geometric mean of grant and
        // commit; the shard is in one measure only, the PUT is not CAS) ×2.
        let a = point(1000.0, lat(10.0, None));
        let b = point(1000.0, lat(40.0, Some(50.0)));
        let c = compare(&a, &b, None).unwrap();
        assert_eq!(c.verdict, Verdict::Worse);
        assert!((c.delta - (0.5f64).ln()).abs() < 1e-9, "{c:?}");
        // Twice the throughput at that latency: the same power.
        let d = point(2000.0, lat(40.0, Some(50.0)));
        assert_eq!(compare(&a, &d, None).unwrap().verdict, Verdict::Same);
    }

    #[test]
    fn one_agitated_measure_does_not_hide_a_loss() {
        let lat = Latency {
            grant_ms: Some(10.0),
            commit_ms_per_mib: Some(10.0),
            shard_ms_per_mib: Some(10.0),
            put_ms_per_mib: None,
        };
        let point = |spread: [f64; 4]| Point {
            copiers: 8.0,
            slices: spread.iter().map(|x| (*x, lat)).collect(),
        };
        let t0 = Instant::now();
        let mut g = Growth::new(64, t0);
        // Three calm measures at ~20 GiB/s (±3 %).
        let calm = point([20_000.0, 20_600.0, 19_400.0, 20_000.0]);
        for k in 0..3 {
            g.pooled_noise(t0 + Duration::from_secs(150 * k), &calm);
        }
        // Then an agitated one, 25 % lower: copiers ending and starting.
        let agitated = point([9_000.0, 19_000.0, 14_000.0, 18_000.0]);
        // On its own spread, the loss hides in the noise...
        assert_eq!(
            compare(&calm, &agitated, None).unwrap().verdict,
            Verdict::Same
        );
        // ...pooled with the calm ones, it shows.
        let noise = g.pooled_noise(t0 + Duration::from_secs(450), &agitated);
        assert_eq!(
            compare(&calm, &agitated, noise).unwrap().verdict,
            Verdict::Worse
        );
    }
}
