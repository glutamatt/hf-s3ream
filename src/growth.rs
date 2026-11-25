//! The size of the fleet: climb toward the most power, `P = throughput /
//! latency`.
//!
//! `P` is Kleinrock's power: it grows while copiers add throughput faster
//! than they add latency, and is highest at the knee, where CAS starts to
//! queue (the point BBR aims at too). It needs no base level and no
//! threshold: only the shape of the curve.
//!
//! The planner measures its fleet over [`MEASURE_FOR`], once the fleet has its
//! size ([`SETTLE_FOR`] after a change):
//! - `X`: the bytes fed to xet, per second;
//! - `L`: the latency of CAS, the geometric mean of its calls (grant, commit,
//!   shard: `brake.rs`), each the median over the copiers. A geometric mean
//!   weighs each call the same; one call twice slower moves `L` by ×1.26.
//! - `P = X / L`, on each of [`SLICES`] slices of the measure. The spread of
//!   the slices, pooled over the measures of the last [`BEST_FOR`], gives the
//!   noise `σ`. Pooled, not per measure: 4 slices tell their noise badly, and
//!   one agitated measure (copiers ending and starting together) gave σ = 14 %
//!   on a real run, which hid a real loss of 25 % of the power.
//!
//! Then the fleet moves by a step, in a direction (`s` = +1 grow, −1 shrink):
//! `N(k+1) = N(k) · (1 + step · s)`. Each measure is compared with the one
//! before (did the last move pay?) and with the best of the last
//! [`BEST_FOR`] (did the fleet slide away from it?):
//!
//! ```text
//! less power than the best, by more than 2σ:  s = toward the best size, step × ½
//! more power than the measure before, > 2σ:   keep s,                   step × 1.2 (at most a)
//! no clear change:                            s = −1 (fewer copiers cost less)
//! ```
//!
//! - latency stable, throughput up: `P` up, the fleet keeps growing;
//! - CAS adds capacity: `P` up more, the fleet keeps growing;
//! - latency up faster than throughput: `P` down, the fleet goes back toward
//!   the best size;
//! - copiers that add nothing (a slow source, our own pipeline): no clear
//!   change, the fleet gives copiers back until the power drops.
//!
//! The steps are Rprop's (×1.2 on a gain, ×½ on a turn): with ×2 and ×½ the
//! fleet swung from 6 to 21 copiers around a best size of 12 in the tests.
//! The best measure is the anchor: comparing only with the measure before, a
//! fleet that gave copiers back on no clear change lost a little power at
//! each step, never enough to show, and slid on. After [`STEP_BACK_AFTER`]
//! at a small step, the fleet probes upward by `a`: the best size moves up
//! when CAS adds capacity (when it moves down, the fleet sees less power at
//! the same size and gives copiers back).
//!
//! One figure: `a` ([`A`]), plus the usual 2σ and Rprop's steps. `N(k)` is the copiers
//! launched (running, or waiting for a machine): a fleet that cannot fill its
//! target (a slow listing, the commit budget) does not grow it further.
//!
//! Errors are the copiers' own business: a copier halves its rate on a 429
//! or a 5xx within seconds (`brake.rs`), as each TCP flow reacts to its own
//! losses. The planner does not watch errors: when copiers brake, the fleet
//! throughput drops (and latency often rises), so the power drops, and the
//! next measure gives copiers back. The reaction is in proportion: one 500
//! on one copier of 60 is noise, an overload that brakes many is a clear
//! loss.

use crate::brake::{median, Latency, Op};
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tracing::info;

/// The first size: a small fleet, and enough copiers to fill the brake's
/// fast start.
pub const INITIAL_COPIERS: usize = 8;
/// `a`: the largest step, as a share of the fleet.
const A: f64 = 0.5;
/// A change of power counts when it is over this many σ.
const SIGNIFICANT: f64 = 2.0;
/// Rprop's steps: a gain grows the step, a turn halves it.
const STEP_UP: f64 = 1.2;
const STEP_DOWN: f64 = 0.5;
/// The best measure is forgotten after this: CAS and its other users change.
const BEST_FOR: Duration = Duration::from_secs(10 * 60);
/// Measure the fleet over this time. Copiers send a shard every 10 to 30 s,
/// and stop and go every ~40 s (they wait for their shards).
const MEASURE_FOR: Duration = Duration::from_secs(120);
/// The measure is cut in this many slices, for its noise.
const SLICES: usize = 4;
/// A slice needs a time of a call in at least this many observations.
const MIN_LATENCY_OBS: usize = 2;
/// After a change, the copiers added get a machine (5 s median, 67 s p90 on a
/// real run) and their full speed (~10 s): the measure starts after this.
const SETTLE_FOR: Duration = Duration::from_secs(60);
/// A fleet that cannot fill its target after this long is measured anyway.
const SETTLE_AT_MOST: Duration = Duration::from_secs(5 * 60);
/// After this long at a step below `a`, the step goes back to `a`.
const STEP_BACK_AFTER: Duration = Duration::from_secs(10 * 60);
/// Below this change of copiers, a measure did not test a new size.
const MIN_MOVE: f64 = 0.5;
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
    Settle { since: Instant },
    Measure(Box<Measure>),
}

/// A measured fleet: copiers launched (average), and per slice its MiB/s and
/// the median latency of each call.
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
    last: Option<Point>,
    /// The measure with the most power lately (clearly more than the one
    /// before it), and when it was taken.
    best: Option<(Point, Instant)>,
    /// The spread of ln P over the slices of each recent measure: (when,
    /// variance, degrees of freedom).
    noise: VecDeque<(Instant, f64, f64)>,
    /// +1: growing; −1: shrinking.
    dir: f64,
    step: f64,
    /// Since when the step is below `a`.
    small_since: Option<Instant>,
}

impl Growth {
    pub fn new(max: usize, now: Instant) -> Self {
        let max = max.max(1);
        Self {
            max,
            target: INITIAL_COPIERS.min(max),
            state: State::Settle { since: now },
            last: None,
            best: None,
            noise: VecDeque::new(),
            dir: 1.0,
            step: A,
            small_since: None,
        }
    }

    /// How many copiers may run now.
    pub fn target(&self) -> usize {
        self.target
    }

    /// Read the fleet (every few seconds).
    pub fn observe(&mut self, now: Instant, v: View) {
        // A measure needs the fleet at its size: the copiers the target
        // allows launched, or all there is to launch (without this, a measure
        // was taken on a fleet still starting: 0 to 2 copiers, seen on a real
        // run), and [`SETTLE_FOR`] for the copiers just added to get a
        // machine and their speed. Not "no copier starting": in a big fleet
        // some copier always is (ranges end, the next copiers start; each
        // ramps ~10 s), so that waited the full [`SETTLE_AT_MOST`] at 41 and
        // 61 copiers on a real run, and decisions slowed to one every 6 min.
        let full = v.active >= self.target || !v.more_to_launch;
        let state = std::mem::replace(&mut self.state, State::Settle { since: now });
        self.state = match state {
            State::Settle { since } => {
                // A fleet with no copier running has nothing to measure.
                let waited = now.duration_since(since);
                let settled = full && waited >= SETTLE_FOR;
                let late = waited >= SETTLE_AT_MOST && v.running > 0;
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
                    State::Settle { since }
                }
            }
            State::Measure(mut m) => {
                m.add(now, &v);
                if now.duration_since(m.since) < MEASURE_FOR {
                    State::Measure(m)
                } else {
                    self.measured(now, m.point());
                    State::Settle { since: now }
                }
            }
        };
    }

    fn halve(&mut self, now: Instant, copiers: f64) {
        self.step = (self.step * STEP_DOWN).max(1.0 / copiers.max(1.0));
        self.small_since.get_or_insert(now);
    }

    /// The size one step away from `copiers`, in the current direction (at
    /// least one copier away).
    fn next_size(&self, copiers: f64) -> usize {
        let base = copiers.round().max(1.0) as i64;
        let mut to = (copiers * (1.0 + self.dir * self.step)).round() as i64;
        if self.dir > 0.0 && to <= base {
            to = base + 1;
        }
        if self.dir < 0.0 && to >= base {
            to = base - 1;
        }
        to.clamp(1, self.max as i64) as usize
    }

    fn measured(&mut self, now: Instant, p: Point) {
        let n = p.copiers;
        if self.step < A
            && self
                .small_since
                .is_some_and(|t| now.duration_since(t) >= STEP_BACK_AFTER)
        {
            // Probe upward: the best size moves up when CAS adds capacity (a
            // move down shows on its own, as less power at the same size). A
            // probe down by `a` halved the throughput for minutes in the
            // tests.
            self.step = A;
            self.dir = 1.0;
            self.small_since = None;
        }
        if self
            .best
            .as_ref()
            .is_some_and(|(_, at)| now.duration_since(*at) >= BEST_FOR)
        {
            self.best = None;
        }
        let noise = self.pooled_noise(now, &p);
        // Did the last move pay? Only a measure at a new size tests one.
        let prev = self.last.replace(p.clone());
        let vs_prev = prev
            .filter(|q| (n - q.copiers).abs() >= MIN_MOVE)
            .and_then(|q| compare(&q, &p, noise));
        // Did the fleet slide away from its best size?
        let vs_best = self
            .best
            .as_ref()
            .and_then(|(b, _)| compare(b, &p, noise).map(|c| (c, b.copiers)));

        let (why, shown) = match (vs_best, vs_prev) {
            (Some((c, best_n)), _) if c.verdict == Verdict::Worse => {
                if (best_n - n).abs() < MIN_MOVE {
                    // Less power at the same size: CAS changed, not us. Give
                    // copiers back, and start from here.
                    self.best = None;
                    self.dir = -1.0;
                } else {
                    self.dir = (best_n - n).signum();
                }
                self.halve(now, n);
                ("less power than the best size", Some(c))
            }
            (_, Some(c)) if c.verdict == Verdict::Better => {
                self.step = (self.step * STEP_UP).min(A);
                if self.step >= A {
                    self.small_since = None;
                }
                ("more power", Some(c))
            }
            (_, Some(c)) => {
                if self.dir > 0.0 {
                    self.halve(now, n);
                }
                self.dir = -1.0;
                ("no more power: fewer copiers cost less", Some(c))
            }
            (best, None) => ("first measure at this size", best.map(|(c, _)| c)),
        };
        // The best: replaced only by a measure with clearly more power. Taking
        // the highest measure picked a lucky one, and every later measure
        // looked worse (the fleet climbed a flat plateau in the tests).
        let better_than_best = match &self.best {
            None => true,
            Some((b, _)) => compare(b, &p, noise).is_some_and(|c| c.verdict == Verdict::Better),
        };
        if better_than_best {
            self.best = Some((p.clone(), now));
        }

        let from = self.target;
        self.target = self.next_size(n);
        // The comparison that decided: with the best size, or with the
        // measure before.
        info!(
            from,
            to = self.target,
            copiers = format!("{n:.1}"),
            why,
            power_change = shown.map(|c| format!("{:+.1}%", (c.delta.exp() - 1.0) * 100.0)),
            sigma = shown.map(|c| format!("{:.1}%", c.sigma * 100.0)),
            best_copiers = self.best.as_ref().map(|(b, _)| format!("{:.1}", b.copiers)),
            step = format!("{:.2}", self.step),
            mibps = format!("{:.0}", p.mibps()),
            "growth: new fleet size"
        );
    }
}

impl Growth {
    /// Add the spread of `p` to the recent ones, and pool them: the variance
    /// of ln P within a measure, weighted by degrees of freedom. None before
    /// any measure with two slices.
    fn pooled_noise(&mut self, now: Instant, p: &Point) -> Option<f64> {
        let lp = p.ln_power(&p.calls());
        if lp.len() >= 2 {
            let (_, var) = mean_var(&lp);
            self.noise.push_back((now, var, (lp.len() - 1) as f64));
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
        fn new(noise: f64) -> Self {
            Self {
                t: Instant::now(),
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

        fn tail(&self, n: usize) -> &[usize] {
            &self.sizes[self.sizes.len().saturating_sub(n)..]
        }
    }

    fn linear(c: usize) -> f64 {
        600.0 * c as f64
    }

    fn flat(_: usize) -> f64 {
        10.0
    }

    /// A CAS that queues past `knee` copiers: its latency grows as
    /// `1 / (1 − ρ)` with `ρ = c / (knee · 1.5)`, so `P` peaks near the knee.
    fn queue(knee: f64) -> impl Fn(usize) -> f64 {
        move |c| {
            let rho = (c as f64 / (knee * 1.5)).min(0.98);
            10.0 / (1.0 - rho)
        }
    }

    #[test]
    fn a_fleet_that_scales_grows_by_half_per_measure() {
        let mut g = Growth::new(64, Instant::now());
        let mut sim = Sim::new(0.0);
        sim.run(&mut g, Duration::from_secs(20 * 60), linear, flat);
        // 8 · 1.5ᵏ, rounded at each step, up to the cap.
        assert_eq!(&sim.sizes[..6], &[8, 12, 18, 27, 41, 62], "{:?}", sim.sizes);
        assert!(sim.sizes.contains(&64), "{:?}", sim.sizes);
    }

    #[test]
    fn a_queueing_cas_holds_the_fleet_near_its_knee_without_drift() {
        // Throughput grows with the copiers, latency as a queue: P = X / L is
        // highest at 12 copiers for a knee at 16 (∂ ln P = 0 at ρ = ½). P is
        // flat near its top (−11 % at 8 copiers): with noise, the fleet
        // wanders there, around 12.
        for noise in [0.0, 0.05] {
            let mut g = Growth::new(64, Instant::now());
            let mut sim = Sim::new(noise);
            sim.run(&mut g, Duration::from_secs(90 * 60), linear, queue(16.0));
            let mut tail = sim.tail(12).to_vec();
            tail.sort();
            let median = tail[tail.len() / 2];
            assert!(
                (8..=14).contains(&median) && tail.iter().all(|s| (5..=20).contains(s)),
                "noise {noise}: {:?}",
                sim.sizes
            );
            assert!(
                sim.sizes.iter().all(|s| *s <= 27),
                "noise {noise}: {:?}",
                sim.sizes
            );
        }
    }

    #[test]
    fn copiers_that_add_nothing_are_given_back() {
        // The fleet tops out at 13 GiB/s (not CAS: latency stays flat); 22
        // copiers fill it.
        let capped = |c: usize| (600.0 * c as f64).min(13_312.0);
        let mut g = Growth::new(64, Instant::now());
        let mut sim = Sim::new(0.05);
        sim.run(&mut g, Duration::from_secs(90 * 60), capped, flat);
        // Mostly close to 22, with a probe now and then (the step comes back
        // to `a` after 10 minutes), never far above.
        let mut tail = sim.tail(20).to_vec();
        tail.sort();
        let median = tail[tail.len() / 2];
        assert!((16..=30).contains(&median), "{median}: {:?}", sim.sizes);
        assert!(sim.sizes.iter().all(|s| *s <= 50), "{:?}", sim.sizes);
    }

    #[test]
    fn noise_does_not_stop_a_fleet_whose_copiers_pay() {
        // Throughput grows with the copiers, latency flat, 10 % noise on both.
        let mut g = Growth::new(64, Instant::now());
        let mut sim = Sim::new(0.1);
        sim.run(&mut g, Duration::from_secs(30 * 60), linear, flat);
        assert!(sim.sizes.contains(&64), "{:?}", sim.sizes);
    }

    #[test]
    fn the_fleet_follows_a_cas_that_adds_capacity() {
        let mut g = Growth::new(64, Instant::now());
        let mut sim = Sim::new(0.0);
        sim.run(&mut g, Duration::from_secs(40 * 60), linear, queue(16.0));
        let before = *sim.sizes.last().unwrap();
        // CAS doubles: the knee moves to 32.
        sim.run(&mut g, Duration::from_secs(60 * 60), linear, queue(32.0));
        let tail = sim.tail(6);
        assert!(
            tail.iter().all(|s| (18..=36).contains(s)) && before < 20,
            "{before} then {:?}",
            sim.sizes
        );
    }

    #[test]
    fn braking_copiers_give_copiers_back_through_the_power() {
        // No error signal reaches the planner: copiers that brake push less,
        // the power drops, the fleet turns back.
        let mut g = Growth::new(64, Instant::now());
        let mut sim = Sim::new(0.0);
        sim.run(&mut g, Duration::from_secs(190), linear, flat);
        assert_eq!(g.target(), 12);
        // At 12 copiers, CAS overloads: every copier halves its rate.
        let braked = |c: usize| {
            if c >= 12 {
                300.0 * c as f64
            } else {
                600.0 * c as f64
            }
        };
        sim.run(&mut g, Duration::from_secs(190), braked, flat);
        assert!(g.target() < 12, "{:?}", sim.sizes);
    }

    #[test]
    fn a_small_step_comes_back_to_a_after_ten_minutes() {
        let t0 = Instant::now();
        let mut g = Growth::new(64, t0);
        g.step = 0.1;
        g.small_since = Some(t0);
        let p = Point {
            copiers: 20.0,
            slices: vec![(6000.0, Latency::default()); SLICES],
        };
        g.measured(t0 + Duration::from_secs(9 * 60), p.clone());
        assert_eq!(g.step, 0.1);
        g.measured(t0 + Duration::from_secs(10 * 60), p);
        assert_eq!(g.step, A);
    }

    #[test]
    fn copiers_waiting_for_a_machine_count_in_the_fleet() {
        let t0 = Instant::now();
        let mut g = Growth::new(64, t0);
        // 8 copiers launched; half the time one of them waits for a machine
        // (a range ended, the next copier starts).
        for k in 1..=40u32 {
            let waiting = (k % 2 == 0) as usize;
            g.observe(
                t0 + TICK * k,
                View {
                    running: 8 - waiting,
                    active: 8,
                    more_to_launch: true,
                    ..View::default()
                },
            );
        }
        // 8 · 1.5 = 12 (counting only the running ones: 7.5 · 1.5 = 11).
        assert_eq!(g.target(), 12);
    }

    #[test]
    fn a_fleet_that_cannot_fill_its_target_does_not_grow_it_further() {
        let mut g = Growth::new(64, Instant::now());
        let mut t = Instant::now();
        let mut bytes = 0u64;
        // Only 4 copiers ever run (the listing is slow).
        for _ in 0..200 {
            t += TICK;
            bytes += 4 * 600 * 1024 * 1024 * 5;
            g.observe(
                t,
                View {
                    running: 4,
                    active: 4,
                    bytes,
                    ..View::default()
                },
            );
        }
        // 4 · 1.5 = 6: from the fleet, not 8 · 1.5ᵏ.
        assert_eq!(g.target(), 6);
    }

    #[test]
    fn a_measure_starts_a_minute_after_a_change_even_with_copiers_starting() {
        // A big fleet always has some copier starting (a range ended, the next
        // copier starts): that must not hold the measure back.
        for v in [
            // 2 of the 8 wait for a machine.
            View {
                running: 6,
                ..View::default()
            },
            // All 8 run (some still ramping up: not in the view at all).
            View {
                running: 8,
                ..View::default()
            },
        ] {
            let t0 = Instant::now();
            let mut g = Growth::new(64, t0);
            let fleet = View {
                active: 8,
                more_to_launch: true,
                ..v
            };
            // 55 s: settling. 60 s + a 2 min measure: 8 · 1.5 = 12.
            for k in 1..=11 {
                g.observe(t0 + TICK * k, fleet);
            }
            assert!(matches!(g.state, State::Settle { .. }), "{v:?}");
            for k in 12..=37 {
                g.observe(t0 + TICK * k, fleet);
            }
            assert_eq!(g.target(), 12, "{v:?}");
        }
    }

    #[test]
    fn a_fleet_below_its_target_is_measured_after_five_minutes() {
        let t0 = Instant::now();
        let mut g = Growth::new(64, t0);
        // 5 of 8 launched, more to launch (the listing is slow).
        let fleet = View {
            running: 5,
            active: 5,
            more_to_launch: true,
            ..View::default()
        };
        for k in 1..=59 {
            g.observe(t0 + TICK * k, fleet);
        }
        assert!(matches!(g.state, State::Settle { .. }));
        for k in 60..=90 {
            g.observe(t0 + TICK * k, fleet);
        }
        // Measured at 5 copiers: 5 · 1.5 = 7.5, rounded to 8.
        assert_eq!(g.target(), 8);
    }

    #[test]
    fn no_measure_while_the_fleet_fills_up() {
        let t0 = Instant::now();
        let mut g = Growth::new(64, t0);
        // The planner lists and launches: 0, then 2, then 5 copiers of 8.
        for (k, running) in (1..=36).map(|k| (k, (k / 6).min(5) as usize)) {
            g.observe(
                t0 + TICK * k,
                View {
                    running,
                    active: running,
                    more_to_launch: true,
                    ..View::default()
                },
            );
        }
        assert_eq!(g.target(), 8);
        assert!(matches!(g.state, State::Settle { .. }), "{:?}", g.state);
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
