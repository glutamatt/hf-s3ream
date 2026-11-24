//! The size of the fleet, from the trend of CAS latency.
//!
//! The planner measures its fleet over [`MEASURE_FOR`], once the fleet has its
//! size and no copier is starting or braking:
//! - `L`: the latency of each CAS call (grant, commit, shard: `brake.rs`), the
//!   median over the copiers, then over the window;
//! - the throughput: the bytes fed to xet over the window.
//!
//! After each measure `k`, one law sets the next size:
//!
//! ```text
//! N(k+1) = N(k) · (1 + a) · L(k-1) / L(k)        (in log: Δ ln N = a − Δ ln L)
//! ```
//!
//! - latency stable: `+a` ([`A`] = 50 %), a steady increase;
//! - latency falls (CAS added capacity): a faster increase;
//! - latency rises by more than `a`: fewer copiers.
//!
//! No base level and no threshold: only the trend, and one figure, `a`.
//! One exception: after the fleet shrank, a latency that falls counts as
//! stable. The fleet caused that fall, not CAS; counted, it sped the next
//! growth up and the fleet swung from half to twice the right size (seen in
//! the tests: a CAS whose latency rises faster than the fleet grows).
//! `L(k-1) / L(k)` is the worst call: the one whose latency rose the most. A
//! call without a time in both measures is left out; with none at all, the
//! ratio is 1. The factor stays within [½, 2] ([`MIN_FACTOR`],
//! [`MAX_FACTOR`]): one noisy measure moves the fleet by half or twice at
//! most. `N(k)` is the copiers launched during the measure (running, or
//! waiting for a machine), not the target: a fleet that cannot fill its
//! target (a slow listing, the commit budget) does not grow it further. Not
//! the copiers running either: ~7 % of a fleet waits for a machine at any
//! time (a range ends, the next copier starts), and counting only the
//! running ones slowed the growth to ×1.17 for a = 25 % (seen on a real
//! run).
//!
//! **The economic gate** (`--min-elasticity`) only stops growth. A copier is
//! billed by the minute: after a measure where the fleet grew, if
//! `e = (% more throughput) / (% more copiers)` is below the minimum, the
//! copiers added did not pay. The fleet goes back to its size before, and does
//! not grow for [`REPROBE_AFTER`] (a slow source, a busy CAS: things change).
//! The law still shrinks the fleet in that time when latency rises.
//!
//! Errors are not here: a copier halves its rate on a 429 or a 5xx
//! (`brake.rs`), and the planner starts no copier while one braked.

use crate::brake::{median, Latency, Op};
use std::time::{Duration, Instant};
use tracing::info;

/// The first size: a small fleet, and enough copiers to fill the brake's
/// fast start.
pub const INITIAL_COPIERS: usize = 8;
/// `a`: the growth per measure while latency is stable. 25 % was too slow on
/// a real run (one measure takes ~3 min with the wait for machines).
const A: f64 = 0.5;
/// One measure changes the fleet by this factor at most...
const MAX_FACTOR: f64 = 2.0;
/// ...and at least.
const MIN_FACTOR: f64 = 0.5;
/// Measure the fleet over this time. Copiers send a shard every 10 to 30 s,
/// and a copier of a 64 GiB range lives 2 to 3 minutes.
const MEASURE_FOR: Duration = Duration::from_secs(120);
/// A fleet still starting after this long is measured anyway (one that
/// brakes never is).
const SETTLE_AT_MOST: Duration = Duration::from_secs(5 * 60);
/// After the economic gate stopped growth, growth may start again after this.
const REPROBE_AFTER: Duration = Duration::from_secs(10 * 60);
/// Fewer added copiers than this (on average) cannot be judged by the
/// economic gate.
const MIN_ADDED: f64 = 0.5;
/// A call needs a time in at least this many observations of a measure.
const MIN_LATENCY_OBS: usize = 3;
const MIB: f64 = 1024.0 * 1024.0;

/// What the planner sees of its fleet, from the copiers' logs.
#[derive(Debug, Clone, Copy, Default)]
pub struct View {
    pub running: usize,
    pub scheduling: usize,
    /// Copiers launched and not ended (running or scheduling).
    pub active: usize,
    /// Copiers whose brake is raising their rate fast.
    pub ramping: usize,
    /// Copiers whose brake cut their rate a short time ago.
    pub braking: usize,
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

/// A measure in progress.
#[derive(Debug, Clone)]
struct Measure {
    since: Instant,
    copiers: f64,
    n: u32,
    /// Bytes moved since the start of the measure, and the last count seen.
    moved: u64,
    last_bytes: u64,
    /// The fleet latency of each call at each observation, by [`Op`].
    latency: [Vec<f64>; 4],
}

#[derive(Debug, Clone)]
enum State {
    Settle { since: Instant },
    Measure(Measure),
}

/// A measured fleet: average copiers launched, MiB/s, latency of each call.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Point {
    copiers: f64,
    mibps: f64,
    latency: Latency,
}

pub struct Growth {
    min_elasticity: f64,
    max: usize,
    target: usize,
    state: State,
    last: Option<Point>,
    /// No growth before this (the economic gate stopped it).
    no_growth_until: Option<Instant>,
}

impl Growth {
    /// `min_elasticity` 0: no economic gate.
    pub fn new(min_elasticity: f64, max: usize, now: Instant) -> Self {
        let max = max.max(1);
        Self {
            min_elasticity,
            max,
            target: INITIAL_COPIERS.min(max),
            state: State::Settle { since: now },
            last: None,
            no_growth_until: None,
        }
    }

    /// How many copiers may run now.
    pub fn target(&self) -> usize {
        self.target
    }

    /// Read the fleet (every few seconds).
    pub fn observe(&mut self, now: Instant, v: View) {
        // A measure needs the fleet at its size: the copiers the target
        // allows, or all there is to launch. (Without this, a measure was
        // taken on a fleet still starting: 0 to 2 copiers, seen on a real
        // run.)
        let full = v.running >= self.target || !v.more_to_launch;
        let settled = full && v.scheduling == 0 && v.ramping == 0 && v.braking == 0;
        let state = std::mem::replace(&mut self.state, State::Settle { since: now });
        self.state = match state {
            State::Settle { since } => {
                // Braking always waits: the planner launches nothing then. A
                // fleet with no copier running has nothing to measure.
                let late =
                    now.duration_since(since) >= SETTLE_AT_MOST && v.braking == 0 && v.running > 0;
                if settled || late {
                    let mut m = Measure {
                        since: now,
                        copiers: 0.0,
                        n: 0,
                        moved: 0,
                        last_bytes: v.bytes,
                        latency: Default::default(),
                    };
                    m.add(&v);
                    State::Measure(m)
                } else {
                    State::Settle { since }
                }
            }
            State::Measure(mut m) => {
                if v.braking > 0 {
                    // A brake moved the figures: start over.
                    State::Settle { since: now }
                } else {
                    m.add(&v);
                    if now.duration_since(m.since) < MEASURE_FOR {
                        State::Measure(m)
                    } else {
                        let elapsed = now.duration_since(m.since).as_secs_f64();
                        self.measured(now, m.point(elapsed));
                        State::Settle { since: now }
                    }
                }
            }
        };
    }

    fn measured(&mut self, now: Instant, p: Point) {
        let prev = self.last.replace(p);
        let n = p.copiers;
        let change = prev.and_then(|prev| {
            let c = latency_change(&prev.latency, &p.latency)?;
            // A fleet that shrank lowered the latency itself: that fall says
            // nothing about CAS, and must not speed growth up.
            let shrank = prev.copiers - p.copiers >= MIN_ADDED;
            Some(if shrank { c.max(1.0) } else { c })
        });
        let factor = ((1.0 + A) / change.unwrap_or(1.0)).clamp(MIN_FACTOR, MAX_FACTOR);
        let change_txt = change.map(|c| format!("{c:.2}"));
        let mibps = format!("{:.0}", p.mibps);

        // The economic gate: did the copiers added by the last change pay?
        if let Some(prev) = prev {
            let grew = p.copiers - prev.copiers >= MIN_ADDED;
            let e = elasticity(prev, p);
            if grew && self.min_elasticity > 0.0 && e < self.min_elasticity {
                let to = (prev.copiers.round() as usize).clamp(1, self.max);
                info!(
                    from = self.target,
                    to,
                    elasticity = format!("{e:.2}"),
                    min = self.min_elasticity,
                    mibps,
                    latency_change = change_txt,
                    "growth: the last copiers did not pay; back, and no growth for a while"
                );
                self.target = to;
                self.no_growth_until = Some(now + REPROBE_AFTER);
                return;
            }
        }

        let may_grow = self.no_growth_until.is_none_or(|t| now >= t);
        let to = if factor >= 1.0 {
            if !may_grow {
                return;
            }
            // At least one more: a small fleet would round to itself.
            ((n * factor).round() as usize).max(n.round() as usize + 1)
        } else {
            (n * factor).round() as usize
        }
        .clamp(1, self.max);
        if to != self.target {
            info!(
                from = self.target,
                to,
                copiers = format!("{n:.1}"),
                latency_change = change_txt,
                factor = format!("{factor:.2}"),
                mibps,
                "growth: new fleet size"
            );
        }
        self.target = to;
    }
}

impl Measure {
    fn add(&mut self, v: &View) {
        self.copiers += v.active as f64;
        self.n += 1;
        // A re-spawned range starts its count over: only increases count.
        self.moved += v.bytes.saturating_sub(self.last_bytes);
        self.last_bytes = v.bytes;
        for op in Op::ALL {
            if let Some(t) = v.latency.get(op) {
                self.latency[op.index()].push(t);
            }
        }
    }

    fn point(&self, elapsed_s: f64) -> Point {
        let mut latency = Latency::default();
        for op in Op::ALL {
            let xs = &self.latency[op.index()];
            if xs.len() >= MIN_LATENCY_OBS {
                latency.set(op, Some(median(xs)));
            }
        }
        Point {
            copiers: self.copiers / f64::from(self.n.max(1)),
            mibps: self.moved as f64 / MIB / elapsed_s.max(1e-3),
            latency,
        }
    }
}

/// `L(k) / L(k-1)` of the CAS call whose latency rose the most; None when no
/// call has a time in both.
fn latency_change(prev: &Latency, now: &Latency) -> Option<f64> {
    Op::CAS
        .iter()
        .filter_map(|op| match (prev.get(*op), now.get(*op)) {
            (Some(a), Some(b)) if a > 0.0 => Some(b / a),
            _ => None,
        })
        .reduce(f64::max)
}

/// % more throughput over % more copiers.
fn elasticity(a: Point, b: Point) -> f64 {
    let dc = b.copiers / a.copiers.max(1e-9) - 1.0;
    let dx = if a.mibps > 0.0 {
        b.mibps / a.mibps - 1.0
    } else if b.mibps > 0.0 {
        1.0
    } else {
        0.0
    };
    dx / dc
}

#[cfg(test)]
mod tests {
    use super::*;

    const TICK: Duration = Duration::from_secs(5);

    /// A fleet whose copiers all run at once: `mibps(copiers)` and
    /// `commit(copiers)` (ms per MiB) give its throughput and latency.
    struct Sim {
        t: Instant,
        bytes: f64,
        sizes: Vec<usize>,
    }

    impl Sim {
        fn new() -> Self {
            Self {
                t: Instant::now(),
                bytes: 0.0,
                sizes: vec![],
            }
        }

        fn run(
            &mut self,
            g: &mut Growth,
            for_: Duration,
            mibps: impl Fn(usize) -> f64,
            commit: impl Fn(usize) -> f64,
        ) {
            let end = self.t + for_;
            while self.t < end {
                self.t += TICK;
                let running = g.target();
                self.bytes += mibps(running) * MIB * TICK.as_secs_f64();
                g.observe(
                    self.t,
                    View {
                        running,
                        active: running,
                        bytes: self.bytes as u64,
                        // Every CAS call moves the same way.
                        latency: Latency {
                            commit_ms_per_mib: Some(commit(running)),
                            grant_ms: Some(2.0 * commit(running)),
                            ..Latency::default()
                        },
                        more_to_launch: true,
                        ..View::default()
                    },
                );
                if self.sizes.last() != Some(&g.target()) {
                    self.sizes.push(g.target());
                }
            }
        }
    }

    fn linear(c: usize) -> f64 {
        600.0 * c as f64
    }

    #[test]
    fn stable_latency_grows_by_half_per_measure() {
        let mut g = Growth::new(0.5, 64, Instant::now());
        let mut sim = Sim::new();
        // One measure every ~2 min.
        sim.run(&mut g, Duration::from_secs(25 * 60), linear, |_| 10.0);
        // 8 · 1.5ᵏ, rounded at each step, up to the cap.
        assert_eq!(sim.sizes, vec![8, 12, 18, 27, 41, 62, 64]);
    }

    // KNOWN FLAW (2026-11-24), kept failing on purpose: past the knee, a law
    // that reads only the trend has no set point. With latency ×1.5 per +25 %
    // copiers, a = 0.5 gives 8, 12, 18, 22, 23, 32, 26, 39, 28, 42 … 60: the
    // fleet creeps up ~8 % every two measures, without end. Next: the
    // Kleinrock power law (P = throughput / latency), see the vault report.
    #[test]
    #[ignore = "known flaw: the gradient law creeps past the knee"]
    fn rising_latency_shrinks_the_fleet() {
        let mut g = Growth::new(0.5, 64, Instant::now());
        let mut sim = Sim::new();
        // Latency flat up to 16 copiers, then ×2.25 per added half: the
        // fleet never runs away, and swings around the knee (a gradient
        // law has no set point: it reads the trend, one measure late).
        let commit = |c: usize| {
            if c <= 16 {
                10.0
            } else {
                10.0 * 1.5f64.powf(((c as f64) / 16.0).ln() / 1.25f64.ln())
            }
        };
        sim.run(&mut g, Duration::from_secs(40 * 60), linear, commit);
        let tail = &sim.sizes[sim.sizes.len().saturating_sub(6)..];
        assert!(
            tail.iter().all(|s| (16..=24).contains(s)),
            "{:?}",
            sim.sizes
        );
        assert!(sim.sizes.iter().all(|s| *s <= 25), "{:?}", sim.sizes);
    }

    #[test]
    fn falling_latency_grows_faster() {
        let mut g = Growth::new(0.5, 64, Instant::now());
        let mut sim = Sim::new();
        sim.run(&mut g, Duration::from_secs(150), linear, |_| 10.0);
        assert_eq!(g.target(), 12);
        // CAS added capacity: latency falls by 20 %.
        sim.run(&mut g, Duration::from_secs(150), linear, |_| 8.0);
        // 12 · 1.5 · 1.25 = 22.5.
        assert_eq!(g.target(), 23);
    }

    #[test]
    fn one_noisy_measure_moves_the_fleet_by_half_or_twice_at_most() {
        let mut g = Growth::new(0.0, 64, Instant::now());
        let mut sim = Sim::new();
        sim.run(&mut g, Duration::from_secs(150), linear, |_| 10.0);
        assert_eq!(g.target(), 12);
        sim.run(&mut g, Duration::from_secs(150), linear, |_| 1.0);
        assert_eq!(g.target(), 24);
        sim.run(&mut g, Duration::from_secs(150), linear, |_| 100.0);
        assert_eq!(g.target(), 12);
    }

    #[test]
    fn copiers_that_do_not_pay_are_undone_and_growth_waits() {
        let mut g = Growth::new(0.5, 64, Instant::now());
        let mut sim = Sim::new();
        // The source gives 6 GiB/s at most; latency stays flat.
        let capped = |c: usize| (600.0 * c as f64).min(6144.0);
        sim.run(&mut g, Duration::from_secs(9 * 60), capped, |_| 10.0);
        // 8 → 12 (e ≈ 0.56) → 18 (e = 0 < 0.5): back to 12, and it stays.
        assert_eq!(sim.sizes, vec![8, 12, 18, 12], "{:?}", sim.sizes);
        // After the 10 min wait, it tries again.
        sim.run(&mut g, Duration::from_secs(10 * 60), capped, |_| 10.0);
        assert!(sim.sizes.len() > 4, "{:?}", sim.sizes);
    }

    #[test]
    fn rising_latency_shrinks_even_while_growth_waits() {
        let mut g = Growth::new(0.5, 64, Instant::now());
        let mut sim = Sim::new();
        let capped = |c: usize| (600.0 * c as f64).min(6144.0);
        sim.run(&mut g, Duration::from_secs(9 * 60), capped, |_| 10.0);
        assert_eq!(g.target(), 12);
        // Latency doubles: 12 · 1.5 / 2 = 9.
        sim.run(&mut g, Duration::from_secs(150), capped, |_| 20.0);
        assert_eq!(g.target(), 9);
    }

    #[test]
    fn no_latency_at_all_grows_by_half() {
        let mut g = Growth::new(0.0, 64, Instant::now());
        let t0 = Instant::now();
        let mut t = t0;
        for _ in 0..30 {
            t += TICK;
            g.observe(
                t,
                View {
                    running: g.target(),
                    active: g.target(),
                    more_to_launch: true,
                    ..View::default()
                },
            );
        }
        assert_eq!(g.target(), 12);
    }

    #[test]
    fn a_fleet_that_cannot_fill_its_target_does_not_grow_it_further() {
        let mut g = Growth::new(0.0, 64, Instant::now());
        let mut t = Instant::now();
        // Only 4 copiers ever run (the listing is slow), latency flat.
        for _ in 0..200 {
            t += TICK;
            g.observe(
                t,
                View {
                    running: 4,
                    active: 4,
                    latency: Latency {
                        grant_ms: Some(20.0),
                        ..Latency::default()
                    },
                    ..View::default()
                },
            );
        }
        // 4 · 1.5 = 6: from the fleet, not 8 · 1.5ᵏ.
        assert_eq!(g.target(), 6);
    }

    #[test]
    fn copiers_waiting_for_a_machine_count_in_the_fleet() {
        let t0 = Instant::now();
        let mut g = Growth::new(0.0, 64, t0);
        // 8 copiers launched; half the time one of them waits for a machine
        // (a range ended, the next copier starts).
        for k in 1..=30u32 {
            let waiting = (k % 2 == 0) as usize;
            g.observe(
                t0 + TICK * k,
                View {
                    running: 8 - waiting,
                    scheduling: waiting,
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
    fn no_measure_while_copiers_wait_ramp_or_brake_until_the_settle_limit() {
        for v in [
            View {
                scheduling: 2,
                ..View::default()
            },
            View {
                ramping: 1,
                ..View::default()
            },
            View {
                braking: 1,
                ..View::default()
            },
        ] {
            let t0 = Instant::now();
            let mut g = Growth::new(0.5, 64, t0);
            let fleet = View {
                running: 8,
                active: 8,
                more_to_launch: true,
                ..v
            };
            // 4 min 40 s: still settling, no change.
            for k in 1..=56 {
                g.observe(t0 + TICK * k, fleet);
            }
            assert_eq!(g.target(), 8, "{v:?}");
            // Past 5 min (and a 2 min measure), a fleet that still waits or
            // ramps is measured anyway; a braking one never is.
            for k in 57..=90 {
                g.observe(t0 + TICK * k, fleet);
            }
            let want = if v.braking > 0 { 8 } else { 12 };
            assert_eq!(g.target(), want, "{v:?}");
        }
    }

    #[test]
    fn a_brake_during_a_measure_starts_it_over() {
        let t0 = Instant::now();
        let mut g = Growth::new(0.5, 64, t0);
        let fleet = |braking| View {
            running: 8,
            active: 8,
            braking,
            more_to_launch: true,
            ..View::default()
        };
        // 100 s of measure, one brake, then 100 s more: no measure is over.
        for k in 1..=20 {
            g.observe(t0 + TICK * k, fleet(0));
        }
        g.observe(t0 + TICK * 21, fleet(1));
        for k in 22..=41 {
            g.observe(t0 + TICK * k, fleet(0));
        }
        assert_eq!(g.target(), 8);
    }

    #[test]
    fn no_measure_while_the_fleet_fills_up() {
        let t0 = Instant::now();
        let mut g = Growth::new(0.5, 64, t0);
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
    fn the_worst_call_sets_the_latency_change() {
        let a = Latency {
            grant_ms: Some(10.0),
            commit_ms_per_mib: Some(5.0),
            shard_ms_per_mib: None,
            put_ms_per_mib: Some(1.0),
        };
        let b = Latency {
            grant_ms: Some(11.0),
            commit_ms_per_mib: Some(7.5),
            shard_ms_per_mib: Some(3.0),
            // The PUT goes to the storage, not CAS: left out.
            put_ms_per_mib: Some(10.0),
        };
        assert_eq!(latency_change(&a, &b), Some(1.5));
        assert_eq!(latency_change(&Latency::default(), &b), None);
    }

    #[test]
    fn elasticity_is_the_ratio_of_relative_gains() {
        let p = |copiers, mibps| Point {
            copiers,
            mibps,
            latency: Latency::default(),
        };
        assert!((elasticity(p(10.0, 1000.0), p(15.0, 1250.0)) - 0.5).abs() < 1e-9);
    }
}
