//! The economic brake of the planner: grow the fleet by steps, and stop when
//! a step no longer pays.
//!
//! A copier is billed by the minute. One that adds little throughput (CAS
//! braking, a slow source, the destination's commit budget...) only costs
//! money. So the planner measures the **elasticity** of the fleet after each
//! step:
//!
//! `e = (% more throughput) / (% more copiers)`
//!
//! - e ≥ `--min-elasticity`: next step, +50 % copiers (at least +1).
//! - e below it: the last step did not pay. Back to the size before it (the
//!   copiers above it are not replaced when they finish), and hold.
//!
//! After a stop it tries one step again every [`REPROBE_AFTER`]: CAS adds
//! capacity, the source changes.
//!
//! This is NOT the safety brake: on a saturated service shared with other
//! users, our elasticity stays high (we take a bigger share of it, at their
//! cost). The safety brake is in each copier (`brake.rs`).
//!
//! A measure needs a settled fleet: no copier waiting for a machine, ramping
//! up its rate, or braking. Then it averages the fleet throughput for
//! [`MEASURE_FOR`].

use std::time::{Duration, Instant};
use tracing::info;

/// The first step: a small fleet, and enough copiers to fill the brake's
/// fast start.
pub const INITIAL_COPIERS: usize = 8;
/// Each step adds this share of the copiers (at least one).
const STEP: f64 = 0.5;
/// Average the fleet throughput over this time.
const MEASURE_FOR: Duration = Duration::from_secs(60);
/// A fleet still ramping after this long is measured anyway (its elasticity
/// then reads a bit high; the safety brake is still there).
const SETTLE_AT_MOST: Duration = Duration::from_secs(5 * 60);
/// After a stop, try one more step this often.
const REPROBE_AFTER: Duration = Duration::from_secs(10 * 60);
/// Fewer added copiers than this (on average) cannot be judged.
const MIN_ADDED: f64 = 0.5;

/// What the planner sees of its fleet, from the copiers' logs.
#[derive(Debug, Clone, Copy, Default)]
pub struct View {
    pub running: usize,
    pub scheduling: usize,
    /// Copiers whose brake is raising their rate.
    pub ramping: usize,
    /// Copiers whose brake cut their rate a short time ago.
    pub braking: usize,
    /// Bytes/s fed to xet by the fleet, MiB/s.
    pub mibps: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Next {
    /// Measure the fleet as it is, then step.
    Base,
    /// Measure the step just taken, then judge it.
    Judge,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum State {
    Settle {
        since: Instant,
        next: Next,
    },
    Measure {
        since: Instant,
        next: Next,
        copiers: f64,
        mibps: f64,
        n: u32,
    },
    Hold {
        since: Instant,
    },
}

/// A measured point: average copiers running and fleet MiB/s.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Point {
    copiers: f64,
    mibps: f64,
}

pub struct Growth {
    min_elasticity: f64,
    max: usize,
    target: usize,
    state: State,
    last: Option<Point>,
}

impl Growth {
    /// `min_elasticity` 0 = no economic brake: `max` copiers at once.
    pub fn new(min_elasticity: f64, max: usize, now: Instant) -> Self {
        let max = max.max(1);
        let off = min_elasticity <= 0.0;
        Self {
            min_elasticity,
            max,
            target: if off { max } else { INITIAL_COPIERS.min(max) },
            state: State::Settle {
                since: now,
                next: Next::Base,
            },
            last: None,
        }
    }

    /// How many copiers may run now.
    pub fn target(&self) -> usize {
        self.target
    }

    fn off(&self) -> bool {
        self.min_elasticity <= 0.0
    }

    /// One more step, if there is room. Returns false at the cap.
    fn step_up(&mut self, now: Instant, why: &str) -> bool {
        if self.target >= self.max {
            return false;
        }
        let next = ((self.target as f64 * (1.0 + STEP)).ceil() as usize)
            .max(self.target + 1)
            .min(self.max);
        info!(
            from = self.target,
            to = next,
            "growth: more copiers ({why})"
        );
        self.target = next;
        self.state = State::Settle {
            since: now,
            next: Next::Judge,
        };
        true
    }

    fn hold(&mut self, now: Instant) {
        self.state = State::Hold { since: now };
    }

    /// Read the fleet (every few seconds).
    pub fn observe(&mut self, now: Instant, v: View) {
        if self.off() {
            return;
        }
        let settled = v.scheduling == 0 && v.ramping == 0 && v.braking == 0;
        self.state = match self.state {
            State::Hold { since } => {
                if now.duration_since(since) >= REPROBE_AFTER && self.target < self.max {
                    State::Settle {
                        since: now,
                        next: Next::Base,
                    }
                } else {
                    State::Hold { since }
                }
            }
            State::Settle { since, next } => {
                // Braking always waits: the planner launches nothing then.
                let late = now.duration_since(since) >= SETTLE_AT_MOST && v.braking == 0;
                if settled || late {
                    State::Measure {
                        since: now,
                        next,
                        copiers: v.running as f64,
                        mibps: v.mibps,
                        n: 1,
                    }
                } else {
                    State::Settle { since, next }
                }
            }
            State::Measure {
                since,
                next,
                copiers,
                mibps,
                n,
            } => {
                if v.braking > 0 {
                    // A brake moved the throughput: start over.
                    State::Settle { since: now, next }
                } else if now.duration_since(since) < MEASURE_FOR {
                    State::Measure {
                        since,
                        next,
                        copiers: copiers + v.running as f64,
                        mibps: mibps + v.mibps,
                        n: n + 1,
                    }
                } else {
                    let n = f64::from(n + 1);
                    let point = Point {
                        copiers: (copiers + v.running as f64) / n,
                        mibps: (mibps + v.mibps) / n,
                    };
                    self.measured(now, next, point);
                    return;
                }
            }
        };
    }

    fn measured(&mut self, now: Instant, next: Next, p: Point) {
        let prev = self.last.replace(p);
        let judged = match (next, prev) {
            (Next::Judge, Some(prev)) if p.copiers - prev.copiers >= MIN_ADDED => {
                Some(elasticity(prev, p))
            }
            _ => None,
        };
        let Some(e) = judged else {
            // A base, or a step the fleet could not take yet (no more ranges
            // listed, the commit budget): measure again.
            if next == Next::Base {
                if !self.step_up(now, "base measured") {
                    self.hold(now);
                }
            } else {
                self.state = State::Settle { since: now, next };
            }
            return;
        };
        let e_txt = format!("{e:.2}");
        if e >= self.min_elasticity {
            if !self.step_up(now, &format!("elasticity {e_txt}")) {
                self.hold(now);
            }
            return;
        }
        // Judged, so `prev` is there: back to the last size that paid.
        let keep = prev.map_or(p.copiers, |prev| prev.copiers).round().max(1.0) as usize;
        info!(
            elasticity = e_txt,
            min = self.min_elasticity,
            copiers = keep,
            mibps = format!("{:.0}", p.mibps),
            "growth: the last copiers did not pay; holding"
        );
        self.target = keep.min(self.max);
        self.hold(now);
    }
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

    /// Run the growth over a fleet whose throughput is `per(copiers)`, with
    /// the copiers starting at once, until `until`.
    fn simulate(
        g: &mut Growth,
        t0: Instant,
        until: Duration,
        per: impl Fn(usize) -> f64,
    ) -> Instant {
        let mut t = t0;
        while t.duration_since(t0) < until {
            t += TICK;
            let running = g.target();
            g.observe(
                t,
                View {
                    running,
                    mibps: per(running),
                    ..View::default()
                },
            );
        }
        t
    }

    #[test]
    fn off_means_every_copier_at_once() {
        let g = Growth::new(0.0, 32, Instant::now());
        assert_eq!(g.target(), 32);
    }

    #[test]
    fn a_fleet_that_scales_grows_to_the_cap() {
        let t0 = Instant::now();
        let mut g = Growth::new(0.5, 32, t0);
        assert_eq!(g.target(), 8);
        // Each copier adds 600 MiB/s: e = 1 at every step.
        simulate(&mut g, t0, Duration::from_secs(10 * 60), |c| {
            600.0 * c as f64
        });
        assert_eq!(g.target(), 32);
    }

    #[test]
    fn growth_stops_where_copiers_no_longer_pay() {
        let t0 = Instant::now();
        let mut g = Growth::new(0.5, 64, t0);
        // A source capped at 10 GiB/s: 600 MiB/s per copier up to ~17.
        let per = |c: usize| (600.0 * c as f64).min(10_240.0);
        simulate(&mut g, t0, Duration::from_secs(15 * 60), per);
        // 8 → 12 (e = 1) → 18 (e ≈ 0.84) → 27 (e = 0): back to 18.
        assert_eq!(g.target(), 18);
    }

    #[test]
    fn a_step_that_loses_throughput_goes_back() {
        let t0 = Instant::now();
        let mut g = Growth::new(0.5, 64, t0);
        // More than 8 copiers make the whole fleet slower.
        let per = |c: usize| if c <= 8 { 600.0 * c as f64 } else { 4000.0 };
        simulate(&mut g, t0, Duration::from_secs(5 * 60), per);
        assert_eq!(g.target(), 8);
    }

    #[test]
    fn after_a_stop_it_tries_again_later() {
        let t0 = Instant::now();
        let mut g = Growth::new(0.5, 64, t0);
        let capped = |c: usize| (600.0 * c as f64).min(10_240.0);
        let t = simulate(&mut g, t0, Duration::from_secs(15 * 60), capped);
        assert_eq!(g.target(), 18);
        // The source got faster: after the reprobe delay, growth goes on.
        simulate(&mut g, t, Duration::from_secs(15 * 60), |c| {
            600.0 * c as f64
        });
        assert!(g.target() > 18, "{}", g.target());
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
                mibps: 4800.0,
                ..v
            };
            // 4 min 40 s: still settling, no step.
            for k in 1..=56 {
                g.observe(t0 + TICK * k, fleet);
            }
            assert_eq!(g.target(), 8, "{v:?}");
            // Past 5 min, a fleet that still waits or ramps is measured
            // anyway; a braking one never is.
            for k in 57..=80 {
                g.observe(t0 + TICK * k, fleet);
            }
            let want = if v.braking > 0 { 8 } else { 12 };
            assert_eq!(g.target(), want, "{v:?}");
        }
    }

    #[test]
    fn a_fleet_braking_for_good_is_never_measured() {
        let t0 = Instant::now();
        let mut g = Growth::new(0.5, 64, t0);
        for k in 1..=200 {
            g.observe(
                t0 + TICK * k,
                View {
                    running: 8,
                    braking: 1,
                    mibps: 4800.0,
                    ..View::default()
                },
            );
        }
        assert_eq!(g.target(), 8);
    }

    #[test]
    fn a_step_the_fleet_could_not_take_is_measured_again() {
        let t0 = Instant::now();
        let mut g = Growth::new(0.5, 64, t0);
        // Only 8 ranges ever run (the listing is slow): no judgement, no stop.
        let mut t = t0;
        for _ in 0..200 {
            t += TICK;
            g.observe(
                t,
                View {
                    running: 8,
                    mibps: 4800.0,
                    ..View::default()
                },
            );
        }
        assert_eq!(g.target(), 12);
    }

    #[test]
    fn elasticity_is_the_ratio_of_relative_gains() {
        let a = Point {
            copiers: 10.0,
            mibps: 1000.0,
        };
        let b = Point {
            copiers: 15.0,
            mibps: 1250.0,
        };
        assert!((elasticity(a, b) - 0.5).abs() < 1e-9);
    }
}
