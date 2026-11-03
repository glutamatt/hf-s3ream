//! Sizing a copy from its dry-run: which copier flavor, how big the ranges,
//! how many copiers at once, how long the planner may run, and what it should
//! cost. The dry-run prints the result as a `DRYRUN_PLAN` line, so every
//! client (the web Space, the `hf s3ream` CLI) launches the same plan from the
//! same numbers — and those numbers ship with the image they size.
//!
//! The figures come from the 2026-07-12 flavor test (403 Jobs, same copy
//! config): the sustained S3 read of one copier is set by its FLAVOR —
//! cpu-performance ~1348 MiB/s (dedicated NIC), cpu-xl ~477, cpu-upgrade ~16
//! and cpu-basic ~8 (small flavors share hosts and stall for minutes on the
//! NIC). At those speeds cpu-performance is also the cheapest per byte moved,
//! so it wins once the copy is bigger than the ~75 s startup cost of a copier
//! (schedule + image pull + ramp). Below ~5 GiB, even cpu-upgrade finishes
//! inside that startup window: one cheap copier is enough.

use serde::Serialize;

/// Per-copier sustained S3 read, MiB/s, by flavor.
fn flavor_mibps(flavor: &str) -> Option<f64> {
    match flavor {
        "cpu-performance" => Some(1300.0),
        "cpu-xl" => Some(450.0),
        "cpu-upgrade" => Some(16.0),
        "cpu-basic" => Some(8.0),
        _ => None,
    }
}

/// Hourly price in USD, by flavor (HF Jobs price list).
fn flavor_usd_hr(flavor: &str) -> Option<f64> {
    match flavor {
        "cpu-performance" => Some(1.9),
        "cpu-xl" => Some(1.0),
        "cpu-upgrade" => Some(0.03),
        "cpu-basic" => Some(0.01),
        _ => None,
    }
}

/// Startup cost of one copier, seconds (schedule + image pull + ramp).
const OVERHEAD_S: f64 = 75.0;
/// Below this many GiB, the copy is all startup: a cheap flavor is enough.
const PERF_MIN_GIB: f64 = 5.0;
/// The destination accepts ~1000 file registrations/s per destination repo
/// and throttles beyond. A copy of many small files is bound by this, not by
/// bandwidth.
const COMMIT_FPS: f64 = 1000.0;
/// Longest planner timeout proposed. The Space sign-in lasts 60 h, so this
/// stays well below it.
const MAX_TIMEOUT_H: f64 = 48.0;
/// Jobs are billed per started clock minute: a Job pays about one minute more
/// than it runs.
const BILLING_ROUND_S: f64 = 60.0;
/// The planner only lists and orchestrates: the cheapest flavor.
pub const PLANNER_FLAVOR: &str = "cpu-basic";
/// Delay between two copier launches, so image pulls do not all start at once.
pub const LAUNCH_STAGGER_MS: u64 = 1500;

/// The planner settings proposed for a source.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Plan {
    /// `--copier-flavor`
    pub copier_flavor: &'static str,
    /// `--range-gib`
    pub range_gib: u64,
    /// `--max-inflight`
    pub max_inflight: u64,
    /// The planner Job's timeout, in hours. It lives until every copier ends.
    pub timeout_h: u64,
}

/// What a plan should take and cost.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Estimate {
    pub copiers: u64,
    /// Expected wall-clock, seconds.
    pub wall_s: f64,
    /// True when the file registrations, not the bytes, set the wall-clock.
    pub commit_bound: bool,
    /// Expected compute cost in USD: every copier and the planner, with the
    /// per-minute rounding.
    pub usd: f64,
}

/// The plan for `gib` GiB in `files` files.
pub fn recommend(gib: f64, files: u64) -> Plan {
    let mut r = if gib < PERF_MIN_GIB {
        Plan {
            copier_flavor: "cpu-upgrade",
            range_gib: gib.ceil().max(1.0) as u64,
            max_inflight: 1,
            timeout_h: 2,
        }
    } else {
        // ~64 GiB ranges keep each copier busy for ≥ ~50 s of pure transfer
        // (more than its startup cost) while fanning out wide; at most 256
        // copiers, 128 at once.
        let target_copiers = (gib / 64.0).ceil().clamp(1.0, 256.0);
        let range_gib = (gib / target_copiers).ceil().max(8.0);
        let inflight = target_copiers.min(128.0);
        // The planner lives until every copier ends: its timeout must outlast
        // the whole copy. Generous on purpose — it runs on cpu-basic and ends
        // as soon as the copy is done.
        let agg_mibps = inflight * flavor_mibps("cpu-performance").unwrap();
        let est_s = 900.0 + (gib * 1024.0 / agg_mibps) * 3.0;
        let hours = (est_s / 3600.0).ceil().clamp(2.0, MAX_TIMEOUT_H);
        Plan {
            copier_flavor: "cpu-performance",
            range_gib: range_gib as u64,
            max_inflight: inflight as u64,
            timeout_h: hours as u64,
        }
    };
    // Commit-bound: when the registrations (files / 1000 s) take longer than
    // the bytes AND the files are really small (average ≤ 4 MiB — the
    // planner's own test), a wide fleet only pays startup costs: the planner
    // holds such copiers at 2 at once anyway. Propose the smallest fleet that
    // still moves the bytes within the commit time (≥ 2, which uses the whole
    // files/s budget), on the cheap flavor when even that one keeps up.
    let commit_s = files as f64 / COMMIT_FPS;
    let copiers = (gib / r.range_gib as f64).ceil().max(1.0);
    let flavor_speed = flavor_mibps(r.copier_flavor).unwrap();
    let byte_s = (gib * 1024.0) / ((r.max_inflight as f64).min(copiers) * flavor_speed);
    let commit_heavy = files > 0 && (gib * 1024.0) / files as f64 <= 4.0;
    if commit_heavy && commit_s > byte_s {
        if (gib * 1024.0) / (2.0 * flavor_mibps("cpu-upgrade").unwrap()) <= commit_s {
            r.copier_flavor = "cpu-upgrade";
        }
        let speed = flavor_mibps(r.copier_flavor).unwrap();
        let need = ((gib * 1024.0) / (speed * commit_s)).ceil().max(2.0);
        r.max_inflight = r.max_inflight.min(need as u64);
        // ~2 ranges per slot for balance; the one extra wave costs 75 s vs hours.
        let balanced = (gib / (r.max_inflight as f64 * 2.0)).ceil();
        let balanced = if balanced == 0.0 { 1.0 } else { balanced };
        r.range_gib = r.range_gib.max(balanced as u64);
        r.timeout_h = ((900.0 + commit_s * 3.0) / 3600.0)
            .ceil()
            .clamp(2.0, MAX_TIMEOUT_H) as u64;
    }
    r
}

/// Expected wall-clock and cost of `plan` for `gib` GiB in `files` files.
/// None for a copier flavor with no known speed or price.
pub fn estimate(gib: f64, plan: &Plan, files: u64) -> Option<Estimate> {
    let speed = flavor_mibps(plan.copier_flavor)?;
    let copier_usd_hr = flavor_usd_hr(plan.copier_flavor)?;
    let planner_usd_hr = flavor_usd_hr(PLANNER_FLAVOR)?;
    let inflight = plan.max_inflight.max(1) as f64;
    let copiers = (gib / plan.range_gib.max(1) as f64).ceil().max(1.0);
    let waves = (copiers / inflight).ceil();
    let commit_s = files as f64 / COMMIT_FPS;
    let byte_wall_s = waves * OVERHEAD_S + (gib * 1024.0) / (inflight.min(copiers) * speed);
    let wall_s = byte_wall_s.max(commit_s);
    let commit_bound = commit_s > byte_wall_s;
    // Commit-bound: ~2 copiers stay busy for the whole commit time — unless
    // the bytes keep the fleet busy even longer (mixed buckets).
    let busy_s = ((gib * 1024.0) / speed).max(if commit_bound { 2.0 * commit_s } else { 0.0 });
    let usd = (copier_usd_hr / 3600.0) * (copiers * (OVERHEAD_S + BILLING_ROUND_S) + busy_s)
        + (planner_usd_hr / 3600.0) * (wall_s + OVERHEAD_S + BILLING_ROUND_S);
    Some(Estimate {
        copiers: copiers as u64,
        wall_s,
        commit_bound,
        usd,
    })
}

/// The `DRYRUN_PLAN` line: the plan for a source of `bytes` bytes in `files`
/// files, with its estimate and the planner Job settings.
pub fn dry_run_plan_line(bytes: u64, files: u64) -> serde_json::Value {
    let gib = bytes as f64 / (1u64 << 30) as f64;
    let plan = recommend(gib, files);
    let est = estimate(gib, &plan, files);
    serde_json::json!({
        "copier_flavor": plan.copier_flavor,
        "range_gib": plan.range_gib,
        "max_inflight": plan.max_inflight,
        "launch_stagger_ms": LAUNCH_STAGGER_MS,
        "planner_flavor": PLANNER_FLAVOR,
        "timeout_s": plan.timeout_h * 3600,
        "copiers": est.as_ref().map(|e| e.copiers),
        "wall_s": est.as_ref().map(|e| e.wall_s.ceil() as u64),
        "commit_bound": est.as_ref().map(|e| e.commit_bound),
        "usd": est.as_ref().map(|e| (e.usd * 10_000.0).round() / 10_000.0),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: f64 = 1.0;
    const TIB: f64 = 1024.0;

    fn plan(flavor: &'static str, range_gib: u64, max_inflight: u64, timeout_h: u64) -> Plan {
        Plan {
            copier_flavor: flavor,
            range_gib,
            max_inflight,
            timeout_h,
        }
    }

    // Expected values: the Space's recommend()/estimate() (space/app.js,
    // v0.3.12) run under Node on the same inputs. The two must agree until
    // the Space reads DRYRUN_PLAN instead of computing it.

    #[test]
    fn a_small_copy_gets_one_cheap_copier() {
        assert_eq!(recommend(2.5 * GIB, 40), plan("cpu-upgrade", 3, 1, 2));
        assert_eq!(recommend(0.0, 0), plan("cpu-upgrade", 1, 1, 2));
    }

    #[test]
    fn a_big_copy_fans_out_on_cpu_performance() {
        // CommonCrawl-like: 107.3 TiB in 500,009 files (~225 MiB each).
        assert_eq!(
            recommend(107.3 * TIB, 500_009),
            plan("cpu-performance", 430, 128, 2)
        );
        assert_eq!(
            recommend(200.0 * GIB, 800),
            plan("cpu-performance", 50, 4, 2)
        );
        assert_eq!(
            recommend(500.0 * TIB, 2_000_000),
            plan("cpu-performance", 2000, 128, 3)
        );
    }

    #[test]
    fn many_small_files_are_commit_bound() {
        // 5 M files of ~400 KiB: the registrations, not the bytes, set the time.
        let gib = 5_000_000.0 * 0.4 / 1024.0;
        assert_eq!(
            recommend(gib, 5_000_000),
            plan("cpu-performance", 489, 2, 5)
        );
        let e = estimate(gib, &recommend(gib, 5_000_000), 5_000_000).unwrap();
        assert!(e.commit_bound);
        assert_eq!(e.copiers, 4);
    }

    #[test]
    fn the_estimate_counts_minute_rounding_and_the_planner() {
        let p = recommend(107.3 * TIB, 500_009);
        let e = estimate(107.3 * TIB, &p, 500_009).unwrap();
        assert_eq!(e.copiers, 256);
        assert!(!e.commit_bound);
        assert!((e.usd - 63.92).abs() < 0.01, "usd = {}", e.usd);
    }

    #[test]
    fn an_unknown_flavor_has_no_estimate() {
        assert_eq!(estimate(10.0, &plan("a10g-small", 8, 2, 2), 10), None);
    }

    /// The whole grid of `scripts/reco-parity.mjs` (the Space's own code under
    /// Node): same plan, same copier count, same regime, same cost to the cent.
    #[test]
    #[ignore = "needs RECO_PARITY_JSON from scripts/reco-parity.mjs"]
    fn matches_the_space_on_a_grid() {
        let path = std::env::var("RECO_PARITY_JSON").expect("RECO_PARITY_JSON");
        let cases: Vec<serde_json::Value> =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert!(!cases.is_empty());
        for c in &cases {
            let gib = c["gib"].as_f64().unwrap();
            let files = c["files"].as_u64().unwrap();
            let p = recommend(gib, files);
            assert_eq!(
                serde_json::to_value(&p).unwrap(),
                c["plan"],
                "plan for {gib} GiB / {files} files"
            );
            let e = estimate(gib, &p, files).unwrap();
            assert_eq!(e.copiers, c["copiers"].as_u64().unwrap(), "{gib}/{files}");
            assert_eq!(
                e.commit_bound,
                c["commit_bound"].as_bool().unwrap(),
                "{gib}/{files}"
            );
            let usd = c["usd"].as_f64().unwrap();
            assert!(
                (e.usd - usd).abs() < 0.005,
                "{gib}/{files}: {} vs {usd}",
                e.usd
            );
        }
    }

    #[test]
    fn the_dry_run_line_carries_everything_a_client_launches() {
        let v = dry_run_plan_line(3 * (1 << 30), 3);
        for k in [
            "copier_flavor",
            "range_gib",
            "max_inflight",
            "launch_stagger_ms",
            "planner_flavor",
            "timeout_s",
            "copiers",
            "wall_s",
            "commit_bound",
            "usd",
        ] {
            assert!(v.get(k).is_some(), "missing {k}");
        }
        assert_eq!(v["timeout_s"], 7200);
    }
}
