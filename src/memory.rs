//! Copier memory sizing: fit `--parallel-files` and `--s3-part-concurrency`
//! into the memory limit of the container the copier runs in.
//!
//! The copier holds whole S3 parts in memory while it reads ahead of the xet
//! cleaner, so its worst case grows with parallel files × parts per file ×
//! part size. The planner does not know the RAM of the machine a copier lands
//! on, so the copier reads its own cgroup limit at startup and lowers the two
//! knobs until the worst case fits. It never raises them.

use serde::Serialize;
use tracing::{info, warn};

use crate::sync::read_ahead_depth;

/// cgroup v2: the memory limit of the cgroup the process runs in. The kernel
/// OOM-kills the container above this. The value `max` means no limit.
const CGROUP_V2_MEMORY_MAX: &str = "/sys/fs/cgroup/memory.max";
/// cgroup v1 equivalent. "No limit" is a huge value (close to i64::MAX,
/// rounded down to a page), not a keyword.
const CGROUP_V1_MEMORY_LIMIT: &str = "/sys/fs/cgroup/memory/memory.limit_in_bytes";
/// A cgroup v1 limit at or above this means "no limit" (1 EiB: no machine
/// has that much RAM).
const CGROUP_V1_NO_LIMIT: u64 = 1 << 60;

/// Share of the memory limit the per-file buffers may use. The rest is left
/// for memory that does not scale with our two knobs: the xorbs being
/// uploaded (the xet client uploads up to 64 in parallel by default, 124 in
/// high-performance mode, each up to 64 MiB: 4 to 8 GiB), the listing queue,
/// the HTTP stack, and freed part
/// buffers the allocator has not returned to the system yet. On a 32 GB
/// machine that leaves ~12 GiB.
const BUDGET_PERCENT: u64 = 60;

/// Xet formation window per active file: the xorb being filled (up to 64 MiB)
/// plus one cut xorb waiting for an upload slot (up to 64 MiB).
const XORB_WINDOW_BYTES: u64 = 2 * 64 * 1024 * 1024;

/// The effective copy settings and how they were chosen.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Sizing {
    /// Memory limit found in the cgroup. `None` = no limit (no clamp).
    pub limit_bytes: Option<u64>,
    /// `BUDGET_PERCENT` of the limit. `None` when there is no limit.
    pub budget_bytes: Option<u64>,
    /// Requested values (after the `max(1)` the copier always applies).
    pub requested_parallel_files: usize,
    pub requested_s3_part_concurrency: usize,
    /// Values the copier runs with.
    pub parallel_files: usize,
    pub s3_part_concurrency: usize,
    /// Worst-case memory of the requested and the effective values.
    pub requested_worst_case_bytes: u64,
    pub worst_case_bytes: u64,
}

impl Sizing {
    pub fn clamped(&self) -> bool {
        self.parallel_files != self.requested_parallel_files
            || self.s3_part_concurrency != self.requested_s3_part_concurrency
    }

    /// False only when even 1 file × 1 GET is above the budget.
    pub fn fits(&self) -> bool {
        self.budget_bytes
            .is_none_or(|budget| self.worst_case_bytes <= budget)
    }

    /// Log the settings once at startup, and print them as a `SIZING` marker
    /// line (JSON) for tools that read the copier's log.
    pub fn report(&self) {
        let gib = |b: u64| format!("{:.1}", b as f64 / (1u64 << 30) as f64);
        let pf = format!(
            "{} → {}",
            self.requested_parallel_files, self.parallel_files
        );
        let s3pc = format!(
            "{} → {}",
            self.requested_s3_part_concurrency, self.s3_part_concurrency
        );
        let worst = format!(
            "{} → {}",
            gib(self.requested_worst_case_bytes),
            gib(self.worst_case_bytes)
        );
        match (self.limit_bytes, self.budget_bytes) {
            (Some(limit), Some(budget)) if !self.fits() => warn!(
                limit_gib = %gib(limit),
                budget_gib = %gib(budget),
                parallel_files = %pf,
                s3_part_concurrency = %s3pc,
                worst_case_gib = %worst,
                "memory limit is below one file in flight; running with the smallest settings"
            ),
            (Some(limit), Some(budget)) if self.clamped() => info!(
                limit_gib = %gib(limit),
                budget_gib = %gib(budget),
                parallel_files = %pf,
                s3_part_concurrency = %s3pc,
                worst_case_gib = %worst,
                "copy settings lowered to fit the memory limit"
            ),
            (Some(limit), Some(budget)) => info!(
                limit_gib = %gib(limit),
                budget_gib = %gib(budget),
                parallel_files = self.parallel_files,
                s3_part_concurrency = self.s3_part_concurrency,
                worst_case_gib = %gib(self.worst_case_bytes),
                "copy settings fit the memory limit"
            ),
            _ => info!(
                parallel_files = self.parallel_files,
                s3_part_concurrency = self.s3_part_concurrency,
                worst_case_gib = %gib(self.worst_case_bytes),
                "no memory limit found; copy settings as requested"
            ),
        }
        println!(
            "SIZING {}",
            serde_json::to_string(self).expect("Sizing serializes")
        );
    }
}

/// Worst-case memory of one file in flight. A multipart read (more than one
/// GET per file) holds `part_concurrency` parts in the parallel GETs, up to
/// `read_ahead_depth` finished parts in the queue to the cleaner, one part the
/// reader is waiting to queue and one part the cleaner is working on. A
/// single-GET read streams: only the chunk the cleaner is working on. Both
/// paths add the xet formation window.
pub fn per_file_bytes(part_concurrency: usize, part_size: u64) -> u64 {
    let parts = if part_concurrency <= 1 {
        1
    } else {
        part_concurrency + read_ahead_depth(part_concurrency) + 2
    };
    (parts as u64)
        .saturating_mul(part_size)
        .saturating_add(XORB_WINDOW_BYTES)
}

fn worst_case_bytes(parallel_files: usize, part_concurrency: usize, part_size: u64) -> u64 {
    (parallel_files as u64).saturating_mul(per_file_bytes(part_concurrency, part_size))
}

/// Pick the copy settings for a memory limit. Without a limit, keep the
/// requested values. With one, lower `part_concurrency` first (down to 1),
/// then `parallel_files` (down to 1), until the worst case fits the budget.
/// Parts per file go first: a file with fewer parts reads slower, but many
/// files in flight still keep the network busy.
pub fn fit(
    limit_bytes: Option<u64>,
    parallel_files: usize,
    part_concurrency: usize,
    part_size: u64,
) -> Sizing {
    let requested_pf = parallel_files.max(1);
    let requested_pc = part_concurrency.max(1);
    let requested_worst = worst_case_bytes(requested_pf, requested_pc, part_size);
    let budget = limit_bytes.map(|l| l / 100 * BUDGET_PERCENT);

    let (pf, pc) = match budget {
        None => (requested_pf, requested_pc),
        Some(budget) => {
            let fits = |pf, pc| worst_case_bytes(pf, pc, part_size) <= budget;
            match (1..=requested_pc).rev().find(|&pc| fits(requested_pf, pc)) {
                Some(pc) => (requested_pf, pc),
                None => {
                    let pf = (1..=requested_pf).rev().find(|&pf| fits(pf, 1));
                    (pf.unwrap_or(1), 1)
                }
            }
        }
    };

    Sizing {
        limit_bytes,
        budget_bytes: budget,
        requested_parallel_files: requested_pf,
        requested_s3_part_concurrency: requested_pc,
        parallel_files: pf,
        s3_part_concurrency: pc,
        requested_worst_case_bytes: requested_worst,
        worst_case_bytes: worst_case_bytes(pf, pc, part_size),
    }
}

/// The memory limit of this process's cgroup, `None` if there is none (or
/// if the files cannot be read, e.g. not on Linux). A cgroup v2 file wins
/// over v1: when it exists, its answer is final.
pub fn cgroup_memory_limit() -> Option<u64> {
    match std::fs::read_to_string(CGROUP_V2_MEMORY_MAX) {
        Ok(content) => parse_cgroup_v2(&content),
        Err(_) => std::fs::read_to_string(CGROUP_V1_MEMORY_LIMIT)
            .ok()
            .and_then(|content| parse_cgroup_v1(&content)),
    }
}

/// Parse cgroup v2 `memory.max`: a byte count, or `max` for no limit.
pub fn parse_cgroup_v2(content: &str) -> Option<u64> {
    match content.trim() {
        "max" => None,
        value => value.parse().ok(),
    }
}

/// Parse cgroup v1 `memory.limit_in_bytes`: a byte count, huge when unset.
pub fn parse_cgroup_v1(content: &str) -> Option<u64> {
    content
        .trim()
        .parse()
        .ok()
        .filter(|&limit| limit < CGROUP_V1_NO_LIMIT)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;
    const PART: u64 = 16 * MIB;

    #[test]
    fn cgroup_v2_max_means_no_limit() {
        assert_eq!(parse_cgroup_v2("max\n"), None);
        assert_eq!(parse_cgroup_v2("34359738368\n"), Some(32 * GIB));
        assert_eq!(parse_cgroup_v2(""), None);
        assert_eq!(parse_cgroup_v2("garbage"), None);
    }

    #[test]
    fn cgroup_v1_huge_value_means_no_limit() {
        assert_eq!(parse_cgroup_v1("9223372036854771712\n"), None);
        assert_eq!(parse_cgroup_v1("17179869184\n"), Some(16 * GIB));
        assert_eq!(parse_cgroup_v1(""), None);
    }

    #[test]
    fn per_file_counts_the_read_ahead_queue_and_the_xorb_window() {
        // 128 GETs + 32 queued (the queue is capped) + 2 in hand.
        assert_eq!(per_file_bytes(128, PART), 162 * PART + 128 * MIB);
        // Below the cap the queue is as deep as the GETs are wide.
        assert_eq!(per_file_bytes(8, PART), 18 * PART + 128 * MIB);
        // A single GET streams.
        assert_eq!(per_file_bytes(1, PART), PART + 128 * MIB);
    }

    #[test]
    fn no_limit_keeps_the_requested_values() {
        let s = fit(None, 32, 128, PART);
        assert_eq!((s.parallel_files, s.s3_part_concurrency), (32, 128));
        assert!(!s.clamped());
        assert!(s.fits());
    }

    #[test]
    fn big_file_sizing_on_256_gb_is_not_clamped() {
        // 32 × (162 × 16 MiB + 128 MiB) ≈ 85 GiB, far below 60% of 256 GB.
        let s = fit(Some(256 * GIB), 32, 128, PART);
        assert_eq!((s.parallel_files, s.s3_part_concurrency), (32, 128));
        assert!(!s.clamped());
    }

    #[test]
    fn big_file_sizing_on_32_gb_lowers_part_concurrency_first() {
        let s = fit(Some(32 * GIB), 32, 128, PART);
        assert!(s.clamped());
        assert_eq!(s.parallel_files, 32);
        assert_eq!(s.s3_part_concurrency, 14);
        assert!(s.worst_case_bytes <= s.budget_bytes.unwrap());
        // One more GET per file would not fit.
        assert!(worst_case_bytes(32, 15, PART) > s.budget_bytes.unwrap());
        assert!(s.requested_worst_case_bytes > 32 * GIB);
    }

    #[test]
    fn parallel_files_go_down_once_part_concurrency_is_1() {
        // 128 files at 1 GET each need 128 × 144 MiB = 18 GiB: too much for 16 GB.
        let s = fit(Some(16 * GIB), 128, 8, PART);
        assert_eq!(s.s3_part_concurrency, 1);
        assert_eq!(s.parallel_files, 68);
        assert!(s.fits());
        assert!(worst_case_bytes(69, 1, PART) > s.budget_bytes.unwrap());
    }

    #[test]
    fn values_are_never_raised() {
        let s = fit(Some(256 * GIB), 4, 2, PART);
        assert_eq!((s.parallel_files, s.s3_part_concurrency), (4, 2));
        // 0 means 1, as in the copier.
        let s = fit(Some(256 * GIB), 0, 0, PART);
        assert_eq!((s.parallel_files, s.s3_part_concurrency), (1, 1));
    }

    #[test]
    fn a_limit_below_one_file_runs_one_file_and_says_it_does_not_fit() {
        let s = fit(Some(100 * MIB), 32, 128, PART);
        assert_eq!((s.parallel_files, s.s3_part_concurrency), (1, 1));
        assert!(!s.fits());
    }
}
