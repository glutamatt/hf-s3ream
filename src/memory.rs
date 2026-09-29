//! Copier memory sizing: fit `--parallel-files` and `--s3-part-concurrency`
//! into the memory limit of the container the copier runs in.
//!
//! The copier holds whole S3 parts in memory while it reads ahead of the xet
//! cleaner, so its worst case grows with parallel files × parts per file ×
//! part size. The planner does not know the RAM of the machine a copier lands
//! on, so the copier reads its own cgroup limit at startup and lowers the two
//! knobs until the worst case fits. It never raises them.

use serde::Serialize;
use std::path::{Component, Path, PathBuf};
use tracing::{info, warn};

use crate::sync::read_ahead_depth;

/// The cgroups of this process, one line per hierarchy:
/// `hierarchy-id:controllers:path`. cgroup v2 is the line `0::path`.
const PROC_SELF_CGROUP: &str = "/proc/self/cgroup";
/// Where the cgroup v2 tree is mounted. Each cgroup directory holds a
/// `memory.max` file: a byte count, or `max` for no limit.
const CGROUP_V2_ROOT: &str = "/sys/fs/cgroup";
/// Where the cgroup v1 memory tree is mounted. Each cgroup directory holds a
/// `memory.limit_in_bytes` file. "No limit" is a huge value (close to
/// i64::MAX, rounded down to a page), not a keyword.
const CGROUP_V1_MEMORY_ROOT: &str = "/sys/fs/cgroup/memory";
/// A cgroup v1 limit at or above this means "no limit" (1 EiB: no machine
/// has that much RAM).
const CGROUP_V1_NO_LIMIT: u64 = 1 << 60;

/// At most this share of the memory limit goes to the per-file buffers. The
/// rest covers memory that grows with the buffers but is not in the per-file
/// count: freed part buffers the allocator has not returned to the system yet.
const BUDGET_PERCENT: u64 = 60;

/// Largest xorb the xet client builds. Also the unit of its uploads.
const MAX_XORB_BYTES: u64 = 64 * 1024 * 1024;

/// Memory kept out of the budget whatever the limit: the tokio runtime, the
/// S3 and Hub HTTP clients, the listing queue and the pending commit ops.
const BASE_RESERVE_BYTES: u64 = 1024 * 1024 * 1024;

/// Xet formation window per active file: the xorb being filled plus one cut
/// xorb waiting for an upload slot.
const XORB_WINDOW_BYTES: u64 = 2 * MAX_XORB_BYTES;

/// Lowest part concurrency the clamp goes down to. At 1 the copier reads each
/// file with one streamed GET: its request timeout goes from 3 minutes to 1
/// hour, and a failed read restarts the whole file instead of one part. So
/// the clamp lowers the number of parts, but never changes the read mode.
const MIN_MULTIPART_CONCURRENCY: usize = 2;

/// The memory the copier may use for its per-file buffers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Budget {
    /// Memory limit found in the cgroup.
    pub limit_bytes: u64,
    /// Share of the limit the per-file buffers may use (see [`budget_bytes`]).
    pub budget_bytes: u64,
}

/// The effective copy settings and how they were chosen.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Sizing {
    /// `None` = no memory limit found (no clamp).
    pub memory: Option<Budget>,
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

    /// False only when even one file at the lowest part concurrency is above
    /// the budget.
    pub fn fits(&self) -> bool {
        self.memory
            .is_none_or(|m| self.worst_case_bytes <= m.budget_bytes)
    }

    /// Log the settings once at startup, and print them as a `SIZING` marker
    /// line (JSON) for tools that read the copier's log.
    pub fn report(&self) {
        let gib = |b: u64| format!("{:.1}", b as f64 / (1u64 << 30) as f64);
        let (limit, budget) = match self.memory {
            Some(m) => (gib(m.limit_bytes), gib(m.budget_bytes)),
            None => ("none".to_string(), "none".to_string()),
        };
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
        if self.fits() {
            let message = match self.memory {
                None => "no memory limit found; copy settings as requested",
                Some(_) if self.clamped() => "copy settings lowered to fit the memory limit",
                Some(_) => "copy settings fit the memory limit",
            };
            info!(
                limit_gib = %limit,
                budget_gib = %budget,
                parallel_files = %pf,
                s3_part_concurrency = %s3pc,
                worst_case_gib = %worst,
                "{message}"
            );
        } else {
            warn!(
                limit_gib = %limit,
                budget_gib = %budget,
                parallel_files = %pf,
                s3_part_concurrency = %s3pc,
                worst_case_gib = %worst,
                "memory limit is below one file in flight; running with the smallest settings"
            );
        }
        println!(
            "SIZING {}",
            serde_json::to_string(self).expect("Sizing serializes")
        );
    }
}

/// Share of `limit` the per-file buffers may use: [`BUDGET_PERCENT`] of it,
/// or less when the memory that does not scale with the limit needs more of
/// it. That memory is [`BASE_RESERVE_BYTES`] plus the xorbs being uploaded:
/// up to `max_xorb_uploads` at a time (the xet client setting, 64 by default
/// and 124 in high-performance mode), each up to [`MAX_XORB_BYTES`].
pub fn budget_bytes(limit: u64, max_xorb_uploads: usize) -> u64 {
    let reserve = (max_xorb_uploads as u64)
        .saturating_mul(MAX_XORB_BYTES)
        .saturating_add(BASE_RESERVE_BYTES);
    (limit / 100 * BUDGET_PERCENT).min(limit.saturating_sub(reserve))
}

/// Worst-case memory of one file in flight. A multipart read (more than one
/// GET per file) holds `part_concurrency` parts in the parallel GETs, up to
/// `read_ahead_depth` finished parts in the queue to the cleaner, one part the
/// reader is waiting to queue and one part the cleaner is working on. A
/// single-GET read streams small HTTP chunks, and the cleaner's copy of each
/// one is part of the xorb window. Both paths add the xorb window. Files no
/// larger than one part always use a single GET, so this is an upper bound
/// for them.
pub fn per_file_bytes(part_concurrency: usize, part_size: u64) -> u64 {
    let parts = if part_concurrency <= 1 {
        0
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
/// requested values. With one, lower `part_concurrency` first (down to
/// [`MIN_MULTIPART_CONCURRENCY`]), then `parallel_files` (down to 1), until the
/// worst case fits the budget. Parts per file go first: a file with fewer
/// parts reads slower, but many files in flight still keep the network busy.
pub fn fit(
    limit_bytes: Option<u64>,
    max_xorb_uploads: usize,
    parallel_files: usize,
    part_concurrency: usize,
    part_size: u64,
) -> Sizing {
    let requested_pf = parallel_files.max(1);
    let requested_pc = part_concurrency.max(1);
    let memory = limit_bytes.map(|limit| Budget {
        limit_bytes: limit,
        budget_bytes: budget_bytes(limit, max_xorb_uploads),
    });

    let (pf, pc) = match memory {
        None => (requested_pf, requested_pc),
        Some(m) => {
            let fits = |pf, pc| worst_case_bytes(pf, pc, part_size) <= m.budget_bytes;
            let floor = requested_pc.min(MIN_MULTIPART_CONCURRENCY);
            match (floor..=requested_pc)
                .rev()
                .find(|&pc| fits(requested_pf, pc))
            {
                Some(pc) => (requested_pf, pc),
                None => {
                    let pf = (1..=requested_pf).rev().find(|&pf| fits(pf, floor));
                    (pf.unwrap_or(1), floor)
                }
            }
        }
    };

    Sizing {
        memory,
        requested_parallel_files: requested_pf,
        requested_s3_part_concurrency: requested_pc,
        parallel_files: pf,
        s3_part_concurrency: pc,
        requested_worst_case_bytes: worst_case_bytes(requested_pf, requested_pc, part_size),
        worst_case_bytes: worst_case_bytes(pf, pc, part_size),
    }
}

/// The memory limit of this process, `None` if there is none (or if the
/// files cannot be read, e.g. not on Linux).
pub fn cgroup_memory_limit() -> Option<u64> {
    let own = std::fs::read_to_string(PROC_SELF_CGROUP).unwrap_or_default();
    lowest_cgroup_limit(&own, |path| std::fs::read_to_string(path).ok())
}

/// The lowest memory limit from the process's own cgroup up to the root of
/// the mounted tree: the kernel enforces the limit of every parent too.
/// `proc_self_cgroup` is the content of `/proc/self/cgroup`; `read` reads a
/// file. A cgroup v2 tree wins over v1 when it has at least one memory file
/// (a hybrid system mounts v2 without the memory controller).
///
/// Inside a container with its own cgroup namespace (the Docker and HF Jobs
/// default), the own path is `/` and the limit is at the mount root. With the
/// host namespace, the path names the container's cgroup under the mount.
fn lowest_cgroup_limit(
    proc_self_cgroup: &str,
    read: impl Fn(&Path) -> Option<String>,
) -> Option<u64> {
    let v2 = limits_up_to_root(
        Path::new(CGROUP_V2_ROOT),
        own_cgroup(proc_self_cgroup, |controllers, id| {
            id == "0" && controllers.is_empty()
        }),
        "memory.max",
        parse_cgroup_v2,
        &read,
    );
    let limits = if v2.is_empty() {
        limits_up_to_root(
            Path::new(CGROUP_V1_MEMORY_ROOT),
            own_cgroup(proc_self_cgroup, |controllers, _| {
                controllers.split(',').any(|c| c == "memory")
            }),
            "memory.limit_in_bytes",
            parse_cgroup_v1,
            &read,
        )
    } else {
        v2
    };
    limits.into_iter().flatten().min()
}

/// The path of the first `/proc/self/cgroup` line whose (controllers,
/// hierarchy id) match. `/` when no line matches.
fn own_cgroup(proc_self_cgroup: &str, matches: impl Fn(&str, &str) -> bool) -> &str {
    proc_self_cgroup
        .lines()
        .find_map(|line| {
            let mut fields = line.splitn(3, ':');
            let (id, controllers, path) = (fields.next()?, fields.next()?, fields.next()?);
            matches(controllers, id).then_some(path)
        })
        .unwrap_or("/")
}

/// Parse `file` in the cgroup directory `root/cgroup` and in each parent up
/// to `root`. One entry per file found; empty when the tree has none. A path
/// that leaves the namespace (`..`, seen from outside it) falls back to the
/// root only.
fn limits_up_to_root(
    root: &Path,
    cgroup: &str,
    file: &str,
    parse: fn(&str) -> Option<u64>,
    read: &impl Fn(&Path) -> Option<String>,
) -> Vec<Option<u64>> {
    let relative = Path::new(cgroup.trim_start_matches('/'));
    let mut dir: PathBuf = if relative
        .components()
        .all(|c| matches!(c, Component::Normal(_)))
    {
        root.join(relative)
    } else {
        root.to_path_buf()
    };
    let mut limits = Vec::new();
    loop {
        if let Some(content) = read(&dir.join(file)) {
            limits.push(parse(&content));
        }
        if dir == root || !dir.pop() {
            return limits;
        }
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
    use std::collections::HashMap;

    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;
    const PART: u64 = 16 * MIB;
    /// The limit of a Jobs flavor sold as 32 GB (cpu-upgrade).
    const JOBS_32_GB: u64 = 32_000_000_000;
    /// xet client upload slots: default and high-performance mode.
    const UPLOADS: usize = 64;
    const UPLOADS_HP: usize = 124;

    fn files(entries: &[(&str, &str)]) -> impl Fn(&Path) -> Option<String> {
        let map: HashMap<PathBuf, String> = entries
            .iter()
            .map(|(p, c)| (PathBuf::from(p), c.to_string()))
            .collect();
        move |path: &Path| map.get(path).cloned()
    }

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
    fn own_cgroup_namespace_reads_the_mount_root() {
        let read = files(&[("/sys/fs/cgroup/memory.max", "32000000000\n")]);
        assert_eq!(lowest_cgroup_limit("0::/\n", &read), Some(JOBS_32_GB));
        // No /proc/self/cgroup: same answer.
        assert_eq!(lowest_cgroup_limit("", &read), Some(JOBS_32_GB));
    }

    #[test]
    fn nested_cgroup_takes_the_lowest_limit_up_to_the_root() {
        let own = "0::/system.slice/docker-abc.scope\n";
        // The container has no limit of its own; its parent slice has one.
        let read = files(&[
            (
                "/sys/fs/cgroup/system.slice/docker-abc.scope/memory.max",
                "max\n",
            ),
            ("/sys/fs/cgroup/system.slice/memory.max", "17179869184\n"),
        ]);
        assert_eq!(lowest_cgroup_limit(own, &read), Some(16 * GIB));
        // Its own limit is lower than the parent's.
        let read = files(&[
            (
                "/sys/fs/cgroup/system.slice/docker-abc.scope/memory.max",
                "8589934592\n",
            ),
            ("/sys/fs/cgroup/system.slice/memory.max", "17179869184\n"),
        ]);
        assert_eq!(lowest_cgroup_limit(own, &read), Some(8 * GIB));
    }

    #[test]
    fn hybrid_system_falls_back_to_the_v1_memory_tree() {
        let own = "12:cpu,memory:/user.slice\n0::/user.slice\n";
        let read = files(&[(
            "/sys/fs/cgroup/memory/user.slice/memory.limit_in_bytes",
            "17179869184\n",
        )]);
        assert_eq!(lowest_cgroup_limit(own, &read), Some(16 * GIB));
    }

    #[test]
    fn v1_path_missing_under_a_namespaced_mount_walks_up_to_the_root() {
        // cgroup v1 containers often show the host path, but only mount their
        // own cgroup, at the root of the tree.
        let own = "4:memory:/docker/abc\n";
        let read = files(&[(
            "/sys/fs/cgroup/memory/memory.limit_in_bytes",
            "17179869184\n",
        )]);
        assert_eq!(lowest_cgroup_limit(own, &read), Some(16 * GIB));
    }

    #[test]
    fn a_path_outside_the_namespace_reads_the_root_only() {
        let read = files(&[
            ("/sys/fs/cgroup/memory.max", "17179869184\n"),
            ("/sys/fs/other/memory.max", "1\n"),
        ]);
        assert_eq!(lowest_cgroup_limit("0::/../other\n", &read), Some(16 * GIB));
    }

    #[test]
    fn no_cgroup_files_means_no_limit() {
        assert_eq!(lowest_cgroup_limit("0::/\n", files(&[])), None);
        let read = files(&[("/sys/fs/cgroup/memory.max", "max\n")]);
        assert_eq!(lowest_cgroup_limit("0::/\n", &read), None);
    }

    #[test]
    fn budget_is_60_percent_unless_the_fixed_reserve_needs_more() {
        // 32 GB: 60% (17.9 GiB) is below limit − reserve.
        assert_eq!(budget_bytes(JOBS_32_GB, UPLOADS), JOBS_32_GB / 100 * 60);
        assert_eq!(budget_bytes(JOBS_32_GB, UPLOADS_HP), JOBS_32_GB / 100 * 60);
        // 16 GB in high-performance mode: 124 × 64 MiB + 1 GiB = 8.75 GiB
        // reserved, more than 40% of the limit.
        let limit = 16_000_000_000;
        assert_eq!(
            budget_bytes(limit, UPLOADS_HP),
            limit - (124 * 64 * MIB + GIB)
        );
        assert!(budget_bytes(limit, UPLOADS_HP) < limit / 100 * 60);
        // A limit below the reserve leaves nothing.
        assert_eq!(budget_bytes(4 * GIB, UPLOADS), 0);
    }

    #[test]
    fn per_file_counts_the_read_ahead_queue_and_the_xorb_window() {
        // 128 GETs + 32 queued (the queue is capped) + 2 in hand.
        assert_eq!(per_file_bytes(128, PART), 162 * PART + 128 * MIB);
        // Below the cap the queue is as deep as the GETs are wide.
        assert_eq!(per_file_bytes(8, PART), 18 * PART + 128 * MIB);
        // A single GET streams: only the xorb window.
        assert_eq!(per_file_bytes(1, PART), 128 * MIB);
    }

    #[test]
    fn no_limit_keeps_the_requested_values() {
        let s = fit(None, UPLOADS, 32, 128, PART);
        assert_eq!((s.parallel_files, s.s3_part_concurrency), (32, 128));
        assert!(!s.clamped());
        assert!(s.fits());
    }

    #[test]
    fn big_file_sizing_on_256_gb_is_not_clamped() {
        // 32 × (162 × 16 MiB + 128 MiB) ≈ 85 GiB, far below 60% of 256 GB.
        let s = fit(Some(256 * GIB), UPLOADS, 32, 128, PART);
        assert_eq!((s.parallel_files, s.s3_part_concurrency), (32, 128));
        assert!(!s.clamped());
    }

    #[test]
    fn big_file_sizing_on_32_gb_lowers_part_concurrency_first() {
        let s = fit(Some(JOBS_32_GB), UPLOADS, 32, 128, PART);
        let budget = s.memory.unwrap().budget_bytes;
        assert_eq!((s.parallel_files, s.s3_part_concurrency), (32, 12));
        assert!(s.worst_case_bytes <= budget);
        // One more GET per file would not fit.
        assert!(worst_case_bytes(32, 13, PART) > budget);
        assert!(s.requested_worst_case_bytes > JOBS_32_GB);
    }

    #[test]
    fn small_file_sizing_on_32_gb_keeps_multipart_reads() {
        // 128 files at 2 GETs each need 128 × 224 MiB = 28 GiB: too much, so
        // parallel files go down instead of reading with a single GET.
        let s = fit(Some(JOBS_32_GB), UPLOADS_HP, 128, 8, PART);
        assert_eq!((s.parallel_files, s.s3_part_concurrency), (81, 2));
        assert!(s.fits());
        assert!(worst_case_bytes(82, 2, PART) > s.memory.unwrap().budget_bytes);
    }

    #[test]
    fn a_requested_single_get_stays_a_single_get() {
        let s = fit(Some(JOBS_32_GB), UPLOADS, 256, 1, PART);
        assert_eq!(s.s3_part_concurrency, 1);
        // 17.9 GiB ÷ 128 MiB per file.
        assert_eq!(s.parallel_files, 143);
    }

    #[test]
    fn values_are_never_raised() {
        let s = fit(Some(256 * GIB), UPLOADS, 4, 2, PART);
        assert_eq!((s.parallel_files, s.s3_part_concurrency), (4, 2));
        // 0 means 1, as in the copier.
        let s = fit(Some(256 * GIB), UPLOADS, 0, 0, PART);
        assert_eq!((s.parallel_files, s.s3_part_concurrency), (1, 1));
    }

    #[test]
    fn a_limit_below_one_file_runs_one_file_and_says_it_does_not_fit() {
        let s = fit(Some(5 * GIB), UPLOADS, 32, 128, PART);
        assert_eq!((s.parallel_files, s.s3_part_concurrency), (1, 2));
        assert!(!s.fits());
    }
}
