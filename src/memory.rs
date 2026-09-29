//! Copier memory sizing: fit the copy into the memory limit of the container
//! the copier runs in.
//!
//! The copier holds whole S3 parts in memory while it reads ahead of the xet
//! cleaner. A fixed setting cannot fit every mix of files: 32 big files × 128
//! parts need ~85 GiB, but the same 128 parts for a file copied alone are what
//! reads it fast. So parts come from one [`PartPool`] shared by all files:
//! a part takes a slot before its GET starts and gives it back once the
//! cleaner has used it. The pool is sized from the cgroup limit at startup
//! (see [`fit`]). A file alone can still use `--s3-part-concurrency` parts;
//! many big files share the pool. `--parallel-files` is lowered only when the
//! per-file xorb windows do not leave room for that.

use serde::Serialize;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
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
/// Memory counters of this process (`VmRSS`, `VmHWM`).
const PROC_SELF_STATUS: &str = "/proc/self/status";

/// At most this share of the memory limit goes to the parts and the xorb
/// windows. The rest covers memory that grows with them but is not counted:
/// freed part buffers the allocator has not returned to the system yet.
const BUDGET_PERCENT: u64 = 60;

/// Largest xorb the xet client builds. Also the unit of its uploads.
const MAX_XORB_BYTES: u64 = 64 * 1024 * 1024;

/// Memory kept out of the budget whatever the limit: the tokio runtime, the
/// S3 and Hub HTTP clients, the listing queue and the pending commit ops.
const BASE_RESERVE_BYTES: u64 = 1024 * 1024 * 1024;

/// Xet formation window per active file: the xorb being filled plus one cut
/// xorb waiting for an upload slot.
const XORB_WINDOW_BYTES: u64 = 2 * MAX_XORB_BYTES;

/// Slots shared by the parts of all files in flight. One slot = one part of
/// `--s3-part-size-mib`, from before its GET starts until the cleaner asks
/// for the next part of that file. Cheap to clone.
#[derive(Clone)]
pub struct PartPool {
    slots: Arc<Semaphore>,
    size: usize,
}

/// A part's slot in the [`PartPool`]; dropping it frees the slot.
pub type PartSlot = OwnedSemaphorePermit;

impl PartPool {
    pub fn new(size: usize) -> Self {
        let size = size.min(Semaphore::MAX_PERMITS);
        Self {
            slots: Arc::new(Semaphore::new(size)),
            size,
        }
    }

    /// Wait for a free slot. Waiters are served in arrival order.
    pub async fn acquire(&self) -> PartSlot {
        self.slots
            .clone()
            .acquire_owned()
            .await
            .expect("the part pool is never closed")
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn in_use(&self) -> usize {
        self.size - self.slots.available_permits()
    }
}

/// The memory the copier may use for its parts and xorb windows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Budget {
    /// Memory limit found in the cgroup.
    pub limit_bytes: u64,
    /// Share of the limit for parts and xorb windows (see [`budget_bytes`]).
    pub budget_bytes: u64,
}

/// The effective copy settings and how they were chosen.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Sizing {
    /// `None` = no memory limit found.
    pub memory: Option<Budget>,
    /// Requested (after the `max(1)` the copier always applies) and effective.
    pub requested_parallel_files: usize,
    pub parallel_files: usize,
    /// Never changed by the sizing.
    pub s3_part_concurrency: usize,
    /// Slots in the part pool, and the most the files could ever hold at once
    /// (a larger pool would never be used). 0 when files are read with a
    /// single GET.
    pub part_pool: usize,
    pub part_pool_unlimited: usize,
    /// Worst-case memory of the requested settings without a pool, and of the
    /// effective settings.
    pub requested_worst_case_bytes: u64,
    pub worst_case_bytes: u64,
}

impl Sizing {
    /// True when the pool or `parallel_files` is smaller than requested.
    pub fn limited(&self) -> bool {
        self.parallel_files != self.requested_parallel_files
            || self.part_pool < self.part_pool_unlimited
    }

    /// False only when even one file at full part concurrency is above the
    /// budget: the copier then runs one file with a smaller pool (or, below
    /// one slot, a pool of one).
    pub fn fits(&self) -> bool {
        self.part_pool >= parts_per_file(self.s3_part_concurrency)
            && self
                .memory
                .is_none_or(|m| self.worst_case_bytes <= m.budget_bytes)
    }

    /// Log the settings once at startup, and print them as a `SIZING` marker
    /// line (JSON) for tools that read the copier's log.
    pub fn report(&self, part_size: u64) {
        let gib = |b: u64| format!("{:.1}", b as f64 / (1u64 << 30) as f64);
        let (limit, budget) = match self.memory {
            Some(m) => (gib(m.limit_bytes), gib(m.budget_bytes)),
            None => ("none".to_string(), "none".to_string()),
        };
        let pf = format!(
            "{} → {}",
            self.requested_parallel_files, self.parallel_files
        );
        let pool = format!(
            "{} of {} parts ({} GiB)",
            self.part_pool,
            self.part_pool_unlimited,
            gib((self.part_pool as u64).saturating_mul(part_size))
        );
        let worst = format!(
            "{} → {}",
            gib(self.requested_worst_case_bytes),
            gib(self.worst_case_bytes)
        );
        if self.fits() {
            let message = match self.memory {
                None => "no memory limit found; copy settings as requested",
                Some(_) if self.limited() => "part pool limited to fit the memory limit",
                Some(_) => "copy settings fit the memory limit",
            };
            info!(
                limit_gib = %limit,
                budget_gib = %budget,
                parallel_files = %pf,
                s3_part_concurrency = self.s3_part_concurrency,
                part_pool = %pool,
                worst_case_gib = %worst,
                "{message}"
            );
        } else {
            warn!(
                limit_gib = %limit,
                budget_gib = %budget,
                parallel_files = %pf,
                s3_part_concurrency = self.s3_part_concurrency,
                part_pool = %pool,
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

/// Share of `limit` for the parts and the xorb windows: [`BUDGET_PERCENT`] of
/// it, or less when the memory that does not scale with the limit needs more
/// of it. That memory is [`BASE_RESERVE_BYTES`] plus the xorbs being
/// uploaded: up to `max_xorb_uploads` at a time (the xet client setting, 64 by
/// default and 124 in high-performance mode), each up to [`MAX_XORB_BYTES`].
pub fn budget_bytes(limit: u64, max_xorb_uploads: usize) -> u64 {
    let reserve = (max_xorb_uploads as u64)
        .saturating_mul(MAX_XORB_BYTES)
        .saturating_add(BASE_RESERVE_BYTES);
    (limit / 100 * BUDGET_PERCENT).min(limit.saturating_sub(reserve))
}

/// Most slots one file can hold at once: `part_concurrency` parts in the
/// parallel GETs, up to `read_ahead_depth` finished parts in the queue to the
/// cleaner, one part the reader is waiting to queue and one part the cleaner
/// is working on. 0 for a single-GET read (`part_concurrency` 1), which
/// streams small HTTP chunks: the cleaner's copy of each one is part of the
/// xorb window.
pub fn parts_per_file(part_concurrency: usize) -> usize {
    if part_concurrency <= 1 {
        0
    } else {
        part_concurrency + read_ahead_depth(part_concurrency) + 2
    }
}

/// Pick the copy settings for a memory limit.
///
/// Without a limit: the requested values, and a pool no file set can fill.
/// With one: every file in flight reserves its xorb window, and the pool gets
/// the rest of the budget, but at least [`parts_per_file`] slots, so a file
/// alone always reads at full part concurrency. When that does not fit,
/// `parallel_files` goes down. `part_concurrency` is never changed: the pool
/// alone bounds the parts.
pub fn fit(
    limit_bytes: Option<u64>,
    max_xorb_uploads: usize,
    parallel_files: usize,
    part_concurrency: usize,
    part_size: u64,
) -> Sizing {
    let requested_pf = parallel_files.max(1);
    let part_concurrency = part_concurrency.max(1);
    let one_file = parts_per_file(part_concurrency);
    let unlimited = |pf: usize| pf.saturating_mul(one_file);
    let windows = |pf: usize| (pf as u64).saturating_mul(XORB_WINDOW_BYTES);
    let memory = limit_bytes.map(|limit| Budget {
        limit_bytes: limit,
        budget_bytes: budget_bytes(limit, max_xorb_uploads),
    });

    let (pf, pool) = match memory {
        None => (requested_pf, unlimited(requested_pf)),
        Some(m) => {
            // Slots left for parts once `pf` xorb windows are reserved.
            let room = |pf: usize| {
                let bytes = m.budget_bytes.saturating_sub(windows(pf));
                usize::try_from(bytes / part_size.max(1)).unwrap_or(usize::MAX)
            };
            match (1..=requested_pf)
                .rev()
                .find(|&pf| windows(pf) <= m.budget_bytes && room(pf) >= one_file)
            {
                Some(pf) => (pf, room(pf).min(unlimited(pf))),
                // Not even one file fits: run one, with the slots it needs to
                // make progress (at least one when it reads in parts).
                None => (1, room(1).clamp(one_file.min(1), one_file)),
            }
        }
    };

    Sizing {
        memory,
        requested_parallel_files: requested_pf,
        parallel_files: pf,
        s3_part_concurrency: part_concurrency,
        part_pool: pool,
        part_pool_unlimited: unlimited(pf),
        requested_worst_case_bytes: windows(requested_pf)
            .saturating_add((unlimited(requested_pf) as u64).saturating_mul(part_size)),
        worst_case_bytes: windows(pf).saturating_add((pool as u64).saturating_mul(part_size)),
    }
}

/// Resident memory of this process and its peak since start, in bytes.
pub fn process_rss() -> (Option<u64>, Option<u64>) {
    std::fs::read_to_string(PROC_SELF_STATUS)
        .map(|status| parse_rss(&status))
        .unwrap_or((None, None))
}

/// `VmRSS` and `VmHWM` of a `/proc/<pid>/status` content, in bytes.
fn parse_rss(status: &str) -> (Option<u64>, Option<u64>) {
    let kib = |name: &str| {
        status.lines().find_map(|line| {
            let value = line.strip_prefix(name)?.strip_prefix(':')?;
            let kib: u64 = value.trim().strip_suffix("kB")?.trim().parse().ok()?;
            Some(kib * 1024)
        })
    };
    (kib("VmRSS"), kib("VmHWM"))
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
    fn a_file_holds_its_gets_the_read_ahead_queue_and_two_parts_in_hand() {
        // 128 GETs + 32 queued (the queue is capped) + 2 in hand.
        assert_eq!(parts_per_file(128), 162);
        // Below the cap the queue is as deep as the GETs are wide.
        assert_eq!(parts_per_file(8), 18);
        // A single GET streams: no parts.
        assert_eq!(parts_per_file(1), 0);
    }

    #[test]
    fn no_limit_keeps_the_requested_values_and_an_unlimited_pool() {
        let s = fit(None, UPLOADS, 32, 128, PART);
        assert_eq!((s.parallel_files, s.s3_part_concurrency), (32, 128));
        assert_eq!(s.part_pool, 32 * 162);
        assert!(!s.limited());
        assert!(s.fits());
    }

    #[test]
    fn big_file_sizing_on_256_gb_is_not_limited() {
        // 32 × (162 × 16 MiB + 128 MiB) ≈ 85 GiB, far below 60% of 256 GB.
        let s = fit(Some(256 * GIB), UPLOADS, 32, 128, PART);
        assert_eq!((s.parallel_files, s.part_pool), (32, 32 * 162));
        assert!(!s.limited());
        assert_eq!(s.worst_case_bytes, s.requested_worst_case_bytes);
    }

    #[test]
    fn big_file_sizing_on_32_gb_shares_a_pool_and_keeps_128_parts() {
        let s = fit(Some(JOBS_32_GB), UPLOADS, 32, 128, PART);
        let budget = s.memory.unwrap().budget_bytes;
        assert_eq!((s.parallel_files, s.s3_part_concurrency), (32, 128));
        // (17.9 GiB − 32 × 128 MiB) ÷ 16 MiB.
        assert_eq!(s.part_pool, ((budget - 32 * 128 * MIB) / PART) as usize);
        assert_eq!(s.part_pool, 888);
        assert!(s.limited());
        assert!(s.worst_case_bytes <= budget);
        assert!(s.requested_worst_case_bytes > JOBS_32_GB);
    }

    #[test]
    fn small_file_sizing_on_32_gb_keeps_all_files_and_eight_parts() {
        let s = fit(Some(JOBS_32_GB), UPLOADS_HP, 128, 8, PART);
        assert_eq!((s.parallel_files, s.s3_part_concurrency), (128, 8));
        // (17.9 GiB − 128 × 128 MiB) ÷ 16 MiB: enough for 6 big files at
        // full speed at once.
        assert_eq!(s.part_pool, 120);
        assert!(s.fits());
    }

    #[test]
    fn parallel_files_go_down_when_the_xorb_windows_leave_no_room_for_one_file() {
        // 16 GB in high-performance mode: 6.2 GiB budget. 128 windows need
        // 16 GiB, so files go down until 18 slots (288 MiB) are left.
        let s = fit(Some(16_000_000_000), UPLOADS_HP, 128, 8, PART);
        assert_eq!(s.parallel_files, 46);
        assert_eq!(s.part_pool, 25);
        assert!(s.fits());
        let budget = s.memory.unwrap().budget_bytes;
        assert!(47 * 128 * MIB + 18 * PART > budget);
    }

    #[test]
    fn a_single_get_copy_has_no_pool_and_fits_its_windows() {
        let s = fit(Some(JOBS_32_GB), UPLOADS, 256, 1, PART);
        assert_eq!((s.s3_part_concurrency, s.part_pool), (1, 0));
        // 17.9 GiB ÷ 128 MiB per file.
        assert_eq!(s.parallel_files, 143);
    }

    #[test]
    fn values_are_never_raised() {
        let s = fit(Some(256 * GIB), UPLOADS, 4, 2, PART);
        assert_eq!((s.parallel_files, s.s3_part_concurrency), (4, 2));
        assert_eq!(s.part_pool, 4 * 6);
        // 0 means 1, as in the copier.
        let s = fit(Some(256 * GIB), UPLOADS, 0, 0, PART);
        assert_eq!((s.parallel_files, s.s3_part_concurrency), (1, 1));
    }

    #[test]
    fn a_limit_below_one_file_runs_one_file_with_one_slot_and_says_it_does_not_fit() {
        let s = fit(Some(5 * GIB), UPLOADS, 32, 128, PART);
        assert_eq!((s.parallel_files, s.part_pool), (1, 1));
        assert!(!s.fits());
    }

    #[test]
    fn a_limit_below_one_file_at_full_concurrency_says_it_does_not_fit() {
        // 6 GiB: 1 GiB budget, room for 1 window + 56 parts, not 162.
        let s = fit(Some(6 * GIB), UPLOADS, 32, 128, PART);
        assert_eq!((s.parallel_files, s.part_pool), (1, 56));
        assert!(s.worst_case_bytes <= s.memory.unwrap().budget_bytes);
        assert!(!s.fits());
    }

    #[test]
    fn rss_is_read_from_proc_status() {
        let status = "Name:\thf-s3ream\nVmHWM:\t  2048 kB\nVmRSS:\t  1024 kB\nThreads:\t8\n";
        assert_eq!(parse_rss(status), (Some(1024 * 1024), Some(2048 * 1024)));
        assert_eq!(parse_rss(""), (None, None));
    }

    #[tokio::test]
    async fn the_pool_counts_slots_in_use() {
        let pool = PartPool::new(3);
        let a = pool.acquire().await;
        let _b = pool.acquire().await;
        assert_eq!((pool.size(), pool.in_use()), (3, 2));
        drop(a);
        assert_eq!(pool.in_use(), 1);
    }
}
