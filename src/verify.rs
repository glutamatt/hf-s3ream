//! The destination check after a copy. Every other check is bookkeeping on the
//! sending side; this one lists what landed, compares it with the plan range
//! by range, names what is missing, and fails the run on a gap.
//!
//! Pass 1 compares counts and bytes per range, not names. A missing file and
//! an unplanned file of the same size in the same range cancel out: that is
//! the price of one listing of the destination for a clean run.

use anyhow::{Context, Result};
use futures::StreamExt;
use serde::Serialize;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{info, warn};

use crate::bucket_client::BucketClient;
use crate::sync::{
    build_globset, destination_path, filter_key, list_page_with_retry, no_sign_request,
    relative_key_path, KeyFilter,
};
use crate::BucketRef;

/// One planned range: its key window (`start_after` exclusive, `stop_at`
/// inclusive, `None` = open) and what the listing kept in it.
#[derive(Debug, Clone)]
pub struct PlannedRange {
    pub start_after: Option<String>,
    pub stop_at: Option<String>,
    pub files: u64,
    pub bytes: u64,
}

/// The source, to re-list a range and to print its re-run command.
pub struct Source {
    pub url: String,
    pub bucket: String,
    pub prefix: String,
    pub exclude_globs: Vec<String>,
}

/// The `VERIFY` line. Not fatal: `extra` (unplanned files in a re-listed
/// range), `raced` (deleted from the source during the run) and `outside`
/// (under the destination prefix, outside every range).
#[derive(Debug, Serialize)]
pub struct Report {
    /// No missing and no short file.
    pub ok: bool,
    /// `updatedAt` stopped moving before the listing.
    pub settled: bool,
    pub planned_files: u64,
    pub planned_bytes: u64,
    pub dest_files: u64,
    pub dest_bytes: u64,
    pub missing: u64,
    /// At the destination with another size.
    pub short: u64,
    pub extra: u64,
    pub raced: u64,
    pub outside: u64,
    pub failed_ranges: Vec<usize>,
    /// The first missing, then short, destination paths.
    pub sample: Vec<String>,
}

impl Report {
    pub fn failure(&self) -> String {
        format!(
            "destination check failed: {} missing, {} short file(s) in range(s) {:?} \
             (see the VERIFY line and the re-run commands above)",
            self.missing, self.short, self.failed_ranges
        )
    }

    fn new(settled: bool, ranges: &[PlannedRange], tally: &Tally) -> Self {
        Self {
            ok: true,
            settled,
            planned_files: ranges.iter().map(|r| r.files).sum(),
            planned_bytes: ranges.iter().map(|r| r.bytes).sum(),
            dest_files: tally.dest_files,
            dest_bytes: tally.dest_bytes,
            missing: 0,
            short: 0,
            extra: 0,
            raced: 0,
            outside: tally.outside,
            failed_ranges: Vec::new(),
            sample: Vec::new(),
        }
    }

    fn add(&mut self, idx: usize, diff: &RangeDiff) {
        self.missing += diff.missing.len() as u64;
        self.short += diff.short.len() as u64;
        self.extra += diff.extra;
        self.raced += diff.raced.len() as u64;
        if diff.failed() {
            self.ok = false;
            self.failed_ranges.push(idx);
        }
        let gaps = diff.missing.iter().chain(diff.short.iter().map(|(o, _)| o));
        let room = SAMPLE.saturating_sub(self.sample.len());
        self.sample
            .extend(gaps.take(room).map(|o| o.dest_path.clone()));
    }
}

/// Paths in the `VERIFY` line, and named at WARN, per run.
const SAMPLE: usize = 10;
/// Settle: poll `updatedAt` every 10s; quiet after 2 unchanged polls; 120s max.
const SETTLE_POLL: Duration = Duration::from_secs(10);
const SETTLE_QUIET_POLLS: u32 = 2;
const SETTLE_MAX: Duration = Duration::from_secs(120);
/// Source HEADs per run to tell `raced` from `missing`. Past the cap a key
/// stays missing: thousands of keys gone mid-copy is not a race.
const MAX_SOURCE_HEADS: usize = 1000;
const HEAD_CONCURRENCY: usize = 16;

/// Run the check and print its `VERIFY` line, also when the check itself
/// fails (`{"ok":false,"error":…}`), so "not verified" is never silent.
pub async fn verify_destination(
    bucket: &BucketClient,
    dest: &BucketRef,
    s3: &aws_sdk_s3::Client,
    source: &Source,
    ranges: &[PlannedRange],
) -> Result<Report> {
    let result = check(bucket, dest, s3, source, ranges).await;
    let line = match &result {
        Ok(report) => serde_json::to_string(report).expect("report serializes"),
        Err(e) => serde_json::json!({ "ok": false, "error": format!("{e:#}") }).to_string(),
    };
    println!("VERIFY {line}");
    result
}

async fn check(
    bucket: &BucketClient,
    dest: &BucketRef,
    s3: &aws_sdk_s3::Client,
    source: &Source,
    ranges: &[PlannedRange],
) -> Result<Report> {
    let planned_files: u64 = ranges.iter().map(|r| r.files).sum();
    let progress = |dest_files: u64| {
        let line = serde_json::json!({ "dest_files": dest_files, "planned_files": planned_files });
        println!("VERIFYING {line}");
    };
    progress(0);
    let settled = settle(bucket, dest).await;

    let mut tally = Tally::new(ranges.len());
    let mut cursor: Option<String> = None;
    loop {
        let (files, next) = bucket
            .tree_page(dest, &dest.path, cursor.as_deref())
            .await
            .context("list destination")?;
        for (path, size) in files {
            tally.observe(&path, size, &dest.path, &source.prefix, ranges);
        }
        progress(tally.dest_files);
        cursor = next;
        if cursor.is_none() {
            break;
        }
    }

    let exclude = build_globset(&source.exclude_globs)?;
    let mut heads_left = MAX_SOURCE_HEADS;
    let mut report = Report::new(settled, ranges, &tally);
    for i in tally.suspects(ranges) {
        let range = &ranges[i];
        let mut diff = check_range(bucket, dest, s3, source, exclude.as_ref(), range).await?;
        split_raced(s3, &source.bucket, &mut diff, &mut heads_left).await;
        diff.set_extra(tally.files[i]);
        if diff.failed() {
            warn!(
                range = i,
                missing = diff.missing.len(),
                short = diff.short.len(),
                rerun = %rerun_command(source, dest, range),
                "destination is short for this range"
            );
        }
        for o in &diff.raced {
            info!(key = %o.key, range = i, "gone from the source during the run (raced)");
        }
        report.add(i, &diff);
    }
    for path in &report.sample {
        warn!(%path, "missing or short at the destination");
    }
    if report.ok {
        info!(
            planned_files = report.planned_files,
            dest_files = report.dest_files,
            "destination verified"
        );
    } else {
        warn!("{}", report.failure());
    }
    Ok(report)
}

/// Poll `updatedAt` until it holds still. A signal, not the verdict: the
/// listing decides, so an unreadable bucket info only skips the wait.
async fn settle(bucket: &BucketClient, dest: &BucketRef) -> bool {
    let started = Instant::now();
    let mut last: Option<String> = None;
    let mut quiet = 0u32;
    loop {
        match bucket.updated_at(dest).await {
            Ok(at) if last.as_ref() == Some(&at) => {
                quiet += 1;
                if quiet >= SETTLE_QUIET_POLLS {
                    return true;
                }
            }
            Ok(at) => {
                quiet = 0;
                last = Some(at);
            }
            Err(e) => {
                warn!("bucket info unavailable, listing without waiting: {e:#}");
                return false;
            }
        }
        if started.elapsed() >= SETTLE_MAX {
            warn!("destination still changing after {SETTLE_MAX:?}, listing anyway");
            return false;
        }
        tokio::time::sleep(SETTLE_POLL).await;
    }
}

/// The source key a destination path came from: the inverse of
/// `destination_path(dest_prefix, relative_key_path(key, src_prefix))`.
/// A key equal to the prefix (a single-object source) keeps its full name.
fn source_key(dest_path: &str, dest_prefix: &str, src_prefix: &str) -> Option<String> {
    let rel = if dest_prefix.is_empty() {
        dest_path
    } else {
        dest_path.strip_prefix(dest_prefix)?.strip_prefix('/')?
    };
    Some(if rel == src_prefix && !src_prefix.ends_with('/') {
        rel.to_string()
    } else if src_prefix.is_empty() || src_prefix.ends_with('/') {
        format!("{src_prefix}{rel}")
    } else {
        format!("{src_prefix}/{rel}")
    })
}

/// The range a source key falls in. Ranges are contiguous and in key order,
/// and `&str` compares bytes like S3 sorts keys, so this does not depend on
/// the order of the destination listing.
fn locate(ranges: &[PlannedRange], key: &str) -> Option<usize> {
    let i = ranges.partition_point(|r| r.stop_at.as_deref().is_some_and(|stop| stop < key));
    let r = ranges.get(i)?;
    if r.start_after.as_deref().is_some_and(|sa| key <= sa) {
        return None;
    }
    Some(i)
}

/// Pass 1: files and bytes per range as the destination lists them.
struct Tally {
    files: Vec<u64>,
    bytes: Vec<u64>,
    outside: u64,
    dest_files: u64,
    dest_bytes: u64,
}

impl Tally {
    fn new(ranges: usize) -> Self {
        Self {
            files: vec![0; ranges],
            bytes: vec![0; ranges],
            outside: 0,
            dest_files: 0,
            dest_bytes: 0,
        }
    }

    fn observe(
        &mut self,
        path: &str,
        size: u64,
        dest_prefix: &str,
        src_prefix: &str,
        ranges: &[PlannedRange],
    ) {
        // `/tree/{path}` matches `path` as a string prefix: `dest-2/a` is
        // listed for `dest`. It is not part of the destination.
        let Some(key) = source_key(path, dest_prefix, src_prefix) else {
            return;
        };
        self.dest_files += 1;
        self.dest_bytes += size;
        match locate(ranges, &key) {
            Some(i) => {
                self.files[i] += 1;
                self.bytes[i] += size;
            }
            None => self.outside += 1,
        }
    }

    /// Ranges whose file count or byte total differs from the plan.
    fn suspects(&self, ranges: &[PlannedRange]) -> Vec<usize> {
        (0..ranges.len())
            .filter(|&i| self.files[i] != ranges[i].files || self.bytes[i] != ranges[i].bytes)
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SourceObject {
    key: String,
    dest_path: String,
    size: u64,
}

/// Pass 2 for one range.
#[derive(Debug, Default)]
struct RangeDiff {
    /// At the destination with the planned size.
    present: u64,
    missing: Vec<SourceObject>,
    /// With the size found at the destination.
    short: Vec<(SourceObject, u64)>,
    raced: Vec<SourceObject>,
    extra: u64,
}

impl RangeDiff {
    /// `found` is what `paths-info` returned: files with a size. A directory,
    /// or a file without a size, at a planned path is not the planned file.
    fn observe(&mut self, obj: SourceObject, found: &HashMap<String, u64>) {
        match found.get(&obj.dest_path) {
            None => self.missing.push(obj),
            Some(&size) if size == obj.size => self.present += 1,
            Some(&size) => self.short.push((obj, size)),
        }
    }

    fn failed(&self) -> bool {
        !self.missing.is_empty() || !self.short.is_empty()
    }

    /// What pass 1 listed in the range beyond the planned files it holds. A
    /// short file is there: it is not an extra.
    fn set_extra(&mut self, listed: u64) {
        self.extra = listed.saturating_sub(self.present + self.short.len() as u64);
    }
}

/// Re-list the range on the source with the copier's own filter, and ask the
/// destination about those paths, one `paths-info` per S3 page.
async fn check_range(
    bucket: &BucketClient,
    dest: &BucketRef,
    s3: &aws_sdk_s3::Client,
    source: &Source,
    exclude: Option<&globset::GlobSet>,
    range: &PlannedRange,
) -> Result<RangeDiff> {
    let mut diff = RangeDiff::default();
    let mut continuation: Option<String> = None;
    loop {
        let page = list_page_with_retry(
            s3,
            &source.bucket,
            &source.prefix,
            continuation.as_deref(),
            range.start_after.as_deref(),
        )
        .await
        .context("re-list the source")?;
        let mut objects = Vec::new();
        let mut range_end = false;
        for obj in page.contents() {
            let Some(key) = obj.key() else { continue };
            if range.stop_at.as_deref().is_some_and(|stop| key > stop) {
                range_end = true;
                break;
            }
            if filter_key(key, &source.prefix, exclude) == KeyFilter::Keep {
                objects.push(SourceObject {
                    key: key.to_string(),
                    dest_path: destination_path(
                        &dest.path,
                        &relative_key_path(key, &source.prefix),
                    ),
                    size: obj.size().unwrap_or(0).max(0) as u64,
                });
            }
        }
        if !objects.is_empty() {
            let paths: Vec<String> = objects.iter().map(|o| o.dest_path.clone()).collect();
            let found = bucket.paths_info(dest, &paths).await?;
            for obj in objects {
                diff.observe(obj, &found);
            }
        }
        match page.next_continuation_token() {
            Some(t) if !range_end => continuation = Some(t.to_string()),
            _ => return Ok(diff),
        }
    }
}

/// HEAD missing keys on the source: a 404 moves the key to `raced`. Any
/// other error keeps it missing, the direction that fails the run.
async fn split_raced(
    s3: &aws_sdk_s3::Client,
    source_bucket: &str,
    diff: &mut RangeDiff,
    heads_left: &mut usize,
) {
    let n = diff.missing.len().min(*heads_left);
    if n < diff.missing.len() {
        warn!(
            unchecked = diff.missing.len() - n,
            "too many missing keys to HEAD on the source; the rest count as missing"
        );
    }
    *heads_left -= n;
    let unchecked = diff.missing.split_off(n);
    let checked = std::mem::take(&mut diff.missing);
    let results: Vec<(SourceObject, bool)> = futures::stream::iter(checked)
        .map(|obj| async move {
            let gone = source_gone(s3, source_bucket, &obj.key).await;
            (obj, gone)
        })
        .buffered(HEAD_CONCURRENCY)
        .collect()
        .await;
    for (obj, gone) in results {
        if gone {
            diff.raced.push(obj);
        } else {
            diff.missing.push(obj);
        }
    }
    diff.missing.extend(unchecked);
}

async fn source_gone(s3: &aws_sdk_s3::Client, bucket: &str, key: &str) -> bool {
    match s3.head_object().bucket(bucket).key(key).send().await {
        Ok(_) => false,
        Err(e) => {
            let status = e.raw_response().map(|r| r.status().as_u16());
            let service = e.into_service_error();
            if service.is_not_found() || status == Some(404) {
                return true;
            }
            warn!(
                key,
                ?status,
                "source HEAD failed, counting the key as missing: {service}"
            );
            false
        }
    }
}

/// The command that re-copies one range. `--skip-existing` makes it copy only
/// what is missing or short.
fn rerun_command(source: &Source, dest: &BucketRef, range: &PlannedRange) -> String {
    let mut argv = vec![
        "hf-s3ream".to_string(),
        sh_quote(&source.url),
        sh_quote(&dest.uri()),
    ];
    let bounds = [
        ("--start-after", &range.start_after),
        ("--stop-at", &range.stop_at),
    ];
    for (flag, value) in bounds {
        if let Some(v) = value {
            argv.extend([flag.to_string(), sh_quote(v)]);
        }
    }
    for g in &source.exclude_globs {
        argv.extend(["--exclude".to_string(), sh_quote(g)]);
    }
    argv.push("--skip-existing".to_string());
    if no_sign_request() {
        argv.push("--no-sign-request".to_string());
    }
    argv.join(" ")
}

/// Single-quote for a POSIX shell when the value needs it.
fn sh_quote(s: &str) -> String {
    let plain = !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_./=:@%+,".contains(&b));
    if plain {
        s.to_string()
    } else {
        format!("'{}'", s.replace('\'', r"'\''"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(
        start_after: Option<&str>,
        stop_at: Option<&str>,
        files: u64,
        bytes: u64,
    ) -> PlannedRange {
        PlannedRange {
            start_after: start_after.map(str::to_string),
            stop_at: stop_at.map(str::to_string),
            files,
            bytes,
        }
    }

    fn obj(key: &str, dest_path: &str, size: u64) -> SourceObject {
        SourceObject {
            key: key.to_string(),
            dest_path: dest_path.to_string(),
            size,
        }
    }

    #[test]
    fn source_key_inverts_the_copier_mapping() {
        let cases: &[(&str, &str, &[&str])] = &[
            ("foo/", "", &["foo/a", "foo/bar/a", "foo/x y/%23"]),
            ("foo", "", &["foo", "foo/a", "foo/bar/a"]),
            ("", "", &["a", "bar/a"]),
            ("foo/", "path/to/dest", &["foo/a", "foo/bar/a"]),
            ("foo", "path/to/dest", &["foo/a"]),
            ("data/x.gz", "dest", &["data/x.gz"]),
            ("", "dest", &["a", "bar/a"]),
        ];
        for (src, dst, keys) in cases {
            for key in *keys {
                let dest_path = destination_path(dst, &relative_key_path(key, src));
                assert_eq!(
                    source_key(&dest_path, dst, src).as_deref(),
                    Some(*key),
                    "{dest_path}"
                );
            }
        }
        assert_eq!(source_key("other/a", "dest", "foo/"), None);
        assert_eq!(source_key("dest", "dest", "foo/"), None);
        assert_eq!(source_key("dest-2/a", "dest", "foo/"), None);
    }

    #[test]
    fn locate_uses_exclusive_lower_and_inclusive_upper_bounds_in_byte_order() {
        let ranges = [
            range(None, Some("p/B"), 0, 0),
            range(Some("p/B"), Some("p/f"), 0, 0),
        ];
        assert_eq!(locate(&ranges, "p/A"), Some(0));
        assert_eq!(locate(&ranges, "p/B"), Some(0));
        assert_eq!(locate(&ranges, "p/B0"), Some(1));
        assert_eq!(locate(&ranges, "p/a"), Some(1)); // `B` < `a` in bytes
        assert_eq!(locate(&ranges, "p/f"), Some(1));
        assert_eq!(locate(&ranges, "p/f0"), None);
        // A windowed single-process copy, and an open one.
        let windowed = [range(Some("p/c"), Some("p/m"), 0, 0)];
        assert_eq!(locate(&windowed, "p/c"), None);
        assert_eq!(locate(&windowed, "p/c0"), Some(0));
        assert_eq!(locate(&[range(None, None, 0, 0)], "zzz"), Some(0));
    }

    /// The 2026-09-18 shape: one range comes up short and only it is
    /// re-listed. A byte gap alone is enough, and so is an extra file.
    #[test]
    fn suspects_are_the_ranges_whose_count_or_bytes_differ() {
        let ranges = [
            range(None, Some("s/b"), 2, 30),
            range(Some("s/b"), Some("s/d"), 2, 70),
            range(Some("s/d"), Some("s/f"), 1, 50),
            range(Some("s/f"), Some("s/h"), 1, 70),
        ];
        let mut t = Tally::new(ranges.len());
        for (path, size) in [
            ("d/a", 10),
            ("d/b", 20),
            ("d/c", 30), // range 1: 1 of 2 files
            ("d/e", 51), // range 2: bytes differ
            ("d/g", 70),
            ("d/g0", 0),  // range 3: an extra empty file
            ("d/z", 5),   // past the last range
            ("d-2/a", 5), // a sibling prefix the listing also returns: ignored
        ] {
            t.observe(path, size, "d", "s/", &ranges);
        }
        assert_eq!(t.suspects(&ranges), vec![1, 2, 3]);
        assert_eq!((t.outside, t.dest_files, t.dest_bytes), (1, 7, 186));
    }

    #[test]
    fn pass_2_sorts_planned_keys_into_present_missing_short_and_extra() {
        let found = HashMap::from([
            ("d/ok".to_string(), 10),
            ("d/empty".to_string(), 0),
            ("d/small".to_string(), 9),
        ]);
        let mut d = RangeDiff::default();
        d.observe(obj("s/ok", "d/ok", 10), &found);
        d.observe(obj("s/empty", "d/empty", 0), &found);
        d.observe(obj("s/small", "d/small", 10), &found);
        d.observe(obj("s/gone", "d/gone", 10), &found);
        assert_eq!(d.present, 2);
        assert_eq!(d.short, vec![(obj("s/small", "d/small", 10), 9)]);
        assert_eq!(d.missing, vec![obj("s/gone", "d/gone", 10)]);
        assert!(d.failed());
        // Pass 1 listed 3 in this range: the short file is not an extra.
        d.set_extra(3);
        assert_eq!(d.extra, 0);
        d.set_extra(5);
        assert_eq!(d.extra, 2);
        // A listing behind the exact lookups is not a negative extra.
        d.set_extra(1);
        assert_eq!(d.extra, 0);
    }

    #[test]
    fn missing_and_short_fail_the_run_raced_extra_and_outside_do_not() {
        let ranges = [range(None, None, 12, 120)];
        let tally = Tally {
            outside: 5,
            dest_files: 14,
            dest_bytes: 140,
            ..Tally::new(1)
        };

        let mut r = Report::new(true, &ranges, &tally);
        let benign = RangeDiff {
            present: 10,
            raced: vec![obj("s/r", "d/r", 1)],
            extra: 2,
            ..RangeDiff::default()
        };
        r.add(0, &benign);
        assert!(r.ok);
        assert_eq!((r.raced, r.extra, r.outside), (1, 2, 5));
        assert!(r.failed_ranges.is_empty() && r.sample.is_empty());

        let many: Vec<_> = (0..12)
            .map(|i| obj(&format!("s/{i:02}"), &format!("d/{i:02}"), 1))
            .collect();
        let lossy = RangeDiff {
            missing: many,
            short: vec![(obj("s/s", "d/s", 10), 9)],
            ..RangeDiff::default()
        };
        r.add(3, &lossy);
        assert!(!r.ok);
        assert_eq!((r.missing, r.short), (12, 1));
        assert_eq!(r.failed_ranges, vec![3]);
        assert_eq!(r.sample.len(), SAMPLE);
        assert_eq!(r.sample[0], "d/00");
        assert!(r.failure().contains("12 missing, 1 short"));

        let line = serde_json::to_string(&r).unwrap();
        assert!(line.starts_with(r#"{"ok":false,"settled":true,"planned_files":12,"planned_bytes":120,"dest_files":14,"#), "{line}");
        for field in ["extra", "raced", "outside", "failed_ranges", "sample"] {
            assert!(line.contains(&format!("\"{field}\":")), "{field}");
        }
    }

    #[test]
    fn rerun_command_is_the_copier_invocation_for_the_range() {
        let source = Source {
            url: "s3://my-bucket/crawl-data/".to_string(),
            bucket: "my-bucket".to_string(),
            prefix: "crawl-data/".to_string(),
            exclude_globs: vec!["*decay*".to_string()],
        };
        let dest = BucketRef {
            org: "org".to_string(),
            name: "name".to_string(),
            path: "sub/dir".to_string(),
        };
        let r = range(
            Some("crawl-data/seg 1/a.gz"),
            Some("crawl-data/b's.gz"),
            0,
            0,
        );
        assert_eq!(
            rerun_command(&source, &dest, &r),
            r#"hf-s3ream s3://my-bucket/crawl-data/ hf://buckets/org/name/sub/dir --start-after 'crawl-data/seg 1/a.gz' --stop-at 'crawl-data/b'\''s.gz' --exclude '*decay*' --skip-existing"#
        );
        let first = range(None, Some("crawl-data/x"), 0, 0);
        assert_eq!(
            rerun_command(&source, &dest, &first),
            r#"hf-s3ream s3://my-bucket/crawl-data/ hf://buckets/org/name/sub/dir --stop-at crawl-data/x --exclude '*decay*' --skip-existing"#
        );
        assert_eq!(sh_quote("a/b-c_d.e=f:g@h%i+j,k"), "a/b-c_d.e=f:g@h%i+j,k");
        assert_eq!(sh_quote(""), "''");
        assert_eq!(sh_quote("$HOME"), "'$HOME'");
    }
}
