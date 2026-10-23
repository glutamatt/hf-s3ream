//! How a copy stops: a copier never outlives its planner.
//!
//! Cancelling the planner Job is the one action that stops a whole copy. HF
//! Jobs sends the container of a canceled Job SIGTERM — and of a Job that
//! reaches its timeout too — then kills it about 2 s later (measured
//! 2026-10-23; what the container prints in between stays in the Job's logs).
//! Two paths end the copiers:
//!
//! 1. The planner catches the signal ([`shutdown_signal`]), stops listing and
//!    spawning, and cancels every copier it started, all requests at once: one
//!    round trip, well inside the 2 s.
//! 2. Each copier watches its planner Job ([`watch_planner`]) and cancels its
//!    own Job once the planner has ended, for any reason ([`cancel_self`]).
//!    This catches what the first path cannot: a copier whose launch was in
//!    flight at the signal, a cancel request that failed, a planner that died
//!    without the signal (a crash, out of memory).
//!
//! The watch is a safety net, so it polls slowly: every poll counts against
//! the account's Hub API rate limit, which the planner needs to spawn copiers.

use anyhow::{bail, Context, Result};
use std::time::Duration;
use tokio::signal::unix::{signal, SignalKind};
use tracing::{info, warn};

use crate::jobs_client::{JobInfo, JobsClient};

/// Env var naming the planner Job a copier watches, as `namespace/id`. An env
/// var, not a flag, so an older copier image ignores it instead of failing.
pub const PLANNER_JOB_ENV: &str = "HF_S3REAM_PLANNER_JOB";
/// Env var with the watch period, in seconds.
pub const PLANNER_WATCH_ENV: &str = "HF_S3REAM_PLANNER_WATCH_S";

/// The planner Job a copier watches, and how often.
#[derive(Debug, Clone, PartialEq)]
pub struct PlannerLink {
    pub namespace: String,
    pub job_id: String,
    pub every: Duration,
}

impl PlannerLink {
    /// The env vars that hand this link to a copier.
    pub fn env(&self) -> [(String, String); 2] {
        [
            (
                PLANNER_JOB_ENV.to_string(),
                format!("{}/{}", self.namespace, self.job_id),
            ),
            (
                PLANNER_WATCH_ENV.to_string(),
                self.every.as_secs().to_string(),
            ),
        ]
    }

    /// The link the planner left in this copier's env, if any.
    pub fn from_env() -> Option<Self> {
        let job = std::env::var(PLANNER_JOB_ENV).ok()?;
        let every = std::env::var(PLANNER_WATCH_ENV).ok();
        Self::parse(&job, every.as_deref())
    }

    fn parse(job: &str, every: Option<&str>) -> Option<Self> {
        let (namespace, job_id) = job.trim().split_once('/')?;
        if namespace.is_empty() || job_id.is_empty() || job_id.contains('/') {
            return None;
        }
        let every = every
            .and_then(|s| s.trim().parse::<u64>().ok())
            .filter(|&s| s > 0)
            .map(Duration::from_secs)
            .unwrap_or(MIN_WATCH_EVERY);
        Some(Self {
            namespace: namespace.to_string(),
            job_id: job_id.to_string(),
            every,
        })
    }
}

/// A copier never polls its planner more often than this.
const MIN_WATCH_EVERY: Duration = Duration::from_secs(60);

/// Watch period for a fleet of up to `max_inflight` copiers: the whole fleet
/// polls at most once every 2 s (≤ 30 requests/min, a small share of the
/// Hub API rate limit), and a copier at most once a minute.
pub fn watch_every(max_inflight: usize) -> Duration {
    Duration::from_secs(2 * max_inflight as u64).max(MIN_WATCH_EVERY)
}

/// The link copiers get to this planner: its own Job, read from `JOB_ID` (set
/// by HF Jobs in every Job), checked once under `namespace` — where the
/// copiers will look for it. None when the planner does not run as a Job of
/// that namespace (e.g. on a laptop): then only the signal stops the copiers.
pub async fn planner_link(
    jobs: &JobsClient,
    namespace: &str,
    max_inflight: usize,
) -> Option<PlannerLink> {
    let job_id = std::env::var("JOB_ID").ok().filter(|s| !s.is_empty())?;
    match jobs.job_status(namespace, &job_id).await {
        Ok(_) => Some(PlannerLink {
            namespace: namespace.to_string(),
            job_id,
            every: watch_every(max_inflight),
        }),
        Err(e) => {
            warn!(job = %job_id, namespace, "planner Job not found in the copiers' namespace; copiers will not watch it: {e:#}");
            None
        }
    }
}

/// Resolves on the first SIGTERM (HF Jobs cancel or timeout) or SIGINT
/// (Ctrl-C on a terminal) and names it. From then on the process keeps
/// these signals for itself: a second one does not kill it.
pub async fn shutdown_signal() -> &'static str {
    let (Ok(mut term), Ok(mut int)) = (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
    ) else {
        warn!("cannot listen for SIGTERM/SIGINT: a stop will not cancel the copiers from here");
        return std::future::pending().await;
    };
    tokio::select! {
        _ = term.recv() => "SIGTERM",
        _ = int.recv() => "SIGINT",
    }
}

/// Poll the planner Job every `link.every` until it has ended; returns its
/// stage. A failed poll is logged and never stops the copy.
pub async fn watch_planner(jobs: &JobsClient, link: &PlannerLink) -> String {
    info!(planner = %link.job_id, every_s = link.every.as_secs(), "watching the planner Job");
    loop {
        tokio::time::sleep(link.every).await;
        match jobs.job_status(&link.namespace, &link.job_id).await {
            Ok(Some(st)) if JobInfo::is_terminal(&st.stage) => return st.stage,
            Ok(_) => {}
            Err(e) => {
                warn!(planner = %link.job_id, "planner status poll failed; the copy goes on: {e:#}")
            }
        }
    }
}

/// This copier's planner has ended: cancel the copier's own Job, so it ends
/// CANCELED like the rest of the run, and wait for HF Jobs to kill it.
/// Returns only on failure, or if the kill never comes.
pub async fn cancel_self(jobs: &JobsClient, link: &PlannerLink, planner_stage: &str) -> Result<()> {
    warn!(planner = %link.job_id, stage = planner_stage, "the planner Job has ended: stopping this copier");
    let id = std::env::var("JOB_ID")
        .ok()
        .filter(|s| !s.is_empty())
        .context("JOB_ID is not set: cannot cancel this copier's own Job")?;
    jobs.cancel_job(&link.namespace, &id)
        .await
        .context("cancel this copier's own Job")?;
    // The kill comes ~2 s after the SIGTERM this cancel sends. Exiting first
    // could end the Job as ERROR instead of CANCELED.
    tokio::time::sleep(Duration::from_secs(60)).await;
    bail!(
        "the planner Job {} has ended ({planner_stage}); this copier stopped",
        link.job_id
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_link_survives_the_trip_through_the_env() {
        let link = PlannerLink {
            namespace: "my-org".to_string(),
            job_id: "6adb690d1e112550ce7d564a".to_string(),
            every: Duration::from_secs(64),
        };
        let [(_, job), (_, every)] = link.env();
        assert_eq!(PlannerLink::parse(&job, Some(&every)), Some(link));
    }

    #[test]
    fn a_malformed_link_is_no_link() {
        assert_eq!(PlannerLink::parse("", None), None);
        assert_eq!(PlannerLink::parse("6adb690d", None), None);
        assert_eq!(PlannerLink::parse("/6adb690d", None), None);
        assert_eq!(PlannerLink::parse("my-org/", None), None);
        assert_eq!(PlannerLink::parse("my-org/a/b", None), None);
    }

    #[test]
    fn a_missing_or_bad_period_falls_back_to_the_minimum() {
        for every in [None, Some(""), Some("0"), Some("soon")] {
            let link = PlannerLink::parse("my-org/6adb690d", every).unwrap();
            assert_eq!(link.every, MIN_WATCH_EVERY);
        }
    }

    #[test]
    fn the_fleet_polls_at_most_every_two_seconds() {
        assert_eq!(watch_every(1), MIN_WATCH_EVERY);
        assert_eq!(watch_every(30), MIN_WATCH_EVERY);
        assert_eq!(watch_every(32), Duration::from_secs(64));
        assert_eq!(watch_every(128), Duration::from_secs(256));
    }
}
