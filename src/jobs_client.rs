//! Minimal REST client for the HF Jobs API — just what the planner needs to
//! spawn copier jobs and watch them.
//!
//! Shape mirrors huggingface_hub `_create_job_spec` / `run_job` / `list_jobs`:
//!   POST {endpoint}/api/jobs/{namespace}       → start a job, response `.id`
//!   GET  {endpoint}/api/jobs/{namespace}       → list recent jobs (+ status)
//!   GET  {endpoint}/api/jobs/{namespace}/{id}  → one job's `.status.stage`
//!
//! Job stages (huggingface_hub `JobStage`): RUNNING **and SCHEDULING** are
//! non-terminal; COMPLETED / ERROR / CANCELED / DELETED are terminal. (The HF API
//! really does return SCHEDULING while a job waits for placement — jobs pass
//! through it before RUNNING; it is NOT a queue-against-a-cap, just placement.)
//!
//! Rate limits: HF's Hub API is rate-limited per member (Free ~200/min, PRO
//! ~500/min, 5-min windows). Unlike huggingface_hub, this raw client doesn't get
//! automatic 429 backoff for free, so every request goes through `send_retry`,
//! which honors `Retry-After` and backs off on 429 / 5xx. A 429 on a `run_job`
//! POST must NOT kill the planner.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;
use tracing::warn;

/// A job spec POSTed to `/api/jobs/{namespace}`. Field names are the exact
/// camelCase keys the API expects; empty optionals are omitted so we send the
/// same minimal body huggingface_hub does.
#[derive(Debug, Clone, Serialize)]
pub struct JobSpec {
    /// argv; `command[0]` is the executable (overrides the image ENTRYPOINT).
    pub command: Vec<String>,
    /// Always empty — HF splits argv into command; we put everything in command.
    pub arguments: Vec<String>,
    pub environment: BTreeMap<String, String>,
    pub flavor: String,
    #[serde(rename = "dockerImage")]
    pub docker_image: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub secrets: BTreeMap<String, String>,
    #[serde(rename = "timeoutSeconds", skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JobStatus {
    pub stage: String,
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JobInfo {
    pub id: String,
    #[serde(default)]
    pub status: Option<JobStatus>,
}

impl JobInfo {
    /// Terminal stages never change again. Unknown/missing → treat as running
    /// (keep polling) rather than declaring done prematurely.
    pub fn is_terminal(stage: &str) -> bool {
        matches!(stage, "COMPLETED" | "ERROR" | "CANCELED" | "DELETED")
    }
}

pub struct JobsClient {
    http: reqwest::Client,
    endpoint: String,
    token: String,
}

impl JobsClient {
    pub fn new(endpoint: String, token: String) -> Self {
        // reqwest has NO default timeout — a hung poll would stall the planner
        // forever. Cap requests, fail fast on connect, keep TCP alive, and
        // don't reuse long-idle pooled connections (NAT-dropped conns hang).
        let http = reqwest::Client::builder()
            .user_agent(concat!("hf-s3ream/", env!("CARGO_PKG_VERSION")))
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(10))
            .tcp_keepalive(Duration::from_secs(30))
            .pool_idle_timeout(Duration::from_secs(30))
            .build()
            .expect("reqwest client");
        Self {
            http,
            endpoint,
            token,
        }
    }

    /// Send a request built by `build`, retrying on 429 / 5xx with backoff
    /// (honoring `Retry-After`), and on transport failures (connect/timeout —
    /// surfaced by the client timeouts above). Transport failures are retried
    /// only when `idempotent`, or when the connection was never established
    /// (`is_connect`: nothing reached the server, so even a POST is safe —
    /// retrying a `run_job` whose response was merely lost could double-spawn
    /// a copier). Returns the final response; the caller still checks the
    /// status for non-transient failures.
    async fn send_retry(
        &self,
        build: impl Fn() -> reqwest::RequestBuilder,
        what: &str,
        idempotent: bool,
    ) -> Result<reqwest::Response> {
        const MAX_ATTEMPTS: u32 = 6;
        let mut attempt = 0u32;
        loop {
            let resp = match build().send().await {
                Ok(r) => r,
                Err(e) => {
                    attempt += 1;
                    let retryable = idempotent || e.is_connect();
                    if !retryable || attempt >= MAX_ATTEMPTS {
                        return Err(e).with_context(|| format!("{what}: request failed"));
                    }
                    let backoff = Duration::from_millis((500u64 << attempt.min(6)).min(30_000));
                    warn!(
                        what,
                        attempt,
                        ?backoff,
                        "request failed (transport), retrying: {e}"
                    );
                    tokio::time::sleep(backoff).await;
                    continue;
                }
            };
            let status = resp.status();
            let transient = status.as_u16() == 429 || status.is_server_error();
            if transient && attempt + 1 < MAX_ATTEMPTS {
                attempt += 1;
                let retry_after = resp
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok());
                // Honor Retry-After; else exponential 500ms,1s,2s,… capped at 30s.
                let backoff = retry_after.map(Duration::from_secs).unwrap_or_else(|| {
                    Duration::from_millis((500u64 << attempt.min(6)).min(30_000))
                });
                warn!(what, attempt, status = %status, ?backoff, "throttled/5xx; backing off");
                tokio::time::sleep(backoff).await;
                continue;
            }
            return Ok(resp);
        }
    }

    /// GET /api/whoami-v2 → `.name`. Resolves the default namespace.
    pub async fn whoami(&self) -> Result<String> {
        let url = format!("{}/api/whoami-v2", self.endpoint);
        let resp = self
            .send_retry(
                || self.http.get(&url).bearer_auth(&self.token),
                "whoami",
                true,
            )
            .await?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("whoami failed: HTTP {status}: {body}");
        }
        let v: serde_json::Value = serde_json::from_str(&body).context("decode whoami")?;
        v.get("name")
            .and_then(|n| n.as_str())
            .map(|s| s.to_string())
            .context("whoami response missing `name`")
    }

    /// POST /api/jobs/{namespace} — start a job. Returns the created JobInfo
    /// (we mostly want `.id`).
    pub async fn run_job(&self, namespace: &str, spec: &JobSpec) -> Result<JobInfo> {
        let url = format!("{}/api/jobs/{}", self.endpoint, namespace);
        let resp = self
            .send_retry(
                || self.http.post(&url).bearer_auth(&self.token).json(spec),
                "run_job",
                false,
            )
            .await?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("run_job failed: HTTP {status}: {body}");
        }
        serde_json::from_str::<JobInfo>(&body)
            .with_context(|| format!("decode run_job response: {body}"))
    }

    /// GET /api/jobs/{namespace} — recent jobs for the namespace (with status).
    /// One call replaces N per-copier polls; the planner maps by id and falls
    /// back to `job_status` only for a copier not present in this window.
    pub async fn list_jobs(&self, namespace: &str) -> Result<Vec<JobInfo>> {
        let url = format!("{}/api/jobs/{}", self.endpoint, namespace);
        let resp = self
            .send_retry(
                || self.http.get(&url).bearer_auth(&self.token),
                "list_jobs",
                true,
            )
            .await?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("list_jobs failed: HTTP {status}: {body}");
        }
        serde_json::from_str::<Vec<JobInfo>>(&body).context("decode list_jobs response")
    }

    /// GET /api/jobs/{namespace}/{id} — current status (stage + message), or None
    /// if the response carried no status. Fallback for copiers outside the
    /// `list_jobs` window.
    pub async fn job_status(&self, namespace: &str, id: &str) -> Result<Option<JobStatus>> {
        let url = format!("{}/api/jobs/{}/{}", self.endpoint, namespace, id);
        let resp = self
            .send_retry(
                || self.http.get(&url).bearer_auth(&self.token),
                "job_status",
                true,
            )
            .await?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("job_status failed: HTTP {status}: {body}");
        }
        let info: JobInfo =
            serde_json::from_str(&body).with_context(|| format!("decode job status: {body}"))?;
        Ok(info.status)
    }
}
