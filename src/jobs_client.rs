//! Minimal REST client for the HF Jobs API — just what the planner needs to
//! spawn copier jobs and watch them.
//!
//! Shape mirrors huggingface_hub `_create_job_spec` / `run_job` / `inspect_job`:
//!   POST {endpoint}/api/jobs/{namespace}       → start a job, response `.id`
//!   GET  {endpoint}/api/jobs/{namespace}/{id}  → `.status.stage`
//!
//! Job stages (huggingface_hub `JobStage`): RUNNING is the only non-terminal
//! one; COMPLETED / ERROR / CANCELED / DELETED are terminal. (The Space shows a
//! synthetic "SCHEDULING" until the first log line — the API stays RUNNING.)

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
        let http = reqwest::Client::builder()
            .user_agent(concat!("hf-s3ream/", env!("CARGO_PKG_VERSION")))
            .build()
            .expect("reqwest client");
        Self {
            http,
            endpoint,
            token,
        }
    }

    /// GET /api/whoami-v2 → `.name`. Used to resolve the default namespace when
    /// the caller didn't pass one explicitly.
    pub async fn whoami(&self) -> Result<String> {
        let url = format!("{}/api/whoami-v2", self.endpoint);
        let resp = self
            .http
            .get(&url)
            .bearer_auth(&self.token)
            .send()
            .await
            .context("GET /api/whoami-v2")?;
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
            .http
            .post(&url)
            .bearer_auth(&self.token)
            .json(spec)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("run_job failed: HTTP {status}: {body}");
        }
        serde_json::from_str::<JobInfo>(&body)
            .with_context(|| format!("decode run_job response: {body}"))
    }

    /// GET /api/jobs/{namespace}/{id} — current status (stage + message), or None
    /// if the response carried no status.
    pub async fn job_status(&self, namespace: &str, id: &str) -> Result<Option<JobStatus>> {
        let url = format!("{}/api/jobs/{}/{}", self.endpoint, namespace, id);
        let resp = self
            .http
            .get(&url)
            .bearer_auth(&self.token)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
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
