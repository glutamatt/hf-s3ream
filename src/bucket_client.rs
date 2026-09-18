//! Minimal REST client for HF Bucket batch operations.
//!
//! POST {endpoint}/api/buckets/{org}/{name}/batch with content-type application/x-ndjson.
//! Body: one JSON op per line (AddFile only — we don't currently issue deletes).
//!
//! Mirrors hf-mount's `src/hub_api.rs` `batch_operations()` flow.

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tracing::warn;

use crate::BucketRef;

/// Response from `/api/buckets/{id}/xet-write-token`. Mirrors hf-mount's `CasTokenInfo`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CasTokenInfo {
    pub cas_url: String,
    /// Unix seconds.
    pub exp: u64,
    pub access_token: String,
}

/// Mirrors hf-mount's `BatchOp` for adds. Serialized as `{"type":"addFile",...}`
/// ndjson lines. We don't currently emit deletes; if we ever support
/// destructive sync, add a `DeleteFile` variant here.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum BatchOp {
    #[serde(rename_all = "camelCase")]
    AddFile {
        path: String,
        xet_hash: String,
        /// Milliseconds since UNIX epoch.
        mtime: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        content_type: Option<String>,
    },
}

/// One entry of a `/api/buckets/{id}/paths-info` response (same item shape as
/// the `tree` listing). Only what --skip-existing compares is decoded; `size`
/// is absent for directories.
#[derive(Debug, Clone, Deserialize)]
pub struct PathInfo {
    #[serde(rename = "type")]
    pub kind: String,
    pub path: String,
    #[serde(default)]
    pub size: Option<u64>,
}

/// Paths per `paths-info` request. The endpoint accepts up to 2000; 1000 is
/// what huggingface_hub sends, and one S3 list page never exceeds it.
const PATHS_INFO_BATCH: usize = 1000;

/// path → size for the files (not directories) of a `paths-info` response.
fn file_sizes(entries: Vec<PathInfo>) -> impl Iterator<Item = (String, u64)> {
    entries
        .into_iter()
        .filter(|e| e.kind == "file")
        .filter_map(|e| e.size.map(|size| (e.path, size)))
}

pub struct BucketClient {
    http: reqwest::Client,
    endpoint: String,
    token: String,
}

impl BucketClient {
    pub fn new(endpoint: String, token: String) -> Self {
        // reqwest has NO default timeout: a black-holed connection would hang a
        // token fetch or a batch commit forever, with zero logs — the copier
        // just sits at 0 MiB/s. Cap every request (180s covers a multi-MB
        // ndjson batch body + slow Hub processing), fail fast on connect, keep
        // the TCP path alive, and don't reuse long-idle pooled connections
        // (a NAT that silently dropped one turns reuse into a hang).
        let http = reqwest::Client::builder()
            .user_agent(concat!("hf-s3ream/", env!("CARGO_PKG_VERSION")))
            .timeout(Duration::from_secs(180))
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

    /// Send a request built by `build`, retrying transport failures
    /// (connect/timeout — now surfaced by the client timeouts above) and
    /// 429/5xx responses with backoff (honoring `Retry-After`). Every endpoint
    /// this client talks to is idempotent — the write token and paths-info are
    /// reads, and the batch is an AddFile upsert (re-sending the same ops
    /// converges) — so retrying a request whose response was lost is safe. Returns the final
    /// response; the caller still checks the status for non-transient failures.
    async fn send_retry(
        &self,
        build: impl Fn() -> reqwest::RequestBuilder,
        what: &str,
    ) -> Result<reqwest::Response> {
        const MAX_ATTEMPTS: u32 = 6;
        let mut attempt = 0u32;
        loop {
            let resp = match build().send().await {
                Ok(r) => r,
                Err(e) => {
                    attempt += 1;
                    if attempt >= MAX_ATTEMPTS {
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

    /// GET /api/buckets/{id}/xet-write-token — returns CAS endpoint + JWT.
    pub async fn get_cas_write_token(&self, bucket: &BucketRef) -> Result<CasTokenInfo> {
        let url = format!(
            "{}/api/buckets/{}/xet-write-token",
            self.endpoint,
            bucket.id()
        );
        let resp = self
            .send_retry(
                || self.http.get(&url).bearer_auth(&self.token),
                "xet-write-token",
            )
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("xet-write-token failed: HTTP {status}: {body}");
        }
        let info = resp
            .json::<CasTokenInfo>()
            .await
            .context("decode CasTokenInfo")?;
        Ok(info)
    }

    /// POST /api/buckets/{id}/paths-info — which of `paths` exist at the
    /// destination, as path → size. Paths the bucket doesn't have are simply
    /// absent from the response (no error), as are directories.
    pub async fn paths_info(
        &self,
        bucket: &BucketRef,
        paths: &[String],
    ) -> Result<HashMap<String, u64>> {
        let url = format!("{}/api/buckets/{}/paths-info", self.endpoint, bucket.id());
        let mut sizes = HashMap::with_capacity(paths.len());
        for chunk in paths.chunks(PATHS_INFO_BATCH) {
            let body = serde_json::json!({ "paths": chunk });
            let resp = self
                .send_retry(
                    || self.http.post(&url).bearer_auth(&self.token).json(&body),
                    "paths-info",
                )
                .await?;
            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                bail!("paths-info failed: HTTP {status}: {body}");
            }
            let entries = resp
                .json::<Vec<PathInfo>>()
                .await
                .context("decode paths-info")?;
            sizes.extend(file_sizes(entries));
        }
        Ok(sizes)
    }

    pub async fn batch(&self, bucket: &BucketRef, ops: &[BatchOp]) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let url = format!("{}/api/buckets/{}/batch", self.endpoint, bucket.id());

        let mut body = String::new();
        for op in ops {
            body.push_str(&serde_json::to_string(op)?);
            body.push('\n');
        }
        let body = Bytes::from(body);

        let resp = self
            .send_retry(
                || {
                    self.http
                        .post(&url)
                        .bearer_auth(&self.token)
                        .header("content-type", "application/x-ndjson")
                        .body(body.clone())
                },
                "bucket batch",
            )
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("bucket batch failed: HTTP {status}: {body}");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_info_keeps_only_files_with_a_size() {
        let entries: Vec<PathInfo> = serde_json::from_str(
            r#"[
                {"type":"file","path":"a/x.bin","size":42,"xetHash":"ab","uploadedAt":"2026-09-14T12:13:09.512Z"},
                {"type":"file","path":"a/empty","size":0,"uploadedAt":"2026-09-14T12:13:09.512Z"},
                {"type":"file","path":"a/no-size","uploadedAt":"2026-09-14T12:13:09.512Z"},
                {"type":"directory","path":"a","uploadedAt":"2026-09-14T12:13:09.512Z"}
            ]"#,
        )
        .unwrap();
        let sizes: HashMap<_, _> = file_sizes(entries).collect();
        assert_eq!(sizes.len(), 2);
        assert_eq!(sizes.get("a/x.bin"), Some(&42));
        assert_eq!(sizes.get("a/empty"), Some(&0));
    }
}
