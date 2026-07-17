//! Minimal REST client for HF Bucket batch operations.
//!
//! POST {endpoint}/api/buckets/{org}/{name}/batch with content-type application/x-ndjson.
//! Body: one JSON op per line (AddFile only — we don't currently issue deletes).
//!
//! Mirrors hf-mount's `src/hub_api.rs` `batch_operations()` flow.

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use serde::{Deserialize, Serialize};
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
    /// 429/5xx responses with backoff (honoring `Retry-After`). Both endpoints
    /// this client talks to are idempotent — the write token is a read, and the
    /// batch is an AddFile upsert (re-sending the same ops converges) — so
    /// retrying a request whose response was lost is safe. Returns the final
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

    /// Refresh route + auth headers for `xet_session`'s `with_token_refresh_url`.
    ///
    /// This is the same `GET .../xet-write-token` that `get_cas_write_token`
    /// issues; its response deserializes byte-for-byte into xet-client's
    /// `CasJWTInfo` (both `camelCase`: `casUrl` / `exp` / `accessToken`), so the
    /// CAS client can refresh the write token itself when the JWT nears expiry —
    /// no custom `TokenRefresher` needed. The Hub token is a long-lived read
    /// credential, so the header stays valid for the whole run.
    pub fn xet_write_token_refresh(&self, bucket: &BucketRef) -> (String, HeaderMap) {
        let url = format!(
            "{}/api/buckets/{}/xet-write-token",
            self.endpoint,
            bucket.id()
        );
        let mut headers = HeaderMap::new();
        let mut auth = HeaderValue::from_str(&format!("Bearer {}", self.token))
            .expect("hub token is a valid header value");
        auth.set_sensitive(true);
        headers.insert(AUTHORIZATION, auth);
        (url, headers)
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
