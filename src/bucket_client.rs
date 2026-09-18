//! Minimal REST client for HF Bucket batch operations.
//!
//! POST {endpoint}/api/buckets/{org}/{name}/batch with content-type application/x-ndjson.
//! Body: one JSON op per line (AddFile only — we don't currently issue deletes).
//!
//! Mirrors hf-mount's `src/hub_api.rs` `batch_operations()` flow.

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
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

impl BatchOp {
    /// The destination path this op registers — the key the server echoes back
    /// in `failed[]`, and therefore how we find the op to re-send.
    fn path(&self) -> &str {
        match self {
            BatchOp::AddFile { path, .. } => path,
        }
    }
}

/// The server's verdict on a /batch, documented identically for the 200 and the
/// 422 in the Hub's public OpenAPI spec (`/.well-known/openapi.json`, path
/// `/api/buckets/{namespace}/{repo}/batch`): `{success, processed, succeeded,
/// failed[{path,error}]}`. A **200 can carry `success:false`** and name every
/// operation that did not land, which is what this client used to discard:
/// 2026-09-18, a 107 TiB / 500,009-object copy reported every batch committed
/// (`failed:0` across 256 ranges) while 3 files from one 19-op batch were
/// missing at the destination. Re-running that key range committed them in 13s,
/// so the condition is transient — retryable, not fatal.
///
/// `success`/`failed` are required because they carry the loss signal; the two
/// counters are optional because we only log and meter them, and a shape change
/// there must not cost us the rest of the report.
#[derive(Debug, Deserialize)]
struct BatchReport {
    /// "True if all operations succeeded".
    success: bool,
    /// "List of failed operations" — empty on a clean batch.
    failed: Vec<FailedOp>,
    /// "Total number of operations attempted".
    #[serde(default)]
    processed: Option<u64>,
    /// "Number of successful operations".
    #[serde(default)]
    succeeded: Option<u64>,
}

/// One entry of `failed[]`. Both fields default: an entry we can only half-read
/// still reaches the retry path (an empty `path` matches no op we sent and is
/// caught there), whereas failing the whole decode would drop us into the
/// trust-the-status-code fallback and lose the signal entirely.
#[derive(Debug, Deserialize)]
struct FailedOp {
    /// "File path that failed".
    #[serde(default)]
    path: String,
    /// "Error message".
    #[serde(default)]
    error: String,
}

/// Decode the documented body. `None` for anything unreadable — empty, an
/// upstream HTML error page, a renamed field — which the caller degrades into
/// the old status-code-only behaviour rather than failing a copy that is
/// otherwise fine.
fn parse_batch_report(body: &str) -> Option<BatchReport> {
    serde_json::from_str::<BatchReport>(body).ok()
}

/// The ops from `sent` that the server named in `failed[]`, in the order we
/// sent them. A reported path we never sent means we cannot re-send it, and
/// dropping it silently is precisely the bug this path exists to kill — so that
/// is an error, not a skip.
fn retry_ops<'a>(sent: &[&'a BatchOp], failed: &[FailedOp]) -> Result<Vec<&'a BatchOp>> {
    let want: HashSet<&str> = failed.iter().map(|f| f.path.as_str()).collect();
    let retry: Vec<&BatchOp> = sent
        .iter()
        .copied()
        .filter(|op| want.contains(op.path()))
        .collect();
    let matched: HashSet<&str> = retry.iter().map(|op| op.path()).collect();
    if matched.len() != want.len() {
        bail!(
            "bucket batch reported {} failed path(s) that are not among the {} ops we sent",
            want.len() - matched.len(),
            sent.len()
        );
    }
    Ok(retry)
}

/// Response bodies end up in logs the Space tails; cap what one unreadable
/// response can spill there.
fn body_snippet(body: &str) -> String {
    const MAX: usize = 300;
    match body.char_indices().nth(MAX) {
        Some((i, _)) => format!("{}…", &body[..i]),
        None => body.to_string(),
    }
}

/// Attempts of one /batch: the initial send plus re-sends of only the ops the
/// server reported as failed. The observed occurrence cleared on the next try
/// (same key range, committed 13s later), so a short bounded loop covers the
/// transient case; anything surviving it is not transient and belongs to the
/// planner's respawn path, which re-copies the whole range.
const BATCH_ATTEMPTS: u32 = 3;

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

    /// POST the ops as ndjson and return how many the server CONFIRMED — not how
    /// many we handed it. A 2xx alone says nothing about individual operations
    /// (see `BatchReport`), so we read the body: ops in `failed[]` are logged
    /// and re-sent up to `BATCH_ATTEMPTS`, and anything still failing is an
    /// error, which fails the chunk → the copier → the range, where the
    /// planner's respawn already knows how to recover. `send_retry` keeps owning
    /// transport/429/5xx; this loop only handles per-operation outcomes.
    pub async fn batch(&self, bucket: &BucketRef, ops: &[BatchOp]) -> Result<u64> {
        if ops.is_empty() {
            return Ok(0);
        }
        let url = format!("{}/api/buckets/{}/batch", self.endpoint, bucket.id());

        let mut pending: Vec<&BatchOp> = ops.iter().collect();
        let mut confirmed = 0u64;
        for attempt in 0..BATCH_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_secs(1u64 << attempt)).await;
            }
            let mut body = String::new();
            for op in &pending {
                body.push_str(&serde_json::to_string(op)?);
                body.push('\n');
            }
            let body = Bytes::from(body);
            let sent = pending.len() as u64;

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
            // The request id is the handle on a recurrence; read it off the
            // headers before the body consumes the response.
            let request_id = resp
                .headers()
                .get("x-request-id")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            if !status.is_success() {
                // Includes the documented 422, which carries the same body: the
                // raw text goes into the error, so the failed paths still reach
                // the log and the copier fails instead of over-counting.
                let body = resp.text().await.unwrap_or_default();
                bail!("bucket batch failed: HTTP {status} (x-request-id: {request_id}): {body}");
            }
            let text = resp.text().await.unwrap_or_default();
            let Some(report) = parse_batch_report(&text) else {
                // Contract shifted, body truncated, proxy error page: a copy that
                // is otherwise moving bytes must not die over an unreadable ack,
                // so trust the 2xx exactly as this client did before — but say so.
                warn!(
                    x_request_id = %request_id,
                    status = %status,
                    sent,
                    body = %body_snippet(&text),
                    "bucket batch: unreadable response body; trusting the status code"
                );
                return Ok(confirmed + sent);
            };
            // Clamped: this count feeds the metric PROGRESS/DONE publish, and it
            // must never claim more than the ops we actually sent.
            confirmed += report
                .succeeded
                .unwrap_or_else(|| sent.saturating_sub(report.failed.len() as u64))
                .min(sent);
            if report.failed.is_empty() {
                if !report.success {
                    // Not everything applied, and nothing named: there is no op
                    // to re-send, so fail the chunk rather than record files we
                    // cannot account for.
                    bail!(
                        "bucket batch reported success=false with an empty failed[] \
                         ({sent} ops, processed={:?}, succeeded={:?}, x-request-id: {request_id})",
                        report.processed,
                        report.succeeded
                    );
                }
                return Ok(confirmed);
            }
            for f in &report.failed {
                warn!(
                    x_request_id = %request_id,
                    attempt,
                    path = %f.path,
                    error = %f.error,
                    processed = ?report.processed,
                    succeeded = ?report.succeeded,
                    "bucket batch: operation not applied; re-sending"
                );
            }
            pending = retry_ops(&pending, &report.failed)?;
        }
        let stuck: Vec<&str> = pending.iter().take(5).map(|op| op.path()).collect();
        bail!(
            "bucket batch: {} operation(s) still failing after {BATCH_ATTEMPTS} attempts: {}{}",
            pending.len(),
            stuck.join(", "),
            if pending.len() > stuck.len() {
                ", …"
            } else {
                ""
            }
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add(path: &str) -> BatchOp {
        BatchOp::AddFile {
            path: path.to_string(),
            xet_hash: "deadbeef".to_string(),
            mtime: 0,
            content_type: None,
        }
    }

    #[test]
    fn clean_batch_confirms_every_op() {
        let r = parse_batch_report(r#"{"success":true,"processed":19,"succeeded":19,"failed":[]}"#)
            .unwrap();
        assert!(r.success);
        assert_eq!(r.succeeded, Some(19));
        assert!(r.failed.is_empty());
    }

    /// The shape of the 2026-09-18 loss: HTTP 200, three ops of nineteen not
    /// applied, said only in the body.
    #[test]
    fn a_200_can_name_failed_operations() {
        let r = parse_batch_report(
            r#"{"success":false,"processed":19,"succeeded":16,"failed":[{"path":"a/b.warc.gz","error":"boom"}]}"#,
        )
        .unwrap();
        assert!(!r.success);
        assert_eq!(r.succeeded, Some(16));
        assert_eq!(r.failed.len(), 1);
        assert_eq!(r.failed[0].path, "a/b.warc.gz");
        assert_eq!(r.failed[0].error, "boom");
    }

    /// Counters are optional (we only log and meter them); the two fields that
    /// carry the loss signal are not.
    #[test]
    fn counters_may_go_missing_but_the_verdict_may_not() {
        assert!(parse_batch_report(r#"{"success":true,"failed":[]}"#).is_some());
        assert!(parse_batch_report(r#"{"processed":19,"succeeded":19}"#).is_none());
    }

    #[test]
    fn unreadable_bodies_decode_to_none() {
        for body in ["", "   ", "<html>502 Bad Gateway</html>", r#"{"ok":true}"#] {
            assert!(parse_batch_report(body).is_none(), "{body}");
        }
    }

    #[test]
    fn retry_targets_only_the_failed_paths() {
        let ops = [add("a"), add("b"), add("c")];
        let sent: Vec<&BatchOp> = ops.iter().collect();
        let failed = vec![
            FailedOp {
                path: "c".to_string(),
                error: "boom".to_string(),
            },
            FailedOp {
                path: "a".to_string(),
                error: "boom".to_string(),
            },
        ];
        let retry = retry_ops(&sent, &failed).unwrap();
        assert_eq!(
            retry.iter().map(|op| op.path()).collect::<Vec<_>>(),
            vec!["a", "c"]
        );
    }

    #[test]
    fn a_failed_path_we_never_sent_is_an_error() {
        let ops = [add("a")];
        let sent: Vec<&BatchOp> = ops.iter().collect();
        let failed = vec![FailedOp {
            path: "".to_string(),
            error: "boom".to_string(),
        }];
        assert!(retry_ops(&sent, &failed).is_err());
    }

    #[test]
    fn body_snippet_cuts_on_a_char_boundary() {
        let long = "é".repeat(400);
        let cut = body_snippet(&long);
        assert_eq!(cut.chars().count(), 301);
        assert!(cut.ends_with('…'));
        assert_eq!(body_snippet("short"), "short");
    }
}
