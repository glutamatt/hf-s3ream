//! Minimal REST client for HF Bucket batch operations.
//!
//! POST {endpoint}/api/buckets/{org}/{name}/batch with content-type application/x-ndjson.
//! Body: one JSON op per line (AddFile only — we don't currently issue deletes).
//!
//! Mirrors hf-mount's `src/hub_api.rs` `batch_operations()` flow.

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use reqwest::StatusCode;
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

/// One /batch response reduced to what settlement needs. `batch()` builds it
/// from the HTTP response; the tests build it from literals.
struct BatchResponse {
    status: StatusCode,
    /// `x-request-id` — the handle on a recurrence, so every WARN and error
    /// about this response carries it.
    request_id: String,
    body: String,
}

/// What `batch()` does after handing a response to `Settlement::observe`.
#[derive(Debug, PartialEq, Eq)]
enum Step {
    /// Every op accounted for; the count `batch()` returns.
    Done(u64),
    /// Sleep this long, then POST `Settlement::pending()` again.
    Retry(Duration),
}

/// The per-operation half of one /batch, with the HTTP kept out: which ops the
/// server has not yet confirmed, and what each response means for them.
/// `batch()` is the shell that POSTs `pending()`, hands the response to
/// `observe()` and sleeps when told to. This part touches no I/O and no clock,
/// so every rule in `observe` is exercised with scripted bodies — the
/// 2026-09-18 loss was three ops of nineteen, reported only in a 200's body,
/// and the rules that decide such a body are the ones an HTTP-bound loop
/// leaves untested.
struct Settlement<'a> {
    /// Ops the server has not confirmed, in send order; the next attempt
    /// sends exactly these.
    pending: Vec<&'a BatchOp>,
    /// Ops the server has confirmed so far, summed over attempts.
    confirmed: u64,
    /// Responses observed so far.
    attempt: u32,
}

impl<'a> Settlement<'a> {
    fn new(ops: &'a [BatchOp]) -> Self {
        Self {
            pending: ops.iter().collect(),
            confirmed: 0,
            attempt: 0,
        }
    }

    fn pending(&self) -> &[&'a BatchOp] {
        &self.pending
    }

    /// Apply one response to the pending ops.
    fn observe(&mut self, resp: &BatchResponse) -> Result<Step> {
        let attempt = self.attempt;
        self.attempt += 1;
        let sent = self.pending.len() as u64;
        let status = resp.status;
        let request_id = resp.request_id.as_str();

        if !status.is_success() {
            // Includes the documented 422, which carries the same body: the
            // raw text goes into the error, so the failed paths still reach
            // the log and the copier fails instead of over-counting.
            bail!(
                "bucket batch failed: HTTP {status} (x-request-id: {request_id}): {}",
                resp.body
            );
        }
        let Some(report) = parse_batch_report(&resp.body) else {
            // Contract shifted, body truncated, proxy error page: a copy that
            // is otherwise moving bytes must not die over an unreadable ack,
            // so trust the 2xx exactly as this client did before — but say so.
            warn!(
                x_request_id = %request_id,
                status = %status,
                sent,
                body = %body_snippet(&resp.body),
                "bucket batch: unreadable response body; trusting the status code"
            );
            return Ok(Step::Done(self.confirmed + sent));
        };
        // Clamped: this count feeds the metric PROGRESS/DONE publish, and it
        // must never claim more than the ops we actually sent.
        self.confirmed += report
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
            return Ok(Step::Done(self.confirmed));
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
        self.pending = retry_ops(&self.pending, &report.failed)?;
        self.next_attempt()
    }

    /// The backoff before re-sending `pending`, or — attempts spent — the
    /// error that fails the chunk, naming what never landed.
    fn next_attempt(&self) -> Result<Step> {
        if self.attempt >= BATCH_ATTEMPTS {
            let stuck: Vec<&str> = self.pending.iter().take(5).map(|op| op.path()).collect();
            bail!(
                "bucket batch: {} operation(s) still failing after {BATCH_ATTEMPTS} attempts: {}{}",
                self.pending.len(),
                stuck.join(", "),
                if self.pending.len() > stuck.len() {
                    ", …"
                } else {
                    ""
                }
            );
        }
        // 2s, 4s.
        Ok(Step::Retry(Duration::from_secs(1u64 << self.attempt)))
    }
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

    /// POST the ops as ndjson and return how many the server CONFIRMED — not how
    /// many we handed it. A 2xx alone says nothing about individual operations
    /// (see `BatchReport`), so we read the body: ops in `failed[]` are logged
    /// and re-sent up to `BATCH_ATTEMPTS`, and anything still failing is an
    /// error, which fails the chunk → the copier → the range, where the
    /// planner's respawn already knows how to recover. `send_retry` keeps owning
    /// transport/429/5xx; `Settlement` owns the per-operation outcomes and is
    /// where the rules live — this is only the HTTP around it.
    pub async fn batch(&self, bucket: &BucketRef, ops: &[BatchOp]) -> Result<u64> {
        if ops.is_empty() {
            return Ok(0);
        }
        let url = format!("{}/api/buckets/{}/batch", self.endpoint, bucket.id());
        let mut settlement = Settlement::new(ops);
        loop {
            let mut body = String::new();
            for op in settlement.pending() {
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
            let response = BatchResponse {
                status: resp.status(),
                // Read off the headers before the body consumes the response.
                request_id: resp
                    .headers()
                    .get("x-request-id")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string(),
                body: resp.text().await.unwrap_or_default(),
            };
            match settlement.observe(&response)? {
                Step::Done(confirmed) => return Ok(confirmed),
                Step::Retry(backoff) => tokio::time::sleep(backoff).await,
            }
        }
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

    /// `n` ops named `f0..fn`.
    fn ops(n: usize) -> Vec<BatchOp> {
        (0..n).map(|i| add(&format!("f{i}"))).collect()
    }

    fn response(status: StatusCode, body: &str) -> BatchResponse {
        BatchResponse {
            status,
            request_id: "req-1".to_string(),
            body: body.to_string(),
        }
    }

    fn ok(body: &str) -> BatchResponse {
        response(StatusCode::OK, body)
    }

    /// The documented clean body for `n` ops.
    fn clean(n: usize) -> String {
        format!(r#"{{"success":true,"processed":{n},"succeeded":{n},"failed":[]}}"#)
    }

    /// The documented partial-failure body for `sent` ops, consistent with
    /// itself: `succeeded` is what `failed[]` leaves.
    fn partial(sent: usize, failed: &[&str]) -> String {
        let entries: Vec<String> = failed
            .iter()
            .map(|p| format!(r#"{{"path":"{p}","error":"boom"}}"#))
            .collect();
        format!(
            r#"{{"success":false,"processed":{sent},"succeeded":{},"failed":[{}]}}"#,
            sent - failed.len(),
            entries.join(",")
        )
    }

    fn pending<'a>(s: &Settlement<'a>) -> Vec<&'a str> {
        s.pending().iter().copied().map(BatchOp::path).collect()
    }

    fn err_text(r: Result<Step>) -> String {
        format!("{:#}", r.expect_err("expected an error"))
    }

    const S2: Duration = Duration::from_secs(2);
    const S4: Duration = Duration::from_secs(4);

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

    // ---- Settlement: the decision table `batch()` drives. ----

    #[test]
    fn a_clean_batch_is_done_on_the_first_attempt() {
        let ops = ops(19);
        let mut s = Settlement::new(&ops);
        assert_eq!(s.observe(&ok(&clean(19))).unwrap(), Step::Done(19));
    }

    /// The 2026-09-18 shape, end to end: three of nineteen named in a 200,
    /// re-sent alone after 2s, confirmed on the next body — 19 in total.
    #[test]
    fn failed_ops_are_re_sent_alone_and_confirmed_on_the_next_body() {
        let ops = ops(19);
        let mut s = Settlement::new(&ops);
        let first = ok(&partial(19, &["f2", "f7", "f18"]));
        assert_eq!(s.observe(&first).unwrap(), Step::Retry(S2));
        assert_eq!(pending(&s), ["f2", "f7", "f18"]);
        assert_eq!(s.observe(&ok(&clean(3))).unwrap(), Step::Done(19));
    }

    /// Each re-send narrows to what the previous body still named; the
    /// backoff grows 2s → 4s.
    #[test]
    fn a_second_partial_failure_narrows_the_re_send_further() {
        let ops = ops(19);
        let mut s = Settlement::new(&ops);
        let first = ok(&partial(19, &["f2", "f7", "f18"]));
        assert_eq!(s.observe(&first).unwrap(), Step::Retry(S2));
        let second = ok(&partial(3, &["f7"]));
        assert_eq!(s.observe(&second).unwrap(), Step::Retry(S4));
        assert_eq!(pending(&s), ["f7"]);
        assert_eq!(s.observe(&ok(&clean(1))).unwrap(), Step::Done(19));
    }

    #[test]
    fn an_unreadable_body_on_the_first_attempt_trusts_the_status_code() {
        for body in ["", "<html>ok</html>", r#"{"ok":true}"#] {
            let ops = ops(19);
            let mut s = Settlement::new(&ops);
            assert_eq!(s.observe(&ok(body)).unwrap(), Step::Done(19), "{body}");
        }
    }

    #[test]
    fn success_false_with_nothing_named_fails_the_chunk() {
        let ops = ops(19);
        let mut s = Settlement::new(&ops);
        let body = r#"{"success":false,"processed":19,"succeeded":19,"failed":[]}"#;
        assert!(err_text(s.observe(&ok(body))).contains("success=false"));
    }

    #[test]
    fn ops_still_failing_after_the_last_attempt_are_an_error() {
        let ops = ops(19);
        let mut s = Settlement::new(&ops);
        let first = ok(&partial(19, &["f2", "f7"]));
        assert_eq!(s.observe(&first).unwrap(), Step::Retry(S2));
        let again = ok(&partial(2, &["f2", "f7"]));
        assert_eq!(s.observe(&again).unwrap(), Step::Retry(S4));
        let msg = err_text(s.observe(&again));
        assert!(msg.contains("after 3 attempts"), "{msg}");
        assert!(msg.contains("f2") && msg.contains("f7"), "{msg}");
    }

    #[test]
    fn a_reported_path_we_never_sent_fails_the_chunk() {
        let ops = ops(3);
        let mut s = Settlement::new(&ops);
        let body = r#"{"success":false,"processed":3,"succeeded":2,"failed":[{"path":"nope","error":"boom"}]}"#;
        assert!(s.observe(&ok(body)).is_err());
    }

    #[test]
    fn a_non_2xx_is_final_and_carries_its_body() {
        for status in [
            StatusCode::FORBIDDEN,
            StatusCode::NOT_FOUND,
            StatusCode::UNPROCESSABLE_ENTITY,
        ] {
            let ops = ops(3);
            let mut s = Settlement::new(&ops);
            let msg = err_text(s.observe(&response(status, "the reason")));
            assert!(msg.contains(status.as_str()), "{msg}");
            assert!(msg.contains("the reason"), "{msg}");
            assert!(msg.contains("req-1"), "{msg}");
        }
    }
}
