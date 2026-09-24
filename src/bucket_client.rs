//! Minimal REST client for HF Bucket batch operations.
//!
//! POST {endpoint}/api/buckets/{org}/{name}/batch with content-type application/x-ndjson.
//! Body: one JSON op per line (AddFile only — we don't currently issue deletes).
//!
//! Mirrors hf-mount's `src/hub_api.rs` `batch_operations()` flow.
//!
//! Read side: `paths-info`, the bucket's `updatedAt` and the recursive `/tree`
//! listing, all in the Hub's public OpenAPI spec.

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tracing::warn;
use url::Url;

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
/// failed[{path,error}]}`, all four required. A **200 can carry
/// `success:false`** and name every operation that did not land, which is
/// what this client used to discard: 2026-09-18, a 107 TiB / 500,009-object
/// copy reported every batch committed (`failed:0` across 256 ranges) while 3
/// files from one 19-op batch were missing at the destination. Re-running that
/// key range committed them in 13s, so the condition is transient —
/// retryable, not fatal.
///
/// Only `success` is required to decode: it carries the verdict. The rest
/// default, because a body that fails to decode drops whole into the
/// trust-the-status-code fallback (see `Settlement::observe`): a server that
/// stopped sending `failed` on clean batches would put every batch back on
/// the pre-fix behaviour, at the cost of one WARN per batch — tens of
/// thousands of lines on a large run and nothing else to show for it.
#[derive(Debug, Deserialize)]
struct BatchReport {
    /// "True if all operations succeeded".
    success: bool,
    /// "List of failed operations" — empty on a clean batch.
    #[serde(default)]
    failed: Vec<FailedOp>,
    /// "Total number of operations attempted". Signed, as the spec types it,
    /// so a nonsensical value reaches the cross-check and fails there instead
    /// of making the body unreadable.
    #[serde(default)]
    processed: Option<i64>,
    /// "Number of successful operations".
    #[serde(default)]
    succeeded: Option<i64>,
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
/// is an error, not a skip, and it names the paths.
fn retry_ops<'a>(sent: &[&'a BatchOp], failed: &[FailedOp]) -> Result<Vec<&'a BatchOp>> {
    let want: HashSet<&str> = failed.iter().map(|f| f.path.as_str()).collect();
    let retry: Vec<&BatchOp> = sent
        .iter()
        .copied()
        .filter(|op| want.contains(op.path()))
        .collect();
    let matched: HashSet<&str> = retry.iter().map(|op| op.path()).collect();
    if matched.len() != want.len() {
        let mut unknown: Vec<&str> = want.difference(&matched).copied().collect();
        unknown.sort_unstable();
        bail!(
            "bucket batch reported {} failed path(s) that are not among the {} ops we sent: {}",
            unknown.len(),
            sent.len(),
            name_some(unknown.iter().copied())
        );
    }
    Ok(retry)
}

/// A few paths, quoted, then an ellipsis: enough to find the ops in the logs
/// without one error line carrying a whole chunk.
fn name_some<'a>(paths: impl ExactSizeIterator<Item = &'a str>) -> String {
    const SHOW: usize = 5;
    let n = paths.len();
    let mut out = paths
        .take(SHOW)
        .map(|p| format!("{p:?}"))
        .collect::<Vec<_>>()
        .join(", ");
    if n > SHOW {
        out.push_str(", …");
    }
    out
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
///
/// Each attempt re-enters `send_retry`'s own budget (6 tries at the 180s
/// request timeout plus ≤31s of backoff, ≈18.5 min), so the loop's worst case
/// is three of those (≈56 min) plus 6s. Reaching it takes both failure modes
/// at once — a Hub that names failed ops in every body AND stalls every
/// request to its timeout — and no shorter clock on the whole call is safe: a
/// batch that is slow but progressing, cut off by one, costs a whole-range
/// re-copy for nothing. The copier's HF Job timeout (`copier_timeout_s`)
/// stays the ceiling on the commit stage, as it was before this loop existed.
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
    /// Ops handed to `batch()` — what `Done` reports (see `observe`).
    total: u64,
    /// Responses observed so far.
    attempt: u32,
}

impl<'a> Settlement<'a> {
    fn new(ops: &'a [BatchOp]) -> Self {
        Self {
            pending: ops.iter().collect(),
            total: ops.len() as u64,
            attempt: 0,
        }
    }

    fn pending(&self) -> &[&'a BatchOp] {
        &self.pending
    }

    /// Apply one response to the pending ops.
    ///
    /// The body is the verdict, for the 200 and the documented 422 alike: the
    /// spec gives both the same shape, and a partial failure delivered as a
    /// 422 deserves its re-send as much as one delivered as a 200 instead of
    /// costing a whole-range re-copy. The body is acted on only when it agrees
    /// with the request — `processed` must be the ops sent, `succeeded` must
    /// be `processed` minus the ops named in `failed[]` — and a report that
    /// says otherwise is an error, not a WARN. Such a report has left ops
    /// unaccounted for WITHOUT naming them: `success:true, processed:16,
    /// succeeded:16, failed:[]` for 19 ops is the observed loss with the signal
    /// in hand and nothing to re-send, and a WARN there lets the copier exit
    /// 0, the job read COMPLETED and the planner — which keys off the job
    /// stage alone — report `failed:0`. The chunk error takes the copier down
    /// and the planner's respawn re-copies the range.
    ///
    /// `Done` carries the number of ops handed to `batch()`, not a sum of the
    /// server's counters: by the time it is reached, every op has been in a
    /// report whose counters were checked against what was sent, so the two
    /// are equal by construction, and summing the counters instead would only
    /// let a pair of inconsistent bodies claim more than was sent.
    fn observe(&mut self, resp: &BatchResponse) -> Result<Step> {
        let attempt = self.attempt;
        self.attempt += 1;
        let sent = self.pending.len() as u64;
        let status = resp.status;
        let request_id = resp.request_id.as_str();

        if !(status.is_success() || status == StatusCode::UNPROCESSABLE_ENTITY) {
            // `send_retry` has already spent the 429/5xx budget; whatever is
            // left is final, and its body goes into the error whole so the
            // reason reaches the log.
            bail!(
                "bucket batch failed: HTTP {status} (x-request-id: {request_id}): {}",
                resp.body
            );
        }
        let Some(report) = parse_batch_report(&resp.body) else {
            if status.is_success() && attempt == 0 {
                // Contract shifted, body truncated, proxy error page: a copy
                // that is otherwise moving bytes must not die over an
                // unreadable ack, so trust the 2xx exactly as this client did
                // before — but say so.
                warn!(
                    x_request_id = %request_id,
                    status = %status,
                    sent,
                    body = %body_snippet(&resp.body),
                    "bucket batch: unreadable response body; trusting the status code"
                );
                return Ok(Step::Done(self.total));
            }
            if status.is_success() {
                // On a re-send the server has already named these ops as
                // failed once; a 2xx with nothing readable behind it is
                // exactly the evidence this loop exists to stop trusting.
                // The attempt is spent, the ops stay pending.
                warn!(
                    x_request_id = %request_id,
                    status = %status,
                    attempt,
                    sent,
                    body = %body_snippet(&resp.body),
                    "bucket batch: unreadable response body on a re-send; ops stay unconfirmed"
                );
                return self.next_attempt();
            }
            // A 422 with no readable report names nothing to re-send.
            bail!(
                "bucket batch failed: HTTP {status} (x-request-id: {request_id}): {}",
                resp.body
            );
        };

        if let Some(processed) = report.processed {
            if processed != sent as i64 {
                bail!(
                    "bucket batch reported processed={processed} for {sent} ops sent \
                     (succeeded={:?}, failed={}, x-request-id: {request_id})",
                    report.succeeded,
                    report.failed.len()
                );
            }
        }
        let retry = retry_ops(&self.pending, &report.failed)?;
        if let Some(succeeded) = report.succeeded {
            let expected = sent - retry.len() as u64;
            if succeeded != expected as i64 {
                bail!(
                    "bucket batch reported succeeded={succeeded} for {sent} ops sent with {} \
                     named failed (expected {expected}, x-request-id: {request_id})",
                    retry.len()
                );
            }
        }
        if retry.is_empty() {
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
            return Ok(Step::Done(self.total));
        }
        for f in &report.failed {
            warn!(
                x_request_id = %request_id,
                status = %status,
                attempt,
                path = %f.path,
                error = %f.error,
                processed = ?report.processed,
                succeeded = ?report.succeeded,
                "bucket batch: operation not applied; re-sending"
            );
        }
        self.pending = retry;
        self.next_attempt()
    }

    /// The backoff before re-sending `pending`, or — attempts spent — the
    /// error that fails the chunk, naming what never landed.
    fn next_attempt(&self) -> Result<Step> {
        if self.attempt >= BATCH_ATTEMPTS {
            bail!(
                "bucket batch: {} operation(s) still failing after {BATCH_ATTEMPTS} attempts: {}",
                self.pending.len(),
                name_some(self.pending.iter().map(|op| op.path()))
            );
        }
        // 2s, 4s.
        Ok(Step::Retry(Duration::from_secs(1u64 << self.attempt)))
    }
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

/// Entries per `/tree` page: the documented maximum.
const TREE_PAGE_LIMIT: u32 = 5000;

/// The `cursor` of the `rel="next"` link. Only the cursor is taken: the next
/// request goes to our own endpoint, so the token never follows a server URL.
fn next_cursor(link: &str) -> Option<String> {
    let part = link.split(',').find(|p| p.contains(r#"rel="next""#))?;
    let url = part.split(';').next()?.trim();
    let url = Url::parse(url.strip_prefix('<')?.strip_suffix('>')?).ok()?;
    url.query_pairs()
        .find(|(k, _)| k == "cursor")
        .map(|(_, v)| v.into_owned())
}

/// path → size for the files (not directories) of a `paths-info` response or
/// a `/tree` page.
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
    /// this client talks to is idempotent — all but the batch are reads, and
    /// the batch is an AddFile upsert (re-sending the same ops
    /// converges) — so retrying a request whose response was lost is safe.
    /// Returns the final response; the caller still checks the status for
    /// non-transient failures.
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

    /// GET /api/buckets/{id} — `updatedAt` moves on every change to the files.
    pub async fn updated_at(&self, bucket: &BucketRef) -> Result<String> {
        #[derive(Deserialize)]
        struct Info {
            #[serde(rename = "updatedAt")]
            updated_at: String,
        }
        let url = format!("{}/api/buckets/{}", self.endpoint, bucket.id());
        let resp = self
            .send_retry(
                || self.http.get(&url).bearer_auth(&self.token),
                "bucket info",
            )
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("bucket info failed: HTTP {status}: {body}");
        }
        let info = resp.json::<Info>().await.context("decode bucket info")?;
        Ok(info.updated_at)
    }

    /// GET /api/buckets/{id}/tree/{path}?recursive=true&sort=path — one page of the files
    /// under `path` as (path, size), and the cursor of the next page.
    pub async fn tree_page(
        &self,
        bucket: &BucketRef,
        path: &str,
        cursor: Option<&str>,
    ) -> Result<(Vec<(String, u64)>, Option<String>)> {
        // The bucket root is `/tree` (`/tree/` redirects). Segments go through
        // the URL parser so a path with spaces or `%` is encoded once.
        let mut url = Url::parse(&format!(
            "{}/api/buckets/{}/tree",
            self.endpoint,
            bucket.id()
        ))
        .context("tree URL")?;
        if !path.is_empty() {
            url.path_segments_mut()
                .map_err(|_| anyhow::anyhow!("tree URL cannot be a base"))?
                .extend(path.split('/'));
        }
        {
            let mut q = url.query_pairs_mut();
            q.append_pair("recursive", "true")
                .append_pair("limit", &TREE_PAGE_LIMIT.to_string())
                .append_pair("sort", "path");
            if let Some(c) = cursor {
                q.append_pair("cursor", c);
            }
        }
        let resp = self
            .send_retry(
                || self.http.get(url.clone()).bearer_auth(&self.token),
                "bucket tree",
            )
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("bucket tree failed: HTTP {status}: {body}");
        }
        let next = resp
            .headers()
            .get("link")
            .and_then(|v| v.to_str().ok())
            .and_then(next_cursor);
        let entries = resp
            .json::<Vec<PathInfo>>()
            .await
            .context("decode bucket tree page")?;
        Ok((file_sizes(entries).collect(), next))
    }

    /// POST the ops as ndjson and return how many the server CONFIRMED. `Ok(n)`
    /// means the server's reports covered every one of the ops (so `n` is
    /// `ops.len()`, returned rather than assumed so the caller meters what was
    /// established, not what was hoped). A 2xx alone says nothing about
    /// individual operations (see `BatchReport`), so the body decides: ops in
    /// `failed[]` are logged and re-sent up to `BATCH_ATTEMPTS`, and anything
    /// still failing — or any report that does not add up against what was
    /// sent — is an error, which fails the chunk → the copier → the range,
    /// where the planner's respawn already knows how to recover. `send_retry`
    /// keeps owning transport/429/5xx; `Settlement` owns the per-operation
    /// outcomes and is where the rules live — this is only the HTTP around it.
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

    fn unprocessable(body: &str) -> BatchResponse {
        response(StatusCode::UNPROCESSABLE_ENTITY, body)
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

    /// Only `success` is required to decode — it carries the verdict. `failed`
    /// and the counters default, so a server that omits them on a clean batch
    /// does not turn every batch into an unreadable body (which would be the
    /// pre-fix behaviour back, plus a WARN per batch).
    #[test]
    fn only_the_verdict_is_required_to_decode() {
        let r = parse_batch_report(r#"{"success":true}"#).unwrap();
        assert!(r.success);
        assert!(r.failed.is_empty());
        assert_eq!((r.processed, r.succeeded), (None, None));
        assert!(parse_batch_report(r#"{"success":true,"failed":[]}"#).is_some());
        assert!(parse_batch_report(r#"{"processed":19,"succeeded":19,"failed":[]}"#).is_none());
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
    fn a_failed_path_we_never_sent_is_an_error_that_names_it() {
        let ops = [add("a")];
        let sent: Vec<&BatchOp> = ops.iter().collect();
        let failed = vec![
            FailedOp {
                path: "".to_string(),
                error: "boom".to_string(),
            },
            FailedOp {
                path: "zz/never".to_string(),
                error: "boom".to_string(),
            },
        ];
        let msg = format!("{:#}", retry_ops(&sent, &failed).unwrap_err());
        assert!(msg.contains("2 failed path(s)"), "{msg}");
        assert!(msg.contains(r#""", "zz/never""#), "{msg}");
    }

    #[test]
    fn name_some_caps_at_five_and_quotes() {
        let many: Vec<&str> = (0..7).map(|_| "p").collect();
        assert_eq!(
            name_some(many.iter().copied()),
            r#""p", "p", "p", "p", "p", …"#
        );
        assert_eq!(name_some(["a", "b"].into_iter()), r#""a", "b""#);
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

    /// `failed[]` is what gets acted on; a `success:true` beside it does not
    /// talk the loop out of the re-send.
    #[test]
    fn named_failures_are_re_sent_whatever_success_says() {
        let ops = ops(3);
        let mut s = Settlement::new(&ops);
        let body = r#"{"success":true,"processed":3,"succeeded":2,"failed":[{"path":"f1","error":"boom"}]}"#;
        assert_eq!(s.observe(&ok(body)).unwrap(), Step::Retry(S2));
        assert_eq!(pending(&s), ["f1"]);
    }

    #[test]
    fn an_unreadable_body_on_the_first_attempt_trusts_the_status_code() {
        for body in ["", "<html>ok</html>", r#"{"ok":true}"#] {
            let ops = ops(19);
            let mut s = Settlement::new(&ops);
            assert_eq!(s.observe(&ok(body)).unwrap(), Step::Done(19), "{body}");
        }
    }

    /// Attempt 0 named these ops as failed; a bodiless 2xx on the re-send is
    /// not evidence they landed. The attempt is spent, the ops stay pending,
    /// and only a readable body confirms them.
    #[test]
    fn an_unreadable_body_on_a_re_send_does_not_confirm_the_ops_it_covered() {
        let ops = ops(19);
        let mut s = Settlement::new(&ops);
        let first = ok(&partial(19, &["f2", "f7", "f18"]));
        assert_eq!(s.observe(&first).unwrap(), Step::Retry(S2));
        assert_eq!(s.observe(&ok("")).unwrap(), Step::Retry(S4));
        assert_eq!(pending(&s), ["f2", "f7", "f18"]);
        assert_eq!(s.observe(&ok(&clean(3))).unwrap(), Step::Done(19));
    }

    #[test]
    fn unreadable_re_sends_run_out_of_attempts_like_failed_ones() {
        let ops = ops(19);
        let mut s = Settlement::new(&ops);
        let first = ok(&partial(19, &["f2", "f7", "f18"]));
        assert_eq!(s.observe(&first).unwrap(), Step::Retry(S2));
        assert_eq!(s.observe(&ok("<html>")).unwrap(), Step::Retry(S4));
        let msg = err_text(s.observe(&ok("<html>")));
        assert!(
            msg.contains("3 operation(s) still failing after 3 attempts"),
            "{msg}"
        );
        assert!(msg.contains(r#""f2", "f7", "f18""#), "{msg}");
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
        assert!(msg.contains(r#""f2", "f7""#), "{msg}");
    }

    /// A 200 whose counters say fewer ops were attempted or applied than were
    /// sent, with nothing named: the observed loss with the signal in hand.
    /// Fatal, because there is nothing to re-send and a WARN would end in
    /// `PLAN_RESULT failed:0`.
    #[test]
    fn a_report_that_does_not_add_up_against_the_request_is_fatal() {
        let ops = ops(19);
        let mut s = Settlement::new(&ops);
        let body = r#"{"success":true,"processed":16,"succeeded":16,"failed":[]}"#;
        let msg = err_text(s.observe(&ok(body)));
        assert!(msg.contains("processed=16 for 19 ops sent"), "{msg}");

        let mut s = Settlement::new(&ops);
        let body = r#"{"success":true,"processed":19,"succeeded":16,"failed":[]}"#;
        let msg = err_text(s.observe(&ok(body)));
        assert!(msg.contains("succeeded=16 for 19 ops sent"), "{msg}");
        assert!(msg.contains("expected 19"), "{msg}");

        // Named failures and a `succeeded` that does not leave room for them.
        let mut s = Settlement::new(&ops);
        let body = r#"{"success":false,"processed":19,"succeeded":19,"failed":[{"path":"f2","error":"boom"}]}"#;
        let msg = err_text(s.observe(&ok(body)));
        assert!(
            msg.contains("succeeded=19 for 19 ops sent with 1 named failed"),
            "{msg}"
        );
        assert!(msg.contains("expected 18"), "{msg}");

        // A negative counter is readable (so `failed[]` is not lost) and fails
        // here rather than falling back to the status code.
        let mut s = Settlement::new(&ops);
        let body = r#"{"success":true,"processed":-1,"succeeded":19,"failed":[]}"#;
        assert!(err_text(s.observe(&ok(body))).contains("processed=-1"));
    }

    /// `Done` never exceeds the ops handed in: a re-send of 3 cannot confirm
    /// 19 whatever the body's counter says.
    #[test]
    fn a_re_send_cannot_confirm_more_than_it_re_sent() {
        let ops = ops(19);
        let mut s = Settlement::new(&ops);
        let first = ok(&partial(19, &["f2", "f7", "f18"]));
        assert_eq!(s.observe(&first).unwrap(), Step::Retry(S2));
        let inflated = r#"{"success":true,"processed":3,"succeeded":19,"failed":[]}"#;
        let msg = err_text(s.observe(&ok(inflated)));
        assert!(msg.contains("succeeded=19 for 3 ops sent"), "{msg}");
    }

    #[test]
    fn counters_may_be_absent_and_the_verdict_still_decides() {
        let ops = ops(19);
        let mut s = Settlement::new(&ops);
        assert_eq!(
            s.observe(&ok(r#"{"success":true}"#)).unwrap(),
            Step::Done(19)
        );

        let mut s = Settlement::new(&ops);
        let body = r#"{"success":false,"failed":[{"path":"f4","error":"boom"}]}"#;
        assert_eq!(s.observe(&ok(body)).unwrap(), Step::Retry(S2));
        assert_eq!(pending(&s), ["f4"]);
    }

    #[test]
    fn a_reported_path_we_never_sent_fails_the_chunk_and_is_named() {
        let ops = ops(3);
        let mut s = Settlement::new(&ops);
        let body = r#"{"success":false,"processed":3,"succeeded":2,"failed":[{"path":"nope","error":"boom"}]}"#;
        let msg = err_text(s.observe(&ok(body)));
        assert!(msg.contains(r#""nope""#), "{msg}");
    }

    /// The documented 422 carries the same body as the 200: a partial failure
    /// delivered that way gets its re-send, not a whole-range re-copy.
    #[test]
    fn a_422_with_the_documented_body_is_settled_like_a_200() {
        let ops = ops(19);
        let mut s = Settlement::new(&ops);
        let first = unprocessable(&partial(19, &["f2"]));
        assert_eq!(s.observe(&first).unwrap(), Step::Retry(S2));
        assert_eq!(pending(&s), ["f2"]);
        assert_eq!(s.observe(&ok(&clean(1))).unwrap(), Step::Done(19));
    }

    /// A 422 without the documented body names nothing to re-send, and its
    /// status rules out trusting it, so it is final — on any attempt.
    #[test]
    fn a_422_without_a_readable_body_is_final() {
        let ops = ops(3);
        let mut s = Settlement::new(&ops);
        let msg = err_text(s.observe(&unprocessable(r#"{"error":"bad request"}"#)));
        assert!(msg.contains("422"), "{msg}");
        assert!(msg.contains("bad request"), "{msg}");
    }

    #[test]
    fn other_non_2xx_statuses_are_final_and_carry_their_body() {
        for status in [StatusCode::FORBIDDEN, StatusCode::NOT_FOUND] {
            let ops = ops(3);
            let mut s = Settlement::new(&ops);
            let msg = err_text(s.observe(&response(status, "the reason")));
            assert!(msg.contains(status.as_str()), "{msg}");
            assert!(msg.contains("the reason"), "{msg}");
            assert!(msg.contains("req-1"), "{msg}");
        }
    }

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

    /// The `Link` header as the Hub sends it (recorded 2026-09-22).
    #[test]
    fn next_cursor_is_read_from_the_rel_next_link() {
        let link = r#"<https://huggingface.co/api/buckets/o/n/tree?limit=3&recursive=true&cursor=eyJwIjoiYSJ9>; rel="next""#;
        assert_eq!(next_cursor(link).as_deref(), Some("eyJwIjoiYSJ9"));
        let two = r#"<https://huggingface.co/x?cursor=first>; rel="first", <https://huggingface.co/x?cursor=abc%2Fdef>; rel="next""#;
        assert_eq!(next_cursor(two).as_deref(), Some("abc/def"));
        assert_eq!(
            next_cursor(r#"<https://huggingface.co/x?cursor=p>; rel="prev""#),
            None
        );
        assert_eq!(
            next_cursor(r#"<https://huggingface.co/x>; rel="next""#),
            None
        );
        assert_eq!(next_cursor(""), None);
    }
}
