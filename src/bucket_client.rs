//! Minimal REST client for HF Bucket batch operations.
//!
//! POST {endpoint}/api/buckets/{org}/{name}/batch with content-type application/x-ndjson.
//! Body: one JSON op per line (AddFile only — we don't currently issue deletes).
//!
//! Mirrors hf-mount's `src/hub_api.rs` `batch_operations()` flow.

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

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

    /// GET /api/buckets/{id}/xet-write-token — returns CAS endpoint + JWT.
    pub async fn get_cas_write_token(&self, bucket: &BucketRef) -> Result<CasTokenInfo> {
        let url = format!("{}/api/buckets/{}/xet-write-token", self.endpoint, bucket.id());
        let resp = self
            .http
            .get(&url)
            .bearer_auth(&self.token)
            .send()
            .await
            .context("GET /api/buckets/.../xet-write-token")?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("xet-write-token failed: HTTP {status}: {body}");
        }
        let info = resp.json::<CasTokenInfo>().await.context("decode CasTokenInfo")?;
        Ok(info)
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
            .http
            .post(&url)
            .bearer_auth(&self.token)
            .header("content-type", "application/x-ndjson")
            .body(body)
            .send()
            .await
            .context("POST /api/buckets/.../batch")?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("bucket batch failed: HTTP {status}: {body}");
        }
        Ok(())
    }
}
