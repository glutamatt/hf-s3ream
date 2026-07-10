//! CAS upload pipeline for HF Buckets.
//!
//! Mirrors hf-mount's `src/xet.rs` streaming-writer pattern, but plumbs the
//! bucket-specific xet-write-token endpoint instead of the repo one.
//!
//! Per file:
//!   1. `FileUploadSession::new(config)` — fresh session per file (matches
//!      hf-mount's `create_streaming_writer` for parallel safety).
//!   2. `session.start_clean(None, None, Sha256Policy::Skip)` →
//!      `(file_id, SingleFileCleaner)`.
//!   3. Drive the cleaner with `cleaner.add_data(&chunk).await` per S3 chunk.
//!   4. `cleaner.finish()` returns `(XetFileInfo, dedup_metrics)` — the
//!      `XetFileInfo.hash` is the `xetHash` we need for `BatchOp::AddFile`.
//!   5. `session.finalize()` to flush any pending xorbs/shards.

use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use tracing::info;

use xet_client::cas_client::auth::{AuthError, TokenInfo, TokenRefresher};
use xet_data::processing::data_client::default_config;
use xet_data::processing::{FileUploadSession, Sha256Policy, XetFileInfo};

use crate::bucket_client::{BucketClient, CasTokenInfo};
use crate::BucketRef;

/// Uploads bytes to CAS and returns `XetFileInfo` (hash + size + optional sha256).
///
/// Owns a single shared `FileUploadSession` for the whole batch — multiple
/// parallel files share one session so xet-data can dedup xorbs across files
/// and emit a single shard upload at finalize. Mirrors the PR #72 batched-flush
/// model that hf-mount uses in `--advanced-writes`, but with no FUSE in the loop.
pub struct CasUploader {
    session: Arc<FileUploadSession>,
}

impl CasUploader {
    /// Build a new uploader: fetches the initial CAS write token from the Hub
    /// and constructs a `TranslatorConfig` with a refresher that re-fetches
    /// when the JWT expires. Wraps the caller's tokio runtime via
    /// `XetContext::from_external` so we share a single runtime.
    pub async fn new(bucket_client: Arc<BucketClient>, bucket: BucketRef) -> Result<Self> {
        let initial = bucket_client
            .get_cas_write_token(&bucket)
            .await
            .context("fetch initial CAS write token")?;
        info!(cas_url = %initial.cas_url, exp = initial.exp, "got CAS write token");

        let refresher: Arc<dyn TokenRefresher> = Arc::new(BucketTokenRefresher {
            client: bucket_client,
            bucket,
        });

        // Use the existing tokio runtime (we're called from #[tokio::main]).
        // XetContext::default() would try to detect/create one; we hand it
        // the current handle explicitly to avoid runtime nesting surprises.
        let ctx = xet_runtime::core::XetContext::from_external(
            tokio::runtime::Handle::current(),
            xet_runtime::config::XetConfig::new(),
        );

        let config = default_config(
            &ctx,
            initial.cas_url.clone(),
            Some((initial.access_token, initial.exp)),
            Some(refresher),
            None,
        )
        .map_err(|e| anyhow::anyhow!("default_config: {e}"))?;

        let session = FileUploadSession::new(Arc::new(config))
            .await
            .map_err(|e| anyhow::anyhow!("FileUploadSession::new: {e}"))?;

        Ok(Self { session })
    }

    /// Upload one file's bytes from an async byte stream into the shared
    /// session. Returns the `XetFileInfo` whose `.hash` is the `xetHash` for
    /// `BatchOp::AddFile`. Safe to call concurrently across parallel files —
    /// `start_clean` takes `&Arc<Self>` and each cleaner is independent.
    ///
    /// `on_ingest(len)` fires after each chunk is ACCEPTED by `add_data` —
    /// distinct from the S3-read credit, so metrics can tell "S3 delivering
    /// but CAS blocked" from "S3 stalled".
    pub async fn upload_stream<S, F>(&self, mut chunks: S, mut on_ingest: F) -> Result<XetFileInfo>
    where
        S: Stream<Item = Result<Bytes>> + Unpin,
        F: FnMut(u64),
    {
        let (_id, mut cleaner) = self
            .session
            .start_clean(None, None, Sha256Policy::Skip)
            .map_err(|e| anyhow::anyhow!("start_clean: {e}"))?;

        while let Some(chunk) = chunks.next().await {
            let buf = chunk.context("read S3 chunk")?;
            cleaner
                .add_data(&buf)
                .await
                .map_err(|e| anyhow::anyhow!("add_data: {e}"))?;
            on_ingest(buf.len() as u64);
        }

        let (info, _metrics) = cleaner
            .finish()
            .await
            .map_err(|e| anyhow::anyhow!("cleaner.finish: {e}"))?;

        Ok(info)
    }

    /// Flush any pending xorbs/shards and close the upload session.
    /// Call once after all `upload_stream` calls have completed.
    pub async fn finalize(&self) -> Result<()> {
        self.session
            .clone()
            .finalize()
            .await
            .map_err(|e| anyhow::anyhow!("session.finalize: {e}"))?;
        Ok(())
    }
}

/// Re-fetches the CAS write token from the Hub when the JWT is about to expire.
struct BucketTokenRefresher {
    client: Arc<BucketClient>,
    bucket: BucketRef,
}

#[async_trait]
impl TokenRefresher for BucketTokenRefresher {
    async fn refresh(&self) -> std::result::Result<TokenInfo, AuthError> {
        let info: CasTokenInfo = self
            .client
            .get_cas_write_token(&self.bucket)
            .await
            .map_err(|e| AuthError::TokenRefreshFailure(e.to_string()))?;
        Ok((info.access_token, info.exp))
    }
}
