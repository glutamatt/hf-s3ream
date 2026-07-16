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
use xet_data::deduplication::DeduplicationMetrics;
use xet_data::processing::data_client::default_config;
use xet_data::processing::{FileUploadSession, Sha256Policy, XetFileInfo};

use crate::bucket_client::{BucketClient, CasTokenInfo};
use crate::BucketRef;

/// One-time CAS plumbing shared by every commit session: the `XetContext`,
/// the `TranslatorConfig` (CAS endpoint + auto-refreshing write token), and —
/// critically — xet-core's adaptive-concurrency state, which is keyed per
/// (context, endpoint) since xet-core#871. Building a fresh context per
/// session would reset the learned upload concurrency back to its floor on
/// every rotation; sharing one factory lets the ramp persist across the run.
pub struct CasUploaderFactory {
    config: Arc<xet_data::processing::configurations::TranslatorConfig>,
}

impl CasUploaderFactory {
    /// Fetch the initial CAS write token from the Hub and build the shared
    /// `TranslatorConfig` with a refresher that re-fetches when the JWT
    /// expires. Wraps the caller's tokio runtime via
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

        Ok(Self {
            config: Arc::new(config),
        })
    }

    /// Open a fresh `FileUploadSession` on the shared config — one per
    /// rotating commit session. Cheap: no token fetch, no new context.
    pub async fn new_uploader(&self) -> Result<CasUploader> {
        let session = FileUploadSession::new(self.config.clone())
            .await
            .map_err(|e| anyhow::anyhow!("FileUploadSession::new: {e}"))?;
        Ok(CasUploader { session })
    }
}

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
    /// Upload one file's bytes from an async byte stream into the shared
    /// session. Returns the `XetFileInfo` whose `.hash` is the `xetHash` for
    /// `BatchOp::AddFile`. Safe to call concurrently across parallel files —
    /// `start_clean` takes `&Arc<Self>` and each cleaner is independent.
    ///
    /// `on_ingest(len)` fires after each chunk is ACCEPTED by `add_data` —
    /// distinct from the S3-read credit, so metrics can tell "S3 delivering
    /// but CAS blocked" from "S3 stalled".
    ///
    /// `expected_size` is the source object's byte length. INTEGRITY GUARD: the
    /// stream may be fed by a decoupled reader task (multipart path) whose death
    /// closes the channel — which this loop would otherwise see as a clean
    /// end-of-stream and finalize a TRUNCATED file with a self-consistent (but
    /// wrong) hash. We refuse to finalize unless the whole object was ingested,
    /// turning any short read (reader panic, partial S3 response, mutated
    /// source) into a retryable error instead of silent corruption.
    pub async fn upload_stream<S, F>(
        &self,
        mut chunks: S,
        expected_size: u64,
        mut on_ingest: F,
    ) -> Result<(XetFileInfo, DeduplicationMetrics)>
    where
        S: Stream<Item = Result<Bytes>> + Unpin,
        F: FnMut(u64),
    {
        let (_id, mut cleaner) = self
            .session
            .start_clean(None, None, Sha256Policy::Skip)
            .map_err(|e| anyhow::anyhow!("start_clean: {e}"))?;

        let mut total: u64 = 0;
        while let Some(chunk) = chunks.next().await {
            let buf = chunk.context("read S3 chunk")?;
            cleaner
                .add_data(&buf)
                .await
                .map_err(|e| anyhow::anyhow!("add_data: {e}"))?;
            total += buf.len() as u64;
            on_ingest(buf.len() as u64);
        }

        if total != expected_size {
            anyhow::bail!(
                "read truncated: ingested {total} of {expected_size} bytes before end-of-stream \
                 — refusing to finalize (reader died, partial S3 response, or source mutated)"
            );
        }

        let (info, metrics) = cleaner
            .finish()
            .await
            .map_err(|e| anyhow::anyhow!("cleaner.finish: {e}"))?;

        Ok((info, metrics))
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
