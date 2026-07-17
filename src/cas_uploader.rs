//! CAS upload pipeline for HF Buckets.
//!
//! Streams each S3 object into HF Xet CAS via the `hf-xet` crate's high-level
//! `xet_session` API and returns the resulting `XetFileInfo` (whose `.hash()`
//! is the `xetHash` we hand to `BatchOp::AddFile`).
//!
//! Per file:
//!   1. `commit.upload_stream(None, Sha256Policy::Skip)` → a streaming handle.
//!   2. Drive the handle with `handle.write(chunk).await` per S3 chunk.
//!   3. `handle.finish()` returns `XetFileMetadata` — `.xet_info.hash()` is the
//!      `xetHash`, `.dedup_metrics` feeds our progress counters.
//!   4. `commit.commit()` flushes any pending xorbs/shards to the CAS server.
//!      This is CAS-side only; registering the files in the bucket is a separate
//!      `/batch` call the caller makes with the returned hashes.

use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use reqwest::header::HeaderMap;
use tracing::info;

use xet::xet_session::{
    DeduplicationMetrics, Sha256Policy, XetFileInfo, XetSession, XetSessionBuilder, XetUploadCommit,
};

use crate::bucket_client::BucketClient;
use crate::BucketRef;

/// One-time CAS plumbing shared by every commit session: a single `XetSession`
/// (which owns the tokio runtime handle + `XetContext`), the CAS endpoint, the
/// initial write token, and the bucket token-refresh route.
///
/// xet-core's adaptive-concurrency state is keyed per (context, endpoint) since
/// xet-core#871. Every `XetUploadCommit` we open is built from this one shared
/// `XetSession` against the same CAS endpoint, so the learned upload concurrency
/// ramp persists across commit rotations instead of resetting to its floor on
/// every rotation.
pub struct CasUploaderFactory {
    session: XetSession,
    cas_url: String,
    token: String,
    exp: u64,
    refresh_url: String,
    refresh_headers: HeaderMap,
}

impl CasUploaderFactory {
    /// Fetch the initial CAS write token from the Hub (for the CAS endpoint plus
    /// the starting JWT) and build the shared `XetSession` on the caller's tokio
    /// runtime via `with_tokio_handle` (we're called from `#[tokio::main]`, so we
    /// hand it the current handle rather than let it detect/create one).
    /// Subsequent commits refresh the token automatically via the bucket
    /// `xet-write-token` route.
    pub async fn new(bucket_client: Arc<BucketClient>, bucket: BucketRef) -> Result<Self> {
        let initial = bucket_client
            .get_cas_write_token(&bucket)
            .await
            .context("fetch initial CAS write token")?;
        info!(cas_url = %initial.cas_url, exp = initial.exp, "got CAS write token");

        let (refresh_url, refresh_headers) = bucket_client.xet_write_token_refresh(&bucket);

        let session = XetSessionBuilder::new()
            .with_tokio_handle(tokio::runtime::Handle::current())
            .build()
            .map_err(|e| anyhow::anyhow!("XetSession build: {e}"))?;

        Ok(Self {
            session,
            cas_url: initial.cas_url,
            token: initial.access_token,
            exp: initial.exp,
            refresh_url,
            refresh_headers,
        })
    }

    /// Open a fresh upload commit on the shared session — one per rotating commit
    /// session. Cheap: no token fetch, no new context/runtime. The commit reuses
    /// the shared endpoint + auto-refreshing write token.
    pub async fn new_uploader(&self) -> Result<CasUploader> {
        let commit = self
            .session
            .new_upload_commit()
            .map_err(|e| anyhow::anyhow!("new_upload_commit: {e}"))?
            .with_endpoint(self.cas_url.clone())
            .with_token_info(self.token.clone(), self.exp)
            .with_token_refresh_url(self.refresh_url.clone(), self.refresh_headers.clone())
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("XetUploadCommit build: {e}"))?;
        Ok(CasUploader { commit })
    }
}

/// Uploads bytes to CAS and returns `XetFileInfo` (hash + size + optional sha256).
///
/// Owns a single `XetUploadCommit` for the whole batch — multiple parallel files
/// share one commit so xet can dedup xorbs across files and emit a single shard
/// upload at `finalize`. Mirrors the batched-flush model hf-mount uses in
/// `--advanced-writes`, but with no FUSE in the loop.
pub struct CasUploader {
    commit: XetUploadCommit,
}

impl CasUploader {
    /// Upload one file's bytes from an async byte stream into the shared commit.
    /// Returns the `XetFileInfo` whose `.hash()` is the `xetHash` for
    /// `BatchOp::AddFile`. Safe to call concurrently across parallel files —
    /// each `upload_stream` handle is independent.
    ///
    /// `on_ingest(len)` fires after each chunk is ACCEPTED by `write` —
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
        let handle = self
            .commit
            .upload_stream(None, Sha256Policy::Skip)
            .await
            .map_err(|e| anyhow::anyhow!("upload_stream: {e}"))?;

        let mut total: u64 = 0;
        while let Some(chunk) = chunks.next().await {
            let buf = chunk.context("read S3 chunk")?;
            let n = buf.len() as u64;
            handle
                .write(buf)
                .await
                .map_err(|e| anyhow::anyhow!("stream write: {e}"))?;
            total += n;
            on_ingest(n);
        }

        if total != expected_size {
            anyhow::bail!(
                "read truncated: ingested {total} of {expected_size} bytes before end-of-stream \
                 — refusing to finalize (reader died, partial S3 response, or source mutated)"
            );
        }

        let meta = handle
            .finish()
            .await
            .map_err(|e| anyhow::anyhow!("stream finish: {e}"))?;

        Ok((meta.xet_info, meta.dedup_metrics))
    }

    /// Flush any pending xorbs/shards to the CAS server and close the commit.
    /// Call once after all `upload_stream` calls have completed.
    pub async fn finalize(&self) -> Result<()> {
        self.commit
            .commit()
            .await
            .map_err(|e| anyhow::anyhow!("commit: {e}"))?;
        Ok(())
    }
}
