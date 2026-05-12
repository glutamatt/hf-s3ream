# hf-s3ream

Stream S3 buckets into [HuggingFace Buckets](https://huggingface.co/docs/hub/storage-backends) — fast, parallel, no disk staging.

`hf-s3ream` reads an S3 prefix and writes it into a HuggingFace Bucket through xet-core's content-addressed upload pipeline. Bytes flow S3 → memory → xet CAS in a single stream; no temporary copy on local disk. Re-running an interrupted clone re-uploads only what's missing (CAS dedup).

Ships as a small (~70 MiB) debian-slim container image on GHCR, runnable on any SLURM cluster with [Pyxis](https://github.com/NVIDIA/pyxis) via a one-liner.

## Quickstart

```bash
curl -fsSL https://github.com/glutamatt/hf-s3ream/releases/latest/download/submit.sh \
  | bash -s -- \
      --partition cpu \
      --src s3://my-bucket/some/prefix/ \
      --dst your-org/your-bucket
```

That submits one SLURM job that pulls the latest released image, mounts your `~/.aws` read-only, and uploads everything under the prefix into `your-org/your-bucket`. `HF_TOKEN` is read from `~/.cache/huggingface/token` if not already in your env.

## Advanced usage

Download the submit script once (pinned to a release), then run it as needed:

```bash
curl -fsSL https://github.com/glutamatt/hf-s3ream/releases/latest/download/submit.sh -o submit.sh
chmod +x submit.sh
./submit.sh --help
./submit.sh \
    --partition cpu \
    --src s3://my-bucket/large-dataset/ \
    --dst your-org/large-dataset \
    --shards 64 \
    --time 12:00:00 \
    --exclude '*.tmp' \
    --exclude '*decay*'
```

The downloaded `submit.sh` has the release's image tag baked in — your job will keep using that exact version even if newer releases ship later. Override with `--image-tag vX.Y.Z` to pin a different version.

Args after a literal `--` are forwarded straight to the `hf-s3ream` binary:

```bash
./submit.sh --partition cpu --src s3://… --dst your-org/… \
    -- --parallel-files 64 --s3-part-concurrency 16
```

## Sharding

For large prefixes, pass `--shards N` to spread the work across a SLURM array of N tasks. The sharder is FNV-1a64 deterministic on the S3 key, so:

- failed array indices can be requeued and reprocess the same file subset
- raising `--shards` later is safe (no overlap with already-uploaded shards on retry, since CAS dedups)
- each task asks for `--cpus-per-task` × `--mem`, letting the scheduler pack tasks onto whatever instance class your partition serves

A common starting point for a multi-TB clone: `--shards 64 --time 8:00:00`.

## Local (no SLURM)

The image runs anywhere Docker does:

```bash
docker run --rm \
    -e HF_TOKEN \
    -v ~/.aws:/root/.aws:ro \
    -e AWS_SHARED_CREDENTIALS_FILE=/root/.aws/credentials \
    ghcr.io/glutamatt/hf-s3ream:latest \
    s3://my-bucket/prefix/ your-org/your-bucket
```

## Architecture

```
                              parallel files (--parallel-files)
                                     ┌───────────────┐
   s3://bucket/prefix/ ──list──▶ work │  task 0..N    │ ── xet CAS upload ──▶ HF CAS
                       (aws-sdk)│queue│  per file:    │   (xet-data)
                                     │  ranged GETs  │
                                     │  via          │
                                     │  object_store │
                                     └───────────────┘
                                            │
                                            ▼
                                  collect XetFileInfo per file
                                            │
                                            ▼
                              single POST /api/buckets/{id}/batch
                                  (ndjson AddFile ops, atomic)
```

- **Listing**: `aws-sdk-s3` (tolerates exotic keys that `object_store::list` rejects, e.g. empty path segments).
- **Reads**: `object_store` with ranged GETs (`--s3-part-concurrency` parallel reads per file).
- **Uploads**: `xet-core`'s `FileUploadSession` shared across all files — xorbs and shards are dedup'd across the whole job.
- **Commit**: one batched ndjson POST at the end. Either the whole job lands or none of it does.

No bytes touch local disk in the hot path; memory is bounded by the xorb formation window (~64–128 MiB per active file) plus stream buffers.

## Credentials

| Service       | Source (in priority order)                                            |
|---------------|------------------------------------------------------------------------|
| HuggingFace   | `--hf-token` flag → `$HF_TOKEN` env → `~/.cache/huggingface/token`     |
| AWS           | standard SDK chain: env vars → `~/.aws/credentials` → IRSA → IMDS      |

Both are passed through to the container; the SLURM submit script reads `HF_TOKEN` on the login node and mounts `~/.aws` read-only inside the container.

## Tuning knobs

The defaults target a ~25 Gbps cloud VM (8 vCPU, 32 GiB RAM, e.g. c6i.4xlarge). For different shapes:

| Flag                       | Default | Description                                            |
|----------------------------|---------|--------------------------------------------------------|
| `--parallel-files`         | 32      | concurrent in-flight file uploads                      |
| `--s3-part-concurrency`    | 8       | parallel ranged GETs per file (multipart download)     |
| `--s3-part-size-mib`       | 16      | size of each ranged GET                                |
| `--limit-gib`              | 0       | stop after N GiB queued (0 = unlimited; benchmarks)    |
| `--xor-byte`               | 0       | XOR data before upload to defeat dedup (benchmarks)    |

Plus xet-core env vars (`HF_XET_CLIENT_AC_MAX_UPLOAD_CONCURRENCY`, `HF_XET_DATA_MAX_CONCURRENT_FILE_INGESTION`, …) for the upload side.

## Contributing

This repo uses [Conventional Commits](https://www.conventionalcommits.org/) — `feat:`, `fix:`, `feat!:` for breaking. [`release-plz`](https://github.com/MarcoIeni/release-plz) reads the log to bump the version, update `CHANGELOG.md`, and open a release PR; merging the PR triggers the image build.

## License

Apache-2.0.
