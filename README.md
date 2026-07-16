# hf-s3ream

Stream S3 buckets into [HuggingFace Buckets](https://huggingface.co/storage) — fast, parallel, no disk staging, no infrastructure.

`hf-s3ream` reads an S3 prefix and writes it into a HuggingFace Bucket through xet-core's content-addressed upload pipeline. Bytes flow S3 → memory → xet CAS in a single stream; no temporary copy on local disk. Re-running an interrupted clone re-uploads only what's missing (CAS dedup).

The main way to use it is the **web UI**: a [Hugging Face Space](https://huggingface.co/spaces/glutamatt/hf-s3ream) that runs the whole copy on [HF Jobs](https://huggingface.co/docs/hub/jobs) for you.

## Quickstart — the Space

Open **[huggingface.co/spaces/glutamatt/hf-s3ream](https://huggingface.co/spaces/glutamatt/hf-s3ream)** and:

1. **Sign in with Hugging Face** (OAuth, in the browser). Jobs are billed per second to *your* account — any account with [pre-paid credits](https://huggingface.co/settings/billing) works.
2. Enter the **S3 source prefix**, the **destination bucket**, and **AWS credentials** for the source.
3. **Analyze** — a cheap `--dry-run` Job lists the source on HF's side: it validates S3 access, auto-detects the region, sizes the transfer, and recommends a configuration before you spend anything.
4. **Run** — the page launches **one planner Job**, then just observes:
   - the planner lists the prefix **once** and cuts it into contiguous key ranges (byte-balanced, layout-agnostic);
   - it spawns one **copier Job per range** (staggered, bounded in-flight), each streaming its slice S3 → xet CAS;
   - it monitors the fleet and respawns failed copiers (idempotent — CAS dedups already-uploaded content).

   The page streams every copier's progress into one live aggregate graph. The planner is fully autonomous: you can close the tab and the copy completes anyway — the fleet stays visible on your [HF Jobs page](https://huggingface.co/jobs) (the Space itself doesn't re-attach to a running fleet yet).

Throughput scales with the number of copiers, since each one is an independent S3 → CAS path. Jobs are billed per second at the [published flavor rates](https://huggingface.co/docs/hub/jobs); the Analyze step sizes the transfer and suggests a configuration before you launch the fleet.

### Trust model

The Space is a **fully static page — there is no backend.** The browser talks straight to the Hugging Face API (OAuth + CORS). Your AWS credentials never touch any server we run: they stay in your browser and are sent only into the Jobs' **encrypted secrets** via the HF API. Prefer a scoped, read-only, short-lived key for just the source prefix.

### Caveats

- **VPC-locked source buckets are unreachable from HF Jobs.** Jobs run on HF's network, outside your AWS VPC, so a bucket policy pinned to `aws:sourceVpce` / `aws:SourceVpc` returns `403` there regardless of valid creds — that's network topology, not a credentials problem. The dry-run preflight surfaces it. For those buckets, run the container [from inside the VPC](#run-the-container-yourself).
- **The source prefix should be frozen during the copy.** The planner cuts key ranges from one listing pass; if the source mutates mid-run, the plan goes stale. Fine for migrations and archival (the intended use), not for live-mutating buckets.

## Run the container yourself

The same image runs anywhere Docker does — useful for VPC-locked buckets or when you'd rather use your own machine's bandwidth:

```bash
eval "$(aws configure export-credentials --format env)"   # or export AWS_ACCESS_KEY_ID=… yourself
docker run --rm \
    -e HF_TOKEN -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
    ghcr.io/glutamatt/hf-s3ream:latest \
    s3://my-bucket/prefix/ your-org/your-bucket
```

One process copies the whole prefix. To split a big prefix across several machines/processes, give each a contiguous key slice with `--start-after K` (exclusive) / `--stop-at K` (inclusive) — that's exactly what the planner automates on HF Jobs.

## Architecture

```
              ┌─────────── planner (1 job) ───────────┐
              │ ListObjectsV2 once → cut key ranges   │
              │ spawn / monitor / respawn copiers     │
              └──────┬──────────┬──────────┬──────────┘
                     ▼          ▼          ▼
                 copier 0    copier 1  …  copier N     (1 job per range)
              ┌──────────────────────────────────────┐
   s3://…  ──▶│ list slice ─▶ ranged GETs ─▶ xet CAS │──▶ HF Bucket
              │   (overlapped: uploads start on the  │
              │    first object; commits pipeline    │
              │    in the background)                │
              └──────────────────────────────────────┘
```

Inside each copier:

- **Listing**: `aws-sdk-s3` (tolerates exotic keys that `object_store::list` rejects, e.g. empty path segments), streamed — listing, uploading, and committing overlap; memory stays bounded.
- **Reads**: `object_store` ranged GETs (`--s3-part-concurrency` parallel reads per file), decoupled from xet compute by a spawned reader + bounded channel so S3 reads never stall behind hashing.
- **Uploads**: `xet-core`'s `FileUploadSession` shared by all files of a commit chunk — xorbs and shards are dedup'd within the chunk.
- **Commit**: one batched ndjson POST per `--commit-chunk` files (or `--commit-gib`), pipelined — sessions rotate as files stream in and finalize in the background, so commits never pause the transfer.

No bytes touch local disk in the hot path; memory is bounded by the xorb formation window (~64–128 MiB per active file) plus stream buffers.

## Credentials

| Service       | Source (in priority order)                                            |
|---------------|------------------------------------------------------------------------|
| HuggingFace   | `--hf-token` flag → `$HF_TOKEN` env → `~/.cache/huggingface/token`     |
| AWS           | env vars (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`); on AWS compute, web identity (IRSA), ECS task roles, and IMDS instance roles also work |

The Space passes both as encrypted Job secrets. **`~/.aws/credentials` profiles are not enough for a manual run**: the listing client resolves them, but the transfer client (`object_store`) only takes env vars or instance/task roles — export env creds (`aws configure export-credentials --format env`) as in the docker example above.

## Tuning knobs

The Space's Analyze step picks these for you. For manual runs (`--help` for all):

| Flag                       | Default | Description                                            |
|----------------------------|---------|--------------------------------------------------------|
| `--parallel-files`         | 32      | concurrent in-flight file uploads                      |
| `--s3-part-concurrency`    | 8       | parallel ranged GETs per file (multipart download)     |
| `--s3-part-size-mib`       | 16      | size of each ranged GET                                |
| `--start-after` / `--stop-at` | —    | copy only a contiguous key slice (exclusive / inclusive) |
| `--exclude GLOB`           | —       | skip keys matching a glob (repeatable)                 |
| `--commit-chunk`           | 1000    | files per batched bucket commit                        |
| `--commit-gib`             | 16      | also commit once the session reaches this many GiB     |
| `--limit-gib`              | 0       | stop after N GiB queued (0 = unlimited; benchmarks)    |
| `--xor-byte`               | 0       | XOR data before upload to defeat dedup (benchmarks)    |
| `--dry-run`                | —       | list + stats only, no transfer                         |
| `--plan`                   | —       | planner mode: cut ranges + spawn copier Jobs (see `--range-gib`, `--max-inflight`, …) |

Plus xet-core env vars (`HF_XET_CLIENT_AC_MAX_UPLOAD_CONCURRENCY`, `HF_XET_DATA_MAX_CONCURRENT_FILE_INGESTION`, …) for the upload side.

## Contributing

This repo uses [Conventional Commits](https://www.conventionalcommits.org/) — `feat:`, `fix:`, `feat!:` for breaking.

The Space front-end lives in [`space/`](space/) and is auto-deployed by GitHub Actions on push. Releases are manual: bump `version` in `Cargo.toml`, then tag and push — `git tag vX.Y.Z && git push origin vX.Y.Z`. The tag triggers `release.yml`, which builds and pushes the container image (`ghcr.io/glutamatt/hf-s3ream:vX.Y.Z` + `:latest`) and publishes a GitHub release.

> Why no release automation? The crate depends on unpublished xet-core APIs via git deps, and release-bot tooling (release-plz) runs `cargo package` *with verification* to compute next versions — the verify build resolves deps from crates.io (git specs are stripped when packaging) and fails on the unpublished APIs. Revisit if xet-core publishes them or release-plz grows a `--no-verify` option for version determination.

## License

Apache-2.0.
