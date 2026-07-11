# hf-s3ream

Stream S3 buckets into [HuggingFace Buckets](https://huggingface.co/storage) — fast, parallel, no disk staging.

`hf-s3ream` reads an S3 prefix and writes it into a HuggingFace Bucket through xet-core's content-addressed upload pipeline. Bytes flow S3 → memory → xet CAS in a single stream; no temporary copy on local disk. Re-running an interrupted clone re-uploads only what's missing (CAS dedup).

Ships as a small (~70 MiB) debian-slim container image on GHCR. Run it three ways: on [**HF Jobs**](https://huggingface.co/docs/hub/jobs) with zero infrastructure (recommended — HF runs the copy for you), on any SLURM cluster with [Pyxis](https://github.com/NVIDIA/pyxis), or locally with Docker.

## Quickstart — run it on HF Jobs (no infrastructure)

The easiest way: let Hugging Face run the copy for you. No cluster, no servers, nothing to provision — the transfer runs in a throwaway container on [HF Jobs](https://huggingface.co/docs/hub/jobs), billed per second. Any account with [pre-paid credits](https://huggingface.co/settings/billing) can use it.

```bash
curl -fsSL https://github.com/glutamatt/hf-s3ream/releases/latest/download/hfjob.sh \
  | bash -s -- \
      --src s3://my-bucket/some/prefix/ \
      --dst your-org/your-bucket
```

That resolves your HF token and AWS credentials locally, forwards them as **encrypted Job secrets** (never on the command line, never in the logs), and launches one `hf jobs run` that streams the prefix into `your-org/your-bucket` — then tails the job logs until it's done.

Prerequisites on your machine: the [`hf` CLI](https://huggingface.co/docs/huggingface_hub/guides/cli) (`curl -LsSf https://hf.co/cli/install.sh | bash`), a login (`hf auth login`), and *static* AWS keys reachable via your env, `~/.aws/credentials`, or the `aws` CLI — HF Jobs runs off-AWS, so instance roles / IMDS don't apply there.

> **When HF Jobs *can't* reach your bucket.** Because the job runs on HF's network (outside your AWS VPC), a source bucket whose policy locks access to a VPC endpoint (`aws:sourceVpce` / `aws:SourceVpc`) is unreachable from Jobs — you'll get `403` that works locally but fails in the job, regardless of valid creds. That's network topology, not a credentials problem. For VPC-locked buckets, run the copy **from inside the VPC** with the [SLURM path](#run-on-a-slurm-cluster-pyxis) or a `docker run` on an in-VPC box. Normal IAM-restricted buckets are fine — HF Jobs is just "some machine on the internet with your keys."

Common tweaks (`./hfjob.sh --help` for all):

```bash
./hfjob.sh --src s3://my-bucket/huge/ --dst my-org/huge \
    --timeout 8h \               # job is KILLED at the timeout; the commit is atomic at the end
    --create-bucket \            # `hf buckets create` the destination first
    -- --exclude '*.tmp'         # args after `--` are forwarded to hf-s3ream
```

### Which flavor?

**Stick with the default `cpu-upgrade` and don't upsize.** The copy is
bandwidth-bound, not CPU-bound — benchmarked on HF Jobs (11 GiB unique data,
dedup defeated with `--xor-byte`), every CPU flavor clocks ~370–420 MiB/s, so the
bigger tiers just cost more per TiB:

| flavor | vCPU / RAM | $/hr | throughput | $/TiB copied |
|---|---|---|---|---|
| `cpu-basic` | 2 / 16 | $0.01 | 371.0 MiB/s | ~$0.008 |
| **`cpu-upgrade`** | 8 / 32 | $0.03 | **419.7 MiB/s** | **~$0.02** |
| `cpu-xl` | 16 / 124 | $1.00 | 404.7 MiB/s | ~$0.72 |
| `cpu-performance` | 32 / 256 | $1.90 | 399.8 MiB/s | ~$1.38 |

Even 2 vCPU nearly saturates the path (~12% off 8 vCPU), so `cpu-basic` is the
cheapest per TiB — but the gap to `cpu-upgrade` is rounding error (10 TB: $0.08 vs
$0.21), and `cpu-upgrade`'s headroom helps many-small-files prefixes. Default
`cpu-upgrade`; drop to `cpu-basic` only to squeeze pennies on large-file prefixes.

To go faster, **don't buy a bigger flavor — run more jobs.** Each job is an
independent ~400 MiB/s path, so sharding a prefix across N `cpu-upgrade` jobs
(`--shard-id`/`--shard-count`, see [Sharding](#sharding)) reaches ≈ N×400 MiB/s
at N×$0.03/hr — faster *and* cheaper than a single large flavor.

### Driving it from an AI agent

An [agent skill](skills/hf-s3ream/SKILL.md) ships in [`skills/hf-s3ream/`](skills/hf-s3ream/). Install it and your coding agent (Claude Code, Codex, Cursor, …) can run S3 → HF-Bucket copies on HF Jobs for you — it knows the preconditions, flavor/timeout guidance, and how to monitor the job:

```bash
cp -r skills/hf-s3ream ~/.claude/skills/     # Claude Code (or point your agent's skills dir here)
```

## Run on a SLURM cluster (Pyxis)

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

- failed array indices can be requeued and reprocess the same file subset — `--shard-count` is pinned at submit time, so even re-submitting a sparse `--array=4,8,46` keeps the original N-way partition
- raising `--shards` later is safe (no overlap with already-uploaded shards on retry, since CAS dedups)
- each task asks for `--cpus-per-task` × `--mem`, letting the scheduler pack tasks onto whatever instance class your partition serves

A common starting point for a multi-TB clone: `--shards 64 --time 8:00:00`.

On spot/preemptible partitions, tasks survive instance reclaims: termination handlers signal SLURM jobs with USR1 before the node dies, and the generated sbatch script traps it and requeues itself within the grace window (`sbatch --requeue` alone only covers NODE_FAIL, which reclaim handlers never trigger). Requeued attempts append to the same log file.

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
- **Uploads**: `xet-core`'s `FileUploadSession` shared by all files of a commit chunk — xorbs and shards are dedup'd within the chunk.
- **Commit**: one batched ndjson POST per `--commit-chunk` files, pipelined — sessions rotate as files stream in, and each chunk's finalize + POST runs in the background while later files keep uploading, so commits never pause the transfer.

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

This repo uses [Conventional Commits](https://www.conventionalcommits.org/) — `feat:`, `fix:`, `feat!:` for breaking.

Releases are manual: bump `version` in `Cargo.toml`, then tag and push — `git tag vX.Y.Z && git push origin vX.Y.Z`. The tag triggers `release.yml`, which builds and pushes the container image and publishes a GitHub release with `submit.sh` (image tag baked in) attached.

> Why no release automation? The crate depends on unpublished xet-core APIs via git deps, and release-bot tooling (release-plz) runs `cargo package` *with verification* to compute next versions — the verify build resolves deps from crates.io (git specs are stripped when packaging) and fails on the unpublished APIs. Revisit if xet-core publishes them or release-plz grows a `--no-verify` option for version determination.

## License

Apache-2.0.
