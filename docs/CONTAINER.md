# Run the container yourself

The `hf s3ream` CLI and the Space run the image `ghcr.io/glutamatt/hf-s3ream` on HF Jobs for you. The same image runs anywhere Docker does: useful for VPC-locked buckets, or when you would rather use your own machine's bandwidth. (`hf s3ream --local` runs it for you in Docker on your machine: see the [guide](GUIDE.md#a-copy-from-a-terminal).)

```bash
eval "$(aws configure export-credentials --format env)"   # or export AWS_ACCESS_KEY_ID=… yourself
docker run --rm \
    -e HF_TOKEN -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
    ghcr.io/glutamatt/hf-s3ream:latest \
    s3://my-bucket/prefix/ hf://buckets/your-org/your-bucket/path/to/dest
```

The path after the bucket name is optional; when provided, every source object is written below that destination prefix. A bare `your-org/your-bucket` remains supported for copying to the bucket root. One process copies the whole source prefix. To split a big prefix across several machines/processes, give each a contiguous key slice with `--start-after K` (exclusive) / `--stop-at K` (inclusive) — that's exactly what the planner automates on HF Jobs (see [ARCHITECTURE.md](ARCHITECTURE.md)).

## Credentials

| Service       | Source (in priority order)                                            |
|---------------|------------------------------------------------------------------------|
| HuggingFace   | `--hf-token` flag → `$HF_TOKEN` env → `~/.cache/huggingface/token`     |
| AWS           | env vars (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`); on AWS compute, web identity (IRSA), ECS task roles, and IMDS instance roles also work |

The Space and `hf s3ream` pass both as encrypted Job secrets. **`~/.aws/credentials` profiles are not enough for a manual run**: the listing client resolves them, but the transfer client (`object_store`) only takes env vars or instance/task roles — export env creds (`aws configure export-credentials --format env`) as in the example above.

The source needs only read access: see the [minimal AWS policy](GUIDE.md#minimal-aws-policy).

## Tuning knobs

`hf s3ream` and the Space's Analyze step pick these for you (see [The plan](DESIGN.md#the-plan)). For manual runs of the binary (`--help` for all):

| Flag                       | Default | Description                                            |
|----------------------------|---------|--------------------------------------------------------|
| `--parallel-files`         | 32      | concurrent in-flight file uploads                      |
| `--s3-part-concurrency`    | 8       | parallel ranged GETs per file (multipart download)     |
| `--s3-part-size-mib`       | 16      | size of each ranged GET                                |
| `--start-after` / `--stop-at` | —    | copy only a contiguous key slice (exclusive / inclusive) |
| `--exclude GLOB`           | —       | skip keys matching a glob (repeatable)                 |
| `--skip-existing`          | —       | skip keys already at the destination with the same size |
| `--commit-chunk`           | 1000    | files per batched bucket commit                        |
| `--commit-gib`             | 16      | also commit once the session reaches this many GiB     |
| `--limit-gib`              | 0       | stop after N GiB queued (0 = unlimited; benchmarks)    |
| `--xor-byte`               | 0       | XOR data before upload to defeat dedup (benchmarks)    |
| `--dry-run`                | —       | list + stats + proposed plan only, no transfer         |
| `--dry-run-max-secs`       | 0       | with `--dry-run`: stop listing after N s, plan from the listed part (0 = list all) |
| `--no-verify`              | —       | skip the [destination check](DESIGN.md#verification) after the copy |
| `--plan`                   | —       | planner mode: cut ranges + spawn copier Jobs           |
| `--copier-flavor`          | cpu-upgrade | planner mode: hardware of the copier Jobs (the plan proposes `cpu-performance` for most copies) |
| `--range-gib`              | 250     | planner mode: average GiB per range (each range takes ½ to 1½ of it) |
| `--max-inflight`           | 32      | planner mode: most copiers at once (the plan proposes up to 64); the [fleet size](ADAPTIVE-CONCURRENCY.md#the-fleet-size) stays within it |
| `--brake`                  | on      | [adaptive rate](ADAPTIVE-CONCURRENCY.md#the-copier-brake) toward CAS: `on`, `shadow` (log only) or `off` |

Plus xet-core env vars (`HF_XET_CLIENT_AC_MAX_UPLOAD_CONCURRENCY`, `HF_XET_DATA_MAX_CONCURRENT_FILE_INGESTION`, …) for the upload side.

How these settings use memory, and how the copier fits them into its container: [Memory](ARCHITECTURE.md#memory).
