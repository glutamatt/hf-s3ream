# Run the container yourself

The `hf s3ream` CLI and the Space run the image `ghcr.io/glutamatt/hf-s3ream` on HF Jobs for you. The same image runs on any machine with Docker. This is useful for a bucket that only accepts your VPC, or when you want to use the bandwidth of your own machine. (`hf s3ream --local` runs it for you in Docker, on your machine: see the [guide](GUIDE.md#a-copy-from-a-terminal).)

```bash
eval "$(aws configure export-credentials --format env)"   # or export AWS_ACCESS_KEY_ID=… yourself
docker run --rm \
    -e HF_TOKEN -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
    ghcr.io/glutamatt/hf-s3ream:latest \
    s3://my-bucket/prefix/ hf://buckets/your-org/your-bucket/path/to/dest
```

The path after the bucket name is optional. When you give it, each source object is written below this destination prefix. A plain `your-org/your-bucket` still works, to copy to the root of the bucket. One process copies the whole source prefix. To split a big prefix between several machines or processes, give each one a contiguous slice of keys, with `--start-after K` (exclusive) / `--stop-at K` (inclusive). The planner does exactly this on HF Jobs (see [ARCHITECTURE.md](ARCHITECTURE.md)).

## Credentials

| Service       | Source (in priority order)                                            |
|---------------|------------------------------------------------------------------------|
| HuggingFace   | `--hf-token` flag → `$HF_TOKEN` env → `~/.cache/huggingface/token`     |
| AWS           | env vars (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`); on AWS machines, web identity (IRSA), ECS task roles and IMDS instance roles also work |

The Space and `hf s3ream` give both to the Jobs as encrypted secrets. **For a manual run, `~/.aws/credentials` profiles are not enough.** The listing client reads them, but the transfer client (`object_store`) only takes env vars or instance/task roles. So export the credentials as env vars (`aws configure export-credentials --format env`), as in the example above.

The source needs only read access: see the [minimal AWS policy](GUIDE.md#minimal-aws-policy).

## Tuning knobs

`hf s3ream` and the Analyze step of the Space choose these for you (see [The plan](DESIGN.md#the-plan)). For manual runs of the binary (`--help` shows all of them):

| Flag                       | Default | Description                                            |
|----------------------------|---------|--------------------------------------------------------|
| `--parallel-files`         | 32      | file uploads in flight at the same time                |
| `--s3-part-concurrency`    | 8       | parallel ranged GETs per file (multipart download)     |
| `--s3-part-size-mib`       | 16      | size of each ranged GET                                |
| `--start-after` / `--stop-at` | —    | copy only a contiguous key slice (exclusive / inclusive) |
| `--exclude GLOB`           | —       | skip the keys that match a glob (repeatable)           |
| `--skip-existing`          | —       | skip keys already at the destination with the same size |
| `--commit-chunk`           | 1000    | files per batched bucket commit                        |
| `--commit-gib`             | 16      | also commit when the session reaches this many GiB     |
| `--limit-gib`              | 0       | stop after N GiB queued (0 = unlimited; benchmarks)    |
| `--xor-byte`               | 0       | XOR data before upload to defeat dedup (benchmarks)    |
| `--dry-run`                | —       | list + stats + proposed plan only, no transfer         |
| `--dry-run-max-secs`       | 0       | with `--dry-run`: stop listing after N s, plan from the listed part (0 = list all) |
| `--no-verify`              | —       | skip the [destination check](DESIGN.md#verification) after the copy |
| `--plan`                   | —       | planner mode: cut ranges + start copier Jobs           |
| `--copier-flavor`          | cpu-upgrade | planner mode: hardware of the copier Jobs (the plan proposes `cpu-performance` for most copies) |
| `--range-gib`              | 250     | planner mode: average GiB per range (each range takes ½ to 1½ of it) |
| `--max-inflight`           | 32      | planner mode: the most copiers at the same time (the plan proposes up to 64); the [fleet size](ADAPTIVE-CONCURRENCY.md#the-fleet-size) stays within it |
| `--brake`                  | on      | [adaptive rate](ADAPTIVE-CONCURRENCY.md#the-copier-brake) toward CAS: `on`, `shadow` (log only) or `off` |

Plus xet-core env vars (`HF_XET_CLIENT_AC_MAX_UPLOAD_CONCURRENCY`, `HF_XET_DATA_MAX_CONCURRENT_FILE_INGESTION`, …) for the upload side.

How these settings use memory, and how the copier fits them in the memory of its container: [Memory](ARCHITECTURE.md#memory).
