---
name: hf-s3ream
description: >-
  Copy / clone / stream an Amazon S3 bucket or prefix into a HuggingFace Bucket
  (hf://buckets/org/name) using hf-s3ream on HF Jobs — no local machine does the
  transfer, it runs on Hugging Face's managed compute. Use whenever the user
  wants to move data from S3 to an HF Bucket (a one-off migration, a mirror, or
  "get this s3://… into my HF bucket"). Not for uploading local files (use
  `hf upload`), and not for git-based model/dataset repos (buckets only).
---

# hf-s3ream on HF Jobs

`hf-s3ream` streams an S3 prefix straight into a HuggingFace Bucket through
xet's content-addressed upload pipeline: S3 → memory → HF CAS, no disk staging,
parallel, and dedup-aware (re-runs only re-upload what's missing). This skill
runs it as a **Hugging Face Job**, so the copy happens on HF infrastructure —
the user needs no cluster, no servers, and doesn't tie up their own machine.

## When to use this vs. alternatives

- **This skill**: bulk S3 → HF Bucket. Large prefixes, many files, multi-GB/TB.
- `hf upload` / `hf buckets`: uploading files that are already on the local disk.
- The `hfjob.sh` wrapper (shipped in this repo's `scripts/`) does everything
  below for a human in one command; as an agent you can either call it or issue
  the raw `hf jobs run` yourself. Prefer the raw command when you need to adapt
  flags or parse output.

## Preconditions (check these first)

1. **`hf` CLI present**: `hf --help`. If missing: `curl -LsSf https://hf.co/cli/install.sh | bash`.
2. **Logged in with write access**: `hf auth whoami`. If not: tell the user to run `hf auth login` (interactive — do NOT attempt it yourself). A token with **write** permission is required (bucket commits).
3. **Pre-paid credits**: HF Jobs is pay-as-you-go. If the job is rejected for billing, point the user to https://huggingface.co/settings/billing.
4. **Static AWS credentials**: HF Jobs runs **off AWS**, so there is no instance role / IMDS / IRSA in the container. You must forward *static* keys. Resolve them locally (in this priority): `AWS_ACCESS_KEY_ID`+`AWS_SECRET_ACCESS_KEY` env → `~/.aws/credentials` `[profile]` → `aws configure export-credentials --format env` (turns an SSO/assume-role login into temporary static keys, including a session token). If none resolve, ask the user to export keys or run `aws configure`.
5. **Destination bucket exists**: `hf buckets create org/name` (add `--private` if needed) — tolerate "already exists". The bucket must exist before the job commits.

## The command (single job)

```bash
# Export the resolved AWS creds so `-s NAME` picks them up from the env
# (keeps secret VALUES off the command line and out of logs).
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...   # + AWS_SESSION_TOKEN if using SSO/STS

hf jobs run \
  --flavor cpu-upgrade \
  --timeout 2h \
  -e AWS_REGION=us-east-1 \
  -s HF_TOKEN \
  -s AWS_ACCESS_KEY_ID \
  -s AWS_SECRET_ACCESS_KEY \
  ghcr.io/glutamatt/hf-s3ream:latest \
  -- hf-s3ream s3://my-bucket/prefix/ your-org/your-bucket
```

Syntax that matters:
- **Job options (`--flavor`, `--timeout`, `-e`, `-s`) go BEFORE the image.**
- Everything after `--` is the container command. **Name the binary**
  (`hf-s3ream`) — HF Jobs sets the container command k8s-style, OVERRIDING the
  image ENTRYPOINT (so a bare `-- --help` fails with `exec "--help": not found`;
  the executable can't be inherited). `hf-s3ream` resolves via `$PATH`.
- Add `-s AWS_SESSION_TOKEN` **only** when the creds include one (SSO/STS).
- Add `--namespace ORG` to bill/run the job under an org you can write to
  (independent of the destination bucket's org).
- Add `--detach` to submit and return the Job ID immediately instead of
  streaming logs. A non-detached `hf jobs run` exits non-zero if the copy fails.

## Choosing the flavor

**Default to `cpu-upgrade` and do NOT upsize.** Benchmarked on real HF Jobs
(2026-07): the copy is **bandwidth-bound, not CPU-bound**, so more vCPU buys no
speed — the pricier tiers just cost more per TiB.

| flavor | vCPU / RAM | $/hr | ~throughput | $/TiB copied |
|---|---|---|---|---|
| `cpu-basic` | 2 / 16 | $0.01 | ~371 MiB/s | ~$0.008 |
| `cpu-upgrade` | 8 / 32 | $0.03 | ~420 MiB/s | **~$0.02** |
| `cpu-xl` | 16 / 124 | $1.00 | ~405 MiB/s | ~$0.72 |
| `cpu-performance` | 32 / 256 | $1.90 | ~400 MiB/s | ~$1.38 |

Even 2 vCPU nearly saturates the ~400 MiB/s path (only ~12% slower than 8), so
`cpu-basic` is technically cheapest per TiB — but the difference is rounding error
(10 TB costs $0.08 vs $0.21), while `cpu-upgrade`'s extra RAM/vCPU is real headroom
for many-small-files prefixes or higher `--parallel-files`. Default `cpu-upgrade`;
drop to `cpu-basic` only for large-file prefixes where you're squeezing pennies.

To go **faster**, don't buy a bigger flavor — **shard across multiple
`cpu-upgrade` jobs** (see below). Each job is an independent ~400 MiB/s path, so
N shards ≈ N×400 MiB/s at N×$0.03/hr: both faster *and* cheaper than one big
flavor. GPU flavors are pointless (pure I/O). `hf jobs hardware` lists all flavors.

## Timeout

Default is 30 min — almost always too short. The job is **killed** at the
timeout, and hf-s3ream commits the bucket batch **atomically at the very end**,
so a timeout mid-transfer means **nothing is committed** (CAS-uploaded chunks
persist and make the retry cheap, but you get no partial bucket). Estimate:
`timeout ≈ (prefix_GB / expected_MiBps) × 1.3`, and round up. When unsure, set
it generously (`--timeout 8h`) — you only pay for seconds actually used.

## Monitoring

```bash
hf jobs ps                      # running jobs
hf jobs logs <job-id> --follow  # stream (or --tail N)
hf jobs inspect <job-id>        # full config + status
hf jobs cancel <job-id>         # stop it
```

hf-s3ream logs a `progress` line every 5s (files done, GiB, MiB/s) and a final
`done` line with throughput. Success = the job exits 0 and the final line reads
`done`. Verify the result with `hf buckets info org/name` or the bucket URL
`https://huggingface.co/storage/org/name`.

Under `Bash run_in_background`, `hf jobs logs <id> --follow` pairs with a
`Monitor` watching for the `done`/error line — don't poll in a tight loop.

## Useful passthrough flags (after `--`)

Forwarded to the hf-s3ream binary:
- `--exclude 'GLOB'` — skip matching keys (repeatable; matched against the full key, e.g. `*.tmp`, `*decay*`).
- `--parallel-files N` — concurrent in-flight files (default 32). **Scale to file size × RAM, not to vCPUs.** Small files (≤16 MiB → single GET, tiny footprint): raise to 128 to hide per-file latency (benchmarked ~1.5× faster on small-file prefixes, safe even on cpu-basic). Big files (multipart: ~200 MiB in-flight each): keep it ≲48 on a 16 GB flavor — `128 × ~200 MiB ≈ 25–32 GB → OOM` on cpu-basic. Default 32 is safe for big files everywhere.
- `--s3-part-concurrency N` / `--s3-part-size-mib N` — parallel ranged GETs per file.
- `--aws-region REGION` — alternative to `-e AWS_REGION` (same effect).
- `--dry-run` — list what would transfer, then exit without uploading (cheap sanity check).

## Common failures

| Symptom | Cause & fix |
|---|---|
| Auth / 401 on bucket commit | `HF_TOKEN` missing or lacks **write**. Ensure `-s HF_TOKEN` and the token has write access. |
| S3 access denied / no such bucket | Wrong or missing AWS keys, or wrong `AWS_REGION`. Set `-e AWS_REGION=<source-bucket-region>`. |
| S3 403 that works locally but fails from the Job | **VPC-locked bucket.** The source bucket's policy restricts access to a VPC endpoint (`aws:sourceVpce` / `aws:SourceVpc`), and HF Jobs runs OFF the user's VPC — so it's unreachable regardless of valid creds. This is network topology, not a creds bug. Run the copy **from inside the VPC** instead: the SLURM/Pyxis `submit.sh` path, or a plain `docker run` on an EC2 in that VPC. (Check with `aws s3api get-bucket-policy --bucket <b>`.) |
| Copy dies partway with 403 after running a while | Temporary (SSO/STS) creds **expired mid-copy** — hf-s3ream captures creds at start and doesn't refresh. Use longer-lived creds, or shard the prefix into jobs that each finish inside the token lifetime. |
| Job killed / incomplete | Timeout too short — raise `--timeout`. Nothing was committed; just re-run (dedup makes it cheap). |
| "bucket not found" at commit | Destination bucket doesn't exist — `hf buckets create org/name` first. |
| Refuses to clone (invalid keys) | Source has S3 keys with empty `//` path segments (unrepresentable). Exclude them with `--exclude` or clone a narrower prefix. |
| Billing rejection | No pre-paid credits — https://huggingface.co/settings/billing. |

## Sharding to go faster (and cheaper)

Throughput is bandwidth-bound (~400 MiB/s per job), so the way to scale is **more
jobs, not a bigger flavor** — N shards ≈ N×400 MiB/s, and N cheap `cpu-basic`/`cpu-upgrade`
jobs beat one `cpu-performance` on both speed and cost. Sharding also splits the
per-file commit tail across jobs (each commits ~total/N ops). The FNV slice is
deterministic and disjoint, so re-running a failed shard is safe (CAS dedups).

The `hfjob.sh` wrapper does this for you — validated end-to-end (2 shards over
5001 files summed to exactly 5001 in the dest):

```bash
hfjob.sh --src s3://b/huge/ --dst org/repo --shards 8 --flavor cpu-basic
# launches 8 detached jobs, waits for all; re-run failed --shard-id K with
# --shards 8 -- --shard-id K --shard-count 8 (CAS dedups the rest)
```

To construct it yourself instead of using the wrapper:

```bash
for i in $(seq 0 7); do
  hf jobs run --detach --flavor cpu-basic --timeout 8h \
    -s HF_TOKEN -s AWS_ACCESS_KEY_ID -s AWS_SECRET_ACCESS_KEY \
    ghcr.io/glutamatt/hf-s3ream:latest \
    -- hf-s3ream s3://b/p/ org/repo --shard-id "$i" --shard-count 8
done
# then: hf jobs ps  /  hf jobs wait <ids...>
```

Each shard commits its own bucket batch independently, so a partial fleet leaves
the bucket with only the shards that finished — re-run the missing `--shard-id`s.
