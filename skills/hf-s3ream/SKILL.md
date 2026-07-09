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

`hf-s3ream`'s defaults (32 parallel files) target ~8 vCPU / 32 GB.

| Prefix size | Flavor | Notes |
|---|---|---|
| small–medium (≤ a few hundred GB) | `cpu-upgrade` | 8 vCPU / 32 GB, ~$0.03/hr. Default. |
| large / multi-TB | `cpu-performance` | 32 vCPU / 256 GB, ~$1.90/hr. Bump `-- --parallel-files 64`. |
| big + lots of network | `cpu-xl` | 16 vCPU / 124 GB, ~$1.00/hr. |

A GPU flavor is pointless here (pure I/O). `hf jobs hardware` lists all flavors + rates.

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
- `--parallel-files N` — concurrent in-flight files (default 32; raise on bigger flavors).
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

## Sharding very large prefixes (advanced, manual)

There is no job-array primitive on HF Jobs yet. For a huge prefix you can fan
out N independent jobs by hand, each taking a deterministic FNV slice — the
partition is stable across re-runs and CAS dedups any overlap:

```bash
for i in $(seq 0 7); do
  hf jobs run --detach --flavor cpu-performance --timeout 8h \
    -s HF_TOKEN -s AWS_ACCESS_KEY_ID -s AWS_SECRET_ACCESS_KEY \
    ghcr.io/glutamatt/hf-s3ream:latest \
    -- hf-s3ream s3://b/p/ org/repo --shard-id "$i" --shard-count 8
done
# then: hf jobs ps  /  hf jobs wait <ids...>
```

Note each shard commits its own bucket batch independently, so a partial fleet
leaves the bucket with only the shards that finished.
