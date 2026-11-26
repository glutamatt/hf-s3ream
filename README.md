# hf-s3ream

Stream S3 buckets into [HuggingFace Buckets](https://huggingface.co/storage) — fast, parallel, no disk staging, no infrastructure.

`hf-s3ream` reads an S3 prefix and writes it into a HuggingFace Bucket through xet-core's content-addressed upload pipeline. Bytes flow S3 → memory → xet CAS in a single stream; no temporary copy on local disk. Re-running an interrupted clone re-uploads only what's missing (CAS dedup). Add `--skip-existing` to also skip the S3 read for files the destination already has at the same path and size.

The main way to use it is the **web UI**: a [Hugging Face Space](https://huggingface.co/spaces/glutamatt/hf-s3ream) that runs the whole copy on [HF Jobs](https://huggingface.co/docs/hub/jobs) for you. The [`hf s3ream`](#run-it-with-the-hf-cli) command does the same from a terminal or a script.

## Quickstart — the Space

Open **[huggingface.co/spaces/glutamatt/hf-s3ream](https://huggingface.co/spaces/glutamatt/hf-s3ream)** and:

1. **Sign in with Hugging Face** (OAuth, in the browser). Jobs are billed per minute to *your* account — any account with [pre-paid credits](https://huggingface.co/settings/billing) works.
2. Enter the **S3 source prefix**, the **destination bucket**, and **AWS credentials** for the source — or switch on **Public bucket** for a source that allows anonymous reads (e.g. the [AWS Open Data registry](https://registry.opendata.aws/)): no credentials are sent and the Jobs read S3 unsigned (`--no-sign-request`).
3. **Analyze** — if the destination bucket does not exist, the page creates it **private** (change its visibility later in the bucket settings). Then a cheap `--dry-run` Job lists the source on HF's side: it validates S3 access, auto-detects the region, sizes the transfer, and recommends a configuration before you spend anything. A very large source is listed for 2 minutes only: the plan is then sized from that part, its cost is shown as a lower bound, and the planner lists the whole source itself.
4. **Run** — the page launches **one planner Job**, then just observes:
   - the planner lists the prefix **once** and cuts it into contiguous key ranges (byte-balanced, layout-agnostic);
   - it spawns one **copier Job per range** (staggered, bounded in-flight), each streaming its slice S3 → xet CAS;
   - it monitors the fleet and respawns failed copiers (idempotent — CAS dedups already-uploaded content);
   - it checks the destination against the plan (see [Verification](#verification)).

   The page streams every copier's progress into one live aggregate graph. The planner is fully autonomous: you can close the tab and the copy completes anyway — the fleet stays visible on your [HF Jobs page](https://huggingface.co/jobs) (the Space itself doesn't re-attach to a running fleet yet).

Throughput scales with the number of copiers, since each one is an independent S3 → CAS path. Jobs are billed per started minute at the [published flavor rates](https://huggingface.co/docs/hub/jobs-pricing); the Analyze step sizes the transfer, suggests a configuration and estimates its cost before you launch the fleet. During the copy, the page reads the cost of every Job from your billing (that is what the `read-billing` scope is for) and shows the total and the cost per copier.

### Stop a copy

- **In the Space**, while the tab is open: click **Stop copy** under the progress bar, then confirm. The page cancels the planner Job, then every Job of the copy that still runs, and shows what was copied.
- **Anywhere else**: cancel the **planner** Job, on your [Jobs page](https://huggingface.co/jobs) or with `hf jobs cancel <planner-job-id>`. Nothing more is needed. The planner cancels its copiers before it ends, and a copier whose planner has ended cancels itself (after a minute, or a few minutes for a big fleet). A planner that reaches its timeout stops the copy the same way.

Every Job of a copy has the label `hf-s3ream-run=<run label>` (on the Job's page). To check that none still runs: `hf jobs ps --label hf-s3ream-run=<run label>`.

Files already in the bucket stay there; the other files are not copied. To finish the copy later, run it again with **Skip existing**. If you cancel one copier only, the planner does not start it again: its key range is not copied, and the copy ends as failed.

### Trust model

The Space is a **fully static page — there is no backend.** The browser talks straight to the Hugging Face API (OAuth + CORS). Your AWS credentials never touch any server we run: they stay in your browser and are sent only into the Jobs' **encrypted secrets** via the HF API. Prefer a scoped, read-only, short-lived key for just the source prefix: see the [minimal AWS policy](#minimal-aws-policy).

### Caveats

- **VPC-locked source buckets are unreachable from HF Jobs.** Jobs run on HF's network, outside your AWS VPC, so a bucket policy pinned to `aws:sourceVpce` / `aws:SourceVpc` returns `403` there regardless of valid creds — that's network topology, not a credentials problem. The dry-run preflight surfaces it. For those buckets, run [`hf s3ream --local`](#run-it-with-the-hf-cli) or [the container](#run-the-container-yourself) from inside the VPC.
- **The source prefix should be frozen during the copy.** The planner cuts key ranges from one listing pass; if the source mutates mid-run, the plan goes stale. Fine for migrations and archival (the intended use), not for live-mutating buckets.

## Run it with the `hf` CLI

`hf s3ream` is an [`hf` CLI extension](https://huggingface.co/docs/huggingface_hub/guides/cli-extensions). Like the Space, it runs the copy on HF Jobs, billed to your account: a dry-run Job proposes a plan and its cost, you confirm, one planner Job runs the copy, and the terminal shows the live progress. It needs only the [`hf` CLI](https://huggingface.co/docs/huggingface_hub/guides/cli) (huggingface_hub 2.4 or later).

```bash
hf extensions install glutamatt/hf-s3ream
hf s3ream s3://my-bucket/prefix/ hf://buckets/your-org/your-bucket/path/to/dest
```

- **Credentials**: your `hf auth login` token (or `HF_TOKEN`), with write access to the bucket's namespace and the right to start Jobs (a fine-grained token needs the Jobs permission). The AWS credentials come from the environment or from the AWS CLI (`AWS_PROFILE`, SSO included) and go to the Jobs as encrypted secrets; `--no-sign-request` reads a public bucket with none.
- **In a terminal**: the plan and its cost come first, then `[y/N]`. During the copy, Ctrl-C asks before it stops anything (Ctrl-C again within 5 s stops the planner and every copier), and `d` detaches: the copy keeps running. Closing the terminal does not stop the copy.
- **For agents and scripts**: `--json` (the default when stdout is not a terminal) prints one JSON event per line, nothing ever waits for an answer (`--yes` to start), `--max-cost USD` guards the budget, and `status` / `attach` / `stop` drive a copy started with `--detach`. Exit codes are documented.
- **`--local`** runs the same pinned image in Docker on your machine instead, for a bucket that HF Jobs cannot reach (one open only to your VPC). Secrets go to the container by name, never on a command line. The image is linux/amd64 only.

Every option, the JSON events and the exit codes: [`cli/README.md`](cli/README.md). `hf s3ream --help` has the short version.

## Run the container yourself

The same image runs anywhere Docker does — useful for VPC-locked buckets or when you'd rather use your own machine's bandwidth:

```bash
eval "$(aws configure export-credentials --format env)"   # or export AWS_ACCESS_KEY_ID=… yourself
docker run --rm \
    -e HF_TOKEN -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
    ghcr.io/glutamatt/hf-s3ream:latest \
    s3://my-bucket/prefix/ hf://buckets/your-org/your-bucket/path/to/dest
```

The path after the bucket name is optional; when provided, every source object is written below that destination prefix. A bare `your-org/your-bucket` remains supported for copying to the bucket root. One process copies the whole source prefix. To split a big prefix across several machines/processes, give each a contiguous key slice with `--start-after K` (exclusive) / `--stop-at K` (inclusive) — that's exactly what the planner automates on HF Jobs.

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
- **Uploads**: `xet-core`'s `FileUploadSession` shared by all files of a commit chunk — xorbs and shards are dedup'd within the chunk. When the Hub offers it, each xorb goes straight to the storage S3 bucket through a short-lived upload URL (xet-core 1.8.0 "direct-to-S3" uploads). If that URL can't be reached, xet-core falls back to the classic upload; `HF_XET_CLIENT_LEGACY_DIRECT_XORB_UPLOAD=1` forces the classic upload.
- **Commit**: one batched ndjson POST per `--commit-chunk` files (or `--commit-gib`), pipelined — sessions rotate as files stream in and finalize in the background, so commits never pause the transfer.

No bytes touch local disk in the hot path. A file read in parts holds, at most: `--s3-part-concurrency` parts being read, up to 32 finished parts waiting for the upload pipeline, 2 parts in hand (one waiting to be queued, one being chunked), and the xorb formation window (~64–128 MiB). A file read with a single GET holds only the xorb window.

The parts of all files come from one **part pool**. A part takes a slot before its GET starts, and gives it back once the upload pipeline has used it. So a file copied alone reads with `--s3-part-concurrency` GETs, and files copied at the same time share the pool. A file takes no new slot while it holds its fair share: the pool size divided by the files reading in parts at that moment. This keeps a file whose upload is slower than its reads from holding slots the other files could use.

At startup the copier reads the memory limit of its cgroup: it follows `/proc/self/cgroup` and takes the lowest limit up to the root (cgroup v2 `memory.max`, or v1 `memory.limit_in_bytes`). No limit found means a pool large enough for every file at full part concurrency. With a limit, the parts and the xorb windows may use 60% of it, or less when the fixed part needs more: 1 GiB, plus 64 MiB for each xorb upload the xet client may run at once. Each file in flight reserves its xorb window (128 MiB), and the pool gets the rest, but at least enough for one file at full part concurrency. When that does not fit, `--parallel-files` is lowered. `--s3-part-concurrency` is never changed. The copier logs the result and prints it as a `SIZING {…}` line; each `PROGRESS` line carries the slots in use and the resident memory. Example: a Jobs flavor sold as 32 GB has a limit of 32,000,000,000 bytes (29.8 GiB). There, the planner's big-file settings (32 files × 128 parts) get a pool of 888 parts, and its small-file settings (128 files × 8 parts) a pool of 120.

## Brakes

CAS, the storage behind HF Buckets, is shared by every Hub user. A fleet of copiers can push faster than it can grow. Two brakes keep a copy fast without overloading it.

**In each copier: an adaptive rate** (`--brake`, default `on`). A token bucket sets how many bytes per second the copier feeds to xet. The copier listens to xet's own events (through a tracing layer, whatever `RUST_LOG` shows) for **errors that say "slow down"** from CAS: 429, 5xx, timeouts, dropped connections (one alone is common; three in 10 s count). Other errors (403, 404, …) do not brake.

The rate follows TCP's slow start, then CUBIC's gentle cut:

| What the copier sees | What it does |
|---|---|
| no error yet | starts at 64 MiB/s and doubles the rate every 2 s while it uses it: full speed in ~10 s, no ceiling |
| a congestion error | cuts the rate it really used to 70 %, then holds 10 s |
| after the hold | doubles back up to 90 % of that rate, then adds 1/32 of it every 2 s |

At most one cut every 15 s; holds are spread by ±20 % so the copiers do not all climb back at once. (Halving and holding 30 s took a fleet of 64 from ~70 to ~5 GiB/s, every copier at once, and it came back slowly.) The rate never goes below 16 MiB/s. A copier that does not use its rate (a slow source, or its own full speed) does not raise it.

The copier also **times each upload call**: the upload grant, the xorb commit and the shard (answered by CAS), and the PUT of each xorb to the storage (timed to tell where a slow upload comes from). Each `PROGRESS` line carries the 90th percentile of each over the last seconds (`brake.latency`, in ms, or ms per MiB for the xorbs and shards): when CAS queues, the slow tail grows first, while the median stays flat. One copier lives a few minutes: too short to tell a trend from noise, so the planner reads these times across the fleet (below).

The copiers need no coordination: they all see the same CAS, so they brake together. How hard the fleet pushes in all is set by the planner, through the number of copiers (below). `--brake shadow` computes and logs the rate without applying it. Each `PROGRESS` line carries the brake's state (`brake`: rate, last action, cuts, errors seen, latency), and each cut is logged.

The xet client also keeps at most 64 xorb uploads in flight per copier (its high-performance mode allows 124). `HF_XET_CLIENT_AC_MAX_UPLOAD_CONCURRENCY` still sets another value.

**In the planner: the fleet size.**

- **The size follows AIMD, as TCP, on the power `P = throughput / latency`** (Kleinrock's power). `P` grows while copiers add throughput faster than they add latency, and is highest where CAS starts to queue. The fleet starts at 16 copiers and gains **4 copiers a minute** (~4 GiB/s more per minute), when it filled its target at some point of that minute. The planner measures it over 2 minutes, the first time once the first 16 copiers run, then one measure after the other, until no range is left to launch: the throughput, and `L`, the geometric mean of the latencies of the grant and the xorb commit (each the 90th percentile of a copier, then the median over the copiers). Not the shard: copiers send one when a session ends, often together, and its time follows that more than CAS. `N` counts the copiers launched (running, or waiting for a machine). The 2 minutes are cut in 4 slices: their spread around their trend, pooled over the last 10 minutes, gives the noise `σ`. Each measure is compared with the best one of the last 10 minutes (of those with the most power, the one with the fewest copiers):

  | What the measure shows | What the fleet does |
  |---|---|
  | less power than the best (by more than 2σ), with more copiers | × 0.7 |
  | less power at the best size (CAS changed) | × 0.7, and the old measures are forgotten |
  | no clear gain with 25 % more copiers than the best | × 0.7 |
  | anything else (fewer copiers than the best included) | +4 copiers a minute |

  A fleet shrinks as its ranges end, so the measure after a × 0.7 sees it shrinking, not its new size: that measure decides nothing (TCP, too, cuts once per congestion event).

  Latency stable and throughput up: the fleet keeps growing. CAS adds capacity: more power, it keeps growing. Latency up faster than throughput: it shrinks. Copiers that add nothing (a slow source, for example): it saw-tooths from its best size to 25 % past it. Adding copiers at a steady pace, not by doublings, leaves CAS time to scale: doubling from 32 to 64 copiers added ~35 GiB/s in a minute, and CAS answered with a wave of 500s.
- **Errors stay with the copiers.** The planner does not watch them: when copiers brake, the fleet throughput drops, so the power drops, and the next measure gives copiers back. The reaction is in proportion: one 500 on one copier of 60 is noise; an overload that brakes many copiers is a clear loss.

It reads the copiers' logs: with `--no-live-progress` it is off, and the planner starts up to `--max-inflight` copiers at once. `PLAN_PROGRESS` carries the fleet latency (`latency`) and the size allowed (`target`); each change of size is logged (`growth:`).

`bench/brake_bench.py` runs real copiers against a local fake of S3, the Hub API and CAS, with a simple model of CAS under load (cores, a queue, 429 and 503, an autoscaler). Nothing leaves the machine.

## Verification

After the copy, the planner (or a single-process run, after `DONE`) reads the destination back:

1. It waits until the bucket's `updatedAt` stops moving (2 minutes at most).
2. It lists the destination prefix once and compares file count and bytes with the plan, range by range.
3. For a range that does not add up, it re-lists the source range and looks up each path (`paths-info`): absent is `missing`, another size is `short`. A missing key that is also gone from the source (`HEAD` 404) is `raced`. A missing key whose `LastModified` is newer than every key of the listing page that covers it is `added`: it arrived in the source after that page was read.
4. It prints one `VERIFY {…}` line, and the command that re-copies each failed range. `PLAN_RESULT` gets `verified: true|false|null`.

`missing` or `short` makes the run exit non-zero. `extra`, `raced`, `added` and `outside` (files outside the plan) are reported only. A range where a missing file and an unplanned file of the same size cancel out is not detected. `--no-verify` skips the check; `--dry-run` never runs it.

A source that keeps growing during the copy gets `added` files: the check compares the destination with the listing, so they do not fail the run. The check logs each range that has some, with its first key and the command that re-copies the range. To copy them, run the same copy again with `--skip-existing` (the CLI prints it as `Resume:`, the Space calls it "Skip existing"): it copies only what is not at the destination yet. The check counts `added` files only in the ranges it re-lists: when a range holds what the listing promised, it does not read the source again.

How a key is told `added`: S3 lists up to 1,000 keys per page, and reads each page at its own moment. For each page, the copy keeps its first and last key and its newest `LastModified`. A missing key between the first and last key of a page is judged by that page: a key the page listed is never newer than the page, so a key newer than the page was not there when the page was read. Both dates come from S3, so a clock offset between S3 and the machine that runs the copy plays no part. Before the first page, after the last, or between two pages, the copy listed no key: a missing key there was not in the source when its page was read, whatever its date, so it is `added` too. Inside a page, when the date cannot prove that the key came after the page, the key stays `missing`: a key written in the same second as the newest key of the page, or a multipart upload (S3 dates it at its start) started before that key.

## Credentials

| Service       | Source (in priority order)                                            |
|---------------|------------------------------------------------------------------------|
| HuggingFace   | `--hf-token` flag → `$HF_TOKEN` env → `~/.cache/huggingface/token`     |
| AWS           | env vars (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`); on AWS compute, web identity (IRSA), ECS task roles, and IMDS instance roles also work |

The Space passes both as encrypted Job secrets. **`~/.aws/credentials` profiles are not enough for a manual run**: the listing client resolves them, but the transfer client (`object_store`) only takes env vars or instance/task roles — export env creds (`aws configure export-credentials --format env`) as in the docker example above.

### Minimal AWS policy

The copy only reads the source: it needs `s3:ListBucket` on the bucket, limited to the prefix, and `s3:GetObject` on the objects under it. For the prefix `my/prefix/` of the bucket `my-bucket`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::my-bucket",
      "Condition": { "StringLike": { "s3:prefix": ["my/prefix/*"] } }
    },
    {
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::my-bucket/my/prefix/*"
    }
  ]
}
```

With this policy, **give the source with its final `/`**: `s3://my-bucket/my/prefix/`. Without it, the listing asks for the prefix `my/prefix`, which the condition `my/prefix/*` does not allow, and the copy fails with `AccessDenied` on `s3:ListBucket`.

Nothing else is needed. The region is still detected: S3 sends the bucket's region even when it refuses the request. A public source needs no policy at all: switch on **Public bucket** (or pass `--no-sign-request`).

## Tuning knobs

The Space's Analyze step picks these for you. For manual runs (`--help` for all):

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
| `--no-verify`              | —       | skip the [destination check](#verification) after the copy |
| `--plan`                   | —       | planner mode: cut ranges + spawn copier Jobs (see `--range-gib`, `--max-inflight`, …) |
| `--brake`                  | on      | [adaptive rate](#brakes) toward CAS: `on`, `shadow` (log only) or `off` |

Plus xet-core env vars (`HF_XET_CLIENT_AC_MAX_UPLOAD_CONCURRENCY`, `HF_XET_DATA_MAX_CONCURRENT_FILE_INGESTION`, …) for the upload side.

## Contributing

This repo uses [Conventional Commits](https://www.conventionalcommits.org/) — `feat:`, `fix:`, `feat!:` for breaking.

The Space front-end lives in [`space/`](space/) and is auto-deployed by GitHub Actions on push. Releases are manual: bump `version` in `Cargo.toml`, then tag and push — `git tag vX.Y.Z && git push origin vX.Y.Z`. The tag triggers `release.yml`, which builds and pushes the container image (`ghcr.io/glutamatt/hf-s3ream:vX.Y.Z` + `:latest`) and publishes a GitHub release.

> Why no release automation? The crate depends on unpublished xet-core APIs via git deps, and release-bot tooling (release-plz) runs `cargo package` *with verification* to compute next versions — the verify build resolves deps from crates.io (git specs are stripped when packaging) and fails on the unpublished APIs. Revisit if xet-core publishes them or release-plz grows a `--no-verify` option for version determination.

## License

Apache-2.0.
