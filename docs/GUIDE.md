# User guide

This guide covers a copy from start to end: from a terminal with `hf s3ream`, or from a browser with the web Space. The first steps are in the [README](../README.md). Every option of the CLI, its JSON events and its exit codes are in [`cli/README.md`](../cli/README.md).

- [Before you start](#before-you-start)
- [A copy from a terminal](#a-copy-from-a-terminal)
- [A copy from the Space](#a-copy-from-the-space)
- [The plan](#the-plan)
- [Stop a copy](#stop-a-copy)
- [Run a copy again](#run-a-copy-again)
- [Your AWS credentials](#your-aws-credentials)
- [Limits](#limits)

## Before you start

- **A Hugging Face account that can pay for Jobs.** The Jobs are billed per started minute at the [published flavor rates](https://huggingface.co/docs/hub/jobs-pricing), to your account (or to an organization, with `--namespace`). Any account with [pre-paid credits](https://huggingface.co/settings/billing) works.
- **A token.** The CLI uses your `hf auth login` token (or `HF_TOKEN`), with write access to the bucket's namespace and the right to start Jobs (a fine-grained token needs the Jobs permission). The Space uses the token of your sign-in.
- **Read access to the source.** AWS credentials that can list and read the prefix: see the [minimal AWS policy](#minimal-aws-policy). A public bucket, for example from the [AWS Open Data registry](https://registry.opendata.aws/), needs none.

## A copy from a terminal

`hf s3ream` is an [`hf` CLI extension](https://huggingface.co/docs/huggingface_hub/guides/cli-extensions). It needs only the [`hf` CLI](https://huggingface.co/docs/huggingface_hub/guides/cli) (huggingface_hub 2.4 or later): no Docker, no Node.

```bash
hf extensions install glutamatt/hf-s3ream
hf auth login        # once
hf s3ream s3://my-bucket/prefix/ hf://buckets/your-org/your-bucket/path/to/dest
```

The path after the bucket name is optional. Then:

1. **Analyze.** If the destination bucket does not exist, `hf s3ream` creates it **private**. A short dry-run Job lists the source on HF's side: it checks S3 access, detects the region, counts the files and bytes, and proposes a [plan](#the-plan) with its time and cost. Nothing is copied yet.
2. **Confirm.** One planner Job runs the copy (see [The life of a copy](ARCHITECTURE.md#the-life-of-a-copy)), and your terminal shows its live progress. Ctrl-C asks before it stops anything (Ctrl-C again within 5 s stops the planner and every copier), and `d` detaches: the copy keeps running. Closing the terminal does not stop the copy.
3. **The end.** The result of the [destination check](DESIGN.md#verification), the files and bytes copied, and the cost.

To try it without AWS keys, pick a public bucket and only analyze it:

```bash
hf s3ream s3://<public-bucket>/<prefix>/ hf://buckets/your-org/your-bucket --no-sign-request --analyze
```

`--analyze` runs the dry-run Job and shows the plan and its cost. It creates the destination bucket if it does not exist, and starts no copy.

- **Credentials**: your `hf auth login` token (or `HF_TOKEN`). The AWS credentials come from the environment or from the AWS CLI (`AWS_PROFILE`, SSO included) and go to the Jobs as encrypted secrets; `--no-sign-request` reads a public bucket with none.
- **Follow or stop a copy from any terminal**: `hf s3ream status RUN` (where it stands), `attach RUN` (follow it to the end), `stop RUN`. `RUN` is the label printed at start, or the planner Job id. This also works for a copy started in the Space.
- **For agents and scripts**: `--json` (the default when stdout is not a terminal) prints one JSON event per line, nothing ever waits for an answer (`--yes` to start), `--max-cost USD` guards the budget, and `--detach` returns once the copy has started. Exit codes are documented.
- **An organization**: `--namespace ORG` runs the Jobs under the organization, so they are billed to it. The cost shown is then an estimate from the Jobs' run times.
- **`--local`** runs the same pinned image in Docker on your machine instead, for a bucket that HF Jobs cannot reach (one open only to your VPC). Secrets go to the container by name, never on a command line. The image is linux/amd64 only.
- **Updates**: `hf extensions update s3ream`. Each version of the extension runs the image it pins; `hf s3ream --version` shows both.

Every option, the JSON events and the exit codes: [`cli/README.md`](../cli/README.md). `hf s3ream --help` has the short version.

## A copy from the Space

The [Space](https://huggingface.co/spaces/glutamatt/hf-s3ream) starts the same copy from your browser: when you have no terminal at hand, or to watch the copy on a live graph. Open **[huggingface.co/spaces/glutamatt/hf-s3ream](https://huggingface.co/spaces/glutamatt/hf-s3ream)** and:

1. **Sign in with Hugging Face** (OAuth, in the browser). Jobs are billed per minute to *your* account — any account with [pre-paid credits](https://huggingface.co/settings/billing) works.
2. Enter the **S3 source prefix**, the **destination bucket**, and **AWS credentials** for the source — or switch on **Public bucket** for a source that allows anonymous reads (e.g. the [AWS Open Data registry](https://registry.opendata.aws/)): no credentials are sent and the Jobs read S3 unsigned (`--no-sign-request`).
3. **Analyze** — if the destination bucket does not exist, the page creates it **private** (change its visibility later in the bucket settings). Then a cheap `--dry-run` Job lists the source on HF's side: it validates S3 access, auto-detects the region, sizes the transfer, and proposes a [plan](#the-plan) with its time and cost before you spend anything. A very large source is listed for 2 minutes only: the plan is then sized from that part, its cost is shown as a lower bound, and the planner lists the whole source itself.
4. **Run copy** — the page launches **one planner Job** (see [The life of a copy](ARCHITECTURE.md#the-life-of-a-copy)), then just observes: it streams every copier's progress into one live aggregate graph. You can close the tab and the copy completes anyway — the fleet stays visible on your [HF Jobs page](https://huggingface.co/jobs). The Space itself doesn't re-attach to a running copy yet: `hf s3ream status` or `attach` with the planner Job id shows where it stands.

During the copy, the page reads the cost of every Job from your billing (that is what the `read-billing` scope is for) and shows the total and the cost of each copier. Once the copy ends, one line sums up the fleet: the copier Jobs (and the ranges run again), the billed minutes, the cost, and the cost per TiB copied.

The Space is a **fully static page — there is no backend.** The browser talks straight to the Hugging Face API (OAuth + CORS).

## The plan

The dry-run proposes a plan from what it listed: the copier flavor, the size of each key range, how many copiers may run at once, a time limit, and the time and cost to expect. `hf s3ream` and the Space get the same plan: it comes from the binary.

- **Most copies are bound by bandwidth**: the plan uses `cpu-performance` copiers, and up to 64 at once. The planner starts 16 copiers, then adds more each minute while they add throughput (see [The fleet size](DESIGN.md#the-fleet-size)).
- **Many small files are bound by the commits**: the destination registers about 1,000 files per second per repo, so the plan uses the smallest fleet that keeps up.
- **A copy under 5 GiB** takes one small copier.

The full rules, and why: [The plan](DESIGN.md#the-plan). You can change the plan by hand (with `--copier-flavor`, `--range-gib` and `--max-inflight`, or under *Advanced* in the Space): there is no cost estimate then.

## Stop a copy

- **From a terminal**: Ctrl-C twice in the `hf s3ream` that started the copy, or `hf s3ream stop RUN` from any terminal.
- **In the Space**, while the tab is open: click **Stop copy** under the progress bar, then confirm. The page cancels the planner Job, then every Job of the copy that still runs, and shows what was copied.
- **Anywhere else**: cancel the **planner** Job, on your [Jobs page](https://huggingface.co/jobs) or with `hf jobs cancel <planner-job-id>`. Nothing more is needed. The planner cancels its copiers before it ends, and a copier whose planner has ended cancels itself (after a minute, or a few minutes for a big fleet). A planner that reaches its timeout stops the copy the same way.

Every Job of a copy has the label `hf-s3ream-run=<run label>` (on the Job's page). To check that none still runs: `hf jobs ps --label hf-s3ream-run=<run label>`.

Files already in the bucket stay there; the other files are not copied. To finish the copy later, run it again with `--skip-existing` (**Skip existing** in the Space). If you cancel one copier only, the planner does not start it again: its key range is not copied, and the copy ends as failed.

## Run a copy again

Re-running an interrupted copy re-uploads only what is missing: Xet only sends the chunks the storage does not have yet. Add `--skip-existing` (**Skip existing** in the Space) to also skip the S3 read for files the destination already has at the same path and size.

## Your AWS credentials

Your AWS credentials go only into the Jobs' **encrypted secrets**, through the HF API: from your machine with `hf s3ream`, from your browser with the Space. They never touch any server we run. Prefer a scoped, read-only, short-lived key for just the source prefix.

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

## Limits

- **VPC-locked source buckets are unreachable from HF Jobs.** Jobs run on HF's network, outside your AWS VPC, so a bucket policy pinned to `aws:sourceVpce` / `aws:SourceVpc` returns `403` there regardless of valid creds — that's network topology, not a credentials problem. The dry-run preflight surfaces it. For those buckets, run [`hf s3ream --local`](#a-copy-from-a-terminal) or [the container](CONTAINER.md) from inside the VPC.
- **The source prefix should be frozen during the copy.** The planner cuts key ranges from one listing pass; if the source mutates mid-run, the plan goes stale. Fine for migrations and archival (the intended use), not for live-mutating buckets.
