# User guide

This guide follows a copy from its start to its end. You can start a copy from a terminal, with `hf s3ream`, or from a browser, with the web Space. Both start the same copy, so choose the one you like. The first steps are in the [README](../README.md). All the options of the CLI, its JSON events and its exit codes are in [`cli/README.md`](../cli/README.md).

- [Before you start](#before-you-start)
- [A copy from a terminal](#a-copy-from-a-terminal)
- [A copy from the Space](#a-copy-from-the-space)
- [The plan](#the-plan)
- [Stop a copy](#stop-a-copy)
- [Run a copy again](#run-a-copy-again)
- [Your AWS credentials](#your-aws-credentials)
- [Limits](#limits)

## Before you start

- **A Hugging Face account that can pay for Jobs.** The Jobs are billed to your account (or to an organization, with `--namespace`). Each started minute is billed, at the [published flavor rates](https://huggingface.co/docs/hub/jobs-pricing). Any account with [pre-paid credits](https://huggingface.co/settings/billing) works.
- **A token.** The CLI uses your `hf auth login` token (or `HF_TOKEN`). The token must be able to write in the namespace of the bucket, and to start Jobs. A fine-grained token needs the Jobs permission. The Space uses the token of your sign-in.
- **Read access to the source.** AWS credentials that can list and read the prefix: see the [minimal AWS policy](#minimal-aws-policy). A public bucket needs none, for example a bucket of the [AWS Open Data registry](https://registry.opendata.aws/).

## A copy from a terminal

`hf s3ream` is an [`hf` CLI extension](https://huggingface.co/docs/huggingface_hub/guides/cli-extensions). It needs only the [`hf` CLI](https://huggingface.co/docs/huggingface_hub/guides/cli) (huggingface_hub 2.4 or later). It needs no Docker and no Node.

```bash
hf extensions install glutamatt/hf-s3ream
hf auth login        # once
hf s3ream s3://my-bucket/prefix/ hf://buckets/your-org/your-bucket/path/to/dest
```

The path after the bucket name is optional. Then:

1. **Dry-run.** A short dry-run Job lists the source from the HF side. It checks the S3 access, finds the region, and counts the files and the bytes. Then it proposes a [plan](#the-plan), with its time and its cost. Nothing is copied or created yet. If the destination bucket does not exist, `hf s3ream` says so. It creates the bucket, as a **private** bucket, after you confirm.
2. **Confirm.** One planner Job runs the copy (see [The life of a copy](ARCHITECTURE.md#the-life-of-a-copy)). Your terminal shows its live progress. Ctrl-C asks before it stops anything. A second Ctrl-C within 5 s stops the planner and all the copiers. `d` detaches: the copy continues. If you close the terminal, the copy continues too.
3. **The end.** You get the result of the [destination check](DESIGN.md#verification), the files and bytes copied, and the cost.

To try it without AWS keys, choose a public bucket, and run only the dry-run:

```bash
hf s3ream s3://<public-bucket>/<prefix>/ hf://buckets/your-org/your-bucket --no-sign-request --dry-run
```

`--dry-run` runs the dry-run Job, and shows the plan and its cost. Nothing is copied or written: a missing destination bucket is not created. The Job is billed (cpu-basic, 5 minutes at most).

- **Credentials**: your `hf auth login` token (or `HF_TOKEN`). The AWS credentials come from the environment, or from the AWS CLI (`AWS_PROFILE`, SSO too). They go to the Jobs as encrypted secrets. With `--no-sign-request`, the Jobs read a public bucket with no credentials.
- **Follow or stop a copy from any terminal**: `hf s3ream status RUN` shows where the copy is, `attach RUN` follows it until its end, and `stop RUN` stops it. `RUN` is the label printed at the start, or the id of the planner Job. This also works for a copy started in the Space.
- **For agents and scripts**: `--json` prints one JSON event per line. It is the default when stdout is not a terminal. Nothing ever waits for an answer: use `--yes` to start. `--max-cost USD` protects your budget. `--detach` returns as soon as the copy has started. The exit codes are documented.
- **An organization**: `--namespace ORG` runs the Jobs in the organization, so the organization pays for them. The cost shown is then an estimate, from the run times of the Jobs.
- **`--local`** runs the same pinned image in Docker, on your machine. Use it for a bucket that HF Jobs cannot reach (a bucket open only to your VPC). The secrets go to the container by name, never on a command line. The image is for linux/amd64 only.
- **Updates**: `hf extensions update s3ream`. Each version of the extension runs the image that it pins. `hf s3ream --version` shows both.

All the options, the JSON events and the exit codes: [`cli/README.md`](../cli/README.md). `hf s3ream --help` gives the short version.

## A copy from the Space

The [Space](https://huggingface.co/spaces/glutamatt/hf-s3ream) starts the same copy from your browser, with the same plan. There is nothing to install, and the whole copy is on a live graph. Open **[huggingface.co/spaces/glutamatt/hf-s3ream](https://huggingface.co/spaces/glutamatt/hf-s3ream)**, then:

1. **Sign in with Hugging Face** (OAuth, in the browser). The Jobs are billed by the minute to *your* account. Any account with [pre-paid credits](https://huggingface.co/settings/billing) works.
2. Enter the **S3 source prefix**, the **destination bucket**, and **AWS credentials** for the source. For a source that allows anonymous reads, switch on **Public bucket** instead (e.g. the [AWS Open Data registry](https://registry.opendata.aws/)). Then no credentials are sent, and the Jobs read S3 without a signature (`--no-sign-request`).
3. **Analyze.** If the destination bucket does not exist, the page creates it as a **private** bucket. You can change its visibility later, in the settings of the bucket. Then a cheap `--dry-run` Job lists the source from the HF side. It checks the S3 access, finds the region, and measures the transfer. It proposes a [plan](#the-plan), with its time and its cost, before you spend anything. A very large source is listed for 2 minutes only. The plan is then made from that part, and its cost is shown as a minimum. The planner lists the whole source itself.
4. **Run copy.** The page starts **one planner Job** (see [The life of a copy](ARCHITECTURE.md#the-life-of-a-copy)), and then only watches. It shows the progress of all the copiers on one live graph. You can close the tab: the copy still ends. You can still see the Jobs on your [HF Jobs page](https://huggingface.co/jobs). The Space cannot follow a running copy again after you close the tab. To see where it is, use `hf s3ream status` or `attach`, with the id of the planner Job.

During the copy, the page reads the cost of each Job from your billing (the `read-billing` scope is for this). It shows the total, and the cost of each copier. At the end of the copy, one line gives the total for all the Jobs: the copier Jobs (and the ranges that ran again), the billed minutes, the cost, and the cost per TiB copied.

The Space is a **static page: it has no backend.** The browser talks directly to the Hugging Face API (OAuth + CORS).

## The plan

The dry-run proposes a plan from what it listed. `hf s3ream` and the Space get the same plan, because the plan comes from the binary. The plan shows:

- **The copiers**: how many key ranges (one copier Job for each range), on which flavor, and how many copiers may run at the same time. Within that limit, the planner sets the number of copiers while the copy runs (see [Adaptive concurrency](ADAPTIVE-CONCURRENCY.md)).
- **The time**, and what limits it. The limit is the bandwidth, or, with many small files, the commits: the destination registers about 1,000 files per second for each bucket.
- **The cost**, billed by started minute, and the time limit of the planner.

You can change the plan by hand: with `--copier-flavor`, `--range-gib` and `--max-inflight`, or under *Advanced* in the Space. Then there is no cost estimate.

How the plan is chosen, and why: [The plan](DESIGN.md#the-plan).

## Stop a copy

- **From a terminal**: press Ctrl-C twice in the `hf s3ream` that started the copy, or run `hf s3ream stop RUN` from any terminal.
- **In the Space**, while the tab is open: click **Stop copy** under the progress bar, then confirm. The page cancels the planner Job, then all the Jobs of the copy that still run. It shows what was copied.
- **From anywhere else**: cancel the **planner** Job, on your [Jobs page](https://huggingface.co/jobs) or with `hf jobs cancel <planner-job-id>`. You need nothing more. The planner cancels its copiers before it ends. A copier whose planner has ended cancels itself (after one minute, or a few minutes for a big fleet). When the planner reaches its time limit, it stops the copy in the same way.

Each Job of a copy has the label `hf-s3ream-run=<run label>` (you see it on the page of the Job). To check that no Job still runs: `hf jobs ps --label hf-s3ream-run=<run label>`.

The files that are already in the bucket stay there. The other files are not copied. To finish the copy later, run it again with `--skip-existing` (**Skip existing** in the Space). If you cancel only one copier, the planner does not start it again. Its key range is not copied, and the copy ends as failed.

## Run a copy again

When you run a stopped copy again, it uploads again only what is missing: Xet only sends the chunks that the storage does not have yet. Add `--skip-existing` (**Skip existing** in the Space) to also skip the S3 read of the files that the destination already has, with the same path and the same size.

A source that grows during the copy: the files that arrive after the listing are not copied, and the copy does not fail because of them. At the end, the copy says how many files it found. To copy them, run the copy again with `--skip-existing` (**Skip existing** in the Space). See [A source that grows during the copy](DESIGN.md#a-source-that-grows-during-the-copy).

## Your AWS credentials

Your AWS credentials go only into the **encrypted secrets** of the Jobs, through the HF API. They go from your machine with `hf s3ream`, and from your browser with the Space. They never reach a server that we run. Use a key with few rights if you can: read-only, valid for a short time, and only for the source prefix.

### Minimal AWS policy

The copy only reads the source. It needs `s3:ListBucket` on the bucket, limited to the prefix, and `s3:GetObject` on the objects in the prefix. For the prefix `my/prefix/` of the bucket `my-bucket`:

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

With this policy, **write the source with a `/` at the end**: `s3://my-bucket/my/prefix/`. Without it, the listing asks for the prefix `my/prefix`. The condition `my/prefix/*` does not allow this prefix, so the copy fails with `AccessDenied` on `s3:ListBucket`.

You need nothing else. The tool still finds the region: S3 sends the region of the bucket, even when it refuses the request. A public source needs no policy at all: switch on **Public bucket** (or pass `--no-sign-request`).

## Limits

- **HF Jobs cannot reach a source bucket that only accepts your VPC.** The Jobs run on the HF network, outside your AWS VPC. So a bucket policy limited to `aws:sourceVpce` / `aws:SourceVpc` answers `403` to them, even with valid credentials. This comes from the network, not from the credentials. The dry-run shows it before the copy. For these buckets, run [`hf s3ream --local`](#a-copy-from-a-terminal) or [the container](CONTAINER.md) from a machine inside the VPC.
- **The source prefix should be frozen during the copy.** The planner cuts key ranges from one listing pass; if the source mutates mid-run, the plan goes stale. Fine for migrations and archival (the intended use), not for live-mutating buckets.
