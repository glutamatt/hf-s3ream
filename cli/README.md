# `hf s3ream`: the command line

`hf s3ream` copies an S3 prefix into a Hugging Face Bucket. It is an
extension of the `hf` CLI:

```
hf extensions install glutamatt/hf-s3ream
```

`hf` installs it in its own Python environment. Nothing else is needed on
your machine: no Docker, no Node.

## How it works

The copy runs on HF Jobs, billed to your account, like the web Space:

1. A short dry-run Job reads the source and proposes a plan: how many copier
   Jobs, on which hardware, for how long, and what it should cost. The plan
   comes from the `hf-s3ream` binary, so the CLI and the Space propose the
   same plan.
2. You confirm. One planner Job starts and lists the source. It starts the
   copier Jobs and watches them.
3. Your terminal shows the progress, from the planner's log. Closing the
   terminal does not stop the copy.

```
hf s3ream s3://my-bucket/data/ hf://buckets/my-org/my-bucket
```

The AWS credentials come from the environment (`AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`), or from the AWS CLI
(`AWS_PROFILE`, SSO included). They go to the Jobs as encrypted secrets.
For a public bucket, pass `--no-sign-request`: no AWS credential is sent.

Your Hugging Face token comes from `hf auth login` (or `HF_TOKEN`). It needs
write access to the bucket's namespace and the right to start Jobs.

## In a terminal

| Key | What it does |
|---|---|
| Ctrl-C | Asks first. Ctrl-C again within 5 s stops the copy: the planner and every copier are canceled. Any other key keeps copying. |
| `d` | Detaches: the command returns, the copy keeps running. |

After a stop, the files already committed stay in the bucket. Run the same
command with `--skip-existing` to finish the copy.

## Options

| Option | |
|---|---|
| `--yes` | Start without asking. |
| `--detach` | Start the copy and return. |
| `--analyze` | Show the plan and its cost, start nothing. |
| `--max-cost USD` | Do not start if the estimate is higher. |
| `--label NAME` | Name the run. Refused while a run with this name is running. |
| `--json` | JSON lines on stdout (the default when stdout is not a terminal). |
| `--skip-existing` | Skip files already at the destination (same path and size). |
| `--no-sign-request` | Read a public source anonymously. |
| `--namespace ORG` | Run the Jobs under an organization. |
| `--copier-flavor`, `--range-gib`, `--max-inflight`, `--timeout` | Set the plan by hand (no cost estimate then). |
| `--local` | Run the copy in Docker on this machine, for a source HF Jobs cannot reach (a bucket open only to your VPC). The image is linux/amd64. |

Other options (for example `--exclude GLOB`) go to the binary as they are.

```
hf s3ream status RUN      # where a copy stands; returns at once
hf s3ream attach RUN      # follow a copy until it ends
hf s3ream stop RUN        # stop a copy
```

`RUN` is the label printed at start (`s3ream-…`, or your `--label`), or the
planner Job id.

## For agents and scripts

- With `--json`, or when stdout is not a terminal, the output is JSON lines:
  one event per line, nothing else on stdout.
- Nothing waits for an answer. Without a terminal, a copy starts only with
  `--yes`. Pass `--max-cost` as a budget guard.
- A copy can last hours: start it with `--detach`, then call `status`, or
  `attach` to wait for the end.
- `--label` makes a start safe to retry: while a run with this label is
  running, a new start is refused (exit code 6) and the error says how to
  attach.
- Killing the process does not stop the copy: the planner runs on its own.
  Use `stop`. A SIGINT stops the copy.

```
hf s3ream s3://src/ org/dst --analyze --json
hf s3ream s3://src/ org/dst --yes --detach --json --max-cost 20 --label nightly
hf s3ream status nightly --json
hf s3ream attach nightly --json      # exit code = the result
hf s3ream stop nightly --json
```

### Exit codes

| Code | Meaning |
|---|---|
| 0 | Copy complete (and verified), started with `--detach`, or the query worked. |
| 1 | The copy ended with failed ranges, or the destination check failed. |
| 2 | Bad arguments. |
| 3 | Token, credits, or bucket access. |
| 4 | Not started: no `--yes` without a terminal, declined, or over `--max-cost`. |
| 5 | The source could not be read by the dry-run (credentials, region, a VPC-only bucket), or listed in 2 minutes. |
| 6 | A run with this `--label` is still running. |
| 7 | `status`, `attach`, `stop`: no such run. |
| 130 | Stopped. |

### Events

Every line is `{"schema": 1, "type": ..., "time": ..., ...}`. Sizes are
bytes, durations seconds, rates MiB/s or files/s, money USD. Costs are
estimates (`"estimated": true` in a plan): the billing page has the exact
figure. Within schema 1, fields can be added, never removed or renamed.

| `type` | Fields |
|---|---|
| `start` | `command`, `version`, `source`, `dest` |
| `account` | `user`, `namespace`, `can_pay` |
| `bucket` | `bucket`, `private`, `created` |
| `listing` | `listed`, `bytes` (during the dry-run) |
| `source` | `files`, `bytes`, `region` |
| `plan` | `copier_flavor`, `range_gib`, `max_inflight`, `timeout_s`, `planner_flavor`, `copiers`, `wall_s`, `usd`, `commit_bound`, `estimated` |
| `started` | `run`, `planner_job`, `namespace`, `url`, `attach`, `stop` |
| `progress` | `run`, `state`, `ranges`, `done`, `active`, `failed`, `canceled`, `listing_done`, `files_planned`, `bytes_planned`, `files_copied`, `bytes_copied`, `committed`, `skipped`, `skipped_bytes`, `bytes_done_total`, `mibps`, `files_per_s`, `pct`, `elapsed_s`, `eta_s`, `cost_usd` |
| `warning` | `message` |
| `stopping` | `run`, `reason` |
| `detached` | `run`, `planner_job`, `attach`, `stop` |
| `result` | `run`, `state` (`completed`, `failed`, `stopped`), `ranges`, `completed`, `failed`, `canceled`, `verified`, `files`, `bytes`, `elapsed_s`, `cost_usd`, `exit_code`, and `resume` when a rerun with `--skip-existing` finishes the copy |
| `status` | `run`, `planner_job`, `state`, `stage`, `progress`, `result`, `cost_usd` |
| `stopped` | `run`, `canceled_jobs`, `left`, `already_ended` |
| `error` | `code`, `message`, `hint`, `exit_code` |

`cli/tests/test_contract.py` checks every event against this table
(`events.py`).

## Development

```
uv run --with pytest --with-editable . pytest
HF_S3REAM_PTY_TEST=1 uv run --with pytest --with pexpect --with-editable . pytest cli/tests/test_pty.py
```

The second line runs the CLI in a pseudo-terminal under the real `hf`
(huggingface_hub 2.4.0), on a fake Hub: it presses Ctrl-C and checks the
copy is stopped only on the second one.
