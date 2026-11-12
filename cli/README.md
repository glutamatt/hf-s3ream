# `hf s3ream`: the command line

`hf s3ream` copies an S3 prefix into a Hugging Face Bucket. It is an
extension of the `hf` CLI:

```
hf extensions install glutamatt/hf-s3ream
```

`hf` installs it in its own Python environment. Nothing else is needed on
your machine: no Docker, no Node (only `--local` needs Docker).

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

A new destination bucket is created **private**.

A very large source (millions of objects) does not list in a short dry-run.
The dry-run stops listing after 2 minutes and sizes the plan from the part
listed: the widest fleet, and a planner timeout of 24 hours. The time and the
cost are then shown as low bounds ("more than"), and the planner lists the
whole source itself. If the copy can take longer than 24 hours, pass
`--timeout SECONDS`.

```
hf s3ream s3://my-bucket/data/ hf://buckets/my-org/my-bucket
```

The AWS credentials come from the environment (`AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`), or from the AWS CLI
(`AWS_PROFILE`, SSO included). They go to the Jobs as encrypted secrets.
For a public bucket, pass `--no-sign-request`: no AWS credential is sent.

Your Hugging Face token comes from `hf auth login` (or `HF_TOKEN`). It needs
write access to the bucket's namespace and the right to start Jobs.

`RUST_LOG`, when set, goes to the Jobs too (the dry-run, the planner and its
copiers): for example `RUST_LOG=hf_s3ream=info,xet_client=debug` logs each
xet upload request in the copiers' logs.

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
| `--label NAME` | Name the copy. One label, one copy: refused once a copy has started with it. |
| `--json` | JSON lines on stdout (the default when stdout is not a terminal). |
| `--skip-existing` | Skip files already at the destination (same path and size). |
| `--no-sign-request` | Read a public source anonymously. |
| `--namespace ORG` | Run the Jobs under an organization. |
| `--copier-flavor`, `--range-gib`, `--max-inflight` | Set the plan by hand (no cost estimate then). |
| `--timeout SECONDS` | Planner timeout: the copy stops after it. It keeps the estimate. |
| `--local` | Run the copy in Docker on this machine. See [below](#in-local-docker). |
| `--version` | The CLI version and the image it runs. |

These options go to the `hf-s3ream` binary as they are:

| Option | |
|---|---|
| `--exclude GLOB` | Skip keys matching GLOB (on the full key; repeatable). |
| `--aws-region REGION` | Region of the source bucket (default: auto-detected). |
| `--no-verify` | Skip the destination check after the copy. |
| `--limit-gib N` | Stop after N GiB (benchmarks). |
| `--commit-chunk N`, `--commit-gib N` | When the copiers commit. |
| `--s3-part-size-mib N`, `--s3-part-concurrency N` | How the copiers read S3. |
| `--xor-byte N` | XOR the data to defeat dedup (benchmarks only). |
| `--no-live-progress` | The planner does not follow its copiers' logs. |
| `--parallel-files N`, `--start-after KEY`, `--stop-at KEY`, `--hub-endpoint URL` | With `--local` only: on HF Jobs the planner sets them. |

An unknown option is an error (exit code 2): nothing is passed on blindly.
`cli/hf_s3ream_cli/binary_options.py` lists every option of the binary, and
CI checks the list against the binary's `--help`.

```
hf s3ream status RUN      # where a copy stands; returns at once
hf s3ream attach RUN      # follow a copy until it ends
hf s3ream stop RUN        # stop a copy
```

`RUN` is the label printed at start (`s3ream-…`, or your `--label`), or the
planner Job id.

## In local Docker

`--local` runs the copy in Docker on this machine, not on HF Jobs. Use it for
a source that HF Jobs cannot reach: a bucket open only to your VPC (run it
from a machine inside that VPC).

```
hf s3ream s3://my-bucket/data/ hf://buckets/my-org/my-bucket --local
hf s3ream s3://my-bucket/data/ my-org/my-bucket --local --analyze   # a dry-run
```

- It runs the same pinned image as the Jobs, `docker run --rm --init --pull
  missing --platform linux/amd64`. The image is built for linux/amd64 only:
  on an arm64 machine (Apple Silicon, Graviton) Docker runs it under
  emulation, which is slow, if it can run it at all. The CLI warns.
- The output and the exit code are those of the `hf-s3ream` binary: no JSON
  events, so `--json` is refused. The options of a copy on HF Jobs
  (`--detach`, `--max-cost`, `--label`, `--namespace`, the plan) are refused too.
- A new destination bucket is created private, as on HF Jobs.
- Your Hugging Face token and the AWS credentials (environment, or `aws
  configure export-credentials`, SSO included) go to the container by name
  (`docker run -e NAME`): no value is ever on a command line. Also passed on
  when set: `AWS_*` (but not `AWS_PROFILE`, `AWS_DEFAULT_REGION` and the
  variables that point to files on this machine), `HF_ENDPOINT`, `HF_XET_*`,
  `HF_S3REAM_*`, `COMMIT_CHUNK`, `COMMIT_GIB`, `RUST_LOG`, `RUST_BACKTRACE`.
- In a terminal, the terminal goes to the container (`-it`): Ctrl-C stops the
  copy at once, and you see the binary's last lines.
- `--docker-arg ARG` adds an argument to `docker run` (repeatable), for
  example `--docker-arg=--network=host` for a source on `localhost`, or
  `--docker-arg=--memory=16g`. `--print-command` prints the `docker run`
  command and runs nothing.

## For agents and scripts

- With `--json`, or when stdout is not a terminal, the output is JSON lines:
  one event per line, nothing else on stdout.
- Nothing waits for an answer. Without a terminal, a copy starts only with
  `--yes`. Pass `--max-cost` as a budget guard. On a source too big to list
  in the dry-run, the cost is unknown: `--max-cost` then refuses to start
  (`cost_unknown`), and the error gives the cost of the part listed.
- A copy can last hours: start it with `--detach`, then call `status`, or
  `attach` to wait for the end.
- `--label` makes a start safe to retry: a label names one copy. Once a
  copy has started with it, a new start is refused (exit code 6), running
  or ended, and the error says how to attach or see it. A start that failed
  before the copy (in the dry-run) can be retried with the same label.
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
| 4 | Not started: no `--yes` without a terminal, declined, over `--max-cost`, or a cost that `--max-cost` cannot check. |
| 5 | The dry-run could not read the source (credentials, region, a VPC-only bucket). |
| 6 | This `--label` already names a copy (running: `already_running`; ended: `label_used`). |
| 7 | `status`, `attach`, `stop`: no such run. |
| 8 | `--local`: Docker is missing or not running. |
| 130 | Stopped. |

With `--local`, the exit code is the binary's: 0 when the copy is complete.

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
| `source` | `files`, `bytes`, `region`, `partial` (the listing was cut short: the figures count the part listed), `listed_in_s` |
| `plan` | `copier_flavor`, `range_gib`, `max_inflight`, `timeout_s`, `planner_flavor`, `copiers`, `wall_s`, `usd`, `commit_bound`, `estimated`, `partial`; when `partial`, `copiers`, `wall_s` and `usd` are null and `min_copiers`, `min_wall_s`, `min_usd` give the figures for the part listed |
| `started` | `run`, `planner_job`, `namespace`, `url`, `attach`, `stop` |
| `progress` | `run`, `state`, `ranges`, `done`, `active`, `failed`, `canceled`, `listing_done`, `files_planned`, `bytes_planned`, `files_copied`, `bytes_copied`, `committed`, `skipped`, `skipped_bytes`, `bytes_done_total`, `files_done_total` (copied or skipped), `mibps`, `files_per_s`, `pct`, `elapsed_s`, `eta_s`, `cost_usd` |
| `warning` | `message` |
| `stopping` | `run`, `reason` |
| `detached` | `run`, `planner_job`, `attach`, `stop` |
| `result` | `run`, `state` (`completed`, `failed`, `stopped`), `ranges`, `completed`, `failed`, `canceled`, `verified`, `files`, `bytes`, `elapsed_s`, `cost_usd`, `exit_code`, and `resume` when a rerun with `--skip-existing` finishes the copy |
| `status` | `run`, `planner_job`, `state`, `stage`, `progress`, `result`, `cost_usd` |
| `stopped` | `run`, `canceled_jobs`, `left`, `already_ended` |
| `error` | `code`, `message`, `hint`, `exit_code` |

The `code` of an `error`: `usage`, `auth`, `billing`, `bucket`, `source`,
`needs_confirmation`, `not_started`, `over_budget`, `cost_unknown`,
`already_running`, `label_used`, `not_found`, `local`, `interrupted`, `internal`.

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

CI runs both (`ci.yml`, job `cli`), plus `ruff check` and `ruff format
--check` (settings in `pyproject.toml`). The Rust job builds the binary and
checks the options table against it:

```
cargo build --release
HF_S3REAM_BIN=target/release/hf-s3ream uv run --with pytest --with-editable . pytest cli/tests/test_binary_options.py
```
