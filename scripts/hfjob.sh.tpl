#!/usr/bin/env bash
#
# hf-s3ream on HF Jobs — clone an S3 prefix into a HuggingFace Bucket on
# Hugging Face's own managed compute. No cluster, no SLURM, no local machine
# doing the transfer: the copy runs in a throwaway container on HF Jobs,
# billed per second. Any account with pre-paid credits can run it.
#
#   # one-shot (always uses the latest release's image):
#   curl -fsSL https://github.com/glutamatt/hf-s3ream/releases/latest/download/hfjob.sh \
#       | bash -s -- --src s3://my-bucket/some/prefix/ --dst your-org/your-bucket
#
#   # download once, run repeatedly (version-pinned):
#   curl -fsSL https://github.com/glutamatt/hf-s3ream/releases/latest/download/hfjob.sh -o hfjob.sh
#   chmod +x hfjob.sh
#   ./hfjob.sh --help
#
# What you need on the machine you run THIS script from (NOT on HF's side):
#   - the `hf` CLI:  curl -LsSf https://hf.co/cli/install.sh | bash
#   - a HuggingFace login:  `hf auth login`  (or HF_TOKEN in your env)
#   - AWS credentials reachable via env vars, ~/.aws/credentials, or the
#     `aws` CLI. IMPORTANT: HF Jobs runs OFF AWS, so there is no IMDS/IRSA
#     instance role there — STATIC keys (access key + secret, plus a session
#     token for SSO/STS) are required. This script resolves them locally and
#     forwards them as encrypted Job secrets (never on the command line, never
#     in the logs).

set -euo pipefail

# __IMAGE_TAG__ is substituted at release time by .github/workflows/release.yml.
# On a non-release checkout it stays literal and the script refuses to run
# unless --image-tag is passed.
DEFAULT_IMAGE_TAG="__IMAGE_TAG__"
IMAGE_REPO="ghcr.io/glutamatt/hf-s3ream"

SRC=""
DST=""
FLAVOR="cpu-upgrade"
TIMEOUT="2h"
REGION=""
PROFILE="${AWS_PROFILE:-default}"
NAMESPACE=""
SHARDS=1
IMAGE_TAG="$DEFAULT_IMAGE_TAG"
DETACH=0
CREATE_BUCKET=0
PRIVATE=0
DRY_RUN=0
EXTRA_ARGS=()

die() { echo "error: $*" >&2; exit 1; }

usage() {
    cat <<'EOF'
hf-s3ream on HF Jobs — clone an S3 prefix into a HuggingFace Bucket, running
the transfer on Hugging Face's managed compute (no cluster required).

Usage:
  hfjob.sh --src s3://... --dst org/repo [opts]

Required:
  --src S3_URL           source S3 URL (e.g. s3://my-bucket/prefix/)
  --dst ORG/REPO         destination HuggingFace Bucket (org/repo)

Common options:
  --flavor NAME          HF Jobs hardware flavor (default: cpu-upgrade,
                         8 vCPU / 32 GB, ~$0.03/hr). The copy is bandwidth-bound,
                         NOT CPU-bound — bigger flavors clock the same ~400 MiB/s
                         and just cost more, so don't upsize; scale with --shards
                         instead. cpu-basic (2 vCPU) is ~12% slower but cheapest.
                         See `hf jobs hardware`.
  --timeout DURATION     wall-clock limit, e.g. 2h, 90m, 1.5h, 7200 (seconds).
                         Default: 2h. The job is KILLED at the timeout, so set
                         it generously — nothing partial is committed (the
                         bucket batch lands atomically at the very end).
  --region REGION        AWS region of the SOURCE bucket. Auto-detected from
                         your env / `aws configure` if omitted; falls back to
                         us-east-1.
  --profile NAME         AWS profile to read credentials from (default: the
                         $AWS_PROFILE env var, else "default")
  --namespace ORG        run the Job under an org account you can write to
                         (independent of --dst). Default: your user namespace.
  --shards N             split the copy across N parallel jobs (default: 1).
                         Each job takes a deterministic FNV slice of the source
                         (--shard-id/--shard-count); slices are disjoint and
                         re-runnable. Each job is its own ~400 MiB/s path, so N
                         shards ≈ N×throughput — faster AND cheaper than a bigger
                         flavor. Waits for all shards unless --detach. Best paired
                         with a cheap flavor: --shards 8 --flavor cpu-basic.
  --create-bucket        `hf buckets create` the destination first (tolerated
                         if it already exists). Add --private to make it private.
  --detach               submit and return immediately, printing the Job ID
                         (default: stream logs until the job finishes).
  --image-tag VER        override the container image tag (default baked in at
                         release time).
  -- ARG ARG ...         args after a literal `--` are forwarded to hf-s3ream
                         (e.g. --exclude '*.tmp', --parallel-files 64).
  --dry-run              print the `hf jobs run` command (secret NAMES only,
                         no values) and exit.
  -h, --help             show this help

Examples:
  # Simplest: copy a prefix, watch the logs until done.
  hfjob.sh --src s3://my-bucket/data/ --dst me/my-bucket

  # Big prefix on beefier hardware, longer timeout, create the dest first.
  hfjob.sh --src s3://my-bucket/huge/ --dst my-org/huge \
      --flavor cpu-performance --timeout 8h --create-bucket

  # Skip some keys and turn up concurrency.
  hfjob.sh --src s3://b/p/ --dst me/p -- --exclude '*.tmp' --parallel-files 64
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --src)             SRC="$2"; shift 2 ;;
        --dst)             DST="$2"; shift 2 ;;
        --flavor)          FLAVOR="$2"; shift 2 ;;
        --timeout)         TIMEOUT="$2"; shift 2 ;;
        --region)          REGION="$2"; shift 2 ;;
        --profile)         PROFILE="$2"; shift 2 ;;
        --namespace)       NAMESPACE="$2"; shift 2 ;;
        --shards)          SHARDS="$2"; shift 2 ;;
        --image-tag)       IMAGE_TAG="$2"; shift 2 ;;
        --create-bucket)   CREATE_BUCKET=1; shift ;;
        --private)         PRIVATE=1; shift ;;
        --detach)          DETACH=1; shift ;;
        --dry-run)         DRY_RUN=1; shift ;;
        -h|--help)         usage; exit 0 ;;
        --)                shift; EXTRA_ARGS+=("$@"); break ;;
        *)                 die "unknown arg: $1 (try --help)" ;;
    esac
done

[[ -n "$SRC" ]]              || die "--src required"
[[ -n "$DST" ]]              || die "--dst required"
[[ "$SRC" =~ ^s3:// ]]       || die "--src must be s3://... (got: $SRC)"
[[ "$DST" =~ ^[^/]+/[^/]+$ ]] || die "--dst must be org/repo (got: $DST)"
[[ "$SHARDS" =~ ^[0-9]+$ && $SHARDS -ge 1 ]] || die "--shards must be a positive integer (got: $SHARDS)"
case "$IMAGE_TAG" in __*__) die "image tag is still a template placeholder ($IMAGE_TAG) — pass --image-tag vX.Y.Z, or download a release asset" ;; esac

command -v hf >/dev/null || die "the 'hf' CLI is not in PATH. Install it with: curl -LsSf https://hf.co/cli/install.sh | bash"

# HF token: passed to the job as `-s HF_TOKEN`, which lets the CLI resolve it
# from the env var OR ~/.cache/huggingface/token. Fail early if neither exists.
if [[ -z "${HF_TOKEN:-}" && ! -r "$HOME/.cache/huggingface/token" ]]; then
    die "no HuggingFace token found — run 'hf auth login', or set HF_TOKEN"
fi

# Resolve STATIC AWS credentials from (in order): env vars, ~/.aws/credentials
# [profile], then the `aws` CLI (which turns SSO/role logins into temporary
# static keys). We export them so the `-s NAME` form below picks the values up
# from the environment — keeping secret values off argv entirely.
AWS_SOURCE=""
if [[ -n "${AWS_ACCESS_KEY_ID:-}" && -n "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
    AWS_SOURCE="environment"
elif [[ -r "$HOME/.aws/credentials" ]]; then
    aws_kv() {
        sed -n "/^\[$PROFILE\]/,/^\[/p" "$HOME/.aws/credentials" \
            | sed -n "s/^$1[[:space:]]*=[[:space:]]*//p" | head -1
    }
    AWS_ACCESS_KEY_ID=$(aws_kv aws_access_key_id)
    AWS_SECRET_ACCESS_KEY=$(aws_kv aws_secret_access_key)
    AWS_SESSION_TOKEN=$(aws_kv aws_session_token)
    [[ -n "$AWS_ACCESS_KEY_ID" && -n "$AWS_SECRET_ACCESS_KEY" ]] \
        && AWS_SOURCE="~/.aws/credentials [$PROFILE]"
fi
if [[ -z "$AWS_SOURCE" ]] && command -v aws >/dev/null; then
    # SSO / assume-role: materialize temporary static creds (incl. session token).
    if exported=$(aws configure export-credentials --profile "$PROFILE" --format env 2>/dev/null) \
        && [[ -n "$exported" ]]; then
        eval "$exported"
        AWS_SOURCE="aws configure export-credentials [$PROFILE]"
    fi
fi
[[ -n "$AWS_SOURCE" && -n "${AWS_ACCESS_KEY_ID:-}" && -n "${AWS_SECRET_ACCESS_KEY:-}" ]] \
    || die "couldn't resolve AWS credentials from env, ~/.aws/credentials [$PROFILE], or the aws CLI.
       HF Jobs runs off-AWS (no instance role), so static keys are required.
       Export AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, or run 'aws configure'."
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
[[ -n "${AWS_SESSION_TOKEN:-}" ]] && export AWS_SESSION_TOKEN || true

# Region of the SOURCE bucket. Not a secret — passed as a plain env var.
if [[ -z "$REGION" ]]; then
    REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-}}"
fi
if [[ -z "$REGION" ]] && command -v aws >/dev/null; then
    REGION=$(aws configure get region --profile "$PROFILE" 2>/dev/null || true)
fi

IMAGE="${IMAGE_REPO}:${IMAGE_TAG}"

if (( CREATE_BUCKET )); then
    create_cmd=(hf buckets create "$DST")
    (( PRIVATE )) && create_cmd+=(--private)
    echo "creating bucket: ${create_cmd[*]}"
    # Tolerate failure: the most common cause is "already exists".
    "${create_cmd[@]}" || echo "note: bucket create returned non-zero (it may already exist) — continuing"
fi

# Assemble the `hf jobs run` invocation.
#
# Secrets (`-s NAME`, no value): the CLI reads NAME from our exported env and
# encrypts it server-side. HF_TOKEN authenticates the bucket commit; the AWS
# keys authenticate the S3 reads.
#
# Command form: we name the binary and separate it with `--`. HF Jobs sets the
# container command k8s-style, OVERRIDING the image ENTRYPOINT (verified on real
# infra 2026-07-09: `hf jobs run IMAGE -- --help` fails with `exec "--help": not
# found`), so the executable must be named — it can't be inherited from
# ENTRYPOINT. `hf-s3ream` resolves via $PATH (it lives in /usr/local/bin). The
# `--` keeps hf-s3ream's own flags (the passthrough) from being parsed as `hf
# jobs` options. Env/secret flags go BEFORE the image; everything after `--` is
# the container command.
# BASE = `hf jobs run` options that go BEFORE the image; APP = the container
# command AFTER `--`. Sharded runs reuse both and append a per-shard
# `--shard-id/--shard-count` to a copy of APP.
BASE=(hf jobs run --flavor "$FLAVOR" --timeout "$TIMEOUT" --label hf-s3ream)
[[ -n "$NAMESPACE" ]] && BASE+=(--namespace "$NAMESPACE")
[[ -n "$REGION" ]] && BASE+=(-e "AWS_REGION=$REGION")
BASE+=(-s HF_TOKEN -s AWS_ACCESS_KEY_ID -s AWS_SECRET_ACCESS_KEY)
[[ -n "${AWS_SESSION_TOKEN:-}" ]] && BASE+=(-s AWS_SESSION_TOKEN)

APP=(-- hf-s3ream "$SRC" "$DST")
APP+=("${EXTRA_ARGS[@]+${EXTRA_ARGS[@]}}")

# `-s NAME` carries only names (values live in the exported env), so printing a
# command verbatim never leaks a secret.
print_cmd() { printf '%q ' "$@"; echo; }

# ---- Single job (default) --------------------------------------------------
if (( SHARDS <= 1 )); then
    CMD=("${BASE[@]}")
    (( DETACH )) && CMD+=(--detach)
    CMD+=("$IMAGE" "${APP[@]}")
    if (( DRY_RUN )); then print_cmd "${CMD[@]}"; exit 0; fi
    echo "submitting to HF Jobs:"
    echo "  image:      $IMAGE"
    echo "  src:        $SRC"
    echo "  dst:        $DST"
    echo "  flavor:     $FLAVOR   timeout: $TIMEOUT   region: ${REGION:-us-east-1 (default)}"
    echo "  aws creds:  $AWS_SOURCE   namespace: ${NAMESPACE:-<your user>}"
    echo "  view:       https://huggingface.co/storage/$DST"
    echo
    exec "${CMD[@]}"
fi

# ---- Sharded fan-out (--shards N > 1) --------------------------------------
# Launch N jobs, each pinned to its FNV1a64(key) % N slice via
# --shard-id/--shard-count. Slices are deterministic and disjoint, so a failed
# shard can be re-run (CAS dedups any overlap), and each shard commits its own
# bucket batch of ~total/N files — smaller commit tail, per-file overhead run in
# parallel. Each job is an independent ~400 MiB/s path, so N shards ≈ N×400 MiB/s
# — faster AND cheaper than one big flavor (benchmark: throughput is
# bandwidth-bound, not CPU-bound). Pair with a cheap flavor (e.g. --flavor
# cpu-basic) for the best $/TiB.
if (( DRY_RUN )); then
    for (( i = 0; i < SHARDS; i++ )); do
        print_cmd "${BASE[@]}" --detach "$IMAGE" "${APP[@]}" --shard-id "$i" --shard-count "$SHARDS"
    done
    exit 0
fi

echo "submitting $SHARDS sharded jobs to HF Jobs:"
echo "  image:   $IMAGE"
echo "  src:     $SRC   ->   dst: $DST"
echo "  flavor:  $FLAVOR   timeout: $TIMEOUT   region: ${REGION:-us-east-1 (default)}"
echo "  creds:   $AWS_SOURCE   namespace: ${NAMESPACE:-<your user>}"
echo

IDS=()
for (( i = 0; i < SHARDS; i++ )); do
    out=$("${BASE[@]}" --detach --label "shard=${i}-of-${SHARDS}" "$IMAGE" \
          "${APP[@]}" --shard-id "$i" --shard-count "$SHARDS")
    # Job IDs are 24 hex chars; grep them out of `id=… url=…` robustly.
    id=$(printf '%s\n' "$out" | grep -oiE '[0-9a-f]{24}' | head -1)
    [[ -n "$id" ]] || die "shard $i: could not parse a job id from hf output: $out"
    echo "  shard $i/$SHARDS  ->  job $id"
    IDS+=("$id")
done
echo
echo "view: https://huggingface.co/storage/$DST"

if (( DETACH )); then
    echo "detached — watch with:  hf jobs ps   |   hf jobs logs <id> --follow"
    printf '  %s\n' "${IDS[@]}"
    exit 0
fi

echo
echo "waiting for all $SHARDS shards (Ctrl-C stops waiting only; jobs keep running)..."
# `hf jobs wait` exits non-zero if ANY shard failed — re-run those --shard-id
# indices (CAS dedups the rest). Requeue a subset by re-invoking with the same
# --shards N and adding `-- --shard-id K --shard-count N` for each failed K.
exec hf jobs wait "${IDS[@]}"
