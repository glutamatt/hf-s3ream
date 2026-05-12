#!/usr/bin/env bash
#
# hf-s3ream — clone an S3 prefix into a HuggingFace Bucket via Pyxis/SLURM.
#
# Two ways to run this script:
#
#   # one-shot (always uses the latest release's image):
#   curl -fsSL https://github.com/glutamatt/hf-s3ream/releases/latest/download/submit.sh \
#       | bash -s -- --partition cpu --src s3://… --dst org/repo
#
#   # download once, run repeatedly (advanced usage, version-pinned):
#   curl -fsSL https://github.com/glutamatt/hf-s3ream/releases/latest/download/submit.sh -o submit.sh
#   chmod +x submit.sh
#   ./submit.sh --partition cpu --src s3://… --dst org/repo --shards 16

set -euo pipefail

# __IMAGE_TAG__ is substituted at release time by .github/workflows/release.yml.
# When running from a non-release source (e.g. a `main` checkout) this stays
# literal and the script will refuse to run unless --image-tag is passed.
DEFAULT_IMAGE_TAG="__IMAGE_TAG__"
IMAGE_REPO="ghcr.io/glutamatt/hf-s3ream"

PARTITION=""
SRC=""
DST=""
SHARDS=1
TIME="4:00:00"
CPUS=8
MEM="32G"
IMAGE_TAG="$DEFAULT_IMAGE_TAG"
EXCLUDES=()
EXTRA_ARGS=()
DRY_RUN=0

die() { echo "error: $*" >&2; exit 1; }

usage() {
    cat <<'EOF'
hf-s3ream — clone an S3 prefix into a HuggingFace Bucket via xet.

Usage:
  submit.sh --partition NAME --src s3://... --dst org/repo [opts]

Required:
  --partition NAME       SLURM partition
  --src S3_URL           source S3 URL (e.g. s3://my-bucket/prefix/)
  --dst ORG/REPO         destination HuggingFace Bucket (org/repo)

Common options:
  --shards N             SLURM array size for sharded clone (default: 1).
                         Each task processes a deterministic FNV1a-sharded
                         subset of the source — failed array indices can be
                         requeued and will reprocess the same files.
  --time HH:MM:SS        wall time limit per task (default: 4:00:00)
  --cpus-per-task N      CPUs per task (default: 8)
  --mem SIZE             memory per task (default: 32G)
  --image-tag VER        override container image tag (default baked in at
                         release time; falls back to the script value below)
  --exclude GLOB         exclude S3 keys matching glob (repeatable; passed
                         through to hf-s3ream)
  -- ARG ARG ...         any args after a literal `--` are passed straight
                         through to hf-s3ream (e.g. --parallel-files, --xor-byte)
  --dry-run              print the generated sbatch script and exit
  -h, --help             show this help

Credentials:
  HF_TOKEN env var (or ~/.cache/huggingface/token) is read on the login node
  and exported into the job. ~/.aws is mounted read-only into the container.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --partition)       PARTITION="$2"; shift 2 ;;
        --src)             SRC="$2"; shift 2 ;;
        --dst)             DST="$2"; shift 2 ;;
        --shards)          SHARDS="$2"; shift 2 ;;
        --time)            TIME="$2"; shift 2 ;;
        --cpus-per-task)   CPUS="$2"; shift 2 ;;
        --mem)             MEM="$2"; shift 2 ;;
        --image-tag)       IMAGE_TAG="$2"; shift 2 ;;
        --exclude)         EXCLUDES+=("$2"); shift 2 ;;
        --dry-run)         DRY_RUN=1; shift ;;
        -h|--help)         usage; exit 0 ;;
        --)                shift; EXTRA_ARGS+=("$@"); break ;;
        *)                 die "unknown arg: $1 (try --help)" ;;
    esac
done

[[ -n "$PARTITION" ]]                   || die "--partition required"
[[ -n "$SRC" ]]                         || die "--src required"
[[ -n "$DST" ]]                         || die "--dst required"
[[ "$SRC" =~ ^s3:// ]]                  || die "--src must be s3://... (got: $SRC)"
[[ "$DST" =~ ^[^/]+/[^/]+$ ]]           || die "--dst must be org/repo (got: $DST)"
case "$IMAGE_TAG" in __*__) die "image tag is still a template placeholder ($IMAGE_TAG) — pass --image-tag vX.Y.Z, or download a release asset" ;; esac
[[ "$SHARDS" =~ ^[0-9]+$ && $SHARDS -ge 1 ]] || die "--shards must be a positive integer"

# Resolve HF token on the login node; it gets exported into the job env.
if [[ -z "${HF_TOKEN:-}" ]]; then
    if [[ -r "$HOME/.cache/huggingface/token" ]]; then
        HF_TOKEN=$(<"$HOME/.cache/huggingface/token")
    else
        die "set HF_TOKEN env var, or run 'hf auth login' first (no token at \$HF_TOKEN nor ~/.cache/huggingface/token)"
    fi
fi
export HF_TOKEN

[[ -d "$HOME/.aws" ]] || die "no AWS credentials at $HOME/.aws — run 'aws configure'"

IMAGE="${IMAGE_REPO}:${IMAGE_TAG}"

# Pass-through args for the hf-s3ream binary inside the container.
PASSTHROUGH=()
for e in "${EXCLUDES[@]}"; do
    PASSTHROUGH+=("--exclude" "$e")
done
PASSTHROUGH+=("${EXTRA_ARGS[@]}")

# Array directive only emitted when --shards > 1. With shards=1, hf-s3ream's
# own defaults (--shard-id=0, --shard-count=1) are used.
ARRAY_DIRECTIVE=""
SHARD_ARGS=""
if (( SHARDS > 1 )); then
    ARRAY_DIRECTIVE="#SBATCH --array=0-$((SHARDS - 1))"
    SHARD_ARGS='--shard-id "$SLURM_ARRAY_TASK_ID" --shard-count "$SLURM_ARRAY_TASK_COUNT"'
fi

# Render the sbatch script. We deliberately put `srun` on a single physical
# line because bash heredocs with `<<EOF` (unquoted) collapse `\<newline>`
# continuations after escape resolution, which silently turned the multi-line
# version into one big malformed line at execution time. Ugly but predictable.
# Bash escapes:
#   - $varname  => expanded NOW (login-node values, e.g. $IMAGE, $SRC)
#   - \$varname => expanded LATER (inside the job, e.g. \$SLURM_ARRAY_TASK_ID)
# $HOME is resolved here at submit time. On typical SLURM clusters the user's
# home is on shared storage, so the login-node path is identical to the
# compute-node path — embedding it absolutely avoids any container-side
# $HOME confusion and lets `printf %q` quote everything uniformly.
SRUN_ARGS=(
    "--container-image=$IMAGE"
    "--container-mounts=$HOME/.aws:/root/.aws:ro"
    "--container-env=HF_TOKEN,AWS_SHARED_CREDENTIALS_FILE,AWS_REGION,HF_HOME"
    "/usr/local/bin/hf-s3ream"
    "$SRC"
    "$DST"
)
if [[ -n "$SHARD_ARGS" ]]; then
    # Word-split: SHARD_ARGS = '--shard-id "$SLURM_ARRAY_TASK_ID" --shard-count "$SLURM_ARRAY_TASK_COUNT"'
    # Those `$SLURM_*` references must stay literal so they expand inside the job, not now.
    # Handle separately below — don't %q-escape them.
    :
fi
SRUN_ARGS+=("${PASSTHROUGH[@]+${PASSTHROUGH[@]}}")

# Render each token shell-quoted so paths with spaces or special chars survive.
SRUN_LINE="srun"
for a in "${SRUN_ARGS[@]}"; do
    SRUN_LINE+=" $(printf '%q' "$a")"
done
# Append SHARD_ARGS verbatim (it contains $SLURM_* that must expand at job time).
if [[ -n "$SHARD_ARGS" ]]; then
    SRUN_LINE+=" $SHARD_ARGS"
fi

SBATCH=$(cat <<EOF
#!/usr/bin/env bash
#SBATCH --job-name=hf-s3ream
#SBATCH --partition=$PARTITION
#SBATCH --cpus-per-task=$CPUS
#SBATCH --mem=$MEM
#SBATCH --time=$TIME
#SBATCH --requeue
#SBATCH --output=hf-s3ream-%A_%a.out
#SBATCH --error=hf-s3ream-%A_%a.err
$ARRAY_DIRECTIVE

set -euo pipefail

# Inside the container we mount the user's ~/.aws read-only and point
# AWS_SHARED_CREDENTIALS_FILE at it explicitly (rather than relying on
# \$HOME resolution inside the container image).
export AWS_SHARED_CREDENTIALS_FILE=/root/.aws/credentials
export HF_HOME=/tmp/hf-cache

$SRUN_LINE
EOF
)

if (( DRY_RUN )); then
    echo "$SBATCH"
    exit 0
fi

command -v sbatch >/dev/null || die "sbatch not in PATH — are you on a SLURM login node?"

echo "submitting:"
echo "  image:     $IMAGE"
echo "  src:       $SRC"
echo "  dst:       $DST"
echo "  partition: $PARTITION"
echo "  shards:    $SHARDS"
echo "  time:      $TIME"
echo "  cpus/task: $CPUS"
echo "  mem/task:  $MEM"

JOB_ID=$(echo "$SBATCH" | sbatch --parsable --export=ALL,HF_TOKEN)
echo "submitted job: $JOB_ID"
echo "logs: hf-s3ream-${JOB_ID}_*.out (current dir)"
