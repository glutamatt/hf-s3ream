"""`--local`: the copy in Docker on this machine, for a source HF Jobs cannot
reach (a bucket open only to a VPC).

The same pinned image as on HF Jobs, run with `docker run`; this process is
replaced by Docker, so the output and the exit code are the binary's. The
secrets go to the container by name (`-e NAME`): no value is ever on a
command line.
"""

from __future__ import annotations

import os
import platform
import shlex
import shutil
import subprocess
import sys

from . import hub
from .events import Exit, Fail

# Forwarded to the container when set.
PREFIXES = ("AWS_", "HF_XET_", "HF_S3REAM_")
NAMES = {"HF_TOKEN", "HF_ENDPOINT", "COMMIT_CHUNK", "COMMIT_GIB", "RUST_LOG", "RUST_BACKTRACE"}
# Not forwarded: names and paths that only mean something on this machine,
# AWS_DEFAULT_REGION (often a shell-wide default: the binary detects the
# bucket's region itself, AWS_REGION forces it), and the settings a planner
# gives the copiers it starts (a copy run by hand is no such copier).
NOT_FORWARDED = {
    "AWS_PROFILE",
    "AWS_DEFAULT_PROFILE",
    "AWS_CONFIG_FILE",
    "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_CA_BUNDLE",
    "AWS_CREDENTIAL_EXPIRATION",
    "AWS_DEFAULT_REGION",
}
NOT_FORWARDED_PREFIXES = ("HF_S3REAM_PLANNER_",)

# Options of the copy on HF Jobs that mean nothing in local Docker.
JOBS_ONLY = {
    "detach": "--detach",
    "max_cost": "--max-cost",
    "label": "--label",
    "namespace": "--namespace",
    "copier_flavor": "--copier-flavor",
    "range_gib": "--range-gib",
    "max_inflight": "--max-inflight",
    "timeout": "--timeout",
    "json": "--json",
}

PLATFORM = "linux/amd64"  # the only platform the image is built for (release.yml)


AWS_KEYS = {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"}


def forwarded(env: dict[str, str], anonymous: bool = False) -> list[str]:
    """The names of the variables the container gets, sorted. A public read
    gets no AWS key, as on HF Jobs."""
    out = []
    for name, value in env.items():
        if not value or name in NOT_FORWARDED or name.startswith(NOT_FORWARDED_PREFIXES):
            continue
        if anonymous and name in AWS_KEYS:
            continue
        if name in NAMES or name.startswith(PREFIXES):
            out.append(name)
    return sorted(out)


def docker_command(
    docker: str,
    image: str,
    source: str,
    dest: str,
    binary_args: list[str],
    env_names: list[str],
    docker_args: list[str],
    tty: bool,
) -> list[str]:
    cmd = [docker, "run", "--rm", "--init", "--pull", "missing", "--platform", PLATFORM]
    if tty:
        # The terminal goes to the container: Ctrl-C reaches the binary as a
        # key, through the container's terminal, and `hf` (our parent) gets
        # no SIGINT, so it does not kill us before the binary has stopped.
        cmd.append("-it")
    for name in env_names:
        cmd += ["-e", name]
    cmd += docker_args
    return [*cmd, image, *binary_args, "--", source, dest]


def check_args(args) -> None:
    for attr, flag in JOBS_ONLY.items():
        value = getattr(args, attr, None)
        if value not in (None, False):
            raise Fail("usage", f"{flag} is for a copy on HF Jobs, not with --local", Exit.USAGE, f"drop {flag}")


def check_docker(docker: str | None, run=subprocess.run) -> str:
    if not docker:
        raise Fail(
            "local",
            "--local needs Docker, and `docker` is not on PATH",
            Exit.LOCAL,
            "install Docker (https://docs.docker.com/get-docker/), or drop --local to copy on HF Jobs",
        )
    try:
        res = run([docker, "info"], capture_output=True, text=True, timeout=30)
        ok = res.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        ok = False
    if not ok:
        raise Fail(
            "local",
            "cannot reach the Docker daemon",
            Exit.LOCAL,
            "start Docker, or check that your user can run `docker info`",
        )
    return docker


def is_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in ("1", "true", "yes", "on")


def run(
    args,
    passed: list[str],
    *,
    api=None,
    env: dict[str, str] | None = None,
    which=shutil.which,
    run_cmd=subprocess.run,
    execve=os.execve,
    machine=platform.machine,
    out=None,
    err=None,
    tty: bool | None = None,
) -> int:
    """Run the copy in local Docker: replaces this process (or prints the
    command, with --print-command). As on HF Jobs, a new bucket is created
    private first; --print-command changes nothing."""
    from huggingface_hub import HfApi, get_token

    env = dict(os.environ if env is None else env)
    out, err = out or sys.stdout, err or sys.stderr
    check_args(args)
    if not args.source.startswith("s3://"):
        raise Fail("usage", "the source must be an S3 URL", Exit.USAGE, "write it as s3://bucket/prefix/")
    hub.split_dest(args.dest, "-")
    docker = which("docker") or "docker"
    if not args.print_command:
        check_docker(which("docker"), run_cmd)

    token = env.get("HF_TOKEN") or get_token()
    if not token:
        raise Fail("auth", "no Hugging Face token", Exit.AUTH, "run `hf auth login`, or set HF_TOKEN")
    env["HF_TOKEN"] = token
    api = api or HfApi(token=token)
    try:
        user = api.whoami()["name"]
    except Exception as e:
        raise Fail("auth", f"could not read your Hugging Face account: {e}", Exit.AUTH, "run `hf auth login`") from e
    dest = hub.full_dest(args.dest, user)
    anonymous = args.no_sign_request or is_truthy(env.get("AWS_NO_SIGN_REQUEST"))
    aws, expires = hub.aws_secrets(anonymous, env=env)
    env.update(aws)

    def note(text: str) -> None:
        print(f"hf s3ream: {text}", file=err)

    if anonymous and (unused := hub.ignored_credentials(env)):
        note(hub.ignored_credentials_message(unused))

    if expires:
        note(f"the AWS credentials expire at {expires}: the copy must end before")
    arch = machine().lower()
    if arch not in ("x86_64", "amd64"):
        note(
            f"the image is built for {PLATFORM} only: on this {arch} machine Docker runs it "
            "under emulation (slow), if it can run it at all"
        )

    binary_args = list(passed)
    if args.analyze:
        binary_args.append("--dry-run")
    if anonymous:
        binary_args.append("--no-sign-request")
    if args.skip_existing:
        binary_args.append("--skip-existing")
    if tty is None:
        tty = sys.stdin.isatty() and out.isatty()
    cmd = docker_command(
        docker, args.image, args.source, dest, binary_args, forwarded(env, anonymous), args.docker_arg or [], tty
    )
    if args.print_command:
        print(shlex.join(cmd), file=out)
        return int(Exit.OK)
    bucket = "/".join(hub.split_dest(dest, user))
    created, private = hub.ensure_bucket(api, bucket)
    if created:
        note(f"bucket {bucket} created, private")
    elif private is False:
        note(f"bucket {bucket} is public: anyone can read the copied files")
    note(f"{'dry-run' if args.analyze else 'copying'} in local Docker ({args.image})")
    out.flush()
    err.flush()
    execve(docker, cmd, env)
    return int(Exit.FAILED)  # not reached
