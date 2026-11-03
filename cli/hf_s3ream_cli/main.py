"""`hf s3ream SRC DEST`: the command line."""

from __future__ import annotations

import argparse
import os
import shutil
import sys

from . import IMAGE
from .remote import CopyError, Options, RemoteCopy, aws_secrets

DESCRIPTION = """\
Copy an S3 prefix into a Hugging Face Bucket.

By default the copy runs on HF Jobs, billed to your account: a short dry-run
sizes the source and proposes a plan with its cost, then one planner Job
starts the copier Jobs. Your machine only shows the progress. Ctrl-C stops
the copy.

--local runs the copy in Docker on this machine instead: for a source that
HF Jobs cannot reach (a bucket open only to your VPC).
"""


def parse_args(argv: list[str]):
    p = argparse.ArgumentParser(
        prog="hf s3ream", description=DESCRIPTION, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("source", help="s3://bucket/prefix/")
    p.add_argument("dest", help="hf://buckets/org/name[/path], or org/name")
    p.add_argument("--no-sign-request", action="store_true", help="read a public source anonymously")
    p.add_argument("--skip-existing", action="store_true", help="skip files already at the destination")
    p.add_argument("-y", "--yes", action="store_true", help="start the copy without asking")
    p.add_argument("--detach", action="store_true", help="start the copy and return; the Jobs run on")
    p.add_argument("--analyze", action="store_true", help="only show the plan and its cost")
    p.add_argument("--namespace", help="run the Jobs under this user or org (default: you)")
    p.add_argument("--copier-flavor", help="override the plan: copier hardware")
    p.add_argument("--range-gib", type=int, help="override the plan: GiB per copier")
    p.add_argument("--max-inflight", type=int, help="override the plan: copiers at once")
    p.add_argument("--timeout", type=int, metavar="SECONDS", help="override the plan: planner timeout")
    p.add_argument("--local", action="store_true", help="run in local Docker, not on HF Jobs")
    p.add_argument("--image", default=IMAGE, help=argparse.SUPPRESS)
    # Other binary options (--exclude GLOB, …) go to the binary as they are.
    return p.parse_known_args(argv)


def ask(question: str) -> bool:
    if not sys.stdin.isatty():
        raise CopyError("not a terminal: pass --yes to start the copy")
    return input(f"{question} [y/N] ").strip().lower() in ("y", "yes")


def run_local(args, extra: list[str]) -> int:
    """The copy in local Docker. Replaces this process: exit code and Ctrl-C
    are Docker's."""
    from huggingface_hub import get_token

    docker = shutil.which("docker")
    if not docker:
        raise CopyError("--local needs Docker")
    token = get_token()
    if not token:
        raise CopyError("no Hugging Face token: run `hf auth login`, or set HF_TOKEN")
    env = {"HF_TOKEN": token, **aws_secrets(args.no_sign_request)}
    os.environ.update(env)
    cmd = [docker, "run", "--rm", "--init", "--pull", "missing"]
    if sys.stdin.isatty():
        cmd.append("-t")
    for name in env:  # by name: no secret value on the command line
        cmd += ["-e", name]
    cmd += [args.image, args.source, args.dest, *extra]
    if args.no_sign_request:
        cmd.append("--no-sign-request")
    if args.skip_existing:
        cmd.append("--skip-existing")
    os.execv(docker, cmd)
    return 1  # not reached


def main(argv: list[str] | None = None) -> int:
    args, extra = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        if args.local:
            return run_local(args, extra)
        from huggingface_hub import HfApi

        opts = Options(
            source=args.source,
            dest=args.dest,
            anonymous=args.no_sign_request,
            skip_existing=args.skip_existing,
            yes=args.yes,
            detach=args.detach,
            analyze_only=args.analyze,
            namespace=args.namespace,
            image=args.image,
            copier_flavor=args.copier_flavor,
            range_gib=args.range_gib,
            max_inflight=args.max_inflight,
            timeout_s=args.timeout,
            extra=tuple(extra),
        )
        return RemoteCopy(HfApi(), opts, ask=ask).run()
    except CopyError as e:
        print(f"hf s3ream: {e}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        # Before the planner exists: nothing runs that needs stopping, except
        # a dry-run Job, which ends by itself within 2 minutes.
        print("\nInterrupted.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
