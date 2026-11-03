"""`hf s3ream`: the command line."""

from __future__ import annotations

import argparse
import contextlib
import os
import queue
import shutil
import sys

from . import IMAGE, hub
from .control import Keys
from .events import Exit, Fail
from .flow import Options, Session

DESCRIPTION = """\
Copy an S3 prefix into a Hugging Face Bucket.

The copy runs on HF Jobs, billed to your account. A short dry-run Job reads
the source and proposes a plan with its cost; once you confirm, one planner
Job starts the copier Jobs. Your machine only shows the progress: closing
the terminal does not stop the copy.

In a terminal: Ctrl-C asks before it stops the copy (Ctrl-C again within
5 s to stop), and `d` detaches (the copy keeps running).

--local runs the copy in Docker on this machine instead, for a source that
HF Jobs cannot reach (a bucket open only to your VPC).
"""

AGENTS = """\
For agents and scripts:
  Output is JSON lines (one event per line) with --json, or when stdout is
  not a terminal. Nothing ever waits for an answer: without a terminal, a
  copy only starts with --yes.

  hf s3ream SRC DEST --analyze --json                  plan and cost, starts nothing
  hf s3ream SRC DEST --yes --detach --json --max-cost 20 --label my-copy
  hf s3ream status my-copy --json                      where it stands, returns at once
  hf s3ream attach my-copy --json                      follows to the end; exit code = result
  hf s3ream stop my-copy                               stops every Job of the copy

  Killing this process does not stop the copy (the planner runs on its own):
  use `stop`. SIGINT stops the copy.

Exit codes:
  0 done (or started with --detach)    1 copy failed (ranges, check)
  2 bad arguments                      3 token, credits or bucket access
  4 not started (no --yes, declined, or over --max-cost)
  5 source unreadable                  6 --label already running
  7 no such run                        130 stopped
"""


def copy_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="hf s3ream",
        usage="hf s3ream SRC DEST [options]\n       hf s3ream {status,attach,stop} RUN [options]",
        description=DESCRIPTION,
        epilog=AGENTS,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("source", metavar="SRC", help="s3://bucket/prefix/")
    p.add_argument("dest", metavar="DEST", help="hf://buckets/org/name[/path], or org/name")
    p.add_argument("--no-sign-request", action="store_true", help="read a public source anonymously")
    p.add_argument("--skip-existing", action="store_true", help="skip files already at the destination")
    p.add_argument("-y", "--yes", action="store_true", help="start without asking")
    p.add_argument("--detach", action="store_true", help="start the copy and return; it keeps running")
    p.add_argument("--analyze", action="store_true", help="only show the plan and its cost")
    p.add_argument("--max-cost", type=float, metavar="USD", help="do not start if the estimate is higher")
    p.add_argument("--label", metavar="NAME", help="name of the run (for status/attach/stop); refused if running")
    p.add_argument("--json", action="store_true", help="JSON lines on stdout (the default without a terminal)")
    p.add_argument("--namespace", help="run the Jobs under this org (default: you)")
    g = p.add_argument_group("plan set by hand (no cost estimate then)")
    g.add_argument("--copier-flavor", help="copier hardware, e.g. cpu-performance")
    g.add_argument("--range-gib", type=int, help="GiB per copier")
    g.add_argument("--max-inflight", type=int, help="copiers at once")
    g.add_argument("--timeout", type=int, metavar="SECONDS", help="planner timeout")
    p.add_argument("--local", action="store_true", help="run in local Docker, not on HF Jobs")
    p.add_argument("--image", default=IMAGE, help=argparse.SUPPRESS)
    return p


def run_parser(verb: str) -> argparse.ArgumentParser:
    what = {
        "status": "Show where a copy stands, and return at once.",
        "attach": "Follow a running copy until it ends (exit code = its result).",
        "stop": "Stop a copy: cancel its planner and every copier still running.",
    }[verb]
    p = argparse.ArgumentParser(prog=f"hf s3ream {verb}", description=what)
    p.add_argument("run", metavar="RUN", help="the run label (s3ream-...) or the planner Job id")
    p.add_argument("--namespace", help="the org the Jobs run under (default: you)")
    p.add_argument("--json", action="store_true", help="JSON lines on stdout (the default without a terminal)")
    return p


def make_ui(json_out: bool, keys_active: bool):
    if json_out:
        from .render import JsonUI

        return JsonUI()
    from .render import TuiUI

    return TuiUI(keys_active=keys_active)


def run_local(args, extra: list[str]) -> int:
    """The copy in local Docker. Replaces this process: exit code and Ctrl-C
    are Docker's."""
    from huggingface_hub import get_token

    docker = shutil.which("docker")
    if not docker:
        raise Fail("usage", "--local needs Docker", Exit.USAGE, "install Docker, or drop --local to run on HF Jobs")
    token = get_token()
    if not token:
        raise Fail("auth", "no Hugging Face token", Exit.AUTH, "run `hf auth login`, or set HF_TOKEN")
    aws, _ = hub.aws_secrets(args.no_sign_request)
    env = {"HF_TOKEN": token, **aws}
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


def main(argv: list[str] | None = None, api=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    verb = argv[0] if argv and argv[0] in ("status", "attach", "stop") else None
    if verb:
        args, extra = run_parser(verb).parse_args(argv[1:]), []
    else:
        args, extra = copy_parser().parse_known_args(argv)
    json_out = args.json or not sys.stdout.isatty() or os.environ.get("TERM") == "dumb"
    q: queue.Queue = queue.Queue()
    keys = Keys(q) if not json_out else None
    ui = make_ui(json_out, keys_active=False)
    try:
        if not verb and args.local:
            return run_local(args, extra)
        if api is None:
            from huggingface_hub import HfApi

            api = HfApi()
        with keys or contextlib.nullcontext():
            ui = make_ui(json_out, keys_active=bool(keys and keys.active))
            s = Session(api, ui, q, keys=bool(keys and keys.active))
            if verb == "status":
                return s.status(args.run, args.namespace)
            if verb == "stop":
                return s.stop(args.run, args.namespace)
            if verb == "attach":
                return s.attach(args.run, args.namespace)
            return s.copy(
                Options(
                    source=args.source,
                    dest=args.dest,
                    anonymous=args.no_sign_request,
                    skip_existing=args.skip_existing,
                    yes=args.yes,
                    detach=args.detach,
                    analyze_only=args.analyze,
                    namespace=args.namespace,
                    label=args.label,
                    max_cost=args.max_cost,
                    image=args.image,
                    copier_flavor=args.copier_flavor,
                    range_gib=args.range_gib,
                    max_inflight=args.max_inflight,
                    timeout_s=args.timeout,
                    extra=tuple(extra),
                )
            )
    except Fail as e:
        ui.close()
        ui.emit(e.event())
        return int(e.exit_code)
    except KeyboardInterrupt:
        # Only outside the copy loops (which take Ctrl-C themselves).
        ui.close()
        ui.emit(Fail("interrupted", "interrupted", Exit.STOPPED).event())
        return int(Exit.STOPPED)
    except Exception as e:
        ui.close()
        ui.emit(Fail("internal", f"{type(e).__name__}: {e}", Exit.FAILED, "please report it").event())
        if os.environ.get("HF_S3REAM_DEBUG"):
            raise
        return int(Exit.FAILED)
    finally:
        ui.close()


if __name__ == "__main__":
    sys.exit(main())
