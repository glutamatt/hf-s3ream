"""The options of the `hf-s3ream` binary (src/main.rs) that the CLI passes on.

Every option of the binary is in exactly one of the tables below:
cli/tests/test_binary_options.py compares them with the binary's `--help`
(CI builds the binary and runs it), so a new option of the binary cannot be
forgotten here.

- PASSED: given to the binary as it is. On HF Jobs it goes to the dry-run and
  to the planner, so only the options the planner honors (or forwards to its
  copiers) are allowed there; the others only make sense with `--local`.
- SET_BY_CLI: the CLI sets these itself, from its own option of the same
  name, or from the one named (then the binary's spelling is refused, with
  a pointer to the CLI's).
- REFUSED: never on the command line of `hf s3ream`, with the reason.
"""

from __future__ import annotations

from dataclasses import dataclass

from .events import Exit, Fail


@dataclass(frozen=True)
class Passed:
    value: str | None  # the value's name in the help, None for a flag
    remote: bool  # honored on HF Jobs (by the planner or its copiers)
    help: str


PASSED: dict[str, Passed] = {
    "--exclude": Passed("GLOB", True, "skip keys matching GLOB (matched on the full key; repeatable)"),
    "--aws-region": Passed("REGION", True, "region of the source bucket (default: auto-detected)"),
    "--no-verify": Passed(None, True, "skip the destination check after the copy"),
    "--limit-gib": Passed("N", True, "stop after N GiB (benchmarks)"),
    "--commit-chunk": Passed("N", True, "files per bucket commit"),
    "--commit-gib": Passed("N", True, "also commit after N GiB per session"),
    "--s3-part-size-mib": Passed("N", True, "size of each ranged S3 GET"),
    "--s3-part-concurrency": Passed("N", True, "parallel ranged GETs per file"),
    "--xor-byte": Passed("N", True, "XOR the data to defeat dedup (benchmarks only)"),
    "--no-live-progress": Passed(None, True, "the planner does not follow its copiers' logs"),
    "--parallel-files": Passed("N", False, "concurrent file uploads"),
    "--start-after": Passed("KEY", False, "only copy keys after KEY"),
    "--stop-at": Passed("KEY", False, "only copy keys up to KEY (inclusive)"),
    "--hub-endpoint": Passed("URL", False, "HF Hub endpoint (default: $HF_ENDPOINT)"),
}

# Binary option -> the CLI option that sets it: the same name, or a hint.
SET_BY_CLI: dict[str, str] = {
    "--no-sign-request": "--no-sign-request",
    "--skip-existing": "--skip-existing",
    "--range-gib": "--range-gib",
    "--max-inflight": "--max-inflight",
    "--copier-flavor": "--copier-flavor",
    "--help": "--help",
    "--version": "--version",
    "--dry-run": "use --analyze",
    "--dry-run-max-secs": "use --analyze: the dry-run stops listing after 2 minutes by itself",
    "--plan": "drop it: `hf s3ream SRC DEST` runs the planner",
    "--copier-image": "use --image",
    "--jobs-namespace": "use --namespace",
    "--run-label": "use --label",
    "--launch-stagger-ms": "drop it: the plan sets it",
    # The estimate counts the planner's default key cap (src/reco.rs).
    "--range-keys": "drop it: the plan sets it, and the cost estimate counts it",
}

REFUSED: dict[str, str] = {
    "--hf-token": "a token on the command line is visible to every user of this machine: "
    "use `hf auth login` or the HF_TOKEN variable",
}


def split(argv: list[str]) -> tuple[list[str], list[str]]:
    """(the arguments for the CLI's own parser, the options for the binary).

    Takes `--opt VALUE` and `--opt=VALUE` alike, wherever they are, so a
    value is never read as SRC or DEST. A binary option the CLI sets from
    another option of its own is refused, with a pointer to that option.
    """
    own: list[str] = []
    passed: list[str] = []
    i = 0
    while i < len(argv):
        arg = argv[i]
        name, eq, value = arg.partition("=")
        if name in REFUSED:
            raise Fail("usage", f"{name} is not accepted", Exit.USAGE, REFUSED[name])
        if SET_BY_CLI.get(name, name) != name:
            raise Fail("usage", f"{name} is set by hf s3ream itself", Exit.USAGE, SET_BY_CLI[name])
        opt = PASSED.get(name)
        if opt is None:
            own.append(arg)
            i += 1
            continue
        if opt.value is None:
            if eq:
                raise Fail("usage", f"{name} takes no value", Exit.USAGE, f"write {name} alone")
            passed.append(name)
            i += 1
            continue
        if not eq:
            if i + 1 >= len(argv):
                raise Fail("usage", f"{name} needs a value", Exit.USAGE, f"write {name} {opt.value}")
            value = argv[i + 1]
            i += 1
        passed += [name, value]
        i += 1
    return own, passed


def local_only(passed: list[str]) -> list[str]:
    """The options in `passed` that HF Jobs would not honor."""
    return [a for a in passed if a in PASSED and not PASSED[a].remote]
