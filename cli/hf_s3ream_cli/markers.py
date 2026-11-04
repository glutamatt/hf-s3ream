"""The marker lines the binary prints for its clients: `NAME {json}`.

The web Space reads the same lines (space/app.js). Any other log line is for
people and passes through untouched.
"""

from __future__ import annotations

import json
import re
from typing import Any

KNOWN = {
    # dry-run
    "LISTING",
    "DRYRUN_STATS",
    "DRYRUN_PLAN",
    "DRYRUN_BUCKET",
    # planner
    "PLANNING",
    "RANGE",
    "COPIER",
    "PLAN_PROGRESS",
    "PLAN_DONE",
    "VERIFYING",
    "VERIFY",
    "PLAN_STOPPED",
    "PLAN_RESULT",
}


class Failure:
    """Why the binary failed, from its last lines: anyhow prints `Error: ...`,
    then `Caused by:` and numbered causes, the root cause last. An AWS error
    shows as `Error { code: "...", message: "...", ... }`."""

    CAUSE = re.compile(r"^\s+\d+: (.*)$")
    AWS = re.compile(r'code: "([^"]+)", message: "([^"]*)"')
    REGION_FALLBACK = "could not auto-detect region"  # the warn! of resolve_region (src/sync.rs)

    def __init__(self) -> None:
        self.lines: list[str] | None = None
        self.region_fallback = False

    def feed(self, line: str) -> None:
        if line.startswith("Error: "):
            self.lines = [line[len("Error: ") :].strip()]
        elif self.lines is not None and (m := self.CAUSE.match(line)):
            self.lines.append(m.group(1).strip())
        elif self.REGION_FALLBACK in line:
            self.region_fallback = True

    def cause(self) -> str | None:
        """The root cause, short: `NoSuchBucket: The specified bucket does not exist`."""
        if not self.lines:
            return None
        root = self.lines[-1]
        m = self.AWS.search(root)
        return f"{m.group(1)}: {m.group(2)}" if m else root


def parse(line: str) -> tuple[str, Any] | None:
    """(name, payload) for a marker line, None for any other line.

    DRYRUN_BUCKET carries a word (`ok` / `error`), the others a JSON object.
    A marker with a broken payload is not a marker.
    """
    name, _, rest = line.partition(" ")
    if name not in KNOWN or not rest:
        return None
    if name == "DRYRUN_BUCKET":
        return name, rest.strip()
    try:
        return name, json.loads(rest)
    except ValueError:
        return None
