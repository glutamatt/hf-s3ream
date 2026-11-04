"""Two ways to show the same events: JSON lines for agents and scripts, a
live terminal view for people.

The flow (flow.py) only emits events and says what the live view shows;
it never prints. So both outputs always say the same thing.
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone

from .events import SCHEMA

HF_YELLOW = "#FFD21E"


# ---------- formatting ----------


def fmt_bytes(n: float | None) -> str:
    if n is None:
        return "?"
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if abs(n) < 1024 or unit == "PiB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PiB"


def fmt_rate(mibps: float) -> str:
    """MiB/s, or GiB/s from 1000 MiB/s (the Space shows the same)."""
    return f"{mibps / 1024:.1f} GiB/s" if mibps >= 1000 else f"{mibps:.0f} MiB/s"


def fmt_dur(s: float | None) -> str:
    if s is None:
        return "?"
    s = round(s)
    if s < 90:
        return f"{s} s"
    if s < 5400:
        return f"{round(s / 60)} min"
    return f"{s / 3600:.1f} h"


def fmt_usd(usd: float | None) -> str:
    if usd is None:
        return "?"
    if 0 < usd < 0.005:
        return "< $0.01"
    return f"${usd:.2f}" if usd < 20 else f"${usd:.0f}"


def approx(usd: float | None) -> str:
    """'≈ $4.10', or '< $0.01' (a bound needs no '≈')."""
    text = fmt_usd(usd)
    return text if text.startswith("<") else f"≈ {text}"


def fmt_clock(epoch_s: float) -> str:
    return datetime.fromtimestamp(epoch_s).strftime("%H:%M")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------- JSON lines ----------


class JsonUI:
    """One event per line on stdout. No live view, no questions."""

    interactive = False

    def __init__(self, out=None):
        self.out = out or sys.stdout

    def emit(self, ev: dict) -> None:
        self.out.write(json.dumps({"schema": SCHEMA, "type": ev["type"], "time": now_iso(), **ev}) + "\n")
        self.out.flush()

    def live(self, view: dict | None) -> None:
        pass

    def close(self) -> None:
        pass


# ---------- terminal ----------


class TuiUI:
    """Event lines that stay in the scrollback, and one live block at the
    bottom (the analysis, the copy, the stop), redrawn in place."""

    interactive = True

    def __init__(self, console=None, keys_active: bool = True):
        from rich.console import Console

        self.console = console or Console(highlight=False)
        self.keys_active = keys_active
        self.view: dict | None = None
        self._live = None
        self._spinners: dict = {}

    # -- the live block
    def live(self, view: dict | None) -> None:
        self.view = view
        if view is None:
            if self._live:
                self._live.stop()
                self._live = None
            return
        if self._live is None:
            from rich.live import Live

            self._live = Live(
                get_renderable=self.render_view,
                console=self.console,
                refresh_per_second=8,
                transient=True,
                redirect_stdout=False,
                redirect_stderr=False,
            )
            self._live.start()
        else:
            self._live.refresh()

    def close(self) -> None:
        self.live(None)

    def render_view(self):
        from rich.text import Text

        v = self.view or {}
        kind = v.get("kind")
        if kind == "analyze":
            return self._analyze(v)
        if kind == "copy":
            return self._copy(v)
        if kind == "sweep":
            return self._spinner(v["text"])
        return Text("")

    def _spinner(self, text: str, style: str = HF_YELLOW):
        """The spinner for this style, with a new text. One object for the
        whole life of the view: a spinner draws its frame from the time since
        it was made, so a new one on each redraw would never move."""
        from rich.spinner import Spinner

        sp = self._spinners.get(style)
        if sp is None:
            sp = self._spinners[style] = Spinner("dots", style=style)
        sp.update(text=text)
        return sp

    def _analyze(self, v: dict):
        if v.get("deadline") is None:
            return self._spinner("Starting a dry-run Job on HF Jobs…")
        listed = v.get("listed")
        so_far = f" · {listed:,} objects, {fmt_bytes(v.get('bytes'))} so far" if listed else ""
        left = max(0, int(v["deadline"] - time.monotonic()))
        return self._spinner(f"Reading the source from HF Jobs{so_far}  [dim]max {left // 60}:{left % 60:02d} left[/]")

    def _copy(self, v: dict):
        from rich.console import Group
        from rich.progress_bar import ProgressBar
        from rich.table import Table
        from rich.text import Text

        p = v.get("progress") or {}
        more = "" if p.get("listing_done") else "+"
        state = v.get("state", "copying")
        head = Table.grid(padding=(0, 1))
        head.add_column(no_wrap=True)
        head.add_column(no_wrap=True)
        head.add_column(no_wrap=True)
        title = {"copying": "Copying", "verifying": "Verifying the destination", "stopping": "Stopping"}.get(
            state, "Copying"
        )
        dot = "[bold red]■[/]" if state == "stopping" else f"[bold {HF_YELLOW}]●[/]"
        pct = p.get("pct") or 0.0
        size = f"{fmt_bytes(p.get('bytes_done_total'))} of {fmt_bytes(p.get('bytes_planned'))}{more}"
        bar = ProgressBar(total=100, completed=pct, width=28, complete_style=HF_YELLOW, finished_style="green")
        head.add_row(Text.from_markup(f"{dot} [bold]{title}[/]"), bar, Text(f"{pct:4.0f} %  {size}"))

        lines = [head]
        if p:
            rate = fmt_rate(p.get("mibps") or 0)
            fps = f"{p.get('files_per_s') or 0:,.0f} files/s committed"
            eta = p.get("eta_s")
            eta_txt = f"ETA {fmt_dur(eta)} ({fmt_clock(time.time() + eta)})" if eta else "ETA –"
            # Small files: the files committed per second set the pace (the
            # destination takes ~1,000/s per bucket), so they come first.
            planned_f, planned_b = p.get("files_planned") or 0, p.get("bytes_planned") or 0
            if planned_f and planned_b / planned_f <= 4 * 2**20:
                lead = f"[bold]{fps}[/] · {rate} · limited by commits"
            else:
                lead = f"[bold]{rate}[/] · {fps}"
            lines.append(Text.from_markup(f"  {lead} · {eta_txt}"))
            failed = f" · [red]{p['failed']} failed[/]" if p.get("failed") else ""
            cost = f" · {approx(p.get('cost_usd'))} so far" if p.get("cost_usd") is not None else ""
            lines.append(
                Text.from_markup(
                    f"  [dim]{p.get('done', 0)}/{p.get('ranges', 0)}{more} ranges done · {p.get('active', 0)} running"
                    f"{failed} · {fmt_dur(p.get('elapsed_s'))}{cost}[/]"
                )
            )
        else:
            lines.append(Text.from_markup("  [dim]waiting for the planner…[/]"))
        lines.append(Text(""))
        lines.append(self._footer(v))
        return Group(*lines)

    def _footer(self, v: dict):
        from rich.console import Group
        from rich.progress_bar import ProgressBar
        from rich.table import Table
        from rich.text import Text

        control = v.get("control", "copying")
        if control == "confirming":
            p = v.get("progress") or {}
            n = p.get("active")
            who = f"{n} running copier{'s' if n != 1 else ''}" if n else "The running copiers"
            left = v.get("left", 0.0)
            row = Table.grid(padding=(0, 1))
            row.add_column(no_wrap=True)
            row.add_column(no_wrap=True)
            row.add_row(
                Text.from_markup("  [bold]Ctrl-C[/] again: stop · [bold]d[/]: detach · any other key: keep copying"),
                ProgressBar(total=v.get("confirm_s", 5.0), completed=left, width=8, complete_style=HF_YELLOW),
            )
            return Group(
                Text.from_markup(f"[bold {HF_YELLOW}]▲ Stop the copy?[/] {who} will be canceled."),
                Text.from_markup("  [dim]Files already committed stay in the bucket.[/]"),
                row,
            )
        if control == "stopping":
            s = v.get("stopping") or {}
            planner = "[green]✓[/] planner canceled" if s.get("planner_done") else "canceling the planner…"
            left = s.get("left")
            jobs = f" · {left} Job{'s' if left != 1 else ''} still running" if left else ""
            return self._spinner(f"{planner}{jobs}  [dim](Ctrl-C: leave now, the stop goes on)[/]", style="red")
        if self.keys_active:
            return Text.from_markup("[dim]  [bold]Ctrl-C[/] stop · [bold]d[/] detach (the copy keeps running)[/]")
        return Text.from_markup("[dim]  Ctrl-C stops the copy[/]")

    # -- questions
    def ask_line(self, question: str) -> None:
        self.console.print(f"\n  {question} [bold]\\[y/N][/] ", end="")

    # -- event lines
    def emit(self, ev: dict) -> None:
        handler = getattr(self, f"_ev_{ev['type']}", None)
        if handler:
            handler(ev)

    def _p(self, text: str = "") -> None:
        self.console.print(text)

    def _ev_start(self, ev):
        if ev.get("source"):
            self._p(f"[bold]hf s3ream[/]  {ev['source']} [dim]→[/] {ev['dest']}\n")

    def _ev_account(self, ev):
        self._p(f"  [green]✓[/] Signed in as [bold]{ev['user']}[/] · Jobs billed to [bold]{ev['namespace']}[/]")

    def _ev_bucket(self, ev):
        if ev["created"]:
            what = "created, private"
        elif ev["private"] is False:
            what = "exists, [bold yellow]public[/] (anyone can read the copied files)"
        else:
            what = "exists, private" if ev["private"] else "exists"
        self._p(f"  [green]✓[/] Bucket [bold]{ev['bucket']}[/] {what}")

    def _ev_source(self, ev):
        if ev.get("partial"):
            mins = (ev.get("listed_in_s") or 120) // 60
            self._p(
                f"  [green]✓[/] Source read from HF Jobs · {ev['region']} · "
                f"[bold]{ev['files']:,}+[/] files · [bold]{fmt_bytes(ev['bytes'])}+[/] "
                f"[dim](listing stopped after {mins} min: the planner lists the rest)[/]"
            )
            return
        self._p(
            f"  [green]✓[/] Source read from HF Jobs · {ev['region']} · "
            f"[bold]{ev['files']:,}[/] files · [bold]{fmt_bytes(ev['bytes'])}[/]"
        )

    def _ev_plan(self, ev):
        def n_copiers(n, more=""):
            return f"{n:,}{more} copier{'s' if n != 1 or more else ''}" if n else "copiers"

        why = "limited by commits (~1,000 files/s per bucket)" if ev["commit_bound"] else "limited by bandwidth"
        by_hand = "unknown (plan set by hand)"
        if ev.get("partial") and ev.get("min_usd") is not None:
            # A source too big to list: the figures are for the part listed.
            copiers = n_copiers(ev.get("min_copiers"), "+")
            time_txt = f"more than {fmt_dur(ev.get('min_wall_s'))} · {why}"
            cost = (
                f"more than [bold]{fmt_usd(ev['min_usd'])}[/] · the part listed alone; "
                "the rest of the source adds to it"
            )
        else:
            copiers = n_copiers(ev["copiers"])
            time_txt = f"≈ {fmt_dur(ev['wall_s'])} · {why}" if ev["wall_s"] is not None else by_hand
            cost = f"[bold]{approx(ev['usd'])}[/] · billed per started minute" if ev["usd"] is not None else by_hand
        self._p(
            f"\n  [dim]Plan[/]   {copiers} × [bold]{ev['copier_flavor']}[/] · {ev['range_gib']} GiB each · "
            f"{ev['max_inflight']} at once\n"
            f"  [dim]Time[/]   {time_txt}\n"
            f"  [dim]Cost[/]   {cost}"
        )
        if ev.get("partial"):
            hours = round(ev["timeout_s"] / 3600)
            self._p(
                f"  [dim]The planner stops the copy after {hours} h: pass --timeout SECONDS if it may take longer.[/]"
            )

    def _cmd(self, label: str, command: str) -> None:
        """A command to copy: on its own line, never broken by a newline
        (the terminal wraps it on screen, a copy-paste gets it whole)."""
        self.console.print(f"    [dim]{label:<9}[/] [bold]{command}[/]", soft_wrap=True)

    def _ev_started(self, ev):
        self._p(f"\n  [bold {HF_YELLOW}]●[/] Copy started · run [bold]{ev['run']}[/]")
        self.console.print(f"    [link={ev['url']}]{ev['url']}[/]", soft_wrap=True)
        self._cmd("Attach:", ev["attach"])
        self._cmd("Stop:", ev["stop"])
        self._p()

    def _ev_warning(self, ev):
        self._p(f"  [yellow]▲[/] {ev['message']}")

    def _ev_detached(self, ev):
        self._p("\n  [bold]↗ Detached.[/] The copy keeps running on HF Jobs.")
        self._cmd("Follow:", ev["attach"])
        self._cmd("Stop:", ev["stop"])

    def _ev_result(self, ev):
        took = f"in {fmt_dur(ev['elapsed_s'])}" if ev.get("elapsed_s") is not None else ""
        cost = (
            f"\n    Cost {approx(ev['cost_usd'])} [dim](from the Jobs' run times; "
            "exact figure: hf.co/settings/billing)[/]"
            if ev.get("cost_usd") is not None
            else ""
        )
        figures = f"{ev['files']:,} files · {fmt_bytes(ev['bytes'])} {took}"
        state = ev["state"]
        if state == "completed":
            check = ", verified" if ev.get("verified") else ""
            self._p(f"  [bold green]✓ Copied{check}[/] · {figures}{cost}")
        elif state == "stopped":
            self._p(
                f"  [bold red]■ Stopped[/] · {ev['completed']}/{ev['ranges']} ranges done · {figures}"
                f"\n    Files already committed stay in the bucket.{cost}"
            )
        else:
            why = []
            if ev.get("failed"):
                why.append(f"{ev['failed']} of {ev['ranges']} ranges failed")
            if ev.get("canceled"):
                why.append(f"{ev['canceled']} canceled")
            if ev.get("verified") is False:
                why.append("the destination check failed")
            self._p(f"  [bold red]✗ {', '.join(why) or 'The copy failed'}[/] · {figures}{cost}")
        if ev.get("resume"):
            self._cmd("Resume:", ev["resume"])
        if ev.get("bucket_url"):
            self.console.print(
                f"    [dim]{'Bucket:':<9}[/] [link={ev['bucket_url']}]{ev['bucket_url']}[/]", soft_wrap=True
            )

    def _ev_status(self, ev):
        p = ev.get("progress") or {}
        self._p(f"[bold]{ev['run']}[/] · {ev['state']} · planner {ev['planner_job']} ({ev['stage']})")
        if p:
            self._p(
                f"  {p.get('pct', 0):.0f} % · {fmt_bytes(p.get('bytes_done_total'))} of "
                f"{fmt_bytes(p.get('bytes_planned'))} · {p.get('done', 0)}/{p.get('ranges', 0)} ranges done · "
                f"{p.get('active', 0)} running · {fmt_rate(p.get('mibps') or 0)}"
            )
        if ev.get("cost_usd") is not None:
            self._p(f"  Cost {approx(ev['cost_usd'])} so far")
        if ev.get("result"):
            self._ev_result(ev["result"])

    def _ev_stopped(self, ev):
        if ev["already_ended"]:
            self._p(f"  [green]✓[/] {ev['run']}: nothing to stop, every Job of this copy has ended.")
        elif ev["left"]:
            self._p(f"  [red]✗[/] {ev['run']}: {ev['left']} Job(s) did not stop: see https://huggingface.co/jobs")
        else:
            self._p(f"  [bold red]■[/] Stopped {ev['run']}: no Job of this copy is running.")

    def _ev_error(self, ev):
        self._p(f"  [bold red]✗[/] {ev['message']}")
        if ev.get("hint"):
            self._p(f"    [dim]→ {ev['hint']}[/]")
