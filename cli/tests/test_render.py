"""What people see: the live block and the event lines, at a fixed width."""

from __future__ import annotations

import io

from rich.console import Console

from hf_s3ream_cli.render import TuiUI, approx, fmt_bytes, fmt_dur, fmt_rate

PROGRESS = {
    "run": "s3ream-1",
    "state": "copying",
    "ranges": 256,
    "done": 124,
    "active": 128,
    "failed": 0,
    "canceled": 0,
    "listing_done": True,
    "bytes_planned": 107.3 * 2**40,
    "bytes_done_total": 51.2 * 2**40,
    "pct": 47.7,
    "mibps": 43110.0,
    "files_per_s": 1204.0,
    "elapsed_s": 720,
    "eta_s": 780,
    "cost_usd": 31.2,
}


def screen(view=None, events=(), keys=True, width=80) -> str:
    console = Console(file=io.StringIO(), width=width, color_system=None, record=True, force_terminal=False)
    ui = TuiUI(console=console, keys_active=keys)
    for ev in events:
        ui.emit(ev)
    if view:
        ui.view = view
        console.print(ui.render_view())
    text = console.export_text()
    for line in text.splitlines():
        # Commands are printed whole (soft wrap): only they may be longer.
        assert len(line) <= width or "hf s3ream" in line, line
    return text


def test_formats():
    assert fmt_bytes(28004946) == "26.7 MiB"
    assert fmt_rate(980) == "980 MiB/s" and fmt_rate(43110) == "42.1 GiB/s"
    assert (fmt_dur(45), fmt_dur(780), fmt_dur(7200)) == ("45 s", "13 min", "2.0 h")
    assert (approx(4.1), approx(0.0017)) == ("≈ $4.10", "< $0.01")  # seen live: "≈ < $0.01"


def test_the_copy_block():
    out = screen({"kind": "copy", "control": "copying", "progress": PROGRESS, "state": "copying"})
    assert "Copying" in out and "48 %" in out and "51.2 TiB of 107.3 TiB" in out
    assert "42.1 GiB/s" in out and "1,204 files/s committed" in out and "ETA 13 min" in out
    assert "124/256 ranges done · 128 running" in out and "≈ $31 so far" in out
    assert "Ctrl-C stop · d detach (the copy keeps running)" in out


def test_without_keys_the_footer_says_ctrl_c_stops():
    out = screen({"kind": "copy", "control": "copying", "progress": PROGRESS}, keys=False)
    assert "Ctrl-C stops the copy" in out and "detach" not in out


def test_the_confirmation():
    out = screen({"kind": "copy", "control": "confirming", "left": 4.0, "confirm_s": 5, "progress": PROGRESS})
    assert "▲ Stop the copy? 128 running copiers will be canceled." in out
    assert "Files already committed stay in the bucket." in out
    assert "Ctrl-C again: stop · d: detach · any other key: keep copying" in out


def test_the_stop():
    out = screen(
        {"kind": "copy", "control": "stopping", "state": "stopping", "progress": PROGRESS,
         "stopping": {"planner_done": True, "left": 12}}
    )  # fmt: skip
    assert "Stopping" in out and "✓ planner canceled · 12 Jobs still running" in out


def test_the_event_lines():
    out = screen(
        events=[
            {"type": "start", "source": "s3://cc/crawl/", "dest": "huggingface/cc"},
            {"type": "account", "user": "alice", "namespace": "alice"},
            {"type": "bucket", "bucket": "huggingface/cc", "private": False, "created": False},
            {"type": "source", "files": 500009, "bytes": 107.3 * 2**40, "region": "us-east-1"},
            {"type": "plan", "copiers": 256, "copier_flavor": "cpu-performance", "range_gib": 430,
             "max_inflight": 128, "wall_s": 1500, "usd": 63.9, "commit_bound": False},
            {"type": "result", "state": "stopped", "completed": 124, "ranges": 256, "files": 250000,
             "bytes": 51.2 * 2**40, "elapsed_s": 720, "cost_usd": 31.2,
             "resume": "hf s3ream s3://cc/crawl/ huggingface/cc --skip-existing"},
            {"type": "error", "message": "nothing started", "hint": "add --yes"},
        ]
    )  # fmt: skip
    for want in [
        "hf s3ream  s3://cc/crawl/ → huggingface/cc",
        "✓ Signed in as alice · Jobs billed to alice",
        "public (anyone can read the copied files)",
        "500,009 files · 107.3 TiB",
        "Plan   256 copiers × cpu-performance · 430 GiB each · 128 at once",
        "Time   ≈ 25 min · limited by bandwidth",
        "Cost   ≈ $64 · billed per started minute",
        "■ Stopped · 124/256 ranges done",
        "Files already committed stay in the bucket.",
        "Resume:   hf s3ream s3://cc/crawl/ huggingface/cc --skip-existing",
        "✗ nothing started",
        "→ add --yes",
    ]:
        assert want in out, f"{want!r} not in:\n{out}"


def test_small_files_lead_with_files_per_second():
    """Seen live: a copy of 141,310 files of ~860 KB ran at ~1,000 files/s."""
    small = {**PROGRESS, "files_planned": 141310, "bytes_planned": 113.6 * 2**30, "files_per_s": 1000.0,
             "mibps": 899.0}  # fmt: skip
    out = screen({"kind": "copy", "control": "copying", "progress": small})
    assert "1,000 files/s committed · 899 MiB/s · limited by commits" in out


def test_the_spinner_moves():
    """Seen live: the analysis spinner stood still (a new one on each redraw)."""
    import time

    ui = TuiUI(console=Console(file=io.StringIO(), width=80, color_system=None), keys_active=True)
    ui.view = {"kind": "analyze", "deadline": time.monotonic() + 100}
    first = ui.render_view()
    ui.view = {"kind": "analyze", "deadline": time.monotonic() + 100, "listed": 5, "bytes": 10}
    second = ui.render_view()
    assert first is second  # one spinner: its frames advance with time
    frames = {str(second.render(t)) for t in (0.0, 0.1, 0.2, 0.3)}
    assert len(frames) > 1
