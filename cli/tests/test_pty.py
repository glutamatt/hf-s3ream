"""Ctrl-C under the real `hf`, in a pseudo-terminal.

`hf` (huggingface_hub 2.4) runs an extension with subprocess.call: on a
SIGINT it kills the extension 0.25 s later. These tests press Ctrl-C in a
real terminal and check that the CLI asks first, stops on the second Ctrl-C
(the cancel comes from a detached process), and leaves on `d`.

Slow and needs `uv` (it installs hf 2.4.0 and this package in a temp HOME):
run with HF_S3REAM_PTY_TEST=1.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

pexpect = pytest.importorskip("pexpect")

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(
    not os.environ.get("HF_S3REAM_PTY_TEST") or not shutil.which("uv"), reason="set HF_S3REAM_PTY_TEST=1 (needs uv)"
)


@pytest.fixture(scope="module")
def home(tmp_path_factory):
    """A HOME with the extension installed the way `hf` installs a Python
    extension (venv + pip install of the repo), started on the fake Hub."""
    home = tmp_path_factory.mktemp("home")
    ext = home / ".local/share/hf/extensions/hf-s3ream"
    ext.mkdir(parents=True)
    subprocess.run(["uv", "venv", "-q", str(ext / "venv")], check=True)
    py = ext / "venv/bin/python"
    subprocess.run(["uv", "pip", "install", "-q", "--python", str(py), str(ROOT)], check=True)
    entry = ext / "hf-s3ream-pty"
    entry.write_text(f'#!/bin/sh\nexec {py} {ROOT / "cli/tests/pty_entry.py"} "$@"\n')
    entry.chmod(0o755)
    manifest = {
        "owner": "glutamatt", "repo": "hf-s3ream", "repo_id": "glutamatt/hf-s3ream", "short_name": "s3ream",
        "executable_path": str(entry), "type": "python", "installed_at": "2026-11-03T00:00:00+00:00",
        "description": "test", "commit_sha": "test",
    }  # fmt: skip
    (ext / "manifest.json").write_text(json.dumps(manifest))
    return home


def spawn(home: Path, tmp_path: Path, *args: str, finish: bool = False):
    env = {
        **os.environ,
        "HOME": str(home),
        "HF_HUB_DISABLE_UPDATE_CHECK": "1",
        "PTY_TEST_FLAG": str(tmp_path / "canceled"),
        "TERM": "xterm-256color",
    }
    if finish:
        env["PTY_TEST_FINISH"] = "1"
    cmd = ["uv", "tool", "run", "-q", "--from", "huggingface_hub==2.4.0", "hf", "s3ream", *args]
    return pexpect.spawn(cmd[0], cmd[1:], env=env, dimensions=(30, 100), timeout=60, encoding="utf-8")


def test_ctrl_c_asks_then_ctrl_c_stops(home, tmp_path):
    child = spawn(home, tmp_path, "s3://src/", "alice/dst", "--yes")
    child.expect("keeps running")  # the copy is live, the footer is up
    child.sendcontrol("c")
    child.expect("Stop the copy")  # still alive after 0.25 s: hf did not kill it
    child.sendcontrol("c")
    child.expect("Stopped")
    child.expect(pexpect.EOF)
    child.close()
    assert child.exitstatus == 130
    assert (tmp_path / "canceled").exists()  # the detached cancel ran


def test_one_ctrl_c_then_nothing_keeps_copying(home, tmp_path):
    child = spawn(home, tmp_path, "s3://src/", "alice/dst", "--yes", finish=True)
    child.expect("keeps running")
    child.sendcontrol("c")
    child.expect("Stop the copy")
    child.expect("keeps running", timeout=10)  # 5 s later, back to copying
    child.expect("Copied")
    child.expect(pexpect.EOF)
    child.close()
    assert child.exitstatus == 0
    assert not (tmp_path / "canceled").exists()


def test_d_detaches(home, tmp_path):
    child = spawn(home, tmp_path, "s3://src/", "alice/dst", "--yes")
    child.expect("keeps running")
    child.send("d")
    child.expect("Detached")
    child.expect(pexpect.EOF)
    child.close()
    assert child.exitstatus == 0
    assert not (tmp_path / "canceled").exists()


def test_the_terminal_is_restored(home, tmp_path):
    """After the CLI leaves, the same terminal has its signal keys back."""
    child = spawn(home, tmp_path, "s3://src/", "alice/dst", "--yes")
    child.close()
    env = dict(child.env)
    cmd = "uv tool run -q --from huggingface_hub==2.4.0 hf s3ream s3://src/ alice/dst --yes; echo EXIT=$?; stty -a"
    sh = pexpect.spawn("sh", ["-c", cmd], env=env, dimensions=(30, 100), timeout=60, encoding="utf-8")
    sh.expect("keeps running")
    sh.send("d")
    sh.expect("EXIT=0")
    sh.expect(pexpect.EOF)
    modes = sh.before.split()
    assert "isig" in modes and "icanon" in modes and "echo" in modes, sh.before
