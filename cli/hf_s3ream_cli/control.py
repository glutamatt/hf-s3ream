"""Keys while a copy runs: Ctrl-C twice to stop, `d` to detach.

`hf` runs an extension with subprocess.call: on a Ctrl-C signal it gives the
extension 0.25 s, then kills it. So in a terminal, the CLI turns the signal
keys off (termios ISIG) for its whole life: Ctrl-C then reaches it as a
plain key (0x03) and neither `hf` nor the CLI gets SIGINT. The CLI asks
before stopping, and the terminal is always set back as it was.
"""

from __future__ import annotations

import os
import queue
import signal
import sys
import threading

from typing_extensions import Self

CTRL_C = "\x03"
CONFIRM_S = 5.0


class StopControl:
    """copying → confirming (5 s) → stopping, or → detached.

    `key` and `tick` return the action for the caller: "confirm", "resume",
    "stop", "detach", "quit" or None.
    """

    def __init__(self, confirm_s: float = CONFIRM_S):
        self.confirm_s = confirm_s
        self.state = "copying"
        self.deadline = 0.0

    def key(self, k: str, now: float) -> str | None:
        if self.state == "copying":
            if k == CTRL_C:
                self.state, self.deadline = "confirming", now + self.confirm_s
                return "confirm"
            if k in ("d", "D"):
                self.state = "detached"
                return "detach"
            return None
        if self.state == "confirming":
            if k == CTRL_C:
                self.state = "stopping"
                return "stop"
            if k in ("d", "D"):
                self.state = "detached"
                return "detach"
            self.state = "copying"
            return "resume"
        if self.state == "stopping" and k == CTRL_C:
            # The stop is under way in the background: leave without waiting.
            return "quit"
        return None

    def signal(self) -> str | None:
        """SIGINT from outside (no terminal, or `kill -INT`): stop, no question."""
        if self.state in ("copying", "confirming"):
            self.state = "stopping"
            return "stop"
        return "quit" if self.state == "stopping" else None

    def tick(self, now: float) -> str | None:
        if self.state == "confirming" and now >= self.deadline:
            self.state = "copying"
            return "resume"
        return None

    def left(self, now: float) -> float:
        return max(0.0, self.deadline - now) if self.state == "confirming" else 0.0


class Keys:
    """Key presses on `q` as ("key", char), with the terminal's signal keys
    off. Does nothing when stdin is not a terminal (or not POSIX)."""

    def __init__(self, q: queue.Queue):
        self.q = q
        self.fd = None
        self.saved = None
        self.stop = threading.Event()
        self.old_handlers: dict = {}

    @property
    def active(self) -> bool:
        return self.saved is not None

    def __enter__(self) -> Self:
        if os.name != "posix" or not sys.stdin.isatty():
            return self
        import termios

        self.fd = sys.stdin.fileno()
        self.saved = termios.tcgetattr(self.fd)
        new = termios.tcgetattr(self.fd)
        new[3] &= ~(termios.ICANON | termios.ECHO | termios.ISIG)
        new[6][termios.VMIN] = 1
        new[6][termios.VTIME] = 0
        termios.tcsetattr(self.fd, termios.TCSANOW, new)
        # The terminal must come back as it was, however this process ends.
        for sig in (signal.SIGTERM, signal.SIGHUP):
            self.old_handlers[sig] = signal.signal(sig, self._on_fatal)
        threading.Thread(target=self._read, daemon=True).start()
        return self

    def __exit__(self, *exc) -> None:
        self.restore()

    def restore(self) -> None:
        self.stop.set()
        if self.saved is not None:
            import termios

            termios.tcsetattr(self.fd, termios.TCSADRAIN, self.saved)
            self.saved = None
            for sig, h in self.old_handlers.items():
                signal.signal(sig, h)

    def _on_fatal(self, signum, frame) -> None:
        self.restore()
        signal.signal(signum, signal.SIG_DFL)
        os.kill(os.getpid(), signum)

    def _read(self) -> None:
        import select

        while not self.stop.is_set():
            r, _, _ = select.select([self.fd], [], [], 0.2)
            if not r:
                continue
            try:
                data = os.read(self.fd, 32)
            except OSError:
                return
            if not data:
                return
            text = data.decode(errors="replace")
            if text.startswith("\x1b"):
                self.q.put(("key", "\x1b"))  # Esc, an arrow…: one key
                continue
            for ch in text:
                self.q.put(("key", ch))
