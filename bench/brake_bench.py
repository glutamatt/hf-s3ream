#!/usr/bin/env python3
"""A local bench for the brakes of hf-s3ream (src/brake.rs, src/growth.rs).

Nothing leaves this machine. One process plays three roles on one port:

- the SOURCE bucket (S3 ListObjectsV2 and ranged GETs), with synthetic data
  that never dedups (every 4 KiB carries a unique stamp);
- the Hub API a copier calls (xet write token, bucket batch commit);
- CAS: upload grants, the staging PUT, commits and shards, with a model of
  its CPU. A commit costs `size / --core-mibps` seconds of one core. Commits
  wait for a free core; past `--slots` commits in flight a new one waits up
  to 5 s, then gets a 429 (the upload slot cap); a commit that waited longer
  than `--fail-wait` for a core gets a 503 (CPU saturated). An optional
  autoscaler adds cores when the CPU stays busy, and another user can take
  part of the cores for a while. A second where a commit waited more than
  --hurt-wait for a core counts as "saturated": the CPU is full, and on the
  real CAS other users' downloads share it.

It then runs real copiers (the release binary, `cargo build --release`), each
on its own key range, prints one line per second, and a summary: what was
committed, how long CAS was saturated (the time other users would have seen
errors), the 429 and 503 it sent, and the cuts of each copier's brake.

    python3 bench/brake_bench.py --copiers 4 --seconds 180 --brake on
    python3 bench/brake_bench.py --copiers 4 --seconds 180 --brake off
    python3 bench/brake_bench.py --other 60:120:0.5      # another user takes half the CPU
    python3 bench/brake_bench.py --autoscale 2                 # +2 cores per minute while busy

The scale is small (a laptop plays the source, CAS and the copiers), so the
fleet figures of the brake are scaled down too (--fast-gibps, --ramp-gibps).

Memory: a copier keeps each xorb (up to 64 MiB) until its commit ends, so a
slow CAS grows it. Each copier runs in its own systemd scope capped at
--copier-mem (its memory sizing reads that cap), with few uploads in flight
(--xorb-uploads): a copier that overflows dies alone, not the session.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import signal
import socket
import statistics
import struct
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

MIB = 1 << 20
BLOCK = MIB
STAMP_EVERY = 4096
BUCKET = "fakesrc"
PREFIX = "data/"

# ── The source data ──────────────────────────────────────────────────────────

POOL: list[bytes] = []


def make_pool(blocks: int) -> None:
    rnd = random.Random(42)  # noqa: S311 - test data, not a secret
    POOL.extend(rnd.randbytes(BLOCK) for _ in range(blocks))


def block_bytes(obj: int, block: int) -> bytes:
    """1 MiB of object `obj`: a random pool block, stamped every 4 KiB with
    (object, block, offset), so no two chunks anywhere are the same."""
    b = bytearray(POOL[(obj * 7919 + block) % len(POOL)])
    for off in range(0, BLOCK, STAMP_EVERY):
        b[off : off + 16] = struct.pack("<QQ", obj, (block << 20) | off)
    return bytes(b)


def object_range(obj: int, start: int, end: int):
    """Yield the bytes [start, end) of object `obj`."""
    pos = start
    while pos < end:
        block, off = divmod(pos, BLOCK)
        data = block_bytes(obj, block)
        take = min(BLOCK - off, end - pos)
        yield data[off : off + take]
        pos += take


# ── The CAS model ────────────────────────────────────────────────────────────


class Cas:
    def __init__(self, a: argparse.Namespace):
        self.cores = float(a.cores)
        self.max_cores = float(a.max_cores or a.cores * 4)
        self.core_mibps = a.core_mibps
        self.slots = a.slots
        self.fail_wait = a.fail_wait
        self.hurt_wait = a.hurt_wait
        self.autoscale = a.autoscale
        self.other = parse_other(a.other)
        self.t0 = time.monotonic()
        self.cond = threading.Condition()
        self.busy = 0  # cores in use by commits
        self.in_flight = 0  # commits holding a slot
        self.waiting_core = 0
        self.grants: dict[str, int] = {}
        self.next_grant = 0
        # Per-second counters, reset by the stats thread.
        self.sec = self.new_sec()
        self.util_window: list[float] = []

    def new_sec(self) -> dict:
        return {"mib": 0.0, "commits": 0, "lat": [], "r429": 0, "r503": 0, "wait": 0.0}

    def now(self) -> float:
        return time.monotonic() - self.t0

    def other_share(self) -> float:
        t = self.now()
        return sum(share for (t0, t1, share) in self.other if t0 <= t < t1)

    def available(self) -> float:
        return max(1.0, self.cores * (1.0 - self.other_share()))

    def grant(self, length: int) -> str:
        with self.cond:
            self.next_grant += 1
            gid = f"g{self.next_grant}"
            self.grants[gid] = length
        return gid

    def commit(self, gid: str) -> int:
        """Run one commit through the model; returns the HTTP status."""
        started = time.monotonic()
        with self.cond:
            size = self.grants.get(gid)
            if size is None:
                return 404
            # The upload slot cap: wait up to 5 s, then 429.
            deadline = started + 5.0
            while self.in_flight >= self.slots:
                left = deadline - time.monotonic()
                if left <= 0:
                    self.sec["r429"] += 1
                    return 429
                self.cond.wait(left)
            self.in_flight += 1
            # The CPU: wait for a core; too long means a saturated CAS (503).
            self.waiting_core += 1
            core_deadline = time.monotonic() + self.fail_wait
            failed = False
            waited_from = time.monotonic()
            while self.busy >= self.available():
                left = core_deadline - time.monotonic()
                if left <= 0:
                    failed = True
                    break
                self.cond.wait(left)
            self.waiting_core -= 1
            self.sec["wait"] = max(self.sec["wait"], time.monotonic() - waited_from)
            if failed:
                self.in_flight -= 1
                self.sec["r503"] += 1
                self.cond.notify_all()
                return 503
            self.busy += 1
        time.sleep(size / MIB / self.core_mibps)
        with self.cond:
            self.busy -= 1
            self.in_flight -= 1
            self.grants.pop(gid, None)
            self.sec["mib"] += size / MIB
            self.sec["commits"] += 1
            self.sec["lat"].append(time.monotonic() - started)
            self.cond.notify_all()
        return 200

    def tick(self) -> dict:
        """Close one second of stats; run the autoscaler."""
        with self.cond:
            sec, self.sec = self.sec, self.new_sec()
            avail = self.available()
            util = min(1.0, (self.busy + self.waiting_core) / avail)
            self.util_window = (self.util_window + [util])[-60:]
            if self.autoscale and len(self.util_window) >= 15 and statistics.mean(self.util_window[-15:]) > 0.6:
                self.cores = min(self.max_cores, self.cores + self.autoscale / 60.0)
            self.cond.notify_all()
            lat = sorted(sec["lat"])
            return {
                "t": round(self.now()),
                "cores": round(avail, 1),
                "busy": self.busy,
                "queue": self.waiting_core,
                "mibps": round(sec["mib"]),
                "commits": sec["commits"],
                "p50_s": round(lat[len(lat) // 2], 2) if lat else None,
                "r429": sec["r429"],
                "r503": sec["r503"],
                "max_wait_s": round(sec["wait"], 2),
                "saturated": sec["wait"] > self.hurt_wait,
            }


def parse_other(spec: str | None) -> list[tuple[float, float, float]]:
    out = []
    for part in (spec or "").split(","):
        if part:
            t0, t1, share = part.split(":")
            out.append((float(t0), float(t1), float(share)))
    return out


# ── The HTTP server ──────────────────────────────────────────────────────────

CAS: Cas
OBJECTS: list[tuple[str, int]] = []
PORT = 0
LIST_PAGE = 1000


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):  # quiet
        pass

    def reply(self, status: int, body: bytes = b"", ctype: str = "application/json", headers=None):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if body and self.command != "HEAD":
            self.wfile.write(body)

    def json(self, status: int, obj) -> None:
        self.reply(status, json.dumps(obj).encode())

    def body(self) -> bytes:
        n = int(self.headers.get("Content-Length") or 0)
        chunks = []
        while n > 0:
            c = self.rfile.read(min(n, MIB))
            if not c:
                break
            chunks.append(c)
            n -= len(c)
        return b"".join(chunks)

    def drain(self) -> int:
        n = int(self.headers.get("Content-Length") or 0)
        left = n
        while left > 0:
            c = self.rfile.read(min(left, MIB))
            if not c:
                break
            left -= len(c)
        return n

    def s3_bucket_style(self) -> tuple[bool, str]:
        """(is S3, key path) for virtual-host and path-style requests."""
        host = (self.headers.get("Host") or "").split(":")[0]
        path = unquote(urlparse(self.path).path)
        if host.startswith(BUCKET + "."):
            return True, path.lstrip("/")
        if path.startswith(f"/{BUCKET}/") or path == f"/{BUCKET}":
            return True, path[len(BUCKET) + 2 :]
        return False, ""

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        is_s3, key = self.s3_bucket_style()
        path = urlparse(self.path).path
        if is_s3:
            return self.s3_get(key)
        if path.endswith("/xet-write-token"):
            return self.json(
                200,
                {"casUrl": f"http://127.0.0.1:{PORT}", "exp": int(time.time()) + 86400, "accessToken": "fake"},
            )
        if path.startswith("/v1/chunks/"):
            return self.reply(404)  # global dedup: never found
        self.reply(404, b"{}")

    def s3_get(self, key: str):
        q = parse_qs(urlparse(self.path).query)
        if key == "" and q.get("list-type") == ["2"]:
            return self.s3_list(q)
        m = re.fullmatch(re.escape(PREFIX) + r"f(\d+)\.bin", key)
        if not m:
            return self.reply(404, b"<Error><Code>NoSuchKey</Code></Error>", "application/xml")
        obj = int(m.group(1))
        size = OBJECTS[obj][1]
        start, end, status = 0, size, 200
        rng = self.headers.get("Range")
        if rng:
            a, b = rng.removeprefix("bytes=").split("-")
            start, end, status = int(a), min(size, int(b) + 1), 206
        self.send_response(status)
        self.send_header("Content-Length", str(end - start))
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("ETag", f'"{obj}"')
        self.send_header("Last-Modified", "Thu, 01 Jan 2026 00:00:00 GMT")
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{end - 1}/{size}")
        self.end_headers()
        if self.command == "HEAD":
            return
        try:
            for piece in object_range(obj, start, end):
                self.wfile.write(piece)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def s3_list(self, q):
        prefix = q.get("prefix", [""])[0]
        after = q.get("continuation-token", q.get("start-after", [""]))[0]
        keys = [(k, s) for (k, s) in OBJECTS if k.startswith(prefix) and k > after]
        page, truncated = keys[:LIST_PAGE], len(keys) > LIST_PAGE
        items = "".join(
            f"<Contents><Key>{k}</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified>"
            f'<ETag>"x"</ETag><Size>{s}</Size><StorageClass>STANDARD</StorageClass></Contents>'
            for (k, s) in page
        )
        token = f"<NextContinuationToken>{page[-1][0]}</NextContinuationToken>" if truncated else ""
        xml = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            f"<Name>{BUCKET}</Name><Prefix>{prefix}</Prefix><KeyCount>{len(page)}</KeyCount>"
            f"<MaxKeys>{LIST_PAGE}</MaxKeys><IsTruncated>{'true' if truncated else 'false'}</IsTruncated>"
            f"{token}{items}</ListBucketResult>"
        )
        self.reply(200, xml.encode(), "application/xml")

    def do_PUT(self):
        path = urlparse(self.path).path
        if path.startswith("/staging/"):
            self.drain()
            return self.reply(200)
        self.drain()
        self.reply(404, b"{}")

    def do_POST(self):
        path = urlparse(self.path).path
        if path.startswith("/v1/xorb-grants/"):
            req = json.loads(self.body() or b"{}")
            gid = CAS.grant(int(req.get("length", 0)))
            return self.json(
                201,
                {"id": gid, "method": "PUT", "url": f"http://127.0.0.1:{PORT}/staging/{gid}", "headers": {}},
            )
        if path.startswith("/v1/xorb-commits/"):
            self.drain()
            gid = path.rsplit("/", 1)[-1]
            status = CAS.commit(gid)
            return self.json(status, {} if status == 200 else {"error": "fake"})
        if path == "/v2/shards":
            self.drain()
            return self.reply(404, b"{}")  # the client falls back to v1
        if path == "/v1/shards":
            self.drain()
            return self.json(200, {"result": 1})
        if path.endswith("/batch"):
            ops = [line for line in self.body().splitlines() if line.strip()]
            return self.json(200, {"success": True, "processed": len(ops), "succeeded": len(ops), "failed": []})
        self.drain()
        self.json(200, {})


class Server(ThreadingHTTPServer):
    address_family = socket.AF_INET6
    daemon_threads = True
    request_queue_size = 1024

    def handle_error(self, request, client_address):
        # A copier stopped at the end of a run drops its connections: no
        # traceback for that.
        if not isinstance(sys.exc_info()[1], (ConnectionError, TimeoutError)):
            super().handle_error(request, client_address)

    def server_bind(self):
        self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        super().server_bind()


# ── The run ──────────────────────────────────────────────────────────────────


def copier_cmd(a, binary: str, start_after: str | None, stop_at: str) -> list[str]:
    cmd = [
        binary,
        f"s3://{BUCKET}/{PREFIX}",
        "hf://buckets/bench/dest",
        "--hub-endpoint",
        f"http://127.0.0.1:{PORT}",
        "--aws-region",
        "us-east-1",
        "--no-sign-request",
        "--no-verify",
        "--stop-at",
        stop_at,
        "--parallel-files",
        str(a.parallel_files),
        "--s3-part-concurrency",
        "2",
        "--brake",
        a.brake,
        "--brake-fast-gibps",
        str(a.fast_gibps),
        "--brake-ramp-gibps",
        str(a.ramp_gibps),
        "--brake-share",
        str(a.copiers),
    ]
    if start_after:
        cmd += ["--start-after", start_after]
    return cmd


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--binary", default=str(Path(__file__).resolve().parent.parent / "target/release/hf-s3ream"))
    p.add_argument("--copiers", type=int, default=4)
    p.add_argument("--seconds", type=int, default=180)
    p.add_argument("--brake", choices=["on", "shadow", "off"], default="on")
    p.add_argument("--fast-gibps", type=float, default=0.15, help="fleet fast-start level (scaled down)")
    p.add_argument("--ramp-gibps", type=float, default=0.1, help="fleet ramp per minute (scaled down)")
    p.add_argument("--parallel-files", type=int, default=4)
    p.add_argument("--copier-mem", type=int, default=1536, help="memory cap of each copier, MiB")
    p.add_argument("--xorb-uploads", type=int, default=8, help="xorb uploads in flight per copier")
    p.add_argument("--objects", type=int, default=4000)
    p.add_argument("--object-mib", type=int, default=64)
    p.add_argument("--cores", type=float, default=4, help="CAS cores at the start")
    p.add_argument("--max-cores", type=float, default=0, help="autoscaler cap (default 4x --cores)")
    p.add_argument("--core-mibps", type=float, default=50, help="MiB/s one CAS core validates")
    p.add_argument("--slots", type=int, default=64, help="CAS upload slots (commits in flight)")
    p.add_argument("--fail-wait", type=float, default=6.0, help="seconds a commit waits for a core before a 503")
    p.add_argument("--hurt-wait", type=float, default=1.0, help="a core wait above this marks a saturated second")
    p.add_argument("--autoscale", type=float, default=0.0, help="cores the autoscaler adds per minute while busy")
    p.add_argument("--other", default="", help="T0:T1:SHARE[,...] another user takes SHARE of the cores")
    p.add_argument("--out", default="", help="directory for the copier logs and the per-second stats")
    a = p.parse_args()

    global CAS, PORT
    if not Path(a.binary).exists():
        print(f"no binary at {a.binary}: cargo build --release", file=sys.stderr)
        return 2
    make_pool(64)
    OBJECTS.extend((f"{PREFIX}f{i:06d}.bin", a.object_mib * MIB) for i in range(a.objects))
    CAS = Cas(a)
    server = Server(("::", 0), Handler)
    PORT = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()

    out = Path(a.out) if a.out else Path(tempfile.mkdtemp(prefix="brake-bench-"))
    out.mkdir(parents=True, exist_ok=True)
    env = dict(
        os.environ,
        HF_TOKEN="fake",  # noqa: S106 - the local fake Hub takes any token
        AWS_ENDPOINT_URL=f"http://localhost:{PORT}",
        AWS_ENDPOINT=f"http://localhost:{PORT}",
        AWS_ALLOW_HTTP="true",
        HF_XET_CLIENT_AC_MAX_UPLOAD_CONCURRENCY=str(a.xorb_uploads),
    )
    env.pop("AWS_PROFILE", None)
    need = a.copiers * a.copier_mem * MIB
    if need > mem_available() // 2:
        print(f"{a.copiers} copiers x {a.copier_mem} MiB is more than half the available memory", file=sys.stderr)
        return 2
    per = a.objects // a.copiers
    procs = []
    brakes: dict[int, dict] = {}
    for i in range(a.copiers):
        start_after = OBJECTS[i * per - 1][0] if i else None
        stop_at = OBJECTS[(i + 1) * per - 1][0]
        log = open(out / f"copier-{i}.log", "w")
        scope = [
            "systemd-run",
            "--user",
            "--scope",
            "--quiet",
            "-p",
            f"MemoryMax={a.copier_mem}M",
            "-p",
            "MemorySwapMax=0",
            "--",
        ]
        proc = subprocess.Popen(  # noqa: S603 - our own binary, our own arguments
            scope + copier_cmd(a, a.binary, start_after, stop_at),
            env=env,
            stdout=subprocess.PIPE,
            stderr=log,
            text=True,
        )
        procs.append(proc)

        def read(i=i, proc=proc, log=log):
            for line in proc.stdout:
                log.write(line)
                if line.startswith("PROGRESS "):
                    v = json.loads(line[9:])
                    brakes[i] = {"hf_mibps": v.get("hf_mibps_5s"), "brake": v.get("brake")}

        threading.Thread(target=read, daemon=True).start()

    stats = []
    stats_file = open(out / "cas.jsonl", "w")
    print(f"port {PORT}, logs in {out}", file=sys.stderr)
    print(" t  cores busy queue  MiB/s  p50_s  429  503  sat | copier rate MiB/s (action)")
    try:
        for _ in range(a.seconds):
            time.sleep(1)
            s = CAS.tick()
            stats.append(s)
            stats_file.write(json.dumps(s) + "\n")
            cop = " ".join(
                f"{(b.get('brake') or {}).get('rate_mibps', '-')}({action_name(b.get('brake'))})"
                for _, b in sorted(brakes.items())
            )
            print(
                f"{s['t']:3d} {s['cores']:5.1f} {s['busy']:4d} {s['queue']:5d} {s['mibps']:6d} "
                f"{s['p50_s'] if s['p50_s'] is not None else '-':>6} {s['r429']:4d} {s['r503']:4d} "
                f"{'X' if s['saturated'] else '.':>4} | {cop}",
                flush=True,
            )
    except KeyboardInterrupt:
        pass
    finally:
        for proc in procs:
            proc.send_signal(signal.SIGTERM)
        for proc in procs:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()

    total = sum(s["mibps"] for s in stats)
    secs = len(stats)
    cuts = {i: (b.get("brake") or {}).get("cuts") for i, b in sorted(brakes.items())}
    errors = {i: (b.get("brake") or {}).get("errors") for i, b in sorted(brakes.items())}
    summary = {
        "brake": a.brake,
        "copiers": a.copiers,
        "seconds": secs,
        "committed_gib": round(total / 1024, 2),
        "mean_mibps": round(total / max(secs, 1)),
        "capacity_mibps_at_end": round(stats[-1]["cores"] * a.core_mibps) if stats else None,
        "saturated_s": sum(1 for s in stats if s["saturated"]),
        "r429": sum(s["r429"] for s in stats),
        "r503": sum(s["r503"] for s in stats),
        "cuts": cuts,
        "errors_seen": errors,
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary))
    return 0


def mem_available() -> int:
    """MemAvailable from /proc/meminfo, bytes."""
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024
    return 0


def action_name(brake: dict | None) -> str:
    if not brake:
        return "-"
    act = brake.get("action")
    if isinstance(act, dict):
        cause = act.get("cut")
        return "cut:" + (cause if isinstance(cause, str) else next(iter(cause.values())))
    return str(act)


if __name__ == "__main__":
    sys.exit(main())
