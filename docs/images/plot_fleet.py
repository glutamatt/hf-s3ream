"""Draw the fleet charts of docs/ADAPTIVE-CONCURRENCY.md from the planner's PLAN_PROGRESS samples.

    uv run --with matplotlib docs/images/plot_fleet.py

Reads docs/images/data/*.csv (see the header of each file) and writes
docs/images/fleet-timeline.png and docs/images/throughput-per-copier.png.
"""

import csv
import itertools
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = Path(__file__).parent
DATA = HERE / "data"

BLUE, ORANGE = "#257de0", "#f47230"
TEXT, MUTED, GRID = "#191919", "#666666", "#e6e6e6"
GIB = 1024.0

plt.rcParams.update(
    {
        "font.size": 11,
        "axes.edgecolor": MUTED,
        "axes.labelcolor": TEXT,
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.color": GRID,
        "grid.linewidth": 0.8,
        "figure.facecolor": "white",
        "axes.facecolor": "white",
    }
)


def load(name):
    with open(DATA / name) as f:
        rows = csv.DictReader(line for line in f if not line.startswith("#"))
        return [{k: float(v) for k, v in r.items()} for r in rows]


def note(ax, text, xy, xytext):
    ax.annotate(
        text,
        xy=xy,
        xytext=xytext,
        color=TEXT,
        fontsize=10,
        arrowprops={"arrowstyle": "-", "color": MUTED, "lw": 0.8},
    )


def timeline():
    """One copy, its first 20 minutes: the fleet grows, holds, is cut, then drains."""
    rows = [r for r in load("goes18-2024.csv") if r["t_s"] <= 20 * 60]
    t = [r["t_s"] / 60 for r in rows]
    fig, (top, bottom) = plt.subplots(2, 1, sharex=True, figsize=(9, 6.2))

    top.plot(t, [r["hf_mibps_5s"] / GIB for r in rows], color=BLUE, lw=2)
    top.set_ylabel("GiB/s")
    top.set_title("Throughput of the whole fleet", loc="left", color=TEXT, fontsize=12)
    top.set_ylim(0, 55)
    note(top, "plateau: the destination registers\n~1,000 files/s", (8.3, 41.4), (0.3, 44))

    bottom.step(t, [r["target"] for r in rows], where="post", color=ORANGE, lw=2)
    bottom.plot(t, [r["running"] for r in rows], color=BLUE, lw=2)
    bottom.set_ylabel("copiers")
    bottom.set_xlabel("minutes since the planner started")
    bottom.set_title("Size of the fleet", loc="left", color=TEXT, fontsize=12)
    bottom.set_ylim(0, 72)
    bottom.set_xlim(-0.5, 20)
    bottom.set_xticks(range(0, 21, 5))
    bottom.text(17.2, 47.5, "allowed", color=ORANGE, fontsize=10)
    bottom.text(5.8, 19, "running", color=BLUE, fontsize=10)
    note(bottom, "+4 copiers a minute", (7.0, 37), (1.0, 52))
    note(bottom, "no gain from 25 % more copiers:\n× 0.7 (64 → 41)", (13.05, 63), (14.0, 60))
    note(bottom, "nothing left to launch:\nthe fleet drains", (16.3, 30), (9.6, 8))

    fig.tight_layout()
    fig.savefig(HERE / "fleet-timeline.png", dpi=150)
    plt.close(fig)


def per_copier():
    """Throughput against running copiers, while the fleet grows."""
    fig, ax = plt.subplots(figsize=(9, 5))
    for name, label, color in (
        ("goes18-2023.csv", "RadF/2023", ORANGE),
        ("goes18-2024.csv", "RadF/2024", BLUE),
    ):
        rows = load(name)
        # The growing part only: from the start to the first cut.
        grow = []
        for prev, r in itertools.pairwise(rows):
            if r["target"] < prev["target"]:
                break
            grow.append(r)
        pts = [(r["running"], r["hf_mibps_5s"] / GIB) for r in grow if r["running"] >= 16]
        ax.scatter(
            [p[0] for p in pts],
            [p[1] for p in pts],
            s=40,
            color=color,
            edgecolor="white",
            linewidth=1.5,
            label=f"s3://noaa-goes18/ABI-L1b-RadF/{label.split('/')[1]}/",
            zorder=3,
        )
    ax.plot([0, 40], [0, 40 * 1.1], color=MUTED, lw=1, ls="--")
    ax.axhline(43, color=MUTED, lw=1, ls="--")
    ax.text(2, 45, "~43 GiB/s: the destination registers ~1,000 files/s", color=TEXT, fontsize=10)
    ax.text(14, 8, "each copier adds ~1.1 GiB/s", color=TEXT, fontsize=10)
    note(ax, "more copiers, more cost,\nno more throughput", (55, 43.5), (49, 25))
    ax.set_xlim(0, 66)
    ax.set_ylim(0, 55)
    ax.set_xlabel("copiers running")
    ax.set_ylabel("GiB/s")
    ax.set_title("Throughput against copiers, while the fleet grows", loc="left", color=TEXT, fontsize=12)
    ax.legend(frameon=False, loc="lower right", labelcolor=TEXT, fontsize=10)
    fig.tight_layout()
    fig.savefig(HERE / "throughput-per-copier.png", dpi=150)
    plt.close(fig)


if __name__ == "__main__":
    timeline()
    per_copier()
