"""Static report figures (PNG). Ported from crypto-market-making (mmlab/plots.py).

Palette: the first four categorical slots of the reference palette, in fixed order
(blue, orange, aqua, yellow), which pass the adjacent-pair CVD checks for line charts.
"""
from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

# reference palette (categorical slots in fixed order) + chrome
BLUE, ORANGE, AQUA, YELLOW = "#2a78d6", "#eb6834", "#1baf7a", "#eda100"
SURFACE, INK, INK2, MUTED, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"


def _style(ax, title: str, ylabel: str = "") -> None:
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", fontsize=11, color=INK, fontweight="bold")
    ax.set_ylabel(ylabel, color=INK2, fontsize=9)
    ax.grid(axis="y", color=GRID, linewidth=0.6)
    ax.tick_params(colors=MUTED, labelsize=8)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(AXIS)


def _label_end(ax, x, y, text, color) -> None:
    ax.annotate(text, (x, y), xytext=(4, 0), textcoords="offset points", va="center",
                fontsize=8, color=INK2)
    ax.plot([x], [y], "o", ms=3, color=color)


def pnl_attribution(states: pd.DataFrame, path: Path, title: str = "") -> None:
    s = states.copy()
    s["t"] = (s["ts"] - s["ts"].iloc[0]) / 3.6e6
    s = s.iloc[:: max(1, len(s) // 4000)]
    fig, ax = plt.subplots(figsize=(9, 4.2), facecolor=SURFACE)
    series = [
        ("Total", s["equity"], BLUE),
        ("Spread capture", s["spread_capture"], AQUA),
        ("Inventory PnL", s["inventory_pnl"], ORANGE),
        ("Fees", -s["fees"], YELLOW),
    ]
    for name, y, c in series:
        ax.plot(s["t"], y, color=c, lw=2 if name == "Total" else 1.4, label=name)
        _label_end(ax, s["t"].iloc[-1], y.iloc[-1], name, c)
    ax.axhline(0, color=AXIS, lw=0.8)
    _style(ax, title or "PnL attribution (marked to mid)", "USDT")
    ax.set_xlabel("hours", color=INK2, fontsize=9)
    ax.legend(frameon=False, fontsize=8, ncol=4, loc="upper left")
    ax.margins(x=0.12)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def inventory(states: pd.DataFrame, max_inventory: float, path: Path, title: str = "") -> None:
    s = states.iloc[:: max(1, len(states) // 4000)]
    t = (s["ts"] - states["ts"].iloc[0]) / 3.6e6
    fig, ax = plt.subplots(figsize=(9, 2.8), facecolor=SURFACE)
    ax.plot(t, s["inventory"], color=BLUE, lw=1.2)
    for lim in (max_inventory, -max_inventory):
        ax.axhline(lim, color=MUTED, lw=0.8, ls="--")
    ax.axhline(0, color=AXIS, lw=0.8)
    _style(ax, title or "Inventory (dashed = limits)", "BTC")
    ax.set_xlabel("hours", color=INK2, fontsize=9)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def markout_curve(curve: pd.DataFrame, path: Path, title: str = "") -> None:
    fig, ax = plt.subplots(figsize=(7, 3.8), facecolor=SURFACE)
    x = curve["horizon_s"].clip(lower=0.05)
    for name, c in (("all", BLUE), ("buys", AQUA), ("sells", ORANGE)):
        ax.plot(x, curve[name], marker="o", ms=4, lw=2 if name == "all" else 1.2, color=c, label=name)
    ax.axhline(0, color=AXIS, lw=0.8)
    ax.set_xscale("log")
    _style(ax, title or "Fill markouts: edge at fill, then adverse selection", "bps (size-weighted)")
    ax.set_xlabel("seconds after fill (log; first point = at fill)", color=INK2, fontsize=9)
    ax.legend(frameon=False, fontsize=8)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def sweep_heatmap(table: pd.DataFrame, value: str, path: Path, title: str = "") -> None:
    pivot = table.pivot(index="skew_ticks_at_max", columns="min_half_spread_ticks", values=value)
    fig, ax = plt.subplots(figsize=(6.5, 4), facecolor=SURFACE)
    lim = max(abs(pivot.min().min()), abs(pivot.max().max())) or 1
    im = ax.imshow(pivot.values, cmap="RdBu", vmin=-lim, vmax=lim, aspect="auto", origin="lower")
    ax.set_xticks(range(len(pivot.columns)), [f"{c:g}" for c in pivot.columns])
    ax.set_yticks(range(len(pivot.index)), [f"{i:g}" for i in pivot.index])
    for i in range(pivot.shape[0]):
        for j in range(pivot.shape[1]):
            ax.text(j, i, f"{pivot.values[i, j]:.0f}", ha="center", va="center", fontsize=8, color=INK)
    ax.set_xlabel("min half-spread (ticks)", color=INK2, fontsize=9)
    ax.set_ylabel("skew at max inventory (ticks)", color=INK2, fontsize=9)
    ax.set_title(title or f"Parameter sweep: {value}", loc="left", fontsize=11, color=INK, fontweight="bold")
    ax.tick_params(colors=MUTED, labelsize=8)
    fig.colorbar(im, ax=ax, shrink=0.8)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
