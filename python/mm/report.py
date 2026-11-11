"""Render results/<run_id>/summary.md from a backtest run's JSON outputs.

    uv run python -m mm.report results/<run_id> [--notes notes.md]

``--notes`` appends a hand-written interpretation section (kept in the run folder as
notes.md so the rendered file can always be regenerated).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

# Hyperliquid reference numbers, from crypto-market-making README ("First results on
# real Hyperliquid data"): 8 free Tardis days of BTC, 6 train / 2 held out,
# touch_ratio fill model, maker fee 1.5 bps, latency 50 ms, requote 250 ms.
HL = {
    "data": "8 non-contiguous days (1st of each month 2026-03..10), Tardis book_snapshot_5 (~545 ms) + trades",
    "train_hs1": "-4764 ± 960 /day, 0/6 profitable",
    "train_hs8": "-1285 ± 334 /day, 0/6 profitable",
    "test": "-1405 ± 171 /day, 0/2 profitable (hs=8 HL ticks = $8, skew 0)",
    "markouts": {"0s": 0.17, "0.5s": -0.54, "60s": -1.14},
    "pnl_per_volume": "-1.1 to -2.1 bps",
    "fee_bps": 1.5,
    "tick": "$1 (~0.125 bps)",
}


def _f(x, nd=2):
    if x is None:
        return "n/a"
    if isinstance(x, str):
        return x
    return f"{x:,.{nd}f}"


def render(run_dir: Path, notes: str = "") -> str:
    s = json.loads((run_dir / "cv_summary.json").read_text())
    c = json.loads((run_dir / "config.json").read_text())
    L = [f"# Backtest {c['run_id']}: Bybit {c['symbol']} linear perpetual", ""]
    L += [f"- Days: {c['days'][0]} .. {c['days'][-1]} ({len(c['days'])} contiguous UTC days, each its own session)",
          f"- Train: {', '.join(s['train_days'])}; held out: {', '.join(s['test_days'])}",
          f"- Git commit: `{c['git_commit']}`; tick {c['tick_size']} USDT, lot {c['lot_size']} BTC",
          f"- Sim: latency {c['sim_config']['latency_ms']} ms, requote {c['sim_config']['requote_ms']} ms, "
          f"maker fee {c['sim_config']['maker_fee'] * 1e4:g} bps, touch_fill_ratio {c['sim_config']['touch_fill_ratio']}",
          f"- Quoter: order {c['base_params']['order_size']} BTC, max inventory {c['base_params']['max_inventory']} BTC, "
          f"vol_mult {c['base_params']['vol_mult']}, vol horizon {c['base_params']['vol_horizon_s']} s",
          f"- Grid: half-spread floor {c['grid']['min_half_spread_ticks']} ticks x skew {c['grid']['skew_ticks_at_max']} ticks, "
          f"two fill models; selection = {c['selection']}", ""]

    L += ["## Held-out result (parameters chosen on train days)", "",
          "| fill model | chosen hs / skew (ticks) | train PnL/day | test PnL/day | test profitable days | "
          "spread capture | inventory | fees | PnL/volume (bps) | fills |",
          "|---|---|---|---|---|---|---|---|---|---|"]
    for m, v in s["models"].items():
        sel, t = v["selected_on_train"], v["test"]
        L.append(f"| {m} | {sel['min_half_spread_ticks']:g} / {sel['skew_ticks_at_max']:g} | "
                 f"{_f(sel['train']['pnl_per_day_mean'])} ± {_f(sel['train']['pnl_per_day_stderr'])} | "
                 f"{_f(t['pnl_per_day_mean'])} ± {_f(t['pnl_per_day_stderr'])} | {t['profitable_days']} | "
                 f"{_f(t['pnl_spread_capture'])} | {_f(t['pnl_inventory'])} | {_f(t['pnl_fees'])} | "
                 f"{_f(t['pnl_per_volume_bps'], 3)} | {t['fills']:,} |")
    L += ["", "PnL in USDT, marked to mid; the three components sum to the total over the held-out days.", ""]

    L += ["## Markouts on held-out days (bps, size-weighted)", "",
          "| fill model | config | " + " | ".join(next(iter(s["models"].values()))["test"]["markout_bps"].keys()) + " |",
          "|---|---|" + "---|" * len(next(iter(s["models"].values()))["test"]["markout_bps"]) ]
    for m, v in s["models"].items():
        sel = v["selected_on_train"]
        L.append(f"| {m} | chosen hs={sel['min_half_spread_ticks']:g} | " +
                 " | ".join(_f(x, 3) for x in v["test"]["markout_bps"].values()) + " |")
        L.append(f"| {m} | HL-equivalent hs=80 | " +
                 " | ".join(_f(x, 3) for x in v["hl_equivalent_test"]["markout_bps"].values()) + " |")
    L += [""]

    L += ["## Train grid: mean PnL per day (USDT)", ""]
    for m, v in s["models"].items():
        g = v["train_grid"]
        hs = sorted({r["min_half_spread_ticks"] for r in g})
        sk = sorted({r["skew_ticks_at_max"] for r in g})
        val = {(r["min_half_spread_ticks"], r["skew_ticks_at_max"]): r for r in g}
        L += [f"**{m}** (rows: skew ticks, columns: half-spread floor ticks)", "",
              "| skew \\ hs | " + " | ".join(f"{h:g}" for h in hs) + " |", "|---|" + "---|" * len(hs)]
        for k in sk:
            L.append(f"| {k:g} | " + " | ".join(_f(val[(h, k)]["pnl_per_day_mean"], 0) for h in hs) + " |")
        L.append("")

    L += ["## Same strategy, Hyperliquid vs Bybit", "",
          "| | Hyperliquid BTC (original repo) | Bybit BTCUSDT (this run, touch_ratio, HL-equivalent hs=80, skew 0) |",
          "|---|---|---|"]
    hv = s["models"]["touch_ratio"]["hl_equivalent_test"]
    L += [f"| data | {HL['data']} | 7 contiguous days, full-depth ob200 deltas every 100 ms + all trades |",
          f"| tick | {HL['tick']} | 0.10 USDT (~0.013 bps) |",
          f"| maker fee | {HL['fee_bps']} bps | {c['sim_config']['maker_fee'] * 1e4:g} bps |",
          f"| held-out PnL/day | {HL['test']} | {_f(hv['pnl_per_day_mean'])} ± {_f(hv['pnl_per_day_stderr'])}, {hv['profitable_days']} profitable |",
          f"| PnL / volume | {HL['pnl_per_volume']} | {_f(hv['pnl_per_volume_bps'], 3)} bps |",
          f"| markout at fill / 0.5 s / 60 s | {HL['markouts']['0s']} / {HL['markouts']['0.5s']} / {HL['markouts']['60s']} bps | "
          f"{_f(hv['markout_bps'].get('0s'), 3)} / {_f(hv['markout_bps'].get('0.5s'), 3)} / {_f(hv['markout_bps'].get('60s'), 3)} bps |",
          ""]
    if notes:
        L += [notes.strip(), ""]

    L += ["## Simplifications (labelled)", "",
          "- **Fill model.** `l2_queue` = L2 queue-position *estimate* (queue ahead = displayed size at our price on "
          "joining; prints at our price consume it first; unexplained size decreases = pro-rata cancels; 100 ms "
          "conflation nets adds/cancels). `touch_ratio` = the original model (25% of a print at our price). A print "
          "through our price fills our full size in both.",
          "- **No market impact**: our orders are not in the replayed book or tape.",
          "- **Latency**: constant 50 ms decision-to-live, no jitter, cancels are instant at requote activation.",
          "- **Fees**: Bybit base-tier maker fee 0.02%, no rebates, no funding payments.",
          "- **Timestamps**: book `ts` (system time) and trade `T` (fill time) merged as one clock; book first at equal ms.",
          "- **Sampling**: equity/inventory sampled every 5 s (1 s in the reference run): drawdown and time-at-limit "
          "are measured on samples.",
          "- **Sessions**: every day starts flat; inventory is not carried overnight.",
          "- **Sample size**: 5 train + 2 held-out days of one week: one regime; per-day t-stats are reported, not one total.",
          ""]
    L += ["## Files", "",
          "`cv_summary.json`, `config.json`, `all_runs_per_day.csv` (every config x day), `cv_train_<model>.csv`, "
          "`cv_test_per_day_<model>.csv`, `markout_curve_test_<model>.csv`; charts: `sweep_train_pnl_<model>.png`, "
          "`markout_curve_test_<model>.png`, `pnl_attribution_<model>_<day>.png`, `inventory_<model>_<day>.png`; "
          f"reference simulator output: `reference_{s['reference']['day']}/` (copied to `{s['reference']['dir']}`).", ""]
    return "\n".join(L)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("run_dir", type=Path)
    p.add_argument("--notes", type=Path, default=None)
    a = p.parse_args()
    notes_path = a.notes or (a.run_dir / "notes.md")
    notes = notes_path.read_text() if notes_path.exists() else ""
    if a.notes is not None and a.notes.resolve() != (a.run_dir / "notes.md").resolve():
        (a.run_dir / "notes.md").write_text(notes)
    (a.run_dir / "summary.md").write_text(render(a.run_dir, notes))
    print(f"wrote {a.run_dir / 'summary.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
