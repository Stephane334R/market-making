"""Leave-days-out backtest on normalized Bybit days (same protocol as crypto-market-making
``--cv``): grid-search on the earlier days, evaluate the train-selected parameters on the
held-out later days, report the per-day PnL distribution, not one total.

    uv run python -m mm.backtest                       # all normalized days
    uv run python -m mm.backtest --start 2026-11-03 --end 2026-11-09

Every parameter set is replayed in one pass per day (see mm.simulator). Each day is a
separate session: inventory and PnL start flat every day (as ``run_by_day`` in the
original), even though these days are contiguous.

Outputs in results/<run_id>/: summary.md, config.json, charts (PNG), CSV tables, and
``reference_<day>/`` with the full simulator output of one day (fills, 1 s states,
summary) used as the reference for the Rust port.
"""

from __future__ import annotations

import argparse
import json
import shutil
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, replace
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from mm import analytics, plots
from mm.download_bybit import REPO_ROOT, RESULTS_DIR, git_commit, norm_path
from mm.normalize_bybit import read_manifest, read_units
from mm.simulator import FILL_MODELS, RunSpec, SimConfig, run_day
from mm.strategy import QuoterParams

SYMBOL = "BTCUSDT"
# Hyperliquid BTC tick = $1, Bybit BTCUSDT tick = $0.10. The original grid
# (hs 1,2,4,8 / skew 0,1,2,4 HL ticks) is mapped to the SAME DOLLAR distances
# (x10 Bybit ticks), plus hs=1 (Bybit's own minimum) and 160/320 ticks, which go past
# the ~150-tick (2 bps) maker fee: the original grid was monotonic to its edge.
GRID_HS_TICKS = [1, 10, 20, 40, 80, 160, 320]
GRID_SKEW_TICKS = [0, 10, 20, 40]
HL_EQUIVALENT = {"min_half_spread_ticks": 80, "skew_ticks_at_max": 0}  # HL's held-out choice (8 HL ticks, skew 0)
REFERENCE_DIR = REPO_ROOT / "tests" / "reference"


def spec_name(model: str, hs: float, sk: float) -> str:
    return f"{model}|hs={hs:g}|skew={sk:g}"


def make_specs(base: QuoterParams, cfg: SimConfig) -> list[RunSpec]:
    return [RunSpec(spec_name(m, hs, sk), replace(base, min_half_spread_ticks=hs, skew_ticks_at_max=sk),
                    replace(cfg, fill_model=m))
            for m in FILL_MODELS for hs in GRID_HS_TICKS for sk in GRID_SKEW_TICKS]


def _markout_sums(mo: pd.DataFrame) -> dict:
    """Size-weighted markout sums per horizon/group, exactly poolable across days."""
    out = {}
    if mo.empty:
        return out
    for c in (c for c in mo.columns if c.startswith("h_")):
        for g, sel in (("all", mo), ("buys", mo[mo["side"] == 1]), ("sells", mo[mo["side"] == -1])):
            v = sel[[c, "qty"]].dropna()
            out[(c, g)] = (float((v[c] * v["qty"]).sum()), float(v["qty"].sum()))
    return out


def _day_worker(args: tuple) -> dict:
    day_s, specs = args
    t0 = time.time()
    bp, tp = norm_path("book_updates", SYMBOL, date.fromisoformat(day_s)), norm_path("trades", SYMBOL, date.fromisoformat(day_s))
    dr = run_day(bp, tp, specs)
    mid_ts, mid = np.asarray(dr.engine.mids_ts), np.asarray(dr.engine.mids)
    out = {"day": day_s, "runs": {}, "book_sha256": read_manifest(bp)["source_sha256"],
           "trades_sha256": read_manifest(tp)["source_sha256"]}
    for s in specs:
        r = dr.runners[s.name]
        fills, states = r.fills_df(), r.states_df()
        mo = analytics.markouts(fills, mid_ts, mid)
        stats = {**analytics.summary(states, fills, mo, s.params.max_inventory), **r.stats()}
        st60 = states.iloc[::12].copy()  # 5 s samples -> 1 min, for charts
        st60 = pd.concat([st60, states.iloc[[-1]]]) if states.index[-1] not in st60.index else st60
        out["runs"][s.name] = {"stats": stats, "mo_sums": _markout_sums(mo), "states_1min": st60}
    out["seconds"] = round(time.time() - t0, 1)
    return out


def _pool_curve(sums_list: list[dict]) -> pd.DataFrame:
    acc: dict = {}
    for sums in sums_list:
        for k, (wx, w) in sums.items():
            a = acc.setdefault(k, [0.0, 0.0])
            a[0] += wx
            a[1] += w
    hs = sorted({k[0] for k in acc}, key=lambda c: float(c[2:-1]))
    rows = [{"horizon_s": float(c[2:-1]),
             **{g: (acc[(c, g)][0] / acc[(c, g)][1] if acc.get((c, g), (0, 0))[1] else np.nan)
                for g in ("all", "buys", "sells")}} for c in hs]
    return pd.DataFrame(rows)


def available_days(start: date | None, end: date | None) -> list[date]:
    d = norm_path("book_updates", SYMBOL, date(2000, 1, 1)).parent
    days = sorted(date.fromisoformat(p.stem) for p in d.glob("*.parquet"))
    days = [x for x in days if norm_path("trades", SYMBOL, x).exists()]
    return [x for x in days if (start is None or x >= start) and (end is None or x <= end)]


def run_reference(day: date, params: QuoterParams, cfg: SimConfig, out: Path, quotes: bool = False) -> dict:
    """Full-resolution single-run output for one day: the reference for the Rust port.
    ``quotes=True`` also writes the quote schedule (quotes.parquet) that rust/sim replays."""
    bp, tp = norm_path("book_updates", SYMBOL, day), norm_path("trades", SYMBOL, day)
    cfg = replace(cfg, sample_ms=1000)
    dr = run_day(bp, tp, [RunSpec("reference", params, cfg, record_quotes=quotes)])
    r = dr.runners["reference"]
    fills, states = r.fills_df(), r.states_df()
    mo = analytics.markouts(fills, np.asarray(dr.engine.mids_ts), np.asarray(dr.engine.mids))
    stats = {**analytics.summary(states, fills, mo, params.max_inventory), **r.stats()}
    out.mkdir(parents=True, exist_ok=True)
    fills.to_csv(out / "fills.csv", index=False)
    states.to_parquet(out / "states.parquet", index=False)
    if quotes:
        r.quotes_df().to_parquet(out / "quotes.parquet", index=False)
    units = read_units(bp)
    cfg_json = {
        "day": day.isoformat(), "symbol": SYMBOL, "params": asdict(params), "sim_config": asdict(cfg),
        "inputs": {"book_updates": str(bp.relative_to(REPO_ROOT)), "trades": str(tp.relative_to(REPO_ROOT)),
                   "book_source_sha256": read_manifest(bp)["source_sha256"],
                   "trades_source_sha256": read_manifest(tp)["source_sha256"]},
        "tick_size": units.tick_size, "lot_size": units.lot_size, "git_commit": git_commit(),
        "events": {"book_messages": len(dr.engine.mids_ts), "fills": len(fills)},
    }
    (out / "config.json").write_text(json.dumps(cfg_json, indent=2))
    (out / "summary.json").write_text(json.dumps(stats, indent=2))
    return stats


def fmt(x, nd=2):
    return "n/a" if x is None or (isinstance(x, float) and np.isnan(x)) else f"{x:,.{nd}f}"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--start", type=date.fromisoformat)
    p.add_argument("--end", type=date.fromisoformat)
    p.add_argument("--test-days", type=int, default=None, help="default: 25%% of days, at least 2 (as original)")
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--order-size", type=float, default=0.02)
    p.add_argument("--max-inventory", type=float, default=0.10)
    p.add_argument("--vol-mult", type=float, default=0.5)
    p.add_argument("--latency-ms", type=int, default=50)
    p.add_argument("--requote-ms", type=int, default=250)
    p.add_argument("--maker-fee", type=float, default=0.0002)
    p.add_argument("--touch-fill-ratio", type=float, default=0.25)
    p.add_argument("--reference-day", type=date.fromisoformat, default=None, help="default: last held-out day")
    a = p.parse_args(argv)

    days = available_days(a.start, a.end)
    n_test = a.test_days if a.test_days is not None else max(2, round(0.25 * len(days)))
    if len(days) - n_test < 2:
        raise SystemExit(f"need at least 2 train + {n_test} test days, have {len(days)}")
    train, test = days[:-n_test], days[-n_test:]
    cfg = SimConfig(latency_ms=a.latency_ms, requote_ms=a.requote_ms, maker_fee=a.maker_fee,
                    touch_fill_ratio=a.touch_fill_ratio)
    base = QuoterParams(order_size=a.order_size, max_inventory=a.max_inventory, vol_mult=a.vol_mult,
                        vol_horizon_s=(a.requote_ms + a.latency_ms) / 1000)
    specs = make_specs(base, cfg)
    run_id = "backtest_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = RESULTS_DIR / run_id
    out.mkdir(parents=True)
    print(f"{run_id}: {len(days)} days, train {train[0]}..{train[-1]}, test {test[0]}..{test[-1]}, "
          f"{len(specs)} parameter sets per pass")

    results = {}
    with ProcessPoolExecutor(max_workers=a.workers) as ex:
        for r in ex.map(_day_worker, [(d.isoformat(), specs) for d in days]):
            results[r["day"]] = r
            print(f"  {r['day']}: {r['seconds']}s")

    rows = []
    for d, r in results.items():
        for name, run in r["runs"].items():
            model, hs, sk = name.split("|")
            st = {k: v for k, v in run["stats"].items() if k != "markout_bps"}
            rows.append({"day": d, "set": "train" if date.fromisoformat(d) in train else "test", "fill_model": model,
                         "min_half_spread_ticks": float(hs[3:]), "skew_ticks_at_max": float(sk[5:]), **st})
    table = pd.DataFrame(rows)
    table.to_csv(out / "all_runs_per_day.csv", index=False)

    keys = ["fill_model", "min_half_spread_ticks", "skew_ticks_at_max"]
    summary: dict = {"train_days": [d.isoformat() for d in train], "test_days": [d.isoformat() for d in test], "models": {}}
    for model in FILL_MODELS:
        tr = table[(table.fill_model == model) & (table.set == "train")]
        grid = []
        for (hs, sk), g in tr.groupby(["min_half_spread_ticks", "skew_ticks_at_max"]):
            grid.append({"min_half_spread_ticks": hs, "skew_ticks_at_max": sk, "fills": int(g.fills.sum()),
                         "pnl_per_volume_bps_mean": round(float(g.pnl_per_volume_bps.mean()), 3),
                         **analytics.per_day_distribution(g.pnl_total)})
        grid = pd.DataFrame(grid)
        grid.to_csv(out / f"cv_train_{model}.csv", index=False)
        plots.sweep_heatmap(grid, "pnl_per_day_mean", out / f"sweep_train_pnl_{model}.png",
                            title=f"Train PnL per day (USDT), {model}")
        best = grid.sort_values("pnl_per_day_mean", ascending=False).iloc[0]
        hs, sk = float(best.min_half_spread_ticks), float(best.skew_ticks_at_max)
        te = table[(table.fill_model == model) & (table.set == "test") &
                   (table.min_half_spread_ticks == hs) & (table.skew_ticks_at_max == sk)].sort_values("day")
        te.to_csv(out / f"cv_test_per_day_{model}.csv", index=False)
        name = spec_name(model, hs, sk)
        curve = _pool_curve([results[d.isoformat()]["runs"][name]["mo_sums"] for d in test])
        curve.to_csv(out / f"markout_curve_test_{model}.csv", index=False)
        hl = te_hl = None
        hl_name = spec_name(model, HL_EQUIVALENT["min_half_spread_ticks"], HL_EQUIVALENT["skew_ticks_at_max"])
        te_hl = table[(table.fill_model == model) & (table.set == "test") &
                      (table.min_half_spread_ticks == HL_EQUIVALENT["min_half_spread_ticks"]) &
                      (table.skew_ticks_at_max == HL_EQUIVALENT["skew_ticks_at_max"])]
        hl_curve = _pool_curve([results[d.isoformat()]["runs"][hl_name]["mo_sums"] for d in test])
        summary["models"][model] = {
            "selected_on_train": {"min_half_spread_ticks": hs, "skew_ticks_at_max": sk,
                                  "train": {k: best[k] for k in ("pnl_per_day_mean", "pnl_per_day_stderr", "profitable_days")}},
            "test": {**analytics.per_day_distribution(te.pnl_total),
                     "pnl_spread_capture": round(float(te.pnl_spread_capture.sum()), 2),
                     "pnl_inventory": round(float(te.pnl_inventory.sum()), 2),
                     "pnl_fees": round(float(te.pnl_fees.sum()), 2),
                     "fills": int(te.fills.sum()),
                     "pnl_per_volume_bps": round(float(te.pnl_total.sum() / te.volume_notional.sum() * 1e4), 3)
                     if te.volume_notional.sum() else None,
                     "markout_bps": {f"{h:g}s": round(v, 3) for h, v in zip(curve.horizon_s, curve["all"])}},
            "hl_equivalent_test": {"params": HL_EQUIVALENT, **analytics.per_day_distribution(te_hl.pnl_total),
                                   "fills": int(te_hl.fills.sum()),
                                   "pnl_per_volume_bps": round(float(te_hl.pnl_total.sum() / te_hl.volume_notional.sum() * 1e4), 3)
                                   if te_hl.volume_notional.sum() else None,
                                   "markout_bps": {f"{h:g}s": round(v, 3) for h, v in zip(hl_curve.horizon_s, hl_curve["all"])}},
            "train_grid": grid.to_dict(orient="records"),
        }
        plots.markout_curve(curve, out / f"markout_curve_test_{model}.png",
                            title=f"Fill markouts, held-out days ({model}, hs={hs:g}, skew={sk:g})")
        for d in test:
            st = results[d.isoformat()]["runs"][name]["states_1min"]
            plots.pnl_attribution(st, out / f"pnl_attribution_{model}_{d}.png",
                                  title=f"PnL attribution {d} ({model}, hs={hs:g}, skew={sk:g})")
            plots.inventory(st, base.max_inventory, out / f"inventory_{model}_{d}.png",
                            title=f"Inventory {d} (dashed = limits)")

    # reference output for the Rust port: chosen l2_queue params, last held-out day
    ref_day = a.reference_day or test[-1]
    sel = summary["models"]["l2_queue"]["selected_on_train"]
    ref_params = replace(base, min_half_spread_ticks=sel["min_half_spread_ticks"], skew_ticks_at_max=sel["skew_ticks_at_max"])
    ref_dir = out / f"reference_{ref_day}"
    ref_stats = run_reference(ref_day, ref_params, replace(cfg, fill_model="l2_queue"), ref_dir)
    keep = REFERENCE_DIR / f"sim_bybit_{ref_day}"
    if keep.exists():
        shutil.rmtree(keep)
    shutil.copytree(ref_dir, keep)
    summary["reference"] = {"day": ref_day.isoformat(), "dir": str(keep.relative_to(REPO_ROOT)),
                            "pnl_total": ref_stats["pnl_total"], "fills": ref_stats["fills"]}

    config = {
        "run_id": run_id, "kind": "backtest_cv", "symbol": SYMBOL, "category": "linear",
        "days": [d.isoformat() for d in days], "train_days": summary["train_days"], "test_days": summary["test_days"],
        "git_commit": git_commit(), "base_params": asdict(base), "sim_config": asdict(cfg),
        "grid": {"min_half_spread_ticks": GRID_HS_TICKS, "skew_ticks_at_max": GRID_SKEW_TICKS, "fill_model": list(FILL_MODELS)},
        "selection": "max mean per-day PnL on train days, per fill model",
        "inputs": {d: {"book_source_sha256": r["book_sha256"], "trades_source_sha256": r["trades_sha256"]} for d, r in results.items()},
        "tick_size": read_units(norm_path("book_updates", SYMBOL, days[0])).tick_size,
        "lot_size": read_units(norm_path("book_updates", SYMBOL, days[0])).lot_size,
    }
    (out / "config.json").write_text(json.dumps(config, indent=2, default=str))
    (out / "cv_summary.json").write_text(json.dumps(summary, indent=2, default=str))
    print(json.dumps({m: {"selected": v["selected_on_train"], "test": {k: v["test"][k] for k in
                                                                       ("pnl_per_day_mean", "pnl_per_day_stderr", "profitable_days")}}
                      for m, v in summary["models"].items()}, indent=2, default=str))
    print(f"results: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
