"""PnL attribution, fill markouts and summary statistics.

Ported from crypto-market-making (mmlab/analytics.py), same definitions; inputs are the
simulator's fills/states DataFrames and the replayed mid series (ts, mid) arrays.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

MARKOUT_HORIZONS_S = (0.1, 0.5, 1, 2, 5, 10, 30, 60)


def markouts(fills: pd.DataFrame, mid_ts: np.ndarray, mid: np.ndarray,
             horizons_s=MARKOUT_HORIZONS_S) -> pd.DataFrame:
    """Per-fill markout in bps: side * (mid(t + h) - fill_px) / fill_px.

    Positive = the fill made money h seconds later. h=0 uses the mid at the fill (the
    spread captured); the decay as h grows is adverse selection. mid(t+h) is the last
    book mid at or before t+h; fills whose t+h is past the session end are NaN.
    """
    if fills.empty or len(mid_ts) == 0:
        return pd.DataFrame()
    ts = fills["ts"].to_numpy(np.int64)
    side = fills["side"].to_numpy(float)
    px = fills["px"].to_numpy(float)
    out = {"ts": ts, "side": fills["side"].to_numpy(), "qty": fills["qty"].to_numpy()}
    out["h_0s"] = side * (fills["mid"].to_numpy(float) - px) / px * 1e4
    for h in horizons_s:
        target = ts + int(h * 1000)
        idx = np.clip(np.searchsorted(mid_ts, target, side="right") - 1, 0, len(mid_ts) - 1)
        mo = side * (mid[idx] - px) / px * 1e4
        out[f"h_{h:g}s"] = np.where(target <= mid_ts[-1], mo, np.nan)
    return pd.DataFrame(out)


def markout_curve(mo: pd.DataFrame) -> pd.DataFrame:
    """Size-weighted average markout per horizon, all fills and by side."""
    cols = [c for c in mo.columns if c.startswith("h_")]
    rows = []
    for c in cols:
        row = {"horizon_s": float(c[2:-1])}
        for name, sel in (("all", mo), ("buys", mo[mo["side"] == 1]), ("sells", mo[mo["side"] == -1])):
            v = sel[[c, "qty"]].dropna()
            row[name] = float(np.average(v[c], weights=v["qty"])) if len(v) else np.nan
        rows.append(row)
    return pd.DataFrame(rows).sort_values("horizon_s").reset_index(drop=True)


def summary(states: pd.DataFrame, fills: pd.DataFrame, mo: pd.DataFrame | None = None,
            max_inventory: float | None = None) -> dict:
    s, f = states, fills
    last = s.iloc[-1]
    hours = (s["ts"].iloc[-1] - s["ts"].iloc[0]) / 3.6e6
    notional = float((f["px"] * f["qty"]).sum()) if len(f) else 0.0
    eq = s.set_index(pd.to_datetime(s["ts"], unit="ms"))["equity"]
    hourly = eq.resample("1h").last().diff().dropna()
    out = {
        "hours": round(hours, 2),
        "fills": int(len(f)),
        "fills_through_pct": round(100 * float(f["through"].mean()), 1) if len(f) else None,
        "volume_notional": round(notional, 2),
        "pnl_total": round(float(last["equity"]), 2),
        "pnl_spread_capture": round(float(last["spread_capture"]), 2),
        "pnl_inventory": round(float(last["inventory_pnl"]), 2),
        "pnl_fees": round(-float(last["fees"]), 2),
        "pnl_per_volume_bps": round(float(last["equity"]) / notional * 1e4, 3) if notional else None,
        "max_abs_inventory": round(float(s["inventory"].abs().max()), 4),
        "time_at_inventory_limit_pct": (
            round(100 * float((s["inventory"].abs() >= max_inventory - 1e-12).mean()), 2)
            if max_inventory is not None else None),
        "hourly_pnl_mean": round(float(hourly.mean()), 2) if len(hourly) else None,
        "hourly_pnl_std": round(float(hourly.std()), 2) if len(hourly) > 1 else None,
        "max_drawdown": round(float((eq - eq.cummax()).min()), 2),
    }
    if mo is not None and not mo.empty:
        curve = markout_curve(mo).set_index("horizon_s")["all"]
        out["markout_bps"] = {f"{h:g}s": (None if np.isnan(v) else round(float(v), 3)) for h, v in curve.items()}
    return out


def per_day_distribution(pnl: list[float] | np.ndarray) -> dict:
    """Mean per-day PnL with its standard error - the honest headline number."""
    v = np.asarray(pnl, float)
    n = len(v)
    sd = float(v.std(ddof=1)) if n > 1 else float("nan")
    se = sd / np.sqrt(n) if n > 1 else float("nan")
    return {"days": n, "pnl_per_day_mean": round(float(v.mean()), 2) if n else None,
            "pnl_per_day_std": round(sd, 2) if n > 1 else None,
            "pnl_per_day_stderr": round(se, 2) if n > 1 else None,
            "t_stat": round(float(v.mean() / se), 2) if n > 1 and se > 0 else None,
            "profitable_days": f"{int((v > 0).sum())}/{n}"}
