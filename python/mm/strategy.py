"""Quoting logic: volatility-scaled spread with inventory skew.

Ported from crypto-market-making (mmlab/strategy.py), same logic, adapted to the
common schema: prices are integer TICKS (mid may be a half tick), sizes are in coin.

Practical simplification of Avellaneda & Stoikov (2008):

    half_spread_bps = max(min_half_spread_ticks * tick_bps, min_half_spread_bps,
                          vol_mult * sigma_bps(vol_horizon_s))
    skew_bps        = skew_ticks_at_max * tick_bps * (q / max_inventory)
    r               = mid * (1 - skew_bps / 1e4)
    bid, ask        = r * (1 -/+ half_spread_bps / 1e4), rounded away from mid to ticks
    post-only clamp : bid <= best_ask - 1 tick, ask >= best_bid + 1 tick

``sigma_bps(h)`` is the expected mid move over ``h`` seconds from a time-aware EWMA of
squared log mid returns. ``vol_horizon_s`` defaults to the quote's real exposure
(requote interval + one latency hop).

Floors are in ticks so a grid means the same thing across instruments. NOTE for Bybit:
the BTCUSDT tick is 0.10 USDT (~0.013 bps), 10x finer than Hyperliquid BTC's $1, so the
same number of ticks is 10x closer to mid; see backtest.py for how the grid is mapped.
"""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class QuoterParams:
    order_size: float = 0.02  # coin per quote
    max_inventory: float = 0.10  # coin, absolute
    min_half_spread_ticks: float = 1.0
    min_half_spread_bps: float = 0.0
    vol_mult: float = 0.5
    vol_horizon_s: float = 0.3
    skew_ticks_at_max: float = 2.0
    vol_halflife_s: float = 60.0


class EwmaVol:
    """Time-aware EWMA of the variance per millisecond of log mid returns."""

    def __init__(self, halflife_s: float, init_bps_per_sqrt_s: float = 0.5):
        self.halflife_ms = halflife_s * 1000
        self.var_per_ms = (init_bps_per_sqrt_s * 1e-4) ** 2 / 1000
        self.last_ts: int | None = None
        self.last_mid: float | None = None

    def update(self, ts: int, mid: float) -> None:
        if self.last_ts is not None and ts > self.last_ts:
            dt = ts - self.last_ts
            r = math.log(mid / self.last_mid)
            w = 1 - 0.5 ** (dt / self.halflife_ms)
            self.var_per_ms = (1 - w) * self.var_per_ms + w * (r * r / dt)
        self.last_ts, self.last_mid = ts, mid

    def sigma_bps(self, horizon_s: float) -> float:
        return math.sqrt(self.var_per_ms * horizon_s * 1000) * 1e4


@dataclass
class Quote:
    bid_px: int | None  # ticks
    ask_px: int | None  # ticks
    bid_sz: float  # coin
    ask_sz: float
    half_spread_bps: float
    skew_bps: float


class InventorySkewQuoter:
    """``vol`` may be shared between quoters that see the same mid series (the
    simulator runs many parameter sets in one replay and updates one EwmaVol)."""

    def __init__(self, params: QuoterParams = QuoterParams(), vol: EwmaVol | None = None):
        self.p = params
        self.vol = vol if vol is not None else EwmaVol(params.vol_halflife_s)

    def on_mid(self, ts: int, mid: float) -> None:
        self.vol.update(ts, mid)

    def quote(self, mid: float, best_bid: int, best_ask: int, inventory: float) -> Quote:
        """``mid``, ``best_bid``, ``best_ask`` in ticks; ``inventory`` in coin."""
        p = self.p
        tick_bps = 1e4 / mid
        half = max(p.min_half_spread_ticks * tick_bps, p.min_half_spread_bps,
                   p.vol_mult * self.vol.sigma_bps(p.vol_horizon_s))
        skew = p.skew_ticks_at_max * tick_bps * inventory / p.max_inventory
        r = mid * (1 - skew / 1e4)
        bid = math.floor(r * (1 - half / 1e4))
        ask = math.ceil(r * (1 + half / 1e4))
        bid = min(bid, best_ask - 1)  # post-only: never cross the current book
        ask = max(ask, best_bid + 1)
        bid_sz = max(0.0, min(p.order_size, p.max_inventory - inventory))
        ask_sz = max(0.0, min(p.order_size, p.max_inventory + inventory))
        return Quote(bid if bid_sz > 0 else None, ask if ask_sz > 0 else None,
                     bid_sz, ask_sz, half, skew)
