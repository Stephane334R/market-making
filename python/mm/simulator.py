"""Event-driven market-making simulator on the common schema (L2 deltas + trades).

Ported from crypto-market-making (mmlab/simulator.py). Same event logic:

* One merged event stream in exchange time: book messages and trades; at equal
  timestamps the book message is processed first (as in the original lexsort).
* A quote decided at a book update goes live ``latency_ms`` later, at the first event
  at or after that time. If it would cross the book then, it is rejected (post-only).
* Requote every ``requote_ms`` (checked on book updates).
* Maker fee on every fill; no taker trades.
* Accounting, everything marked to mid (identity checked in tests)::

      equity = cash + q * mid = spread_capture + inventory_pnl - fees
      spread_capture = sum over fills of side * qty * (mid_at_fill - fill_px)
      inventory_pnl  = sum over mid changes of q * (mid_t - mid_{t-1})

What changes vs the original, and why
-------------------------------------
* The book is rebuilt from FULL-DEPTH deltas (Bybit ob200 every 100 ms) instead of
  5-level snapshots, so the displayed size at our exact price is known at every update.
* Two fill models (``SimConfig.fill_model``):

  - ``"touch_ratio"`` - the ORIGINAL model: a print through our price fills our full
    remaining size; a print AT our price fills ``touch_fill_ratio * print size``.
    Every activation resets the order (fresh size, no queue state).
  - ``"l2_queue"`` - NEW, the default: L2 queue-position estimate. SIMPLIFICATION,
    labelled: we see price levels, not individual orders, so our place in the queue
    is ESTIMATED:
      * joining a level: queue ahead = displayed size at our price (our own order is
        not in the replayed book: no market impact);
      * a print at our price consumes the queue ahead first, the excess fills us;
      * a print through our price fills our full remaining size (as in the original);
      * a size decrease at our level not explained by prints at our price is
        treated as cancellations spread pro-rata over the queue (those ahead of us
        cancel in proportion to their share); increases join behind us;
      * the book is conflated to 100 ms, so adds and cancels inside one update net
        out (we may under-estimate cancels ahead of us);
      * requoting at the SAME price with size <= remaining keeps the queue position
        (amend down); a larger size or a new price is cancel-replace to the back.

* Inventory PnL is accumulated lazily at fills and samples (telescoping sum, exactly
  equal to the original per-update sum).
* States are sampled every ``sample_ms`` (default 5 s) instead of at every book update,
  to bound memory over ~864k updates/day x many parameter sets. Hourly PnL, drawdown
  and time-at-limit are computed on these samples (SIMPLIFICATION: drawdown between
  samples is not seen).
* Many parameter sets are replayed in ONE pass over a day (the book rebuild dominates
  the cost). They share the event stream, the volatility estimate, the requote clock
  and the latency; each has its own orders, inventory and fill model.
"""

from __future__ import annotations

from array import array
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Iterator

import pandas as pd
import pyarrow.parquet as pq

from mm.book import BookMessage
from mm.normalize_bybit import iter_book_messages, read_units
from mm.strategy import EwmaVol, InventorySkewQuoter, Quote, QuoterParams

FILL_MODELS = ("l2_queue", "touch_ratio")
EPS = 1e-12


@dataclass(frozen=True)
class SimConfig:
    latency_ms: int = 50
    requote_ms: int = 250
    # Bybit USDT perpetual base-tier maker fee 0.02% (bybit.com help center, "Futures
    # Contracts Fees Explained", checked 2026-11-11). Negative = rebate.
    maker_fee: float = 0.0002
    touch_fill_ratio: float = 0.25
    fill_model: str = "l2_queue"
    sample_ms: int = 5_000

    def __post_init__(self):
        if self.fill_model not in FILL_MODELS:
            raise ValueError(f"fill_model must be one of {FILL_MODELS}")


class Order:
    __slots__ = ("px", "rem", "queue", "traded")

    def __init__(self, px: int, rem: float, queue: float):
        self.px = px  # ticks
        self.rem = rem  # coin
        self.queue = queue  # coin ahead of us at our price (l2_queue only)
        self.traded = 0.0  # coin printed at our price since the last level update


class Runner:
    """One parameter set: quoter, orders, inventory and PnL accounting."""

    def __init__(self, name: str, params: QuoterParams, cfg: SimConfig, vol: EwmaVol):
        self.name, self.params, self.cfg = name, params, cfg
        self.quoter = InventorySkewQuoter(params, vol)
        self.l2 = cfg.fill_model == "l2_queue"
        self.q = self.cash = self.edge = self.inv = self.fees = 0.0
        self.mark_mid: float | None = None
        self.bid: Order | None = None
        self.ask: Order | None = None
        self.pending: Quote | None = None
        self.n_quotes = self.n_rejects = 0
        self.half_spread_sum = 0.0
        self.fills: list[tuple] = []
        self.s_ts, self.s_mid, self.s_q = array("q"), array("d"), array("d")
        self.s_cash, self.s_edge, self.s_inv, self.s_fee = array("d"), array("d"), array("d"), array("d")

    # -- accounting ---------------------------------------------------------
    def settle(self, mid: float) -> None:
        if self.mark_mid is not None:
            self.inv += self.q * (mid - self.mark_mid)
        self.mark_mid = mid

    def fill(self, ts: int, side: int, px: float, qty: float, mid: float, through: bool) -> None:
        self.settle(mid)
        fee = self.cfg.maker_fee * px * qty
        self.edge += side * qty * (mid - px)
        self.cash -= side * px * qty + fee
        self.fees += fee
        self.q += side * qty
        self.fills.append((ts, side, px, qty, mid, fee, self.q, through))

    def sample(self, ts: int, mid: float) -> None:
        self.settle(mid)
        self.s_ts.append(ts); self.s_mid.append(mid); self.s_q.append(self.q)
        self.s_cash.append(self.cash); self.s_edge.append(self.edge)
        self.s_inv.append(self.inv); self.s_fee.append(self.fees)

    # -- results ------------------------------------------------------------
    def fills_df(self) -> pd.DataFrame:
        return pd.DataFrame(self.fills, columns=["ts", "side", "px", "qty", "mid", "fee", "inventory", "through"])

    def states_df(self) -> pd.DataFrame:
        s = pd.DataFrame({"ts": self.s_ts, "mid": self.s_mid, "inventory": self.s_q, "cash": self.s_cash,
                          "spread_capture": self.s_edge, "inventory_pnl": self.s_inv, "fees": self.s_fee})
        s["equity"] = s["cash"] + s["inventory"] * s["mid"]
        return s

    def stats(self) -> dict:
        return {"quotes": self.n_quotes, "post_only_rejects": self.n_rejects,
                "mean_half_spread_bps": self.half_spread_sum / self.n_quotes if self.n_quotes else None}


class Engine:
    """Replays one session (one day) for several runners sharing the same clock."""

    def __init__(self, runners: list[Runner], tick_size: float, lot_size: float, vol: EwmaVol):
        cfgs = {(r.cfg.latency_ms, r.cfg.requote_ms, r.cfg.sample_ms) for r in runners}
        if len(cfgs) != 1:
            raise ValueError("runners in one pass must share latency_ms, requote_ms and sample_ms")
        self.latency_ms, self.requote_ms, self.sample_ms = cfgs.pop()
        self.runners, self.tick, self.lot, self.vol = runners, tick_size, lot_size, vol
        self.bids: dict[int, int] = {}
        self.asks: dict[int, int] = {}
        self.watch_b: dict[int, list] = {}  # price -> [(runner, order)] resting l2 bids
        self.watch_a: dict[int, list] = {}
        self.best_bid = self.best_ask = None
        self.mid_ticks: float | None = None
        self.mid: float | None = None  # USDT
        self.pending_ts: int | None = None
        self.last_quote_ts = -(10**18)
        self.next_sample = -(10**18)
        self.max_bid = self.min_ask = None  # bounds over resting orders, to skip trades fast
        self.mids_ts, self.mids = array("q"), array("d")
        self.last_ts: int | None = None

    # -- order bookkeeping ----------------------------------------------------
    def _unwatch(self, r: Runner, o: Order | None, watch: dict) -> None:
        if o is None or not r.l2:
            return
        lst = watch.get(o.px)
        if lst:
            lst[:] = [x for x in lst if x[1] is not o]
            if not lst:
                del watch[o.px]

    def _place(self, r: Runner, side: int, px: int | None, sz: float) -> None:
        book, watch = (self.bids, self.watch_b) if side == 1 else (self.asks, self.watch_a)
        cur = r.bid if side == 1 else r.ask
        if px is None:
            new = None
        elif not r.l2:
            new = Order(px, sz, 0.0)  # original semantics: fresh order every activation
        elif cur is not None and cur.px == px and sz <= cur.rem + EPS:
            cur.rem = sz  # amend down: keep queue position
            return
        else:
            new = Order(px, sz, book.get(px, 0) * self.lot)  # join the back of the level
        self._unwatch(r, cur, watch)
        if new is not None and r.l2:
            watch.setdefault(px, []).append((r, new))
        if side == 1:
            r.bid = new
        else:
            r.ask = new

    def _activate(self) -> None:
        bb, ba = self.best_bid, self.best_ask
        for r in self.runners:
            qt = r.pending
            if qt is None:
                continue
            r.pending = None
            bid_ok = qt.bid_px is not None and qt.bid_px < ba
            ask_ok = qt.ask_px is not None and qt.ask_px > bb
            r.n_rejects += (qt.bid_px is not None and not bid_ok) + (qt.ask_px is not None and not ask_ok)
            self._place(r, 1, qt.bid_px if bid_ok else None, qt.bid_sz)
            self._place(r, -1, qt.ask_px if ask_ok else None, qt.ask_sz)
        self._bounds()

    def _bounds(self) -> None:
        bids = [r.bid.px for r in self.runners if r.bid is not None and r.bid.rem > EPS]
        asks = [r.ask.px for r in self.runners if r.ask is not None and r.ask.rem > EPS]
        self.max_bid = max(bids) if bids else None
        self.min_ask = min(asks) if asks else None

    def _event_start(self, ts: int) -> None:
        if self.pending_ts is not None and self.pending_ts <= ts and self.mid is not None:
            self.pending_ts = None
            self._activate()
        self.last_ts = ts

    # -- level updates (queue estimate) ---------------------------------------
    def _level_change(self, lst: list, old_lots: int, new_lots: int) -> None:
        old, new = old_lots * self.lot, new_lots * self.lot
        for _, o in lst:
            traded, o.traded = o.traded, 0.0
            others = old - traded  # displayed size after prints, before cancels/adds
            cancels = (old - new) - traded
            if cancels > EPS and others > EPS:
                o.queue -= cancels * o.queue / others
            o.queue = min(max(o.queue, 0.0), new)

    def _apply_book(self, m: BookMessage) -> None:
        if m.is_snapshot:
            old_b, old_a = self.bids, self.asks
            self.bids = {p: s for p, s in m.bids if s}
            self.asks = {p: s for p, s in m.asks if s}
            for watch, old, new in ((self.watch_b, old_b, self.bids), (self.watch_a, old_a, self.asks)):
                for px, lst in watch.items():
                    if old.get(px, 0) != new.get(px, 0):
                        self._level_change(lst, old.get(px, 0), new.get(px, 0))
            return
        for book, watch, levels in ((self.bids, self.watch_b, m.bids), (self.asks, self.watch_a, m.asks)):
            for p, s in levels:
                old = book.get(p, 0)
                if s:
                    book[p] = s
                else:
                    book.pop(p, None)
                if old != s and p in watch:
                    self._level_change(watch[p], old, s)

    # -- events ------------------------------------------------------------------
    def on_book(self, m: BookMessage) -> None:
        ts = m.ts_exchange_ms
        self._event_start(ts)
        self._apply_book(m)
        if not self.bids or not self.asks:
            return  # one-sided book: keep the last mid (never seen in validated data)
        self.best_bid, self.best_ask = max(self.bids), min(self.asks)
        self.mid_ticks = (self.best_bid + self.best_ask) / 2
        self.mid = self.mid_ticks * self.tick
        self.vol.update(ts, self.mid_ticks)
        self.mids_ts.append(ts)
        self.mids.append(self.mid)
        if ts - self.last_quote_ts >= self.requote_ms:
            for r in self.runners:
                qt = r.quoter.quote(self.mid_ticks, self.best_bid, self.best_ask, r.q)
                r.pending = qt
                r.n_quotes += 1
                r.half_spread_sum += qt.half_spread_bps
            self.pending_ts = ts + self.latency_ms
            self.last_quote_ts = ts
        if ts >= self.next_sample:
            for r in self.runners:
                r.sample(ts, self.mid)
            self.next_sample = ts - ts % self.sample_ms + self.sample_ms

    def on_trade(self, ts: int, side: int, px: int, sz_lots: int) -> None:
        """``side`` = taker side: +1 buyer-initiated (hits asks), -1 seller-initiated."""
        self._event_start(ts)
        if self.mid is None:
            return
        if side == -1:
            if self.max_bid is None or px > self.max_bid:
                return
        elif self.min_ask is None or px < self.min_ask:
            return
        sz = sz_lots * self.lot
        removed = False
        for r in self.runners:
            o = r.bid if side == -1 else r.ask
            if o is None or o.rem <= EPS:
                continue
            if side == -1 and px > o.px or side == 1 and px < o.px:
                continue
            through = px != o.px
            if through:
                qty = o.rem
            elif r.l2:
                qty = min(o.rem, max(0.0, sz - o.queue))
                o.queue = max(0.0, o.queue - sz)
                o.traded += sz
            else:
                qty = min(o.rem, sz * r.cfg.touch_fill_ratio)
            if qty > EPS:
                r.fill(ts, -side, o.px * self.tick, qty, self.mid, through)
                o.rem -= qty
                if o.rem <= EPS:
                    self._unwatch(r, o, self.watch_b if side == -1 else self.watch_a)
                    removed = True
        if removed:
            self._bounds()

    def finish(self) -> None:
        if self.last_ts is not None and self.mid is not None:
            for r in self.runners:
                r.sample(self.last_ts, self.mid)


def iter_trades(path: Path, batch_rows: int = 262_144) -> Iterator[tuple[int, int, int, int]]:
    """(ts_exchange_ms, taker side +1/-1, price ticks, size lots), in file order."""
    pf = pq.ParquetFile(path)
    for b in pf.iter_batches(batch_size=batch_rows, columns=["ts_exchange_ms", "side", "price", "size"]):
        ts, side, px, sz = (b.column(n).to_pylist() for n in ("ts_exchange_ms", "side", "price", "size"))
        for i in range(len(ts)):
            yield ts[i], (1 if side[i] == "buy" else -1), px[i], sz[i]


def replay(book: Iterable[BookMessage], trades: Iterable[tuple[int, int, int, int]], engine: Engine) -> Engine:
    """Merge book messages and trades by exchange time (book first at equal ts)."""
    it = iter(trades)
    nxt = next(it, None)
    for m in book:
        while nxt is not None and nxt[0] < m.ts_exchange_ms:
            engine.on_trade(*nxt)
            nxt = next(it, None)
        engine.on_book(m)
    while nxt is not None:
        engine.on_trade(*nxt)
        nxt = next(it, None)
    engine.finish()
    return engine


@dataclass
class RunSpec:
    name: str
    params: QuoterParams
    cfg: SimConfig


@dataclass
class DayRun:
    engine: Engine
    runners: dict[str, Runner] = field(default_factory=dict)


def run_day(book_path: Path, trades_path: Path, specs: list[RunSpec]) -> DayRun:
    """Replay one normalized day for every spec in a single pass (state starts flat)."""
    units = read_units(book_path)
    halflives = {s.params.vol_halflife_s for s in specs}
    if len(halflives) != 1:
        raise ValueError("specs in one pass must share vol_halflife_s (shared EWMA)")
    vol = EwmaVol(halflives.pop())
    runners = [Runner(s.name, s.params, s.cfg, vol) for s in specs]
    eng = Engine(runners, float(units.tick_size), float(units.lot_size), vol)
    replay(iter_book_messages(book_path), iter_trades(trades_path), eng)
    return DayRun(eng, {r.name: r for r in runners})
