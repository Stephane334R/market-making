"""L2 order book rebuild and integrity checks (snapshot + delta stream).

Processing rules come from Bybit's official orderbook docs
(https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook):
  * size 0 -> delete the level; unknown price -> insert; known price -> overwrite size
  * a new ``snapshot`` resets the local book
  * ``u == 1`` is a snapshot sent after a service restart; overwrite the local book

Gap detection relies on an EMPIRICAL property, not a documented one: in the archive
files ``u`` (update id) increases by exactly 1 per message (checked on a full day:
863,876 of 863,876 consecutive deltas had du == 1). A delta whose ``u`` is not
``prev_u + 1`` is counted as a sequence gap. ``seq`` (cross sequence) is only
checked for being non-decreasing, since it is shared with other streams and jumps.

Prices and sizes are integer ticks / lots (see ``mm.units``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Sequence

Level = tuple[int, int]  # (price_ticks, size_lots)


@dataclass
class BookMessage:
    """One snapshot or delta message, after unit conversion."""

    ts_exchange_ms: int
    seq: int
    update_id: int
    is_snapshot: bool
    bids: Sequence[Level]
    asks: Sequence[Level]


class L2Book:
    """Price-level book: ``{price_ticks: size_lots}`` per side."""

    def __init__(self) -> None:
        self.bids: dict[int, int] = {}
        self.asks: dict[int, int] = {}

    def reset(self, bids: Iterable[Level], asks: Iterable[Level]) -> None:
        self.bids = {p: s for p, s in bids if s != 0}
        self.asks = {p: s for p, s in asks if s != 0}

    def apply_delta(self, bids: Iterable[Level], asks: Iterable[Level]) -> int:
        """Apply delta levels; return how many deletes hit a non-existent level."""
        missing = 0
        for book, levels in ((self.bids, bids), (self.asks, asks)):
            for p, s in levels:
                if s == 0:
                    if book.pop(p, None) is None:
                        missing += 1
                else:
                    book[p] = s
        return missing

    def best_bid(self) -> int | None:
        return max(self.bids) if self.bids else None

    def best_ask(self) -> int | None:
        return min(self.asks) if self.asks else None


@dataclass
class Event:
    kind: str  # gap | resync | service_restart | crossed | locked | snapshot_mismatch | ...
    update_id: int
    ts_exchange_ms: int
    detail: str = ""


@dataclass
class BookReport:
    messages: int = 0
    snapshots: int = 0
    deltas: int = 0
    rows: int = 0
    gaps: int = 0  # deltas with u != prev_u + 1
    missing_updates: int = 0  # sum of (u - prev_u - 1) over forward gaps
    resyncs: int = 0  # snapshots that start a new book (incl. service restarts)
    service_restarts: int = 0  # snapshots with u == 1
    snapshot_checks: int = 0  # snapshots repeating prev u: rebuilt book compared to them
    snapshot_mismatches: int = 0
    deltas_before_snapshot: int = 0
    crossed: int = 0  # messages after which best bid > best ask
    locked: int = 0  # messages after which best bid == best ask
    one_sided: int = 0  # messages after which a side is empty
    delete_missing_level: int = 0
    seq_decreases: int = 0
    ts_decreases: int = 0
    max_ts_interval_ms: int = 0
    intervals_over_1s: int = 0
    max_depth_bids: int = 0
    max_depth_asks: int = 0
    depth_over_limit: int = 0  # messages after which a side exceeds the stated depth
    first_update_id: int | None = None
    last_update_id: int | None = None
    first_ts_ms: int | None = None
    last_ts_ms: int | None = None
    events: list[Event] = field(default_factory=list)

    def to_dict(self, max_events: int = 50) -> dict:
        d = {k: v for k, v in self.__dict__.items() if k != "events"}
        d["events_total"] = len(self.events)
        d["events"] = [e.__dict__ for e in self.events[:max_events]]
        return d


class BookValidator:
    """Feed messages in file order; rebuilds the book and records every anomaly.

    Nothing is dropped or reordered: after a gap the deltas are still applied (the book
    may then be wrong until the next snapshot, which is exactly what a live consumer
    would experience), and the gap is recorded as an event.
    """

    def __init__(self, depth_limit: int | None = None, max_events: int = 10_000):
        self.book = L2Book()
        self.depth_limit = depth_limit
        self.max_events = max_events
        self.r = BookReport()
        self._have_book = False
        self._prev_u: int | None = None
        self._prev_seq: int | None = None
        self._prev_ts: int | None = None

    def _event(self, kind: str, m: BookMessage, detail: str = "") -> None:
        if len(self.r.events) < self.max_events:
            self.r.events.append(Event(kind, m.update_id, m.ts_exchange_ms, detail))

    def feed(self, m: BookMessage) -> None:
        r = self.r
        r.messages += 1
        r.rows += len(m.bids) + len(m.asks)
        if r.first_update_id is None:
            r.first_update_id, r.first_ts_ms = m.update_id, m.ts_exchange_ms
        r.last_update_id, r.last_ts_ms = m.update_id, m.ts_exchange_ms

        if self._prev_seq is not None and m.seq < self._prev_seq:
            r.seq_decreases += 1
            self._event("seq_decrease", m, f"{self._prev_seq} -> {m.seq}")
        if self._prev_ts is not None:
            dt = m.ts_exchange_ms - self._prev_ts
            if dt < 0:
                r.ts_decreases += 1
                self._event("ts_decrease", m, f"dt={dt}ms")
            r.max_ts_interval_ms = max(r.max_ts_interval_ms, dt)
            if dt > 1000:
                r.intervals_over_1s += 1
                self._event("ts_interval_over_1s", m, f"dt={dt}ms")

        if m.is_snapshot:
            r.snapshots += 1
            self._on_snapshot(m)
        else:
            r.deltas += 1
            if not self._have_book:
                r.deltas_before_snapshot += 1
                self._event("delta_before_snapshot", m)
            else:
                pu = self._prev_u
                if pu is not None and m.update_id != pu + 1:
                    r.gaps += 1
                    if m.update_id > pu + 1:
                        r.missing_updates += m.update_id - pu - 1
                    self._event("gap", m, f"prev_u={pu} u={m.update_id}")
                r.delete_missing_level += self.book.apply_delta(m.bids, m.asks)

        self._prev_u, self._prev_seq, self._prev_ts = m.update_id, m.seq, m.ts_exchange_ms
        if self._have_book:
            self._check_book(m)

    def _on_snapshot(self, m: BookMessage) -> None:
        r = self.r
        if self._have_book and m.update_id == self._prev_u and m.update_id != 1:
            # Same update id as the last delta: the snapshot describes the book we
            # should already have. Compare instead of blindly resetting.
            r.snapshot_checks += 1
            exp_b = {p: s for p, s in m.bids if s}
            exp_a = {p: s for p, s in m.asks if s}
            if exp_b != self.book.bids or exp_a != self.book.asks:
                r.snapshot_mismatches += 1
                self._event("snapshot_mismatch", m, _diff_summary(self.book, exp_b, exp_a))
        else:
            r.resyncs += 1
            if m.update_id == 1:
                r.service_restarts += 1
                self._event("service_restart", m)
            elif self._have_book:
                self._event("resync", m, f"prev_u={self._prev_u} u={m.update_id}")
        self.book.reset(m.bids, m.asks)
        self._have_book = True

    def _check_book(self, m: BookMessage) -> None:
        r, b = self.r, self.book
        nb, na = len(b.bids), len(b.asks)
        r.max_depth_bids = max(r.max_depth_bids, nb)
        r.max_depth_asks = max(r.max_depth_asks, na)
        if self.depth_limit is not None and (nb > self.depth_limit or na > self.depth_limit):
            r.depth_over_limit += 1
            self._event("depth_over_limit", m, f"bids={nb} asks={na}")
        if not nb or not na:
            r.one_sided += 1
            self._event("one_sided", m, f"bids={nb} asks={na}")
            return
        bb, ba = max(b.bids), min(b.asks)
        if bb > ba:
            r.crossed += 1
            self._event("crossed", m, f"best_bid={bb} best_ask={ba}")
        elif bb == ba:
            r.locked += 1
            self._event("locked", m, f"best_bid={bb} best_ask={ba}")


def _diff_summary(book: L2Book, exp_b: dict, exp_a: dict) -> str:
    def n_diff(have: dict, exp: dict) -> int:
        return sum(1 for k in have.keys() | exp.keys() if have.get(k) != exp.get(k))

    return f"bid_levels_differ={n_diff(book.bids, exp_b)} ask_levels_differ={n_diff(book.asks, exp_a)}"
