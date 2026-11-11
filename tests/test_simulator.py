"""Simulator tests: the original repo's tests, ported, plus L2 queue-model tests.

Fixtures use tick = lot = 1.0 so prices/sizes are hand-checkable.
"""

import random

import numpy as np
import pytest

from mm import analytics
from mm.book import BookMessage
from mm.simulator import Engine, Runner, SimConfig, replay
from mm.strategy import EwmaVol, InventorySkewQuoter, QuoterParams


def snap(ts, bids, asks, u=1):
    return BookMessage(ts, u, u, True, bids, asks)


def delta(ts, bids=(), asks=(), u=2):
    return BookMessage(ts, u, u, False, list(bids), list(asks))


class FixedQuoter(InventorySkewQuoter):
    """Always quotes a fixed bid/ask (ticks), for hand-checkable tests."""

    def __init__(self, bid, ask, size=1.0, vol=None):
        super().__init__(QuoterParams(order_size=size, max_inventory=10.0), vol)
        self.b, self.a = bid, ask

    def quote(self, mid, best_bid, best_ask, inventory):
        q = super().quote(mid, best_bid, best_ask, inventory)
        q.bid_px, q.ask_px = self.b, self.a
        return q


def run(book, trades, cfg, quoter=None, params=QuoterParams()):
    vol = EwmaVol(params.vol_halflife_s)
    r = Runner("x", params, cfg, vol)
    if quoter is not None:
        quoter.vol = vol
        r.quoter = quoter
    eng = Engine([r], 1.0, 1.0, vol)
    replay(book, trades, eng)
    return r, eng


def synthetic(n_s=1800, seed=1):
    """Random-walk book (1-tick spread, 10 levels per side, random sizes), emitted as
    deltas against the previous state, with prints at and through the touch."""
    rng = random.Random(seed)
    bb = 100_000

    def levels(bb):
        return ({bb - i: rng.randint(1, 9) for i in range(10)},
                {bb + 1 + i: rng.randint(1, 9) for i in range(10)})

    cur_b, cur_a = levels(bb)
    book = [snap(0, sorted(cur_b.items()), sorted(cur_a.items()), u=1)]
    trades = []
    for k in range(1, n_s * 10):
        ts = k * 100
        bb += rng.choice((-1, 0, 0, 1))
        new_b, new_a = levels(bb)
        db = [(p, new_b.get(p, 0)) for p in cur_b.keys() | new_b.keys()]
        da = [(p, new_a.get(p, 0)) for p in cur_a.keys() | new_a.keys()]
        book.append(BookMessage(ts, k + 1, k + 1, False, db, da))
        cur_b, cur_a = new_b, new_a
        for _ in range(rng.randint(0, 3)):
            side = rng.choice((1, -1))
            depth = rng.choice((0, 0, 0, 1, 2, 6))
            px = (bb + 1 + depth) if side == 1 else (bb - depth)
            trades.append((ts + rng.randint(1, 99), side, px, rng.randint(1, 4)))
    trades.sort(key=lambda t: t[0])
    return book, trades


@pytest.mark.parametrize("model", ["l2_queue", "touch_ratio"])
def test_accounting_identity_on_synthetic_data(model):
    book, trades = synthetic()
    p = QuoterParams(order_size=2.0, max_inventory=10.0, min_half_spread_ticks=1, skew_ticks_at_max=2)
    r, _ = run(book, trades, SimConfig(fill_model=model, sample_ms=1000), params=p)
    s = r.states_df()
    assert len(r.fills) > 0
    lhs = s["cash"] + s["inventory"] * s["mid"]
    rhs = s["spread_capture"] + s["inventory_pnl"] - s["fees"]
    assert np.allclose(lhs, rhs, atol=1e-6)


@pytest.mark.parametrize("model", ["l2_queue", "touch_ratio"])
def test_print_through_our_bid_fills_full_size_at_our_price(model):
    book = [snap(0, [(99, 5)], [(101, 5)]), delta(1000), delta(2000, u=3)]
    trades = [(1500, -1, 97, 1)]
    r, _ = run(book, trades, SimConfig(latency_ms=10, requote_ms=10_000, maker_fee=0, fill_model=model),
               FixedQuoter(98, 102, size=1.0))
    f = r.fills_df().iloc[0]
    assert (f["side"], f["px"], f["qty"]) == (1, 98.0, 1.0)
    assert f["through"]


def test_print_at_our_bid_fills_queue_fraction_touch_model():
    book = [snap(0, [(98, 5)], [(101, 5)]), delta(1000)]
    trades = [(500, -1, 98, 2)]
    r, _ = run(book, trades, SimConfig(latency_ms=10, requote_ms=10_000, maker_fee=0,
                                       fill_model="touch_ratio", touch_fill_ratio=0.25),
               FixedQuoter(98, 102, size=1.0))
    assert r.fills_df()["qty"].iloc[0] == pytest.approx(0.5)


def test_quote_not_live_before_latency_elapsed():
    book = [snap(0, [(99, 5)], [(101, 5)]), delta(1000)]
    trades = [(20, -1, 97, 1)]
    r, _ = run(book, trades, SimConfig(latency_ms=50, requote_ms=10_000), FixedQuoter(98, 102))
    assert r.fills_df().empty


def test_crossing_quote_rejected_post_only():
    # quote decided at ts=0 with bid 98; by activation the ask has dropped to 97
    book = [snap(0, [(99, 5)], [(101, 5)]), snap(30, [(96, 5)], [(97, 5)], u=5), delta(1000, u=6)]
    trades = [(60, -1, 95, 1)]
    r, _ = run(book, trades, SimConfig(latency_ms=50, requote_ms=10_000), FixedQuoter(98, 102))
    assert r.fills_df().empty
    assert r.n_rejects == 1


def test_markout_sign():
    book = [snap(0, [(99, 5)], [(101, 5)]), delta(1000), snap(2000, [(103, 5)], [(105, 5)], u=3),
            delta(3000, u=4)]
    trades = [(500, -1, 97, 1)]
    r, eng = run(book, trades, SimConfig(latency_ms=10, requote_ms=10_000, maker_fee=0), FixedQuoter(98, 110))
    mo = analytics.markouts(r.fills_df(), np.asarray(eng.mids_ts), np.asarray(eng.mids), horizons_s=(2,))
    # bought at 98, mid 100 at fill and 104 two seconds later
    assert mo["h_0s"].iloc[0] == pytest.approx(2 / 98 * 1e4)
    assert mo["h_2s"].iloc[0] == pytest.approx(6 / 98 * 1e4)


@pytest.mark.parametrize("model", ["l2_queue", "touch_ratio"])
def test_inventory_limit_respected_exactly_without_latency(model):
    book, trades = synthetic(seed=3)
    p = QuoterParams(max_inventory=5.0, order_size=2.0, min_half_spread_ticks=1, skew_ticks_at_max=0)
    r, _ = run(book, trades, SimConfig(latency_ms=0, fill_model=model, sample_ms=100), params=p)
    assert len(r.fills) > 0
    assert max(abs(x[6]) for x in r.fills) <= p.max_inventory + 1e-9


@pytest.mark.parametrize("model", ["l2_queue", "touch_ratio"])
def test_inventory_overshoot_is_bounded_by_one_order_size(model):
    book, trades = synthetic(seed=3)
    p = QuoterParams(max_inventory=5.0, order_size=2.0, min_half_spread_ticks=1, skew_ticks_at_max=0)
    r, _ = run(book, trades, SimConfig(latency_ms=150, fill_model=model, sample_ms=100), params=p)
    peak = max(abs(x[6]) for x in r.fills)
    assert peak <= p.max_inventory + p.order_size + 1e-9


# ---------------------------------------------------------------- L2 queue model

L2 = SimConfig(latency_ms=10, requote_ms=100_000, maker_fee=0, fill_model="l2_queue")


def test_queue_ahead_is_consumed_before_we_fill():
    book = [snap(0, [(98, 5), (99, 1)], [(101, 5)]), delta(10_000)]
    # we join 98 behind 5; prints at 98: 3 (queue 5->2, no fill), then 4 (2 ahead, 2 for us)
    trades = [(100, -1, 98, 3), (200, -1, 98, 4)]
    r, _ = run(book, trades, L2, FixedQuoter(98, 110, size=3.0))
    f = r.fills_df()
    assert list(f["qty"]) == [pytest.approx(2.0)]
    assert not f["through"].iloc[0]


def test_cancellations_ahead_reduce_queue_pro_rata():
    # level 98 has 10 ahead of us; 5 cancel (no prints): queue -> 5; a print of 6 fills 1
    book = [snap(0, [(98, 10), (99, 1)], [(101, 5)]), delta(100, bids=[(98, 5)]), delta(10_000, u=3)]
    trades = [(200, -1, 98, 6)]
    r, _ = run(book, trades, L2, FixedQuoter(98, 110, size=3.0))
    assert r.fills_df()["qty"].iloc[0] == pytest.approx(1.0)


def test_prints_are_not_double_counted_as_cancels():
    # 10 ahead; a print of 4 at our price (queue 6) and the book then shows 6: no cancels
    book = [snap(0, [(98, 10), (99, 1)], [(101, 5)]), delta(200, bids=[(98, 6)]), delta(10_000, u=3)]
    trades = [(100, -1, 98, 4), (300, -1, 98, 7)]
    r, _ = run(book, trades, L2, FixedQuoter(98, 110, size=3.0))
    assert r.fills_df()["qty"].iloc[0] == pytest.approx(1.0)  # 7 - 6 ahead


def test_new_level_has_empty_queue():
    # nobody at 98.. we are alone: first print at 98 fills us
    book = [snap(0, [(97, 5)], [(101, 5)]), delta(10_000)]
    trades = [(100, -1, 98, 2)]
    r, _ = run(book, trades, L2, FixedQuoter(98, 110, size=3.0))
    assert r.fills_df()["qty"].iloc[0] == pytest.approx(2.0)


def test_requote_same_price_keeps_queue_larger_size_loses_it():
    book = [snap(0, [(98, 10), (99, 1)], [(101, 5)])] + [delta(100 * k, u=k + 1) for k in range(1, 30)]
    q = FixedQuoter(98, 110, size=2.0)
    cfg = SimConfig(latency_ms=10, requote_ms=500, maker_fee=0, fill_model="l2_queue")
    vol = EwmaVol(60)
    r = Runner("x", QuoterParams(), cfg, vol)
    q.vol = vol
    r.quoter = q
    eng = Engine([r], 1.0, 1.0, vol)
    eng.on_book(book[0])
    eng.on_trade(50, -1, 98, 1)  # activates: queue 10 -> 9
    for m in book[1:6]:  # requote at the same price & size: queue kept
        eng.on_book(m)
    assert r.bid.queue == pytest.approx(9.0)
    r.quoter = FixedQuoter(98, 110, size=5.0, vol=vol)  # bigger size: cancel-replace
    for m in book[6:12]:
        eng.on_book(m)
    assert r.bid.queue == pytest.approx(10.0)  # back of the displayed level
