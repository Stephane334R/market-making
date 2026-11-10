"""Delta application, crossed-book detection and gap detection on small fixtures."""

from mm.book import BookMessage, BookValidator, L2Book


def snap(u, bids, asks, seq=None, ts=None):
    return BookMessage(ts or 1000 + u, seq or 100 + u, u, True, bids, asks)


def delta(u, bids=(), asks=(), seq=None, ts=None):
    return BookMessage(ts or 1000 + u, seq or 100 + u, u, False, list(bids), list(asks))


def test_delta_insert_update_delete():
    b = L2Book()
    b.reset([(100, 5), (99, 3)], [(101, 2), (102, 7)])
    missing = b.apply_delta(
        bids=[(100, 0), (99, 4), (98, 1)],  # delete, update, insert
        asks=[(101, 9), (103, 0)],  # update, delete of an unknown level
    )
    assert b.bids == {99: 4, 98: 1}
    assert b.asks == {101: 9, 102: 7}
    assert missing == 1
    assert (b.best_bid(), b.best_ask()) == (99, 101)


def test_clean_stream_has_no_anomalies():
    v = BookValidator(depth_limit=200)
    v.feed(snap(10, [(100, 1)], [(101, 1)]))
    v.feed(delta(11, bids=[(100, 2)]))
    v.feed(delta(12, asks=[(101, 0), (102, 3)]))
    # end-of-file snapshot repeating the last u: compared with the rebuilt book
    v.feed(snap(12, [(100, 2)], [(102, 3)], seq=112))
    r = v.r
    assert (r.messages, r.snapshots, r.deltas) == (4, 2, 2)
    assert r.gaps == r.crossed == r.locked == r.resyncs - 1 == 0
    assert r.snapshot_checks == 1 and r.snapshot_mismatches == 0
    assert r.events == []


def test_crossed_and_locked_detected():
    v = BookValidator()
    v.feed(snap(1_000, [(100, 1)], [(101, 1)]))
    v.feed(delta(1_001, bids=[(101, 1)]))  # bid == ask -> locked
    v.feed(delta(1_002, bids=[(102, 1)]))  # bid > ask -> crossed
    v.feed(delta(1_003, bids=[(101, 0), (102, 0)]))  # back to normal
    assert v.r.locked == 1
    assert v.r.crossed == 1
    assert [e.kind for e in v.r.events] == ["locked", "crossed"]


def test_crossing_checked_per_message_not_per_level():
    # Within one message the ask moves up before the bid: intermediate states would
    # cross, the final state does not. Only the post-message state counts.
    v = BookValidator()
    v.feed(snap(1, [(100, 1)], [(101, 1)]))
    v.feed(delta(2, bids=[(100, 0), (102, 1)], asks=[(101, 0), (103, 1)]))
    assert v.r.crossed == 0


def test_gap_detection():
    v = BookValidator()
    v.feed(snap(10, [(100, 1)], [(101, 1)]))
    v.feed(delta(11))
    v.feed(delta(14))  # 12, 13 missing
    v.feed(delta(15))
    v.feed(delta(15))  # duplicate / out of order
    assert v.r.gaps == 2
    assert v.r.missing_updates == 2
    gaps = [e for e in v.r.events if e.kind == "gap"]
    assert gaps[0].detail == "prev_u=11 u=14"


def test_snapshot_resync_and_service_restart():
    v = BookValidator()
    v.feed(snap(10, [(100, 1)], [(101, 1)]))
    v.feed(delta(11))
    v.feed(snap(50, [(200, 1)], [(201, 1)]))  # new snapshot jumping ahead: resync
    v.feed(snap(1, [(300, 1)], [(301, 1)]))  # u == 1: service restart
    v.feed(delta(2, bids=[(299, 1)]))
    assert v.r.resyncs == 3  # initial + 2
    assert v.r.service_restarts == 1
    assert v.r.gaps == 0  # resets are not delta gaps
    assert v.book.bids == {300: 1, 299: 1}


def test_snapshot_mismatch_detected():
    v = BookValidator()
    v.feed(snap(10, [(100, 1)], [(101, 1)]))
    v.feed(delta(11, bids=[(100, 2)]))
    v.feed(snap(11, [(100, 1)], [(101, 1)]))  # disagrees with rebuilt book
    assert v.r.snapshot_mismatches == 1


def test_delta_before_snapshot_and_depth_limit():
    v = BookValidator(depth_limit=2)
    v.feed(delta(5, bids=[(100, 1)]))
    v.feed(snap(6, [(100, 1), (99, 1), (98, 1)], [(101, 1)]))
    assert v.r.deltas_before_snapshot == 1
    assert v.r.depth_over_limit == 1


def test_seq_and_ts_monotonicity():
    v = BookValidator()
    v.feed(snap(1, [(100, 1)], [(101, 1)], seq=500, ts=10_000))
    v.feed(delta(2, seq=400, ts=9_000))
    v.feed(delta(3, seq=600, ts=12_000))
    assert v.r.seq_decreases == 1
    assert v.r.ts_decreases == 1
    assert v.r.intervals_over_1s == 1
    assert v.r.max_ts_interval_ms == 3_000
