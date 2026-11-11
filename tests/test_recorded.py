"""Check a live-recorded session (rust/recorder) with the same validator as the
historical pipeline.

Recorded data is git-ignored, so this test skips when there is none. Point it at a
session with MM_RECORDED_SESSION=<dir>; otherwise the newest session under
data/live/BTCUSDT/ is used.
"""

import os
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from mm.normalize_bybit import read_manifest, read_units
from mm.validate import validate_live_session

LIVE_ROOT = Path(__file__).resolve().parents[1] / "data" / "live" / "BTCUSDT"


def _session() -> Path | None:
    env = os.environ.get("MM_RECORDED_SESSION")
    if env:
        return Path(env)
    sessions = sorted(p for p in LIVE_ROOT.glob("*") if (p / "summary.json").exists())
    return sessions[-1] if sessions else None


SESSION = _session()
pytestmark = pytest.mark.skipif(SESSION is None, reason="no recorded session available")


def test_files_follow_common_schema():
    books = sorted((SESSION / "book_updates").glob("*.parquet"))
    trades = sorted((SESSION / "trades").glob("*.parquet"))
    assert books and trades
    assert pq.read_schema(books[0]).names == [
        "ts_exchange_ms", "ts_local_ms", "seq", "update_id", "is_snapshot", "side", "price", "size"]
    assert pq.read_schema(trades[0]).names == [
        "ts_exchange_ms", "ts_local_ms", "trade_id", "side", "price", "size"]
    assert read_units(books[0]).tick_size == "0.10"
    assert read_manifest(books[0])["topics"] == ["orderbook.1000.BTCUSDT"]
    assert not list(SESSION.glob("*/*.tmp")), "unfinished file left behind"


def test_recorded_book_passes_validator():
    rep = validate_live_session(SESSION)
    b, rec = rep["book"], rep["recorder_summary"]["stats"]
    assert b["message_count_matches_source"]
    assert b["messages"] == rec["book_messages"]
    assert b["rows"] == rec["book_rows"]
    # Same gap rule in Rust and Python: same gaps.
    assert b["gaps"] == rec["gaps"]
    assert b["resyncs"] == rec["resyncs"]
    assert b["deltas_before_snapshot"] == 0
    assert b["crossed"] == 0 and b["locked"] == 0
    assert b["snapshot_mismatches"] == 0
    if rec["gaps"] == 0 and rec["disconnects"] == 0:
        # A clean session must be fully clean.
        assert b["max_depth_bids"] <= 1000 and b["max_depth_asks"] <= 1000
        assert b["seq_decreases"] == b["ts_decreases"] == b["delete_missing_level"] == 0
    else:
        # After a gap the Python validator keeps applying deltas (the book is knowingly
        # wrong until the next snapshot), and a resubscribe snapshot can be older than
        # the last delta seen. Anomalies are allowed only at gap / resync points.
        points = {e["update_id"] for e in b["events"] if e["kind"] in ("gap", "resync")}
        for e in b["events"]:
            if e["kind"] in ("seq_decrease", "ts_decrease", "depth_over_limit"):
                assert e["update_id"] in points, e


def test_recorded_trades():
    t = validate_live_session(SESSION)["trades"]
    assert t["rows"] == validate_live_session(SESSION)["recorder_summary"]["stats"]["trades"]
    assert t["duplicate_trade_ids"] == 0
    assert t["ts_local_nulls"] == 0
