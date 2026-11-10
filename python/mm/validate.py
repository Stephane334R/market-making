"""Validate normalized Parquet files: rebuild the book, check trades, build a report."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

from mm.book import BookValidator
from mm.normalize_bybit import iter_book_messages, read_manifest


def validate_book_file(path: Path, depth_limit: int | None) -> dict:
    manifest = read_manifest(path)
    v = BookValidator(depth_limit=depth_limit)
    empty = set(manifest.get("empty_update_ids", []))
    for msg in iter_book_messages(path):
        v.feed(msg)
    rep = v.r.to_dict()
    # Messages with no levels leave no rows; they were recorded at parse time.
    rep["empty_messages_in_source"] = len(empty)
    rep["message_count_matches_source"] = rep["messages"] + len(empty) == manifest["messages"]
    if empty:
        # A gap fully explained by empty messages is not a data gap. Re-derive honestly:
        # count gap events whose missing ids are all in the empty set.
        explained = 0
        for e in v.r.events:
            if e.kind == "gap":
                prev_u = int(e.detail.split()[0].split("=")[1])
                if all(u in empty for u in range(prev_u + 1, e.update_id)):
                    explained += 1
        rep["gaps_explained_by_empty_messages"] = explained
    return rep


def validate_trades_file(path: Path, day: date) -> dict:
    t = pq.read_table(path, columns=["ts_exchange_ms", "trade_id", "price", "size"])
    ts = t.column("ts_exchange_ms")
    n = t.num_rows
    day_start = int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp() * 1000)
    day_end = day_start + 86_400_000
    if n:
        decreases = pc.sum(pc.less(ts.slice(1), ts.slice(0, n - 1))).as_py() or 0
        outside = pc.sum(pc.or_(pc.less(ts, day_start), pc.greater_equal(ts, day_end))).as_py() or 0
        first, last = ts[0].as_py(), ts[-1].as_py()
    else:
        decreases = outside = 0
        first = last = None
    return {
        "rows": n,
        "duplicate_trade_ids": n - pc.count_distinct(t.column("trade_id")).as_py(),
        "ts_decreases": decreases,
        "outside_utc_day": outside,
        "non_positive_price": int(pc.sum(pc.less_equal(t.column("price"), 0)).as_py() or 0),
        "non_positive_size": int(pc.sum(pc.less_equal(t.column("size"), 0)).as_py() or 0),
        "first_ts_ms": first,
        "last_ts_ms": last,
        "sequence_gap_detection": "not possible: archive trades carry no sequence number",
    }


def continuity(day_reports: list[dict]) -> list[dict]:
    """Check that day N+1's first update id continues day N's last one."""
    out = []
    for a, b in zip(day_reports, day_reports[1:]):
        ba, bb = a.get("book"), b.get("book")
        if not ba or not bb or ba.get("last_update_id") is None:
            continue
        lu, fu = ba["last_update_id"], bb["first_update_id"]
        out.append(
            {
                "from": a["date"],
                "to": b["date"],
                "last_u": lu,
                "next_first_u": fu,
                # The next file starts with a snapshot; u == last_u or last_u + 1 means
                # no update was lost between files.
                "contiguous": fu in (lu, lu + 1),
            }
        )
    return out
