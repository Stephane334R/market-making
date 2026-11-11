"""Validate normalized Parquet files: rebuild the book, check trades, build a report."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
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


def validate_book_files(paths: list[Path], depth_limit: int | None) -> dict:
    """Validate several consecutive files as ONE stream (e.g. a recorder session rotated
    every N minutes): the book carries over file boundaries, so a file that starts with
    a delta is not flagged as 'delta before snapshot'."""
    v = BookValidator(depth_limit=depth_limit)
    source_messages = 0
    empty: set[int] = set()
    for p in paths:
        man = read_manifest(p)
        source_messages += man["messages"]
        empty.update(man.get("empty_update_ids", []))
        for msg in iter_book_messages(p):
            v.feed(msg)
    rep = v.r.to_dict()
    rep["files"] = len(paths)
    rep["empty_messages_in_source"] = len(empty)
    rep["message_count_matches_source"] = rep["messages"] + len(empty) == source_messages
    return rep


def validate_live_session(session_dir: Path) -> dict:
    """Validate a recorder session directory (rust/recorder output)."""
    session_dir = Path(session_dir)
    books = sorted((session_dir / "book_updates").glob("*.parquet"))
    trades = sorted((session_dir / "trades").glob("*.parquet"))
    depth = None
    if books:
        topic = read_manifest(books[0])["topics"][0]  # orderbook.<depth>.<symbol>
        depth = int(topic.split(".")[1])
    out: dict = {"session": session_dir.name, "book_depth": depth}
    out["book"] = validate_book_files(books, depth) if books else None
    if trades:
        t = pa.concat_tables([pq.read_table(p) for p in trades])
        out["trades"] = {
            "files": len(trades),
            "rows": t.num_rows,
            "duplicate_trade_ids": t.num_rows - pc.count_distinct(t.column("trade_id")).as_py(),
            "ts_local_nulls": t.column("ts_local_ms").null_count,
        }
    summary = session_dir / "summary.json"
    if summary.exists():
        out["recorder_summary"] = json.loads(summary.read_text())
    return out


if __name__ == "__main__":
    import sys

    rep = validate_live_session(Path(sys.argv[1]))
    print(json.dumps({k: v for k, v in rep.items() if k != "recorder_summary"}, indent=2, default=str))
