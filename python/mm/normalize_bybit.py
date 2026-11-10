"""Parse Bybit archive files into the common schema (docs/SCHEMA.md), Parquet per day.

Order book archive (``YYYY-MM-DD_BTCUSDT_ob200.data.zip``): one zip member, JSON lines,
each line a message in the same shape as the public websocket ``orderbook.{depth}``
topic::

    {"topic":"orderbook.200.BTCUSDT","type":"snapshot"|"delta","ts":<ms>,
     "data":{"s":"BTCUSDT","b":[["price","size"],...],"a":[...],"u":<int>,"seq":<int>},
     "cts":<ms>}

Each message is flattened to one row per price level, in file order; inside a message
all bid levels come first, then all ask levels, each in exchange order. ``u`` ->
``update_id``, ``seq`` -> ``seq``, ``ts`` -> ``ts_exchange_ms``.

Trades archive (``BTCUSDTYYYY-MM-DD.csv.gz``): CSV with header
``timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,
foreignNotional,RPI``; ``timestamp`` is seconds with a fractional part.

SIMPLIFICATIONS / LOSSY POINTS (all deliberate, all labelled):
  * ``ts_local_ms`` is null for historical data: the archive has no receive timestamps.
  * ``cts`` (matching-engine timestamp) is not part of the common schema and is dropped.
  * Messages with no levels at all would produce no rows; their update ids are kept in
    the per-day manifest (``empty_update_ids``) so the validator does not count them as
    gaps.
  * Trade timestamps are truncated (not rounded) from 0.1 ms to ms resolution.
  * Trade columns tickDirection, grossValue, homeNotional, foreignNotional and RPI are not
    in the common schema and are dropped (derivable or not needed yet).
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import os
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import orjson
import pyarrow as pa
import pyarrow.parquet as pq

from mm.book import BookMessage
from mm.units import BTCUSDT_LOT_SIZE, BTCUSDT_TICK_SIZE, UnitCache

BOOK_SCHEMA = pa.schema(
    [
        ("ts_exchange_ms", pa.int64()),
        ("ts_local_ms", pa.int64()),
        ("seq", pa.int64()),
        ("update_id", pa.int64()),
        ("is_snapshot", pa.bool_()),
        ("side", pa.string()),  # "bid" | "ask"
        ("price", pa.int64()),  # integer ticks
        ("size", pa.int64()),  # integer lots
    ]
)

TRADE_SCHEMA = pa.schema(
    [
        ("ts_exchange_ms", pa.int64()),
        ("ts_local_ms", pa.int64()),
        ("trade_id", pa.string()),
        ("side", pa.string()),  # taker side: "buy" | "sell"
        ("price", pa.int64()),  # integer ticks
        ("size", pa.int64()),  # integer lots
    ]
)

FLUSH_ROWS = 500_000  # keeps memory bounded (the dev box has 2 GB of RAM)


@dataclass(frozen=True)
class Units:
    tick_size: str = BTCUSDT_TICK_SIZE
    lot_size: str = BTCUSDT_LOT_SIZE


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _with_units(schema: pa.Schema, units: Units, symbol: str) -> pa.Schema:
    """Units are needed to interpret price/size, so they travel in the file footer."""
    meta = {"symbol": symbol, "tick_size": units.tick_size, "lot_size": units.lot_size}
    return schema.with_metadata({b"mm": json.dumps(meta).encode()})


def read_units(parquet_path: Path) -> Units:
    meta = json.loads(pq.read_schema(parquet_path).metadata[b"mm"])
    return Units(meta["tick_size"], meta["lot_size"])


def _open_single_member(zip_path: Path) -> tuple[zipfile.ZipFile, str]:
    z = zipfile.ZipFile(zip_path)
    names = z.namelist()
    if len(names) != 1:
        z.close()
        raise ValueError(f"{zip_path}: expected exactly one member, got {names}")
    return z, names[0]


def normalize_orderbook(
    zip_path: Path, out_path: Path, symbol: str, units: Units = Units()
) -> dict:
    """Stream the zipped JSONL into Parquet. Returns the manifest (also written as JSON).

    Reading the zip member to the end verifies its CRC, so a truncated or corrupt
    download raises here rather than producing a silently short file.
    """
    pc, sc = UnitCache(units.tick_size), UnitCache(units.lot_size)
    tmp = out_path.with_suffix(".parquet.tmp")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    stats = {"messages": 0, "snapshots": 0, "deltas": 0, "rows": 0}
    empty_ids: list[int] = []
    topics: set[str] = set()
    cols: dict[str, list] = {n: [] for n in BOOK_SCHEMA.names if n != "ts_local_ms"}

    z, member = _open_single_member(zip_path)
    schema = _with_units(BOOK_SCHEMA, units, symbol)
    writer = pq.ParquetWriter(tmp, schema, compression="zstd")
    try:
        with z, z.open(member) as f:
            for line_no, line in enumerate(f, 1):
                if not line.strip():
                    raise ValueError(f"{zip_path}:{line_no}: blank line")
                m = orjson.loads(line)
                d = m["data"]
                if d["s"] != symbol:
                    raise ValueError(f"{zip_path}:{line_no}: symbol {d['s']!r} != {symbol!r}")
                topics.add(m["topic"])
                typ = m["type"]
                if typ not in ("snapshot", "delta"):
                    raise ValueError(f"{zip_path}:{line_no}: unknown type {typ!r}")
                snap = typ == "snapshot"
                stats["messages"] += 1
                stats["snapshots" if snap else "deltas"] += 1
                ts, seq, u = m["ts"], d["seq"], d["u"]
                nrow = 0
                for side, levels in (("bid", d["b"]), ("ask", d["a"])):
                    n = len(levels)
                    if not n:
                        continue
                    nrow += n
                    cols["ts_exchange_ms"].extend([ts] * n)
                    cols["seq"].extend([seq] * n)
                    cols["update_id"].extend([u] * n)
                    cols["is_snapshot"].extend([snap] * n)
                    cols["side"].extend([side] * n)
                    cols["price"].extend([pc[lv[0]] for lv in levels])
                    cols["size"].extend([sc[lv[1]] for lv in levels])
                if nrow == 0:
                    empty_ids.append(u)
                stats["rows"] += nrow
                if len(cols["price"]) >= FLUSH_ROWS:
                    _flush(writer, cols, schema)
            _flush(writer, cols, schema)
    except BaseException:
        writer.close()
        tmp.unlink(missing_ok=True)
        raise
    writer.close()

    manifest = {
        "kind": "book_updates",
        "symbol": symbol,
        "source_file": zip_path.name,
        "source_sha256": sha256_file(zip_path),
        "topics": sorted(topics),
        "tick_size": units.tick_size,
        "lot_size": units.lot_size,
        "empty_update_ids": empty_ids,
        **stats,
    }
    os.replace(tmp, out_path)
    _manifest_path(out_path).write_text(json.dumps(manifest, indent=2))
    return manifest


def _flush(writer: pq.ParquetWriter, cols: dict[str, list], schema: pa.Schema) -> None:
    n = len(next(iter(cols.values())))
    if n == 0:
        return
    arrays = []
    for name in schema.names:
        if name == "ts_local_ms":
            arrays.append(pa.nulls(n, pa.int64()))
        else:
            arrays.append(pa.array(cols[name], type=schema.field(name).type))
            cols[name].clear()
    writer.write_table(pa.Table.from_arrays(arrays, schema=schema))


def _manifest_path(parquet_path: Path) -> Path:
    return parquet_path.with_suffix(".manifest.json")


def read_manifest(parquet_path: Path) -> dict:
    return json.loads(_manifest_path(parquet_path).read_text())


def _ts_seconds_to_ms(s: str) -> int:
    """'1794182400.0708' -> 1794182400070 (exact string arithmetic, truncation)."""
    whole, _, frac = s.partition(".")
    if not whole.isdigit() or (frac and not frac.isdigit()):
        raise ValueError(f"bad timestamp {s!r}")
    return int(whole) * 1000 + int((frac + "000")[:3])


TRADE_REQUIRED = ("timestamp", "symbol", "side", "size", "price", "trdMatchID")
SIDE_MAP = {"Buy": "buy", "Sell": "sell"}


def normalize_trades(gz_path: Path, out_path: Path, symbol: str, units: Units = Units()) -> dict:
    pc, sc = UnitCache(units.tick_size), UnitCache(units.lot_size)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(".parquet.tmp")
    cols: dict[str, list] = {n: [] for n in TRADE_SCHEMA.names if n != "ts_local_ms"}
    rows = 0
    schema = _with_units(TRADE_SCHEMA, units, symbol)
    writer = pq.ParquetWriter(tmp, schema, compression="zstd")
    try:
        with gzip.open(gz_path, "rt", newline="") as f:
            reader = csv.reader(f)
            header = next(reader)
            missing = [c for c in TRADE_REQUIRED if c not in header]
            if missing:
                raise ValueError(f"{gz_path}: missing columns {missing}; header={header}")
            ix = {c: header.index(c) for c in TRADE_REQUIRED}
            its, isym, iside = ix["timestamp"], ix["symbol"], ix["side"]
            isz, ipx, iid = ix["size"], ix["price"], ix["trdMatchID"]
            for line_no, rec in enumerate(reader, 2):
                if len(rec) != len(header):
                    raise ValueError(f"{gz_path}:{line_no}: {len(rec)} fields, expected {len(header)}")
                if rec[isym] != symbol:
                    raise ValueError(f"{gz_path}:{line_no}: symbol {rec[isym]!r}")
                side = SIDE_MAP.get(rec[iside])
                if side is None:
                    raise ValueError(f"{gz_path}:{line_no}: side {rec[iside]!r}")
                cols["ts_exchange_ms"].append(_ts_seconds_to_ms(rec[its]))
                cols["trade_id"].append(rec[iid])
                cols["side"].append(side)
                cols["price"].append(pc[rec[ipx]])
                cols["size"].append(sc[rec[isz]])
                rows += 1
                if len(cols["price"]) >= FLUSH_ROWS:
                    _flush(writer, cols, schema)
            _flush(writer, cols, schema)
    except BaseException:
        writer.close()
        tmp.unlink(missing_ok=True)
        raise
    writer.close()
    manifest = {
        "kind": "trades",
        "symbol": symbol,
        "source_file": gz_path.name,
        "source_sha256": sha256_file(gz_path),
        "source_header": header,
        "tick_size": units.tick_size,
        "lot_size": units.lot_size,
        "rows": rows,
    }
    os.replace(tmp, out_path)
    _manifest_path(out_path).write_text(json.dumps(manifest, indent=2))
    return manifest


def iter_book_messages(parquet_path: Path, batch_rows: int = 262_144) -> Iterator[BookMessage]:
    """Regroup normalized rows into messages, streaming.

    A message is a run of consecutive rows sharing (update_id, seq, is_snapshot) — the
    end-of-day snapshot repeats the last delta's ``u`` but differs in ``is_snapshot``.
    """
    pf = pq.ParquetFile(parquet_path)
    cur_key = None
    cur: BookMessage | None = None
    names = ["ts_exchange_ms", "seq", "update_id", "is_snapshot", "side", "price", "size"]
    for batch in pf.iter_batches(batch_size=batch_rows, columns=names):
        ts, seq, uid, snap, side, px, sz = (batch.column(n).to_pylist() for n in names)
        for i in range(len(px)):
            key = (uid[i], seq[i], snap[i])
            if key != cur_key:
                if cur is not None:
                    yield cur
                cur_key = key
                cur = BookMessage(ts[i], seq[i], uid[i], snap[i], [], [])
            (cur.bids if side[i] == "bid" else cur.asks).append((px[i], sz[i]))
    if cur is not None:
        yield cur
