"""End-to-end on tiny fixtures shaped like the real archive files."""

import gzip
import json
import zipfile
from datetime import date

import pyarrow.parquet as pq
import pytest

from mm.normalize_bybit import (
    iter_book_messages,
    normalize_orderbook,
    normalize_trades,
    read_manifest,
    read_units,
)
from mm.units import UnitCache, from_units, to_units
from mm.validate import validate_book_file, validate_trades_file


def msg(typ, u, seq, ts, b, a, sym="BTCUSDT"):
    return {"topic": "orderbook.200.BTCUSDT", "type": typ, "ts": ts,
            "data": {"s": sym, "b": b, "a": a, "u": u, "seq": seq}, "cts": ts - 2}


def write_ob_zip(path, messages):
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(path.name.replace(".zip", ""), "".join(json.dumps(m) + "\n" for m in messages))


OB_FIXTURE = [
    msg("snapshot", 100, 9000, 1_000, [["100.00", "1.000"], ["99.90", "0.500"]], [["100.10", "2.000"]]),
    msg("delta", 101, 9001, 1_100, [["100.00", "0"], ["99.90", "0.700"]], [["100.20", "0.001"]]),
    msg("delta", 102, 9005, 1_200, [], []),  # empty message: no rows, but not a gap
    msg("delta", 103, 9007, 1_300, [["100.10", "0.300"]], [["100.10", "0"]]),
    msg("delta", 106, 9010, 1_400, [], [["100.30", "1"]]),  # gap: 104, 105 missing
    msg("snapshot", 106, 9010, 1_400, [["100.10", "0.300"], ["99.90", "0.700"]],
        [["100.20", "0.001"], ["100.30", "1"]]),
]


def test_units_exact():
    assert to_units("74810.40", "0.10") == 748104
    assert to_units("0.001", "0.001") == 1
    assert to_units("0", "0.001") == 0
    assert str(from_units(748104, "0.10")) == "74810.40"
    with pytest.raises(ValueError):
        to_units("74810.45", "0.10")  # not on the tick grid: never round silently
    c = UnitCache("0.001")
    assert c["1.500"] == 1500 and "1.500" in c


def test_orderbook_roundtrip_and_validation(tmp_path):
    src = tmp_path / "2026-11-09_BTCUSDT_ob200.data.zip"
    write_ob_zip(src, OB_FIXTURE)
    out = tmp_path / "book_updates" / "2026-11-09.parquet"
    man = normalize_orderbook(src, out, "BTCUSDT")

    assert man["messages"] == 6 and man["snapshots"] == 2
    assert man["empty_update_ids"] == [102]
    assert read_manifest(out)["rows"] == man["rows"] == 3 + 3 + 2 + 1 + 4
    assert read_units(out).tick_size == "0.10"

    t = pq.read_table(out)
    assert t.schema.names == ["ts_exchange_ms", "ts_local_ms", "seq", "update_id",
                              "is_snapshot", "side", "price", "size"]
    first = t.slice(0, 3).to_pylist()
    assert [(r["side"], r["price"], r["size"]) for r in first] == [
        ("bid", 1000, 1000), ("bid", 999, 500), ("ask", 1001, 2000)]
    assert t.column("ts_local_ms").null_count == t.num_rows

    msgs = list(iter_book_messages(out, batch_rows=2))  # tiny batches: test regrouping
    assert [m.update_id for m in msgs] == [100, 101, 103, 106, 106]
    assert [m.is_snapshot for m in msgs] == [True, False, False, False, True]

    rep = validate_book_file(out, depth_limit=200)
    assert rep["message_count_matches_source"]
    assert rep["gaps"] == 2  # 101->103 (explained by empty 102) and 103->106 (real)
    assert rep["gaps_explained_by_empty_messages"] == 1
    assert rep["missing_updates"] == 1 + 2
    # after u=103 the bid at 100.10 meets an ask that was just deleted: no cross
    assert rep["crossed"] == 0 and rep["locked"] == 0
    assert rep["snapshot_checks"] == 1 and rep["snapshot_mismatches"] == 0


def test_orderbook_rejects_wrong_symbol(tmp_path):
    src = tmp_path / "x.data.zip"
    write_ob_zip(src, [msg("snapshot", 1, 1, 1, [], [], sym="ETHUSDT")])
    out = tmp_path / "o.parquet"
    with pytest.raises(ValueError, match="symbol"):
        normalize_orderbook(src, out, "BTCUSDT")
    assert not out.exists() and not out.with_suffix(".parquet.tmp").exists()


TRADES_CSV = (
    "timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional,RPI\n"
    "1794182400.0708,BTCUSDT,Buy,0.001,74816.90,PlusTick,id-1,7.48e+09,0.001,74.8,0\n"
    "1794182400.0719,BTCUSDT,Sell,0.480,74816.80,MinusTick,id-2,3.59e+12,0.48,35912.1,0\n"
    "1794182401.5,BTCUSDT,Buy,1.000,74817.00,PlusTick,id-3,1,1,1,1\n"
)


def test_trades_parse_and_validate(tmp_path):
    src = tmp_path / "BTCUSDT2026-11-09.csv.gz"
    with gzip.open(src, "wt") as f:
        f.write(TRADES_CSV)
    out = tmp_path / "trades" / "2026-11-09.parquet"
    man = normalize_trades(src, out, "BTCUSDT")
    assert man["rows"] == 3
    rows = pq.read_table(out).to_pylist()
    assert rows[0] == {"ts_exchange_ms": 1794182400070, "ts_local_ms": None, "trade_id": "id-1",
                       "side": "buy", "price": 748169, "size": 1}
    assert rows[2]["ts_exchange_ms"] == 1794182401500
    assert rows[1]["side"] == "sell" and rows[1]["size"] == 480

    rep = validate_trades_file(out, date(2026, 11, 9))
    assert rep["rows"] == 3 and rep["duplicate_trade_ids"] == 0
    assert rep["ts_decreases"] == 0 and rep["outside_utc_day"] == 0
