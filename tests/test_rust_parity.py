"""Parity: rust/sim must reproduce python/mm/simulator.py EXACTLY (bit-identical floats).

Both engines replay the same book + trades with the same quote schedule (recorded from
the Python strategy). Layers:
  * synthetic day (always, needs `cargo`): both fill models, written to real Parquet;
  * reference day(s) from tests/reference/ (MM_RUN_REFERENCE=1, needs the normalized
    data): Rust vs a fresh Python run AND vs the committed reference fills.
"""

import json
import os
import shutil
import subprocess
from dataclasses import fields
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from mm.backtest import REPO_ROOT, run_reference
from mm.normalize_bybit import BOOK_SCHEMA, TRADE_SCHEMA, Units, _with_units
from mm.simulator import RunSpec, SimConfig, run_day
from mm.strategy import QuoterParams
from test_simulator import synthetic

RUST_DIR = REPO_ROOT / "rust"
SIM_BIN = RUST_DIR / "target" / "release" / "sim"
REF = sorted((REPO_ROOT / "tests" / "reference").glob("sim_bybit_*"))


@pytest.fixture(scope="session")
def sim_bin() -> Path:
    if shutil.which("cargo") is None and not SIM_BIN.exists():
        pytest.skip("cargo not available")
    subprocess.run(["cargo", "build", "--release", "-q", "-p", "sim"], cwd=RUST_DIR, check=True)
    return SIM_BIN


def run_rust(sim_bin: Path, book: Path, trades: Path, quotes: Path, out: Path, cfg: SimConfig) -> dict:
    cmd = [str(sim_bin), "--book", str(book), "--trades", str(trades), "--quotes", str(quotes), "--out", str(out),
           "--latency-ms", str(cfg.latency_ms), "--maker-fee", repr(cfg.maker_fee),
           "--fill-model", cfg.fill_model, "--touch-fill-ratio", repr(cfg.touch_fill_ratio),
           "--sample-ms", str(cfg.sample_ms)]
    subprocess.run(cmd, check=True, capture_output=True)
    return json.loads((out / "run.json").read_text())


def assert_identical(py_fills: pd.DataFrame, py_states: pd.DataFrame, out: Path) -> None:
    rs_fills = pd.read_parquet(out / "fills.parquet")
    rs_states = pd.read_parquet(out / "states.parquet")
    assert len(py_fills) > 0
    pd.testing.assert_frame_equal(py_fills.reset_index(drop=True), rs_fills, check_exact=True)
    pd.testing.assert_frame_equal(py_states.reset_index(drop=True), rs_states, check_exact=True)


def write_synthetic_day(tmp: Path, tick: str, lot: str) -> tuple[Path, Path]:
    """The test_simulator synthetic market as common-schema Parquet files."""
    book, trades = synthetic(n_s=900, seed=7)
    rows = {n: [] for n in BOOK_SCHEMA.names}
    for m in book:
        for side, levels in (("bid", m.bids), ("ask", m.asks)):
            for p, s in levels:
                for n, v in zip(BOOK_SCHEMA.names, (m.ts_exchange_ms, None, m.seq, m.update_id, m.is_snapshot, side, p, s)):
                    rows[n].append(v)
    units = Units(tick, lot)
    bp, tp = tmp / "book.parquet", tmp / "trades.parquet"
    pq.write_table(pa.table(rows, schema=_with_units(BOOK_SCHEMA, units, "SYN")), bp)
    tr = {"ts_exchange_ms": [t[0] for t in trades], "ts_local_ms": [None] * len(trades),
          "trade_id": [str(i) for i in range(len(trades))], "side": ["buy" if t[1] == 1 else "sell" for t in trades],
          "price": [t[2] for t in trades], "size": [t[3] for t in trades]}
    pq.write_table(pa.table(tr, schema=_with_units(TRADE_SCHEMA, units, "SYN")), tp)
    return bp, tp


@pytest.mark.parametrize("model", ["l2_queue", "touch_ratio"])
def test_parity_synthetic(sim_bin, tmp_path, model):
    # non-trivial units so float rounding paths are exercised (0.1 and 0.001 are inexact)
    bp, tp = write_synthetic_day(tmp_path, "0.10", "0.001")
    params = QuoterParams(order_size=0.004, max_inventory=0.01, min_half_spread_ticks=1, skew_ticks_at_max=2)
    cfg = SimConfig(fill_model=model, sample_ms=1000, maker_fee=0.0002)
    dr = run_day(bp, tp, [RunSpec("x", params, cfg, record_quotes=True)])
    r = dr.runners["x"]
    r.quotes_df().to_parquet(tmp_path / "quotes.parquet", index=False)
    run = run_rust(sim_bin, bp, tp, tmp_path / "quotes.parquet", tmp_path / "rs", cfg)
    assert run["post_only_rejects"] == r.n_rejects
    assert_identical(r.fills_df(), r.states_df(), tmp_path / "rs")


@pytest.mark.skipif(not REF or os.environ.get("MM_RUN_REFERENCE") != "1",
                    reason="set MM_RUN_REFERENCE=1 (needs normalized data, ~1-2 min each)")
@pytest.mark.parametrize("ref", REF, ids=lambda p: p.name)
def test_parity_reference_day(sim_bin, tmp_path, ref):
    cfg_json = json.loads((ref / "config.json").read_text())
    bp, tp = REPO_ROOT / cfg_json["inputs"]["book_updates"], REPO_ROOT / cfg_json["inputs"]["trades"]
    if not (bp.exists() and tp.exists()):
        pytest.skip("reference input data not present")
    params = QuoterParams(**cfg_json["params"])
    cfg = SimConfig(**{f.name: cfg_json["sim_config"][f.name] for f in fields(SimConfig)})
    py = tmp_path / "py"
    run_reference(date.fromisoformat(cfg_json["day"]), params, cfg, py, quotes=True)
    run_rust(sim_bin, bp, tp, py / "quotes.parquet", tmp_path / "rs", cfg)
    py_fills = pd.read_csv(py / "fills.csv")
    assert_identical(py_fills, pd.read_parquet(py / "states.parquet"), tmp_path / "rs")
    # and against the committed reference (guards against both engines drifting together)
    pd.testing.assert_frame_equal(pd.read_csv(ref / "fills.csv"), pd.read_parquet(tmp_path / "rs" / "fills.parquet"),
                                  check_exact=True)
