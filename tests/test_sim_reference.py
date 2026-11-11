"""Regression test: re-run the simulator on the reference day and compare with the
committed reference output (tests/reference/sim_bybit_<day>/).

Needs the normalized input data (git-ignored) and takes about a minute, so it runs only
with MM_RUN_REFERENCE=1. The reference is also the target for the Rust port.
"""

import json
import os
from dataclasses import fields
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from mm.backtest import REPO_ROOT, run_reference
from mm.normalize_bybit import read_manifest
from mm.simulator import SimConfig
from mm.strategy import QuoterParams

REF = sorted((REPO_ROOT / "tests" / "reference").glob("sim_bybit_*"))


def _inputs_present(cfg: dict) -> bool:
    bp = REPO_ROOT / cfg["inputs"]["book_updates"]
    tp = REPO_ROOT / cfg["inputs"]["trades"]
    return bp.exists() and tp.exists() and read_manifest(bp)["source_sha256"] == cfg["inputs"]["book_source_sha256"]


@pytest.mark.skipif(not REF, reason="no reference output committed")
@pytest.mark.parametrize("ref", REF, ids=lambda p: p.name)
def test_reference_files_are_consistent(ref):
    cfg = json.loads((ref / "config.json").read_text())
    summ = json.loads((ref / "summary.json").read_text())
    fills = pd.read_csv(ref / "fills.csv")
    states = pd.read_parquet(ref / "states.parquet")
    assert len(fills) == summ["fills"] == cfg["events"]["fills"]
    last = states.iloc[-1]
    assert abs(last["equity"] - (last["spread_capture"] + last["inventory_pnl"] - last["fees"])) < 1e-6
    assert round(float(last["equity"]), 2) == summ["pnl_total"]


@pytest.mark.skipif(not REF or os.environ.get("MM_RUN_REFERENCE") != "1",
                    reason="set MM_RUN_REFERENCE=1 (needs normalized data, ~1 min each)")
@pytest.mark.parametrize("ref", REF, ids=lambda p: p.name)
def test_simulator_reproduces_reference(ref, tmp_path):
    cfg = json.loads((ref / "config.json").read_text())
    if not _inputs_present(cfg):
        pytest.skip("reference input data not present or different")
    params = QuoterParams(**cfg["params"])
    sim = SimConfig(**{f.name: cfg["sim_config"][f.name] for f in fields(SimConfig)})
    run_reference(date.fromisoformat(cfg["day"]), params, sim, tmp_path)
    pd.testing.assert_frame_equal(pd.read_csv(tmp_path / "fills.csv"), pd.read_csv(ref / "fills.csv"))
    pd.testing.assert_frame_equal(pd.read_parquet(tmp_path / "states.parquet"), pd.read_parquet(ref / "states.parquet"))
