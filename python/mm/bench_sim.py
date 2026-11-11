"""Benchmark the fill simulator: Python vs Rust on the same day and quote schedule.

    uv run python -m mm.bench_sim [--day 2026-11-09] [--rust-runs 3]

Three timings, all including Parquet reading (they are end-to-end tools):
  * python_strategy_and_sim : mm.simulator.run_day with the strategy computing quotes
  * python_sim_only         : the same engine, quotes replayed from the schedule (the
                              apples-to-apples comparison with Rust)
  * rust_sim                : rust/sim binary, median of N runs (wall time of the process)
Events = book messages + trades. Also checks that all three produce identical fills.
Results: results/bench_sim_<UTC>/ (bench.json, bench.md, config.json).
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import subprocess
import time
from dataclasses import asdict, fields
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow

from mm.download_bybit import REPO_ROOT, RESULTS_DIR, git_commit, norm_path
from mm.normalize_bybit import iter_book_messages, read_manifest, read_units
from mm.simulator import Engine, Runner, RunSpec, SimConfig, iter_trades, replay, run_day
from mm.strategy import EwmaVol, InventorySkewQuoter, Quote, QuoterParams

SYMBOL = "BTCUSDT"
REF_CFG = REPO_ROOT / "tests" / "reference" / "sim_bybit_2026-11-09_hs80" / "config.json"
SIM_BIN = REPO_ROOT / "rust" / "target" / "release" / "sim"


class ScheduleQuoter(InventorySkewQuoter):
    """Returns the recorded quotes in order, ignoring the market: Python sim-only mode."""

    def __init__(self, params: QuoterParams, schedule: pd.DataFrame, vol=None):
        super().__init__(params, vol)
        self.rows = schedule.itertuples(index=False)

    def quote(self, mid, best_bid, best_ask, inventory) -> Quote:
        r = next(self.rows)
        bid = None if pd.isna(r.bid_px) else int(r.bid_px)
        ask = None if pd.isna(r.ask_px) else int(r.ask_px)
        return Quote(bid, ask, float(r.bid_sz), float(r.ask_sz), 0.0, 0.0)


def machine_info() -> dict:
    cpu = "unknown"
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    except OSError:
        pass
    mem = None
    try:
        mem = int(Path("/proc/meminfo").read_text().split()[1]) // 1024
    except (OSError, ValueError, IndexError):
        pass
    rustc = subprocess.run(["rustc", "--version"], capture_output=True, text=True).stdout.strip()
    return {"cpu": cpu, "cpus": os.cpu_count(), "mem_mb": mem, "platform": platform.platform(),
            "python": platform.python_version(), "pyarrow": pyarrow.__version__, "rustc": rustc}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--day", type=date.fromisoformat, default=date(2026, 11, 9))
    p.add_argument("--rust-runs", type=int, default=3)
    a = p.parse_args(argv)

    ref = json.loads(REF_CFG.read_text())
    params = QuoterParams(**ref["params"])
    cfg = SimConfig(**{f.name: ref["sim_config"][f.name] for f in fields(SimConfig)})
    bp, tp = norm_path("book_updates", SYMBOL, a.day), norm_path("trades", SYMBOL, a.day)
    run_id = "bench_sim_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = RESULTS_DIR / run_id
    out.mkdir(parents=True)

    print("python: strategy + sim ...")
    t = time.perf_counter()
    dr = run_day(bp, tp, [RunSpec("py", params, cfg, record_quotes=True)])
    t_py_full = time.perf_counter() - t
    r = dr.runners["py"]
    py_fills = r.fills_df()
    schedule = r.quotes_df()
    schedule.to_parquet(out / "quotes.parquet", index=False)
    book_msgs = dr.engine.n_book
    n_trades = read_manifest(tp)["rows"]
    events = book_msgs + n_trades
    book_rows = read_manifest(bp)["rows"]

    print("python: sim only (schedule) ...")
    t = time.perf_counter()
    units = read_units(bp)
    vol = EwmaVol(params.vol_halflife_s)
    runner = Runner("sched", params, cfg, vol)
    runner.quoter = ScheduleQuoter(params, schedule, vol)
    eng = Engine([runner], float(units.tick_size), float(units.lot_size), vol)
    replay(iter_book_messages(bp), iter_trades(tp), eng)
    t_py_sim = time.perf_counter() - t
    pd.testing.assert_frame_equal(runner.fills_df(), py_fills, check_exact=True)

    print("rust: building and running ...")
    subprocess.run(["cargo", "build", "--release", "-q", "-p", "sim"], cwd=REPO_ROOT / "rust", check=True)
    rust_times, rust_runs = [], []
    for i in range(a.rust_runs):
        rs_out = out / "rust_output"
        cmd = [str(SIM_BIN), "--book", str(bp), "--trades", str(tp), "--quotes", str(out / "quotes.parquet"),
               "--out", str(rs_out), "--latency-ms", str(cfg.latency_ms), "--maker-fee", repr(cfg.maker_fee),
               "--fill-model", cfg.fill_model, "--touch-fill-ratio", repr(cfg.touch_fill_ratio),
               "--sample-ms", str(cfg.sample_ms)]
        t = time.perf_counter()
        subprocess.run(cmd, check=True, capture_output=True)
        rust_times.append(time.perf_counter() - t)
        rust_runs.append(json.loads((rs_out / "run.json").read_text()))
    rs_fills = pd.read_parquet(out / "rust_output" / "fills.parquet")
    pd.testing.assert_frame_equal(py_fills, rs_fills, check_exact=True)
    t_rs = statistics.median(rust_times)

    def row(name, secs):
        return {"name": name, "wall_s": round(secs, 3), "events_per_s": round(events / secs),
                "book_rows_per_s": round(book_rows / secs)}

    rows = [row("python_strategy_and_sim", t_py_full), row("python_sim_only", t_py_sim), row("rust_sim", t_rs)]
    bench = {
        "day": a.day.isoformat(), "events": events, "book_messages": book_msgs, "book_rows": book_rows,
        "trades": n_trades, "quotes": len(schedule), "fills": len(py_fills), "fills_identical": True,
        "results": rows, "speedup_rust_vs_python_sim_only": round(t_py_sim / t_rs, 1),
        "rust_runs_wall_s": [round(x, 3) for x in rust_times],
        "rust_internal_seconds_median_run": rust_runs[rust_times.index(t_rs)]["seconds"],
    }
    config = {"run_id": run_id, "kind": "benchmark_fill_simulator", "symbol": SYMBOL, "day": a.day.isoformat(),
              "params": asdict(params), "sim_config": asdict(cfg), "git_commit": git_commit(),
              "inputs": {"book_source_sha256": read_manifest(bp)["source_sha256"],
                         "trades_source_sha256": read_manifest(tp)["source_sha256"]},
              "machine": machine_info(),
              "notes": ["single-threaded in both languages", "wall times include Parquet reading and output",
                        f"python timed once per mode; rust median of {a.rust_runs} runs (warm page cache)"]}
    (out / "bench.json").write_text(json.dumps(bench, indent=2))
    (out / "config.json").write_text(json.dumps(config, indent=2))
    md = [f"# Fill simulator benchmark ({a.day}, BTCUSDT)", "",
          f"{events:,} events ({book_msgs:,} book messages carrying {book_rows:,} level updates + {n_trades:,} trades), "
          f"{len(schedule):,} scheduled quotes, {len(py_fills):,} fills (identical in all three runs).", "",
          "| run | wall time (s) | events/s | book level updates/s |", "|---|---|---|---|"]
    md += [f"| {x['name']} | {x['wall_s']:,} | {x['events_per_s']:,} | {x['book_rows_per_s']:,} |" for x in rows]
    md += ["", f"Rust is **{bench['speedup_rust_vs_python_sim_only']}x** faster than the Python simulator on the same "
           f"quote schedule.", "", f"Machine: {config['machine']['cpu']}, {config['machine']['cpus']} CPUs, "
           f"{config['machine']['mem_mb']} MB RAM; {config['machine']['rustc']}; Python {config['machine']['python']}.",
           "Both single-threaded; wall times include reading Parquet and writing outputs. "
           f"Python timed once per mode, Rust median of {a.rust_runs} runs."]
    (out / "bench.md").write_text("\n".join(md) + "\n")
    print("\n".join(md))
    print(f"\nresults: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
