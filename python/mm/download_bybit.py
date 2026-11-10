"""Download Bybit historical BTCUSDT linear data, normalize it, validate it.

Sources (verified 2026-11-10):
  * Order book: Bybit's history-data archive (https://www.bybit.com/derivatives/en/history-data)
    serves files from
      https://quote-saver.bycsi.com/orderbook/linear/{SYMBOL}/{DATE}_{SYMBOL}_ob{DEPTH}.data.zip
    Depth is 500 up to 2025-08-20 and 200 from 2025-08-21 (Bybit API announcement of
    2025-08-21: "adjusted from level 500 @ 100 ms to level 200 @ 100 ms"; the file
    listing switches on that date).
  * Trades: https://public.bybit.com/trading/{SYMBOL}/{SYMBOL}{DATE}.csv.gz

CLI::

    uv run mm-bybit run                       # last 7 complete UTC days
    uv run mm-bybit run --start 2026-11-01 --end 2026-11-07
    uv run mm-bybit download|normalize|validate --start ... --end ...
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"
RESULTS_DIR = REPO_ROOT / "results"

OB_BASE = "https://quote-saver.bycsi.com/orderbook/linear"
TRADES_BASE = "https://public.bybit.com/trading"
OB200_FROM = date(2025, 8, 21)
USER_AGENT = "mm-research-downloader/0.1"


# --------------------------------------------------------------------------- paths


def ob_depth_for(day: date) -> int:
    return 200 if day >= OB200_FROM else 500


def ob_url(symbol: str, day: date, depth: int) -> str:
    return f"{OB_BASE}/{symbol}/{day.isoformat()}_{symbol}_ob{depth}.data.zip"


def trades_url(symbol: str, day: date) -> str:
    return f"{TRADES_BASE}/{symbol}/{symbol}{day.isoformat()}.csv.gz"


def raw_dir(kind: str, symbol: str) -> Path:
    return DATA_DIR / "raw" / "bybit" / "linear" / kind / symbol


def norm_path(kind: str, symbol: str, day: date) -> Path:
    return DATA_DIR / "normalized" / "bybit" / "linear" / symbol / kind / f"{day.isoformat()}.parquet"


# ------------------------------------------------------------------------ download


class NotFound(Exception):
    pass


def fetch(url: str, dest: Path, retries: int = 5, backoff_s: float = 2.0, timeout_s: float = 60.0) -> str:
    """Download ``url`` to ``dest`` atomically. Returns "cached" or "downloaded".

    Cache: a file at ``dest`` only ever exists after a complete download (we write to
    ``.part`` and rename), so its presence means "done". Size is checked against
    Content-Length when the server sends it. 404 is not retried.
    """
    if dest.exists():
        return "cached"
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout_s) as r, open(part, "wb") as f:
                expected = r.headers.get("Content-Length")
                ctype = r.headers.get("Content-Type", "")
                if ctype.startswith("text/html"):
                    raise NotFound(f"{url}: got HTML instead of a data file")
                n = 0
                while chunk := r.read(1 << 20):
                    f.write(chunk)
                    n += len(chunk)
            if expected is not None and n != int(expected):
                raise IOError(f"{url}: got {n} bytes, expected {expected}")
            os.replace(part, dest)
            return "downloaded"
        except urllib.error.HTTPError as e:
            part.unlink(missing_ok=True)
            if e.code in (403, 404):  # S3 answers 403/404 for missing keys
                raise NotFound(f"{url}: HTTP {e.code}") from e
            last_err = e
        except NotFound:
            part.unlink(missing_ok=True)
            raise
        except (urllib.error.URLError, OSError, TimeoutError) as e:
            part.unlink(missing_ok=True)
            last_err = e
        if attempt < retries - 1:
            wait = backoff_s * 2**attempt
            print(f"  retry {attempt + 1}/{retries - 1} in {wait:.0f}s: {url} ({last_err})", file=sys.stderr)
            time.sleep(wait)
    raise IOError(f"{url}: failed after {retries} attempts: {last_err}")


@dataclass
class DayFiles:
    day: date
    ob_path: Path | None = None
    ob_depth: int | None = None
    trades_path: Path | None = None


def download_day(symbol: str, day: date) -> tuple[DayFiles, list[str]]:
    files, log = DayFiles(day), []
    primary = ob_depth_for(day)
    for depth in (primary, 700 - primary):  # fall back to the other depth (500 <-> 200)
        url = ob_url(symbol, day, depth)
        dest = raw_dir("orderbook", symbol) / url.rsplit("/", 1)[1]
        try:
            status = fetch(url, dest)
            files.ob_path, files.ob_depth = dest, depth
            log.append(f"{day} orderbook ob{depth}: {status}")
            if depth != primary:
                log.append(f"{day} WARNING: expected ob{primary}, found ob{depth}")
            break
        except NotFound:
            continue
    else:
        log.append(f"{day} orderbook: MISSING (no ob500/ob200 file)")
    url = trades_url(symbol, day)
    dest = raw_dir("trades", symbol) / url.rsplit("/", 1)[1]
    try:
        log.append(f"{day} trades: {fetch(url, dest)}")
        files.trades_path = dest
    except NotFound:
        log.append(f"{day} trades: MISSING")
    return files, log


def download_range(symbol: str, days: list[date], workers: int = 3) -> list[DayFiles]:
    with ThreadPoolExecutor(max_workers=workers) as ex:
        results = list(ex.map(lambda d: download_day(symbol, d), days))
    for _, log in results:
        for line in log:
            print(line)
    return [f for f, _ in results]


# ----------------------------------------------------------------- normalize / validate


def _normalize_one(args: tuple[str, str, str | None, str | None, bool]) -> list[str]:
    from mm.normalize_bybit import normalize_orderbook, normalize_trades

    symbol, day_s, ob, tr, force = args
    day, log = date.fromisoformat(day_s), []
    for kind, src, fn in (("book_updates", ob, normalize_orderbook), ("trades", tr, normalize_trades)):
        out = norm_path(kind, symbol, day)
        if src is None:
            log.append(f"{day} {kind}: no raw file, skipped")
        elif out.exists() and not force:
            log.append(f"{day} {kind}: already normalized")
        else:
            t0 = time.time()
            m = fn(Path(src), out, symbol)
            log.append(f"{day} {kind}: {m['rows']:,} rows in {time.time() - t0:.0f}s")
    return log


def normalize_range(symbol: str, files: list[DayFiles], workers: int, force: bool) -> None:
    jobs = [
        (symbol, f.day.isoformat(), str(f.ob_path) if f.ob_path else None,
         str(f.trades_path) if f.trades_path else None, force)
        for f in files
    ]
    with ProcessPoolExecutor(max_workers=workers) as ex:
        for log in ex.map(_normalize_one, jobs):
            for line in log:
                print(line)


def _validate_one(args: tuple[str, str]) -> dict:
    from mm.normalize_bybit import read_manifest
    from mm.validate import validate_book_file, validate_trades_file

    symbol, day_s = args
    day = date.fromisoformat(day_s)
    rep: dict = {"date": day_s}
    bp, tp = norm_path("book_updates", symbol, day), norm_path("trades", symbol, day)
    if bp.exists():
        man = read_manifest(bp)
        depth = int(man["topics"][0].split(".")[1]) if man["topics"] else None
        rep["book_depth"] = depth
        rep["book_source"] = man["source_file"]
        rep["book"] = validate_book_file(bp, depth_limit=depth)
    else:
        rep["book"] = None
    rep["trades"] = validate_trades_file(tp, day) if tp.exists() else None
    return rep


def validate_range(symbol: str, days: list[date], workers: int) -> list[dict]:
    with ProcessPoolExecutor(max_workers=workers) as ex:
        return list(ex.map(_validate_one, [(symbol, d.isoformat()) for d in days]))


# ------------------------------------------------------------------------- report


def git_commit() -> str:
    try:
        sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True).strip()
        dirty = subprocess.call(["git", "diff", "--quiet", "HEAD"], cwd=REPO_ROOT) != 0
        return sha + ("-dirty" if dirty else "")
    except Exception:
        return "unknown"


BOOK_COLS = [
    ("messages", "msgs"), ("rows", "rows"), ("snapshots", "snaps"), ("gaps", "gaps"),
    ("missing_updates", "missing_u"), ("resyncs", "resyncs"), ("crossed", "crossed"),
    ("locked", "locked"), ("one_sided", "1-sided"), ("snapshot_mismatches", "snap_mismatch"),
    ("delete_missing_level", "del_missing"), ("max_ts_interval_ms", "max_dt_ms"),
]


def format_report(days: list[dict], cont: list[dict]) -> str:
    lines = ["## Order book (rebuilt from snapshot + deltas)", ""]
    hdr = ["date", "depth"] + [h for _, h in BOOK_COLS] + ["end_snap_ok"]
    lines += ["| " + " | ".join(hdr) + " |", "|" + "---|" * len(hdr)]
    for d in days:
        b = d.get("book")
        if not b:
            lines.append(f"| {d['date']} | MISSING |")
            continue
        cells = [d["date"], f"ob{d['book_depth']}"] + [f"{b[k]:,}" for k, _ in BOOK_COLS]
        end_ok = "n/a" if b["snapshot_checks"] == 0 else ("yes" if b["snapshot_mismatches"] == 0 else "NO")
        lines.append("| " + " | ".join(cells + [end_ok]) + " |")
    lines += ["", "## Trades", "", "| date | rows | dup_ids | ts_decreases | outside_day |", "|---|---|---|---|---|"]
    for d in days:
        t = d.get("trades")
        if not t:
            lines.append(f"| {d['date']} | MISSING | | | |")
            continue
        lines.append(f"| {d['date']} | {t['rows']:,} | {t['duplicate_trade_ids']} | {t['ts_decreases']} | {t['outside_utc_day']} |")
    if cont:
        lines += ["", "## Cross-day update-id continuity", ""]
        for c in cont:
            ok = "ok" if c["contiguous"] else "BREAK"
            lines.append(f"- {c['from']} -> {c['to']}: last u {c['last_u']}, next first u {c['next_first_u']} ({ok})")
    return "\n".join(lines)


def write_results(symbol: str, days: list[date], reports: list[dict], cont: list[dict], args: dict) -> Path:
    run_id = "validate_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = RESULTS_DIR / run_id
    out.mkdir(parents=True, exist_ok=True)
    from mm.units import BTCUSDT_LOT_SIZE, BTCUSDT_TICK_SIZE

    config = {
        "run_id": run_id,
        "kind": "data_validation",
        "symbol": symbol,
        "category": "linear",
        "start": days[0].isoformat(),
        "end": days[-1].isoformat(),
        "git_commit": git_commit(),
        "tick_size": BTCUSDT_TICK_SIZE,
        "lot_size": BTCUSDT_LOT_SIZE,
        "sources": {"orderbook": OB_BASE, "trades": TRADES_BASE},
        "args": args,
        "simplifications": [
            "historical ts_local_ms is null (archive has no receive timestamps)",
            "cts (matching-engine ts) dropped: not in common schema",
            "trade timestamps truncated from 0.1 ms to ms",
            "gap detection assumes u increments by exactly 1 per message (empirical, not documented)",
            "trade sequence gaps cannot be detected (no sequence number in archive)",
        ],
    }
    (out / "config.json").write_text(json.dumps(config, indent=2))
    (out / "validation.json").write_text(json.dumps({"days": reports, "continuity": cont}, indent=2))
    (out / "validation.md").write_text(format_report(reports, cont) + "\n")
    return out


# ---------------------------------------------------------------------------- CLI


def parse_day(s: str) -> date:
    return date.fromisoformat(s)


def default_range(today: date | None = None, n: int = 7) -> tuple[date, date]:
    """Last ``n`` complete UTC days (today's file is not published yet)."""
    today = today or datetime.now(timezone.utc).date()
    end = today - timedelta(days=1)
    return end - timedelta(days=n - 1), end


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="mm-bybit", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["download", "normalize", "validate", "run"])
    p.add_argument("--symbol", default="BTCUSDT")
    p.add_argument("--start", type=parse_day, help="first UTC day (default: 7 days ago)")
    p.add_argument("--end", type=parse_day, help="last UTC day, inclusive (default: yesterday)")
    p.add_argument("--workers", type=int, default=2, help="parallel days for normalize/validate (RAM-bound)")
    p.add_argument("--force", action="store_true", help="re-normalize even if Parquet exists")
    a = p.parse_args(argv)

    start, end = default_range()
    start, end = a.start or start, a.end or end
    if end < start:
        p.error("--end before --start")
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    print(f"{a.symbol} linear, {start} .. {end} ({len(days)} days)")

    if a.command in ("download", "normalize", "run"):
        files = download_range(a.symbol, days)
        if a.command in ("normalize", "run"):
            normalize_range(a.symbol, files, a.workers, a.force)
    if a.command in ("validate", "run"):
        from mm.validate import continuity

        reports = validate_range(a.symbol, days, a.workers)
        cont = continuity(reports)
        out = write_results(a.symbol, days, reports, cont, {k: str(v) for k, v in vars(a).items()})
        print()
        print(format_report(reports, cont))
        print(f"\nreport: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
