# LEARNING

A running log of each component: what it does, why, its assumptions,
what can go wrong, and how to make it production-grade.

## 0. Repository scaffolding

- **What:** project layout with a Python research package (`python/mm`, managed by uv)
  and a Rust workspace (`rust/`) for latency- and throughput-sensitive parts.
- **Why:** Python is fastest to iterate on for research; Rust gives predictable latency
  for live recording and speed for large-scale simulation.
- **Assumptions:** a single venue and instrument (Bybit BTCUSDT linear perpetual);
  public market data only.
- **What can go wrong:** the two languages drifting apart on data formats. Mitigation:
  one common schema (`docs/SCHEMA.md`) shared by historical and live pipelines.
- **Production-grade:** CI running Python and Rust tests, pinned toolchains, data
  versioning for raw captures.

## 1. Historical downloader, normalizer and book validator

Code: `python/mm/download_bybit.py` (sources, download, cache, CLI `mm-bybit`),
`mm/normalize_bybit.py` (parsing to Parquet), `mm/book.py` (L2 rebuild + checks),
`mm/validate.py` (per-day report), `mm/units.py` (exact decimal -> int).
Tests: `tests/test_book.py`, `tests/test_normalize.py`, `tests/test_download.py`.

- **What:** downloads Bybit's daily archives for BTCUSDT linear, converts them to the
  common schema (Parquet per day), rebuilds the order book from snapshot + deltas, and
  writes a validation report to `results/validate_<UTC time>/` with `config.json`.
- **Sources (verified 2026-11-10):**
  - Order book: `https://quote-saver.bycsi.com/orderbook/linear/BTCUSDT/<DATE>_BTCUSDT_ob<DEPTH>.data.zip`,
    the files behind Bybit's history-data page. Depth 500 up to 2025-08-20, **200 from
    2025-08-21** (Bybit API announcement: "level 500 @ 100 ms to level 200 @ 100 ms").
    Each zip holds one JSONL file in the websocket `orderbook.200` format: one snapshot,
    then a delta about every 100 ms (~864k messages, 25-85M level rows, 1.7 GB unzipped per day).
  - Trades: `https://public.bybit.com/trading/BTCUSDT/BTCUSDT<DATE>.csv.gz`
    (header `timestamp,symbol,side,size,price,tickDirection,trdMatchID,...,RPI`).
  - Tick 0.10 and lot 0.001 come from `GET https://api.bybit.com/v5/market/instruments-info`.
- **Parsing:** each message turns into one row per level. `ts` maps to `ts_exchange_ms`,
  `u` to `update_id`, `seq` to `seq`, and `type` to `is_snapshot`. Prices and sizes become
  int64 ticks/lots through `Decimal`, cached per distinct string. Conversion raises
  instead of rounding. Rows are flushed every 500k, so memory stays bounded (the dev box
  has 2 GB of RAM). Writes are atomic (`.tmp` then rename). A sidecar manifest holds the
  source SHA-256, message counts and the ids of empty messages.
- **Book rebuild rules** come from Bybit's docs: size 0 deletes a level, an unknown price
  inserts one, a known price overwrites it, a new snapshot resets the book, and `u == 1`
  marks a service-restart snapshot.
- **Checks:** a gap is a delta with `u != prev_u + 1`. Also counted: crossed (bid > ask)
  and locked (bid == ask) books after each *message* (not after each level, since
  intermediate states inside a message can cross legitimately), one-sided books, depth
  above the stated level, deletes of unknown levels, `seq`/`ts` going backwards, intervals
  over 1 s, and the end-of-file snapshot (it repeats the last `u`) compared level by level
  with the rebuilt book. The next day's first `u` must continue the previous day's last `u`.
  Trades: duplicate ids, timestamps going backwards, rows outside the UTC day.
- **Result, 2026-11-03..09:** 0 gaps, 0 crossed or locked books, 0 one-sided books. All 7
  end-of-day snapshots match the rebuilt book exactly. Update ids continue across all
  6 day boundaries. Max interval between messages: 500 ms. 433M book rows, 11.6M trades.
- **Assumptions / simplifications (labelled):**
  - `u` going up by 1 per message is **empirical**, not documented. Every message checked
    so far satisfies it.
  - Historical `ts_local_ms` is null. `cts` is dropped. Trade timestamps are truncated
    from 0.1 ms to 1 ms. Trade columns RPI and tickDirection are dropped.
  - Trade gaps cannot be detected: the archive has no trade sequence number.
  - Tick and lot sizes are assumed constant over the range. A change would raise during
    conversion, not round silently.
  - The 100 ms archive is a *conflated* feed: changes inside 100 ms are merged, so
    queue dynamics shorter than that are invisible. Depth is limited to 200 levels.
- **What can go wrong:** Bybit moving or renaming the archive host (it is not part of
  the documented API). An HTML error page served with status 200 (handled: rejected by
  Content-Type). Truncated downloads (handled: Content-Length check, zip/gzip CRC checked
  on full read). A depth change mid-history (handled: fallback to the other depth with a
  warning). Slow processing: Python takes about 6 min per order book day.
- **Production-grade:** move parsing and validation to Rust (or Polars) for a 10-50x
  speedup. Checksum raw files against a published manifest. Store `cts` and the RPI flag
  once the schema is extended. Cross-check trades against the book (trade price within
  the spread at `cts`). Alert on any non-zero anomaly counter. Pin and record the
  instrument spec per day from `instruments-info`.
