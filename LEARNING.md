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

## 2. Live recorder (`rust/recorder`)

Code: `src/main.rs` (CLI, connection loop, message handling, REST check), `src/book.rs`
(local L2 book + gap rule), `src/sink.rs` (rotated Parquet), `src/bybit.rs` (wire formats,
REST), `src/units.rs` (exact decimal -> int). Rust unit tests in `book.rs`/`units.rs`;
`tests/test_recorded.py` checks a recorded session with the Python validator.

- **What:** subscribes to `orderbook.1000.BTCUSDT` (deepest public level, 200 ms) and
  `publicTrade.BTCUSDT` on `wss://stream.bybit.com/v5/public/linear`, keeps a local book,
  writes the common schema to Parquet rotated every N minutes (`--rotate-minutes`, default 5),
  and logs every anomaly to `events.jsonl`. Run: `rust/target/release/recorder --minutes 10`.
- **Why Rust:** one long-running process with predictable latency and memory; the Python
  validator stays the single reference for correctness checks.
- **Verified in Bybit docs (2026-11):** endpoint; linear depths 1/50/200/1000; snapshot/delta
  format with `u` (update id) and `seq` (cross sequence); `publicTrade` fields
  (`T,s,S,v,p,L,i,BT,RPI,seq`); ping `{"op":"ping"}` every 20 s (connection cut after 10 min
  without traffic); `unsubscribe` op; REST `/v5/market/orderbook` (limit 1-1000).
- **Gap handling:** a delta must have `u == prev_u + 1` (empirical, holds on archive and
  live data). On a gap: log it, mark the book invalid, `unsubscribe` + `subscribe` the
  orderbook topic, and rebuild from the snapshot that follows. Deltas that arrive while
  invalid are still **written** (never dropped) but not applied. On disconnect/stale stream
  (no frame for 30 s): reconnect with exponential backoff 1 s -> 60 s, reset to 1 s after a
  session that produced a valid book.
- **Surprise found live:** the snapshot sent after a resubscribe can be *older* than the
  deltas already received (seen: 16 updates / 3.2 s behind); Bybit then re-sends those
  update ids. So `update_id` is not unique in a session, and `seq`/`ts` go backwards exactly
  at resync points. The recorder clears its REST-alignment history on every reset; the
  Python test allows those anomalies only at gap/resync points.
- **REST quality check:** every 60 s fetch the top 50 levels from REST. The REST `u` is in the
  same id space as `orderbook.1000` (verified live), so the check compares against the local
  book **at exactly that `u`** (a ring buffer of recent top-50 states; waits up to 10 s if the
  stream lags behind REST). A level-by-level comparison means any mismatch is a real bug,
  not timing noise. `rest_unaligned` is logged when that `u` is not in the buffer (e.g. a
  REST reply that predates a reconnect).
- **Testing the failure path:** `--simulate-gap-secs N` (TESTING ONLY) discards one delta
  before writing every N s. A 3-minute run: 2 drops -> 2 gaps -> 2 resubscribes -> 2 rebuilds,
  all REST checks matched afterwards; Python found the same 2 gaps and 3 resyncs.
- **Clean shutdown:** Ctrl-C or `--minutes` -> close the socket, flush buffers, close Parquet
  writers, rename `.tmp` -> `.parquet`, write manifests and `summary.json`. A crash leaves only
  `.tmp` files, never a truncated `.parquet`.
- **Assumptions / simplifications (labelled):**
  - `ts_local_ms` is the local wall clock at socket read; clock offset to Bybit is not
    measured (**latency simplification**). `--minutes` counts monotonic time, which does not
    advance while the machine sleeps (seen in the 10-minute test run).
  - `cts` and the trade `RPI`/`BT`/`L`/`seq` fields are not in the common schema and are dropped.
  - RPI orders are excluded from the public orderbook stream by Bybit.
  - Trade loss cannot be detected (no trade sequence); disconnect windows in `events.jsonl`
    mark where trades may be missing.
  - The tick/lot sizes are read once at start-up from `instruments-info`.
- **What can go wrong:** local network or sleep/resume (handled: reconnect + fresh snapshot);
  Bybit changing message shape (parsing fails loudly and stops rather than writing wrong data);
  an off-tick price (fatal by design); disk full (write error stops the session); a slow
  consumer — everything runs on one task, so a long Parquet flush delays reads (fine at
  ~5 msg/s, not for many symbols).
- **Production-grade:** run on a VPS close to Bybit (Singapore/Tokyo) with chrony and a
  measured clock offset; supervise with systemd and alert on gaps, disconnects and REST
  mismatches; two redundant recorders on separate connections and a merge step; separate
  the network reader from the writer (bounded channel) and record raw frames as well;
  extend the schema with `cts` and trade `seq`/RPI; capture the instrument spec per file.

## 3. Strategy, simulator and analytics (ported from crypto-market-making)

Code: `python/mm/strategy.py`, `mm/simulator.py`, `mm/analytics.py`, `mm/plots.py`,
`mm/backtest.py` (leave-days-out CV, `python -m mm.backtest`), `mm/report.py` (summary.md).
Tests: `tests/test_simulator.py` (original tests ported plus L2 queue tests),
`tests/test_sim_reference.py` (exact re-run of the committed reference outputs;
`MM_RUN_REFERENCE=1`).

- **What:** the same research loop as the Hyperliquid repo. A vol-scaled spread with
  inventory skew (a practical Avellaneda-Stoikov), replayed event by event with latency,
  post-only rejection and maker fees. PnL is split exactly into spread capture, inventory
  and fees (identity tested), with fill markouts from 0 to 60 s. Parameters are selected on
  the earlier days and the held-out days are reported as a per-day distribution.
- **Adapted to the common schema:** prices are integer ticks, the book is rebuilt from
  full-depth deltas (`iter_book_messages`), trades come from the normalized Parquet, and
  units come from the file footer.
- **New: L2 queue-position estimate** (`fill_model="l2_queue"`, the default; the original
  `touch_ratio` model is kept for comparison).
  - Joining a level puts the displayed size at our price ahead of us.
  - Prints at our price consume that queue first.
  - Size decreases not explained by prints are treated as pro-rata cancels.
  - Amending down keeps priority; a larger size or a new price goes to the back.
  - A print through our price fills our full size, as in the original.
- **Performance design:** all 56 parameter sets replay in one pass per day. The book
  rebuild dominates the cost, so the sets share the event stream, the EWMA vol, the
  requote clock and the latency. Inventory PnL is accumulated lazily (exact telescoping
  sum). States are sampled every 5 s. One pass takes 2.5-6 min per day; the 7-day CV takes
  16 min on 2 workers (2 GB RAM).
- **Bybit-specific choices:**
  - Maker fee 0.02%, from Bybit's fee page.
  - The grid is mapped to the Hyperliquid grid's *dollar* distances (Bybit's tick is 10x
    finer), and extended past the ~150-tick fee break-even.
- **Result (5 train / 2 held-out days, 2026-11-03..09):** no edge, as on Hyperliquid. All
  configurations lose on train, and PnL improves monotonically with width up to the grid
  edge. Held out: -109 ± 164 USDT/day (|t| < 1).
  - Fees dominate near the touch.
  - Markouts fall from +0.74 bps at the fill to -0.40 bps within 100 ms (hs=80).
  - 80-92% of fills are prints through our price (sweeps), so the queue model changes PnL
    by < 1%.
  - At the Hyperliquid-equivalent setting Bybit loses more per day and per unit volume:
    a higher fee, ~8x more trades, and a finer book; see the run's summary.md.
- **Assumptions / simplifications (labelled in code and summary):**
  - The queue position is estimated, not observed.
  - No market impact.
  - Constant 50 ms latency, instant cancels.
  - No funding payments.
  - Book `ts` and trade `T` are merged as one clock.
  - Drawdown and time-at-limit are measured on 5 s samples.
  - Every day starts flat.
  - Selection rests on 5 days of one regime.
- **What can go wrong:**
  - Reading the grid-edge "best" as an optimum: it is the "don't trade" limit.
  - Trusting per-day t-stats from 2 held-out days.
  - Mapping parameters across venues in ticks rather than in dollars or bps.
  - A different `vol_halflife_s` per set would break the shared-EWMA design; it is checked
    and raises.
- **Production-grade:**
  - Port the event loop to Rust (`rust/sim`), validated against
    `tests/reference/sim_bybit_2026-11-09*`.
  - Model latency jitter and cancel latency.
  - Model queue position with order-level (L3) data where available.
  - Add funding and a toxicity filter.
  - Validate the fill model against live fills on testnet.
  - Use many more days, across regimes.
