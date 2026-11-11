# Common data schema

Shared by historical (downloaded) and live (recorded) data. Stored as Parquet (zstd),
one file per UTC day and stream:

```
data/normalized/bybit/linear/<SYMBOL>/book_updates/YYYY-MM-DD.parquet  (+ .manifest.json)
data/normalized/bybit/linear/<SYMBOL>/trades/YYYY-MM-DD.parquet        (+ .manifest.json)
```

## Exact units

Prices and sizes are **int64 multiples of the instrument step**, never floats:

- `price` = price / `tick_size` (BTCUSDT linear: `0.10`) → `74810.40` is `748104`
- `size`  = size / `lot_size` (BTCUSDT linear: `0.001`) → `0.480` is `480`

The steps are stored in each Parquet footer (schema metadata key `mm`, JSON with
`symbol`, `tick_size`, `lot_size`; read with `mm.normalize_bybit.read_units`) and in the
manifest. Conversion uses `Decimal` and **raises** if a value is not an exact multiple
(`mm.units.to_units`), so a tick-size change can never be rounded away silently.

## book_updates

One row per price level per message. Row order = message order in the source; inside
a message, all bid levels, then all ask levels, each in exchange order. A message is a
run of consecutive rows with the same (`update_id`, `seq`, `is_snapshot`).

| field | type | description |
|---|---|---|
| ts_exchange_ms | int64 | Bybit `ts`: when the system generated the message, ms |
| ts_local_ms | int64, nullable | local receive time, ms. **Null for historical data** (archive has none) |
| seq | int64 | Bybit `data.seq`, cross sequence (non-decreasing, not contiguous) |
| update_id | int64 | Bybit `data.u`. +1 per message in the archive (empirical); `u == 1` means a service restart snapshot |
| is_snapshot | bool | true for snapshot rows (replace the book), false for deltas |
| side | string | `bid` / `ask` |
| price | int64 | ticks |
| size | int64 | lots. In deltas, `0` = delete the level; otherwise it is the new absolute size |

Not carried: `cts` (matching-engine timestamp), `topic`, `data.s` (constant per file).
Messages with no levels have no rows; their ids are listed in the manifest
(`empty_update_ids`) so they are not mistaken for gaps.

## trades

| field | type | description |
|---|---|---|
| ts_exchange_ms | int64 | trade time, ms. Historical: archive `timestamp` (s, 0.1 ms resolution) **truncated** to ms |
| ts_local_ms | int64, nullable | local receive time, ms. Null for historical data |
| trade_id | string | Bybit trade id (archive `trdMatchID`, websocket `i`), a UUID |
| side | string | taker side: `buy` / `sell` |
| price | int64 | ticks |
| size | int64 | lots |

Row order = source file order. The trade archive has no sequence number, so trade
gaps cannot be detected from historical files alone.

## Manifest (`<day>.manifest.json`)

Source file name and SHA-256, tick/lot sizes, row/message counts, snapshot/delta counts,
topics seen, `empty_update_ids` (book only), source CSV header (trades only).

## Live recordings (rust/recorder)

Same columns, units, footer metadata and manifests as above, written by the Rust recorder:

```
data/live/<SYMBOL>/<session UTC start>/book_updates/<window>.parquet (+ .manifest.json)
data/live/<SYMBOL>/<session UTC start>/trades/<window>.parquet       (+ .manifest.json)
data/live/<SYMBOL>/<session UTC start>/events.jsonl   gaps, resyncs, reconnects, REST checks
data/live/<SYMBOL>/<session UTC start>/summary.json   counters + list of files
```

Differences from historical files:

- Source topic `orderbook.1000.<SYMBOL>` (deepest public level, 200 ms) instead of the 200-level archive.
- `ts_local_ms` is filled: wall-clock time when the frame was read from the socket
  (local clock; its offset to exchange time is not measured: a **latency simplification**).
- trades `ts_exchange_ms` = websocket `T` (fill time), the same meaning as the archive `timestamp`.
- Files are rotated on wall-clock windows (default 5 min). The book carries across files,
  so validate a session's files **as one stream** (`mm.validate.validate_live_session`).
- `update_id` is **not unique** within a session: after a gap or reconnect the recorder
  resubscribes, and Bybit's fresh snapshot can be older than deltas already received
  (observed: up to ~3 s), after which the following update ids are sent again. Always
  process rows in file order and reset the book at each snapshot.
