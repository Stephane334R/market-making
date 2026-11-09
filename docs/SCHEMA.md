# Common data schema

Shared by historical (downloaded) and live (recorded) data. To be specified in detail
(field types, units, exact-decimal representation, gap semantics) once each field
has been verified against Bybit's official documentation.

## book_updates

| field | description |
|---|---|
| ts_exchange_ms | exchange timestamp, ms |
| ts_local_ms | local receive timestamp, ms |
| seq | sequence number |
| update_id | order book update id |
| is_snapshot | true for snapshot rows, false for deltas |
| side | bid / ask |
| price | exact (decimal string or integer ticks) |
| size | exact (decimal string or integer lots) |

## trades

| field | description |
|---|---|
| ts_exchange_ms | exchange timestamp, ms |
| ts_local_ms | local receive timestamp, ms |
| trade_id | exchange trade id |
| side | taker side |
| price | exact (decimal string or integer ticks) |
| size | exact (decimal string or integer lots) |
