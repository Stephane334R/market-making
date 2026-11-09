# market-making

Market-making research framework for the Bybit BTCUSDT linear perpetual.

- **Python** (`python/mm/`): historical data download, normalization, strategy, simulation, analytics.
- **Rust** (`rust/`): live websocket recorder and, later, a fast fill simulator.

## Layout

```
python/mm/        Python package (uv project in python/)
rust/             Cargo workspace (recorder/, sim/ later)
data/raw/         raw downloaded / recorded data (git-ignored)
data/normalized/  data in the common schema (git-ignored)
results/<run_id>/ run outputs: summaries, charts, config.json (git-ignored)
tests/            Python tests
docs/SCHEMA.md    common schema for historical and live data
```

Public market data only. No API keys are used or stored.

Status: scaffolding only, no logic yet.
