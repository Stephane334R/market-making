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
