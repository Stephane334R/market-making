//! rust/sim: fill simulator. Replays a quote schedule exported by the Python strategy over
//! one day of normalized book_updates + trades, and writes fills and PnL states.
//!
//! sim --book B.parquet --trades T.parquet --quotes Q.parquet --out DIR
//!     [--latency-ms 50] [--maker-fee 0.0002] [--fill-model l2_queue|touch_ratio]
//!     [--touch-fill-ratio 0.25] [--sample-ms 5000]
//!
//! Writes DIR/fills.parquet, DIR/states.parquet and DIR/run.json (counts, PnL, timings).

mod engine;
mod input;
mod output;

use std::path::PathBuf;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use serde_json::json;

use engine::{Engine, FillModel, SimConfig};
use input::{load_quotes, load_trades, read_units, BookMsg, BookReader};

struct Args {
    book: PathBuf,
    trades: PathBuf,
    quotes: PathBuf,
    out: PathBuf,
    cfg: SimConfig,
}

fn parse_args() -> Result<Args> {
    let (mut book, mut trades, mut quotes, mut out) = (None, None, None, None);
    let mut cfg = SimConfig { latency_ms: 50, maker_fee: 0.0002, touch_fill_ratio: 0.25,
                              fill_model: FillModel::L2Queue, sample_ms: 5000 };
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        let mut val = || it.next().with_context(|| format!("{a} needs a value"));
        match a.as_str() {
            "--book" => book = Some(PathBuf::from(val()?)),
            "--trades" => trades = Some(PathBuf::from(val()?)),
            "--quotes" => quotes = Some(PathBuf::from(val()?)),
            "--out" => out = Some(PathBuf::from(val()?)),
            "--latency-ms" => cfg.latency_ms = val()?.parse()?,
            "--maker-fee" => cfg.maker_fee = val()?.parse()?,
            "--touch-fill-ratio" => cfg.touch_fill_ratio = val()?.parse()?,
            "--sample-ms" => cfg.sample_ms = val()?.parse()?,
            "--fill-model" => {
                cfg.fill_model = match val()?.as_str() {
                    "l2_queue" => FillModel::L2Queue,
                    "touch_ratio" => FillModel::TouchRatio,
                    other => bail!("unknown fill model {other:?}"),
                }
            }
            "-h" | "--help" => {
                println!("sim --book B --trades T --quotes Q --out DIR [--latency-ms N] [--maker-fee F] \
                          [--fill-model l2_queue|touch_ratio] [--touch-fill-ratio R] [--sample-ms N]");
                std::process::exit(0);
            }
            other => bail!("unknown argument {other:?}"),
        }
    }
    if cfg.sample_ms <= 0 {
        bail!("--sample-ms must be > 0");
    }
    Ok(Args {
        book: book.context("--book is required")?,
        trades: trades.context("--trades is required")?,
        quotes: quotes.context("--quotes is required")?,
        out: out.context("--out is required")?,
        cfg,
    })
}

fn main() -> Result<()> {
    let a = parse_args()?;
    let t0 = Instant::now();
    let (tick, lot) = read_units(&a.book)?;
    let trades = load_trades(&a.trades)?;
    let quotes = load_quotes(&a.quotes)?;
    let n_quotes = quotes.len();
    let t_load = t0.elapsed();

    // Replay: merge book messages and trades by exchange time, book first at equal ts.
    let t1 = Instant::now();
    let mut reader = BookReader::open(&a.book)?;
    let mut eng = Engine::new(a.cfg.clone(), tick, lot, quotes);
    let mut msg = BookMsg::default();
    let mut ti = 0;
    while reader.next_message(&mut msg)? {
        while ti < trades.len() && trades[ti].ts < msg.ts {
            eng.on_trade(&trades[ti]);
            ti += 1;
        }
        eng.on_book(&msg)?;
    }
    for t in &trades[ti..] {
        eng.on_trade(t);
    }
    eng.finish();
    let t_replay = t1.elapsed(); // includes streaming + decoding the book Parquet
    if eng.quotes_used() != n_quotes {
        bail!("only {} of {n_quotes} scheduled quotes matched a book message", eng.quotes_used());
    }

    let t2 = Instant::now();
    std::fs::create_dir_all(&a.out)?;
    output::write_fills(&a.out.join("fills.parquet"), &eng.fills)?;
    output::write_states(&a.out.join("states.parquet"), &eng.states)?;
    let t_write = t2.elapsed();

    let (q, cash, edge, inv, fees) = eng.totals();
    let events = eng.n_book + eng.n_trades;
    let total = t0.elapsed().as_secs_f64();
    let run = json!({
        "book_messages": eng.n_book, "book_rows": reader.rows_read, "trades": eng.n_trades, "quotes": n_quotes,
        "fills": eng.fills.len(), "post_only_rejects": eng.n_rejects,
        "final": {"inventory": q, "cash": cash, "spread_capture": edge, "inventory_pnl": inv, "fees": fees},
        "seconds": {"load_trades_quotes": t_load.as_secs_f64(), "replay_incl_book_read": t_replay.as_secs_f64(),
                    "write": t_write.as_secs_f64(), "total": total},
        "events_per_second": events as f64 / total,
    });
    std::fs::write(a.out.join("run.json"), serde_json::to_string_pretty(&run)?)?;
    println!("{}", serde_json::to_string_pretty(&run)?);
    Ok(())
}
