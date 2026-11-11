//! Reading the inputs: normalized book_updates / trades Parquet (docs/SCHEMA.md) and the
//! quote schedule exported by the Python strategy (`Runner.quotes_df()`).

use std::fs::File;
use std::path::Path;

use anyhow::{bail, Context, Result};
use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;

const BATCH_ROWS: usize = 262_144;

/// Tick and lot size from the Parquet footer (`mm` metadata key, written by both the
/// Python normalizer and the Rust recorder), as f64 exactly like Python's `float("0.10")`.
pub fn read_units(path: &Path) -> Result<(f64, f64)> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let meta = builder
        .schema()
        .metadata()
        .get("mm")
        .cloned()
        .or_else(|| {
            let kv = builder.metadata().file_metadata().key_value_metadata()?;
            kv.iter().find(|e| e.key == "mm").and_then(|e| e.value.clone())
        })
        .with_context(|| format!("{}: no `mm` metadata (tick/lot sizes)", path.display()))?;
    let v: serde_json::Value = serde_json::from_str(&meta)?;
    let get = |k: &str| -> Result<f64> {
        v[k].as_str().with_context(|| format!("mm.{k} missing"))?.parse::<f64>().context(k.to_string())
    };
    Ok((get("tick_size")?, get("lot_size")?))
}

fn open(path: &Path, columns: &[&str]) -> Result<ParquetRecordBatchReader> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).with_context(|| path.display().to_string())?)?;
    let mask = ProjectionMask::columns(builder.parquet_schema(), columns.iter().copied());
    Ok(builder.with_projection(mask).with_batch_size(BATCH_ROWS).build()?)
}

fn col<'a>(b: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef> {
    b.column_by_name(name).with_context(|| format!("missing column {name}"))
}

fn i64s(b: &RecordBatch, name: &str) -> Result<Int64Array> {
    let c = col(b, name)?;
    Ok(c.as_any().downcast_ref::<Int64Array>().with_context(|| format!("{name}: not int64"))?.clone())
}

fn f64s(b: &RecordBatch, name: &str) -> Result<Float64Array> {
    let c = col(b, name)?;
    Ok(c.as_any().downcast_ref::<Float64Array>().with_context(|| format!("{name}: not float64"))?.clone())
}

fn bools(b: &RecordBatch, name: &str) -> Result<BooleanArray> {
    let c = col(b, name)?;
    Ok(c.as_any().downcast_ref::<BooleanArray>().with_context(|| format!("{name}: not bool"))?.clone())
}

/// String columns may come back as Utf8, LargeUtf8 or a view type depending on the
/// writer; normalize to Utf8.
fn strs(b: &RecordBatch, name: &str) -> Result<StringArray> {
    let c = cast(col(b, name)?, &DataType::Utf8)?;
    Ok(c.as_any().downcast_ref::<StringArray>().context("utf8 cast")?.clone())
}

/// One book message (a run of rows sharing update_id, seq, is_snapshot). Owned by the
/// caller and refilled in place by `BookReader::next_message`, so the hot loop does not
/// allocate a new message per update.
#[derive(Default, Debug)]
pub struct BookMsg {
    pub ts: i64,
    pub seq: i64,
    pub update_id: i64,
    pub is_snapshot: bool,
    pub bids: Vec<(i64, i64)>, // (price ticks, size lots), in file order
    pub asks: Vec<(i64, i64)>,
}

/// Streams book_updates rows batch by batch and regroups them into messages (same rule
/// as `mm.normalize_bybit.iter_book_messages`).
pub struct BookReader {
    batches: ParquetRecordBatchReader,
    cols: Option<(Int64Array, Int64Array, Int64Array, BooleanArray, StringArray, Int64Array, Int64Array)>,
    row: usize,
    pub rows_read: u64,
}

impl BookReader {
    pub fn open(path: &Path) -> Result<BookReader> {
        let names = ["ts_exchange_ms", "seq", "update_id", "is_snapshot", "side", "price", "size"];
        Ok(BookReader { batches: open(path, &names)?, cols: None, row: 0, rows_read: 0 })
    }

    fn load_next_batch(&mut self) -> Result<bool> {
        match self.batches.next() {
            None => Ok(false),
            Some(b) => {
                let b = b?;
                self.cols = Some((
                    i64s(&b, "ts_exchange_ms")?,
                    i64s(&b, "seq")?,
                    i64s(&b, "update_id")?,
                    bools(&b, "is_snapshot")?,
                    strs(&b, "side")?,
                    i64s(&b, "price")?,
                    i64s(&b, "size")?,
                ));
                self.row = 0;
                Ok(true)
            }
        }
    }

    /// Fill `out` with the next message. Returns false at end of file.
    pub fn next_message(&mut self, out: &mut BookMsg) -> Result<bool> {
        out.bids.clear();
        out.asks.clear();
        let mut started = false;
        loop {
            let exhausted = match &self.cols {
                None => true,
                Some(c) => self.row >= c.0.len(),
            };
            if exhausted && !self.load_next_batch()? {
                return Ok(started);
            }
            let (ts, seq, uid, snap, side, px, sz) = self.cols.as_ref().expect("loaded");
            while self.row < ts.len() {
                let i = self.row;
                let key = (uid.value(i), seq.value(i), snap.value(i));
                if !started {
                    out.ts = ts.value(i);
                    (out.update_id, out.seq, out.is_snapshot) = key;
                    started = true;
                } else if key != (out.update_id, out.seq, out.is_snapshot) {
                    return Ok(true); // row i belongs to the next message
                }
                let level = (px.value(i), sz.value(i));
                match side.value(i) {
                    "bid" => out.bids.push(level),
                    "ask" => out.asks.push(level),
                    other => bail!("unknown side {other:?}"),
                }
                self.row += 1;
                self.rows_read += 1;
            }
        }
    }
}

/// One trade: taker side +1 (buyer-initiated, hits asks) / -1 (seller-initiated).
#[derive(Clone, Copy, Debug)]
pub struct Trade {
    pub ts: i64,
    pub side: i8,
    pub px: i64,
    pub sz: i64,
}

/// Trades are small enough (~2.5M/day, 32 bytes each with padding) to load whole; simpler than a
/// second streaming cursor for the time merge.
pub fn load_trades(path: &Path) -> Result<Vec<Trade>> {
    let mut out = Vec::new();
    for b in open(path, &["ts_exchange_ms", "side", "price", "size"])? {
        let b = b?;
        let (ts, side, px, sz) = (i64s(&b, "ts_exchange_ms")?, strs(&b, "side")?, i64s(&b, "price")?, i64s(&b, "size")?);
        for i in 0..ts.len() {
            let s = match side.value(i) {
                "buy" => 1,
                "sell" => -1,
                other => bail!("unknown trade side {other:?}"),
            };
            out.push(Trade { ts: ts.value(i), side: s, px: px.value(i), sz: sz.value(i) });
        }
    }
    Ok(out)
}

/// One requote decided by the Python strategy at book message `msg_index`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct QuoteRow {
    pub msg_index: u64,
    pub ts: i64,
    pub bid_px: Option<i64>, // None = no bid (inventory at the limit)
    pub ask_px: Option<i64>,
    pub bid_sz: f64,
    pub ask_sz: f64,
}

pub fn load_quotes(path: &Path) -> Result<Vec<QuoteRow>> {
    let mut out = Vec::new();
    for b in open(path, &["msg_index", "ts", "bid_px", "ask_px", "bid_sz", "ask_sz"])? {
        let b = b?;
        let (mi, ts, bp, ap) = (i64s(&b, "msg_index")?, i64s(&b, "ts")?, i64s(&b, "bid_px")?, i64s(&b, "ask_px")?);
        let (bs, asz) = (f64s(&b, "bid_sz")?, f64s(&b, "ask_sz")?);
        for i in 0..mi.len() {
            let opt = |a: &Int64Array| if a.is_null(i) { None } else { Some(a.value(i)) };
            out.push(QuoteRow {
                msg_index: mi.value(i) as u64,
                ts: ts.value(i),
                bid_px: opt(&bp),
                ask_px: opt(&ap),
                bid_sz: bs.value(i),
                ask_sz: asz.value(i),
            });
        }
    }
    if out.windows(2).any(|w| w[1].msg_index <= w[0].msg_index) {
        bail!("quote schedule must be strictly increasing in msg_index");
    }
    Ok(out)
}
