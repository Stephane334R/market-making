//! Write fills and sampled PnL states as Parquet, with the same columns and dtypes as
//! the Python simulator's `fills_df()` / `states_df()`.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::engine::{Fill, States};

fn write(path: &Path, fields: Vec<(&str, DataType)>, cols: Vec<ArrayRef>) -> Result<()> {
    let schema = Arc::new(Schema::new(fields.into_iter().map(|(n, t)| Field::new(n, t, false)).collect::<Vec<_>>()));
    let batch = RecordBatch::try_new(schema.clone(), cols)?;
    let props = WriterProperties::builder().set_compression(Compression::ZSTD(ZstdLevel::default())).build();
    let mut w = ArrowWriter::try_new(File::create(path)?, schema, Some(props))?;
    w.write(&batch)?;
    w.close()?;
    Ok(())
}

pub fn write_fills(path: &Path, fills: &[Fill]) -> Result<()> {
    let f64col = |g: fn(&Fill) -> f64| Arc::new(Float64Array::from_iter_values(fills.iter().map(g))) as ArrayRef;
    write(
        path,
        vec![
            ("ts", DataType::Int64), ("side", DataType::Int64), ("px", DataType::Float64), ("qty", DataType::Float64),
            ("mid", DataType::Float64), ("fee", DataType::Float64), ("inventory", DataType::Float64),
            ("through", DataType::Boolean),
        ],
        vec![
            Arc::new(Int64Array::from_iter_values(fills.iter().map(|f| f.ts))),
            Arc::new(Int64Array::from_iter_values(fills.iter().map(|f| f.side))),
            f64col(|f| f.px), f64col(|f| f.qty), f64col(|f| f.mid), f64col(|f| f.fee), f64col(|f| f.inventory),
            Arc::new(BooleanArray::from(fills.iter().map(|f| f.through).collect::<Vec<_>>())),
        ],
    )
}

pub fn write_states(path: &Path, s: &States) -> Result<()> {
    // equity = cash + inventory * mid, same expression as pandas in states_df()
    let equity: Vec<f64> = (0..s.ts.len()).map(|i| s.cash[i] + s.inventory[i] * s.mid[i]).collect();
    let f = |v: &Vec<f64>| Arc::new(Float64Array::from(v.clone())) as ArrayRef;
    write(
        path,
        vec![
            ("ts", DataType::Int64), ("mid", DataType::Float64), ("inventory", DataType::Float64),
            ("cash", DataType::Float64), ("spread_capture", DataType::Float64), ("inventory_pnl", DataType::Float64),
            ("fees", DataType::Float64), ("equity", DataType::Float64),
        ],
        vec![
            Arc::new(Int64Array::from(s.ts.clone())), f(&s.mid), f(&s.inventory), f(&s.cash),
            f(&s.spread_capture), f(&s.inventory_pnl), f(&s.fees), f(&equity),
        ],
    )
}
