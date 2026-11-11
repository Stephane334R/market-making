//! Bybit v5 wire formats and REST calls (verified against bybit-exchange.github.io/docs).
//!
//! - WS public linear: wss://stream.bybit.com/v5/public/linear
//! - topics: `orderbook.{depth}.{symbol}` (linear depths 1, 50, 200, 1000), `publicTrade.{symbol}`
//! - heartbeat: send {"op":"ping"} every 20 s
//! - REST: GET /v5/market/orderbook (limit 1..1000 for linear), GET /v5/market/instruments-info

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::value::RawValue;

pub const WS_URL: &str = "wss://stream.bybit.com/v5/public/linear";
pub const REST_BASE: &str = "https://api.bybit.com";
pub const PING_INTERVAL_SECS: u64 = 20;

/// Any message on the public stream: data pushes have `topic`; op replies have `op`.
#[derive(Deserialize)]
pub struct Envelope<'a> {
    #[serde(borrow)]
    pub topic: Option<&'a str>,
    #[serde(rename = "type")]
    pub kind: Option<&'a str>,
    pub ts: Option<i64>,
    #[serde(borrow)]
    pub data: Option<&'a RawValue>,
    pub op: Option<String>,
    pub success: Option<bool>,
    pub ret_msg: Option<String>,
}

/// `data` of an orderbook message (and `result` of the REST orderbook).
#[derive(Deserialize)]
pub struct BookData {
    pub s: String,
    pub b: Vec<(String, String)>,
    pub a: Vec<(String, String)>,
    pub u: i64,
    pub seq: i64,
}

/// One element of a publicTrade `data` array.
#[derive(Deserialize)]
pub struct TradeData {
    #[serde(rename = "T")]
    pub time_ms: i64,
    pub s: String,
    #[serde(rename = "S")]
    pub side: String,
    pub v: String,
    pub p: String,
    pub i: String,
}

pub fn subscribe_msg(op: &str, topics: &[String]) -> String {
    serde_json::json!({ "op": op, "args": topics }).to_string()
}

pub fn ping_msg() -> String {
    r#"{"op":"ping"}"#.to_string()
}

#[derive(Deserialize)]
struct RestReply<T> {
    #[serde(rename = "retCode")]
    ret_code: i64,
    #[serde(rename = "retMsg")]
    ret_msg: String,
    result: Option<T>,
}

fn unwrap_reply<T>(r: RestReply<T>, what: &str) -> Result<T> {
    if r.ret_code != 0 {
        bail!("{what}: retCode {} {}", r.ret_code, r.ret_msg);
    }
    r.result.with_context(|| format!("{what}: missing result"))
}

/// Tick size and qty step for a linear symbol.
pub async fn fetch_steps(client: &reqwest::Client, symbol: &str) -> Result<(String, String)> {
    #[derive(Deserialize)]
    struct List {
        list: Vec<Instrument>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Instrument {
        price_filter: PriceFilter,
        lot_size_filter: LotFilter,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct PriceFilter {
        tick_size: String,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct LotFilter {
        qty_step: String,
    }
    let url = format!("{REST_BASE}/v5/market/instruments-info?category=linear&symbol={symbol}");
    let reply: RestReply<List> = client.get(&url).send().await?.error_for_status()?.json().await?;
    let list = unwrap_reply(reply, "instruments-info")?;
    let inst = list.list.into_iter().next().with_context(|| format!("unknown symbol {symbol}"))?;
    Ok((inst.price_filter.tick_size, inst.lot_size_filter.qty_step))
}

/// REST order book snapshot (top `limit` levels per side).
pub async fn fetch_orderbook(client: &reqwest::Client, symbol: &str, limit: u32) -> Result<BookData> {
    let url = format!("{REST_BASE}/v5/market/orderbook?category=linear&symbol={symbol}&limit={limit}");
    let reply: RestReply<BookData> = client.get(&url).send().await?.error_for_status()?.json().await?;
    unwrap_reply(reply, "orderbook")
}
