//! Live Bybit recorder: orderbook.1000 + publicTrade for one linear symbol, written to
//! the common schema as rotated Parquet files. Local testing tool (no VPS, no keys).
//!
//! Usage: recorder [--symbol BTCUSDT] [--minutes N] [--out data/live] [--rotate-minutes 5]
//!                 [--simulate-gap-secs N]   (TESTING ONLY: drop one delta every N s)

mod book;
mod bybit;
mod sink;
mod units;

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use book::{Book, Level, Outcome};
use bybit::{BookData, Envelope, TradeData};
use sink::{FileMeta, Sink};
use units::Step;

const DEPTH: u32 = 1000; // deepest linear orderbook level on the public stream
const REST_CHECK_SECS: u64 = 60;
const REST_LEVELS: usize = 50; // top levels compared against the REST snapshot
const HISTORY_LEN: usize = 600; // local top-of-book states kept for REST alignment (~2 min)
const STALE_SECS: u64 = 30; // no message at all for this long -> reconnect
const MAX_BACKOFF_SECS: u64 = 60;

// ------------------------------------------------------------------------- CLI

struct Config {
    symbol: String,
    minutes: Option<f64>,
    out: PathBuf,
    rotate_minutes: u64,
    simulate_gap_secs: Option<u64>,
}

fn parse_args() -> Result<Config> {
    let mut cfg = Config { symbol: "BTCUSDT".into(), minutes: None, out: "data/live".into(), rotate_minutes: 5, simulate_gap_secs: None };
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut val = || args.next().with_context(|| format!("{a} needs a value"));
        match a.as_str() {
            "--symbol" => cfg.symbol = val()?.to_uppercase(),
            "--minutes" => cfg.minutes = Some(val()?.parse().context("--minutes")?),
            "--out" => cfg.out = val()?.into(),
            "--rotate-minutes" => cfg.rotate_minutes = val()?.parse().context("--rotate-minutes")?,
            "--simulate-gap-secs" => cfg.simulate_gap_secs = Some(val()?.parse().context("--simulate-gap-secs")?),
            "-h" | "--help" => {
                println!("recorder [--symbol BTCUSDT] [--minutes N] [--out data/live] [--rotate-minutes 5] [--simulate-gap-secs N]");
                std::process::exit(0);
            }
            other => bail!("unknown argument {other:?} (see --help)"),
        }
    }
    if cfg.rotate_minutes == 0 {
        bail!("--rotate-minutes must be >= 1");
    }
    Ok(cfg)
}

// --------------------------------------------------------------- events & stats

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

/// Append-only JSONL log of everything unusual (gaps, reconnects, REST checks, ...),
/// mirrored to stderr. Nothing is hidden: the summary counters are derived from it.
struct EventLog {
    file: File,
}

impl EventLog {
    fn log(&mut self, kind: &str, detail: serde_json::Value) {
        let line = json!({"ts_local_ms": now_ms(), "kind": kind, "detail": detail});
        eprintln!("[{}] {kind} {detail}", chrono::Utc::now().format("%H:%M:%S%.3f"));
        // A failed event write must not kill the recorder, but it must be visible.
        if let Err(e) = writeln!(self.file, "{line}") {
            eprintln!("WARNING: cannot write event log: {e}");
        }
    }
}

#[derive(Default, serde::Serialize)]
struct Stats {
    book_messages: u64,
    book_snapshots: u64,
    book_deltas: u64,
    book_rows: u64,
    trade_messages: u64,
    trades: u64,
    gaps: u64,
    resubscribes: u64,
    resyncs: u64,
    service_restarts: u64,
    snapshot_checks: u64,
    snapshot_mismatches: u64,
    deltas_not_applied: u64,
    crossed_books: u64,
    connects: u64,
    disconnects: u64,
    rest_checks: u64,
    rest_matches: u64,
    rest_mismatches: u64,
    rest_unaligned: u64,
    rest_errors: u64,
}

// ----------------------------------------------------------------- recorder

/// Local book state at one update id, kept to compare against a REST snapshot that
/// carries the same `u` (REST /v5/market/orderbook shares the orderbook.1000 `u` space,
/// verified live).
struct TopState {
    u: i64,
    bids: Vec<Level>,
    asks: Vec<Level>,
}

struct Recorder {
    symbol: String,
    ob_topic: String,
    trade_topic: String,
    tick: Step,
    lot: Step,
    book: Book,
    sink: Sink,
    events: EventLog,
    stats: Stats,
    history: VecDeque<TopState>,
    pending_rest: Option<(BookData, Instant)>,
    /// TESTING ONLY: discard one delta (before writing) every N seconds to exercise the
    /// gap -> resubscribe -> rebuild path against the live feed.
    simulate_gap: Option<(Duration, Instant)>,
    simulated_drops: u64,
}

enum SessionEnd {
    Stop,
    Disconnected { reason: String, was_healthy: bool },
}

impl Recorder {
    fn levels(&self, raw: &[(String, String)]) -> Result<Vec<Level>> {
        raw.iter().map(|(p, s)| Ok((self.tick.to_units(p)?, self.lot.to_units(s)?))).collect()
    }

    /// Handle one text frame. Returns topics to resubscribe (non-empty after a gap).
    fn on_text(&mut self, text: &str, ts_local: i64) -> Result<bool> {
        let env: Envelope = serde_json::from_str(text).with_context(|| format!("bad json: {text:.200}"))?;
        if let Some(op) = env.op.as_deref() {
            // op replies: subscribe/unsubscribe acks and pongs
            if env.success == Some(false) {
                self.events.log("op_failed", json!({"op": op, "ret_msg": env.ret_msg}));
            } else if op != "ping" && op != "pong" {
                self.events.log("op_ok", json!({"op": op, "ret_msg": env.ret_msg}));
            }
            return Ok(false);
        }
        let (Some(topic), Some(data)) = (env.topic, env.data) else {
            self.events.log("unknown_message", json!({"text": format!("{text:.200}")}));
            return Ok(false);
        };
        let ts = env.ts.context("message without ts")?;
        if topic == self.ob_topic {
            let d: BookData = serde_json::from_str(data.get())?;
            if d.s != self.symbol {
                bail!("orderbook for {} on {} topic", d.s, self.symbol);
            }
            let is_snapshot = match env.kind {
                Some("snapshot") => true,
                Some("delta") => false,
                other => bail!("unknown orderbook type {other:?}"),
            };
            if !is_snapshot && self.book.valid {
                if let Some((every, last)) = self.simulate_gap.as_mut() {
                    if last.elapsed() >= *every {
                        *last = Instant::now();
                        self.simulated_drops += 1;
                        self.events.log("simulated_drop", json!({"u": d.u, "note": "testing only"}));
                        return Ok(false);
                    }
                }
            }
            self.on_book(ts, ts_local, is_snapshot, &d)
        } else if topic == self.trade_topic {
            let trades: Vec<TradeData> = serde_json::from_str(data.get())?;
            self.stats.trade_messages += 1;
            for t in trades {
                if t.s != self.symbol {
                    bail!("trade for {} on {} topic", t.s, self.symbol);
                }
                let buy = match t.side.as_str() {
                    "Buy" => true,
                    "Sell" => false,
                    other => bail!("unknown trade side {other:?}"),
                };
                let (p, v) = (self.tick.to_units(&t.p)?, self.lot.to_units(&t.v)?);
                self.sink.write_trade(t.time_ms, ts_local, t.i, buy, p, v)?;
                self.stats.trades += 1;
            }
            Ok(false)
        } else {
            self.events.log("unexpected_topic", json!({"topic": topic}));
            Ok(false)
        }
    }

    /// Write the message (always: never drop), then update the local book.
    /// Returns true if the orderbook topic must be resubscribed.
    fn on_book(&mut self, ts: i64, ts_local: i64, is_snapshot: bool, d: &BookData) -> Result<bool> {
        let (bids, asks) = (self.levels(&d.b)?, self.levels(&d.a)?);
        self.sink.write_book(ts, ts_local, d.seq, d.u, is_snapshot, &bids, &asks)?;
        let s = &mut self.stats;
        s.book_messages += 1;
        s.book_rows += (bids.len() + asks.len()) as u64;
        let mut resubscribe = false;
        let outcome = if is_snapshot {
            s.book_snapshots += 1;
            self.book.on_snapshot(d.u, d.seq, &bids, &asks)
        } else {
            s.book_deltas += 1;
            self.book.on_delta(d.u, d.seq, &bids, &asks)
        };
        match outcome {
            Outcome::Applied => {}
            Outcome::Reset { service_restart } => {
                // A resubscribe snapshot can be OLDER than deltas already seen (observed
                // live: up to ~3 s / 16 updates behind), after which Bybit replays those
                // update ids. Forget pre-reset states so REST checks never match them.
                self.history.clear();
                self.stats.resyncs += 1;
                self.stats.service_restarts += service_restart as u64;
                self.events.log("book_reset", json!({"u": d.u, "seq": d.seq, "service_restart": service_restart,
                    "bids": bids.len(), "asks": asks.len()}));
            }
            Outcome::SnapshotCheck { matches } => {
                self.stats.snapshot_checks += 1;
                if !matches {
                    self.stats.snapshot_mismatches += 1;
                    self.events.log("snapshot_mismatch", json!({"u": d.u}));
                }
            }
            Outcome::Gap { prev_u, u } => {
                self.stats.gaps += 1;
                self.events.log("gap", json!({"prev_u": prev_u, "u": u, "missing": u - prev_u - 1}));
                resubscribe = true;
            }
            Outcome::NotReady => self.stats.deltas_not_applied += 1,
        }
        if self.book.valid {
            if self.book.is_crossed() {
                self.stats.crossed_books += 1;
                self.events.log("crossed_book", json!({"u": d.u, "best_bid": self.book.best_bid(),
                    "best_ask": self.book.best_ask()}));
            }
            let (b, a) = self.book.top(REST_LEVELS);
            if self.history.len() == HISTORY_LEN {
                self.history.pop_front();
            }
            self.history.push_back(TopState { u: d.u, bids: b, asks: a });
            self.try_rest_check(false);
        }
        Ok(resubscribe)
    }

    /// Compare a pending REST snapshot with the local book at the same `u`. If the
    /// local book has not reached that `u` yet, wait (up to 10 s, then give up).
    fn try_rest_check(&mut self, force: bool) {
        let Some((rest, received)) = self.pending_rest.as_ref() else { return };
        let local_last = self.history.back().map(|t| t.u);
        let reached = local_last.is_some_and(|u| u >= rest.u);
        if !reached && !force && received.elapsed() < Duration::from_secs(10) {
            return;
        }
        let (rest, _) = self.pending_rest.take().expect("checked above");
        self.stats.rest_checks += 1;
        let Some(local) = self.history.iter().find(|t| t.u == rest.u) else {
            self.stats.rest_unaligned += 1;
            let oldest = self.history.front().map(|t| t.u);
            self.events.log("rest_unaligned", json!({"rest_u": rest.u, "local_oldest_u": oldest, "local_last_u": local_last}));
            return;
        };
        let (rb, ra) = match (self.levels(&rest.b), self.levels(&rest.a)) {
            (Ok(b), Ok(a)) => (b, a),
            (Err(e), _) | (_, Err(e)) => {
                self.stats.rest_errors += 1;
                self.events.log("rest_error", json!({"error": e.to_string()}));
                return;
            }
        };
        let n = REST_LEVELS.min(rb.len()).min(ra.len());
        let diff = |x: &[Level], y: &[Level]| (0..n).filter(|&i| x.get(i) != y.get(i)).count();
        let (db, da) = (diff(&local.bids, &rb), diff(&local.asks, &ra));
        let detail = json!({"u": rest.u, "levels_compared": n, "bid_levels_differ": db, "ask_levels_differ": da,
            "local_best": [local.bids.first(), local.asks.first()], "rest_best": [rb.first(), ra.first()]});
        if db == 0 && da == 0 {
            self.stats.rest_matches += 1;
            self.events.log("rest_match", detail);
        } else {
            self.stats.rest_mismatches += 1;
            self.events.log("rest_mismatch", detail);
        }
    }

    /// One websocket connection: subscribe, then pump messages until stop or failure.
    async fn session(
        &mut self,
        stop: &mut (impl std::future::Future<Output = ()> + Unpin),
        rest_rx: &mut mpsc::Receiver<Result<BookData>>,
    ) -> SessionEnd {
        let connect = tokio::time::timeout(Duration::from_secs(10), tokio_tungstenite::connect_async(bybit::WS_URL));
        let ws = tokio::select! {
            _ = &mut *stop => return SessionEnd::Stop,
            r = connect => match r {
                Ok(Ok((ws, _))) => ws,
                Ok(Err(e)) => return SessionEnd::Disconnected { reason: format!("connect: {e}"), was_healthy: false },
                Err(_) => return SessionEnd::Disconnected { reason: "connect timeout".into(), was_healthy: false },
            },
        };
        self.stats.connects += 1;
        self.events.log("connected", json!({"url": bybit::WS_URL}));
        let (mut tx, mut rx) = ws.split();
        let topics = vec![self.ob_topic.clone(), self.trade_topic.clone()];
        if let Err(e) = tx.send(Message::text(bybit::subscribe_msg("subscribe", &topics))).await {
            return SessionEnd::Disconnected { reason: format!("subscribe send: {e}"), was_healthy: false };
        }
        let mut ping = tokio::time::interval(Duration::from_secs(bybit::PING_INTERVAL_SECS));
        let mut housekeeping = tokio::time::interval(Duration::from_secs(1));
        let mut last_rx = Instant::now();
        let healthy = std::cell::Cell::new(false); // set once a valid book exists
        loop {
            let end = |reason: String| SessionEnd::Disconnected { reason, was_healthy: healthy.get() };
            tokio::select! {
                _ = &mut *stop => {
                    let _ = tx.send(Message::Close(None)).await;
                    return SessionEnd::Stop;
                }
                _ = ping.tick() => {
                    if let Err(e) = tx.send(Message::text(bybit::ping_msg())).await {
                        return end(format!("ping send: {e}"));
                    }
                }
                _ = housekeeping.tick() => {
                    if let Err(e) = self.sink.rotate_if_due(now_ms()) {
                        self.events.log("write_error", json!({"error": e.to_string()}));
                        return SessionEnd::Stop;
                    }
                    self.try_rest_check(false);
                    if last_rx.elapsed() > Duration::from_secs(STALE_SECS) {
                        return end(format!("no message for {STALE_SECS}s"));
                    }
                }
                Some(rest) = rest_rx.recv() => match rest {
                    Ok(b) => {
                        if self.pending_rest.is_some() {
                            self.try_rest_check(true);
                        }
                        self.pending_rest = Some((b, Instant::now()));
                        self.try_rest_check(false);
                    }
                    Err(e) => {
                        self.stats.rest_errors += 1;
                        self.events.log("rest_error", json!({"error": e.to_string()}));
                    }
                },
                msg = rx.next() => {
                    let ts_local = now_ms();
                    last_rx = Instant::now();
                    match msg {
                        Some(Ok(Message::Text(t))) => match self.on_text(t.as_str(), ts_local) {
                            Ok(resubscribe) => {
                                healthy.set(healthy.get() || self.book.valid);
                                if resubscribe {
                                    self.stats.resubscribes += 1;
                                    let ob = std::slice::from_ref(&self.ob_topic);
                                    for op in ["unsubscribe", "subscribe"] {
                                        if let Err(e) = tx.send(Message::text(bybit::subscribe_msg(op, ob))).await {
                                            return end(format!("resubscribe send: {e}"));
                                        }
                                    }
                                    self.events.log("resubscribed", json!({"topic": self.ob_topic}));
                                }
                            }
                            Err(e) => {
                                // Data we cannot represent exactly (e.g. off-tick price) is
                                // fatal: stop rather than write something wrong.
                                self.events.log("fatal", json!({"error": format!("{e:#}")}));
                                return SessionEnd::Stop;
                            }
                        },
                        Some(Ok(Message::Close(frame))) => return end(format!("server close: {frame:?}")),
                        Some(Ok(_)) => {} // ping/pong frames are answered by tungstenite
                        Some(Err(e)) => return end(format!("read: {e}")),
                        None => return end("stream ended".into()),
                    }
                }
            }
        }
    }
}

async fn rest_poller(client: reqwest::Client, symbol: String, tx: mpsc::Sender<Result<BookData>>) {
    let mut every = tokio::time::interval(Duration::from_secs(REST_CHECK_SECS));
    every.tick().await; // first tick is immediate: skip it, give the book time to build
    loop {
        every.tick().await;
        let r = bybit::fetch_orderbook(&client, &symbol, REST_LEVELS as u32).await;
        if tx.send(r).await.is_err() {
            return;
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = parse_args()?;
    let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).user_agent("mm-recorder/0.1").build()?;
    let (tick, lot) = bybit::fetch_steps(&client, &cfg.symbol).await.context("instruments-info")?;

    let session_id = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let root = cfg.out.join(&cfg.symbol).join(&session_id);
    std::fs::create_dir_all(&root)?;
    let ob_topic = format!("orderbook.{DEPTH}.{}", cfg.symbol);
    let trade_topic = format!("publicTrade.{}", cfg.symbol);
    let meta = |topic: &str| FileMeta {
        symbol: cfg.symbol.clone(),
        tick_size: tick.clone(),
        lot_size: lot.clone(),
        topic: topic.to_string(),
        session: session_id.clone(),
    };
    let sink = Sink::new(root.clone(), cfg.rotate_minutes, meta(&ob_topic), meta(&trade_topic));
    let events = EventLog { file: OpenOptions::new().create(true).append(true).open(root.join("events.jsonl"))? };
    let mut rec = Recorder {
        symbol: cfg.symbol.clone(),
        ob_topic,
        trade_topic,
        tick: Step::parse(&tick)?,
        lot: Step::parse(&lot)?,
        book: Book::default(),
        sink,
        events,
        stats: Stats::default(),
        history: VecDeque::with_capacity(HISTORY_LEN),
        pending_rest: None,
        simulate_gap: cfg.simulate_gap_secs.map(|s| (Duration::from_secs(s), Instant::now())),
        simulated_drops: 0,
    };
    rec.events.log("start", json!({"symbol": cfg.symbol, "tick_size": tick, "lot_size": lot, "minutes": cfg.minutes,
        "rotate_minutes": cfg.rotate_minutes, "simulate_gap_secs": cfg.simulate_gap_secs, "out": root}));

    // Stop on Ctrl-C or after --minutes, whichever comes first.
    let deadline = cfg.minutes.map(|m| Duration::from_secs_f64(m * 60.0));
    let mut stop = Box::pin(async move {
        let timer = async {
            match deadline {
                Some(d) => tokio::time::sleep(d).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => eprintln!("Ctrl-C: flushing and stopping"),
            _ = timer => eprintln!("--minutes reached: flushing and stopping"),
        }
    });

    let (rest_tx, mut rest_rx) = mpsc::channel(4);
    let poller = tokio::spawn(rest_poller(client, cfg.symbol.clone(), rest_tx));

    let mut backoff = Duration::from_secs(1);
    loop {
        match rec.session(&mut stop, &mut rest_rx).await {
            SessionEnd::Stop => break,
            SessionEnd::Disconnected { reason, was_healthy } => {
                rec.stats.disconnects += 1;
                rec.book.invalidate(); // rebuilt from the snapshot sent on resubscribe
                rec.pending_rest = None;
                rec.history.clear();
                if was_healthy {
                    backoff = Duration::from_secs(1);
                }
                rec.events.log("disconnected", json!({"reason": reason, "retry_in_s": backoff.as_secs()}));
                tokio::select! {
                    _ = &mut stop => break,
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(Duration::from_secs(MAX_BACKOFF_SECS));
            }
        }
    }
    poller.abort();
    rec.sink.close_files()?;
    let summary = json!({"session": session_id, "stats": rec.stats, "simulated_drops": rec.simulated_drops,
        "files": rec.sink.files_written});
    std::fs::write(root.join("summary.json"), serde_json::to_string_pretty(&summary)?)?;
    rec.events.log("stop", json!({}));
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}
