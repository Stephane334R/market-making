//! Fill engine: a line-by-line port of `python/mm/simulator.py` (Engine + Runner) for ONE
//! quote schedule. The strategy is not ported: quotes come from the schedule exported by
//! Python, keyed by the index of the book message they were decided on.
//!
//! Parity rule: every floating-point expression is written in the same order as the
//! Python code, and Python's `min`/`max` builtins are mirrored by `py_min`/`py_max`
//! (they return the FIRST argument on ties), so outputs are bit-identical, not just close.

use std::collections::BTreeMap;

use crate::input::{BookMsg, QuoteRow, Trade};

pub const EPS: f64 = 1e-12;

/// Python `min(a, b)`: returns `a` unless `b < a`.
#[inline]
fn py_min(a: f64, b: f64) -> f64 {
    if b < a { b } else { a }
}

/// Python `max(a, b)`: returns `a` unless `b > a`.
#[inline]
fn py_max(a: f64, b: f64) -> f64 {
    if b > a { b } else { a }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FillModel {
    L2Queue,
    TouchRatio,
}

#[derive(Clone, Debug)]
pub struct SimConfig {
    pub latency_ms: i64,
    pub maker_fee: f64,
    pub touch_fill_ratio: f64,
    pub fill_model: FillModel,
    pub sample_ms: i64,
}

/// One resting order. `queue` = coin ahead of us at our price (l2 only); `traded` = coin
/// printed at our price since the last level update (to tell prints from cancels).
/// `watched` mirrors Python's watch dict membership: level updates at `px` touch it.
#[derive(Clone, Debug)]
pub struct Order {
    pub px: i64,
    pub rem: f64,
    pub queue: f64,
    pub traded: f64,
    pub watched: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Fill {
    pub ts: i64,
    pub side: i64,
    pub px: f64,
    pub qty: f64,
    pub mid: f64,
    pub fee: f64,
    pub inventory: f64,
    pub through: bool,
}

/// Sampled PnL states, struct-of-arrays (one Vec per output column).
#[derive(Default, Debug)]
pub struct States {
    pub ts: Vec<i64>,
    pub mid: Vec<f64>,
    pub inventory: Vec<f64>,
    pub cash: Vec<f64>,
    pub spread_capture: Vec<f64>,
    pub inventory_pnl: Vec<f64>,
    pub fees: Vec<f64>,
}

pub struct Engine {
    cfg: SimConfig,
    tick: f64,
    lot: f64,
    // market state
    bids: BTreeMap<i64, i64>, // price ticks -> size lots; best bid = last key
    asks: BTreeMap<i64, i64>, // best ask = first key
    best_bid: i64,
    best_ask: i64,
    mid: Option<f64>, // USDT
    // quote schedule
    quotes: Vec<QuoteRow>,
    cursor: usize,
    pending: Option<QuoteRow>,
    pending_ts: Option<i64>,
    // our orders
    bid: Option<Order>,
    ask: Option<Order>,
    // accounting
    q: f64,
    cash: f64,
    edge: f64,
    inv: f64,
    fees: f64,
    mark_mid: Option<f64>,
    // bookkeeping
    next_sample: i64,
    last_ts: Option<i64>,
    pub n_book: u64,
    pub n_trades: u64,
    pub n_rejects: u64,
    pub fills: Vec<Fill>,
    pub states: States,
}

impl Engine {
    pub fn new(cfg: SimConfig, tick: f64, lot: f64, quotes: Vec<QuoteRow>) -> Engine {
        Engine {
            cfg, tick, lot,
            bids: BTreeMap::new(), asks: BTreeMap::new(), best_bid: 0, best_ask: 0, mid: None,
            quotes, cursor: 0, pending: None, pending_ts: None,
            bid: None, ask: None,
            q: 0.0, cash: 0.0, edge: 0.0, inv: 0.0, fees: 0.0, mark_mid: None,
            next_sample: -1_000_000_000_000_000_000, last_ts: None,
            n_book: 0, n_trades: 0, n_rejects: 0, fills: Vec::new(), states: States::default(),
        }
    }

    pub fn quotes_used(&self) -> usize {
        self.cursor
    }

    pub fn totals(&self) -> (f64, f64, f64, f64, f64) {
        (self.q, self.cash, self.edge, self.inv, self.fees)
    }

    // ------------------------------------------------------------- accounting

    fn settle(&mut self, mid: f64) {
        if let Some(mark) = self.mark_mid {
            self.inv += self.q * (mid - mark);
        }
        self.mark_mid = Some(mid);
    }

    fn fill(&mut self, ts: i64, side: i64, px: f64, qty: f64, mid: f64, through: bool) {
        self.settle(mid);
        let s = side as f64;
        let fee = self.cfg.maker_fee * px * qty;
        self.edge += s * qty * (mid - px);
        self.cash -= s * px * qty + fee;
        self.fees += fee;
        self.q += s * qty;
        self.fills.push(Fill { ts, side, px, qty, mid, fee, inventory: self.q, through });
    }

    fn sample(&mut self, ts: i64, mid: f64) {
        self.settle(mid);
        let st = &mut self.states;
        st.ts.push(ts);
        st.mid.push(mid);
        st.inventory.push(self.q);
        st.cash.push(self.cash);
        st.spread_capture.push(self.edge);
        st.inventory_pnl.push(self.inv);
        st.fees.push(self.fees);
    }

    // ------------------------------------------------------- order bookkeeping

    /// Python `Engine._place`: cancel / amend / replace our order on one side.
    fn place(&mut self, is_bid: bool, px: Option<i64>, sz: f64) {
        let l2 = self.cfg.fill_model == FillModel::L2Queue;
        let level = |px: i64, book: &BTreeMap<i64, i64>| *book.get(&px).unwrap_or(&0) as f64 * self.lot;
        let new = match px {
            None => None,
            Some(px) if !l2 => Some(Order { px, rem: sz, queue: 0.0, traded: 0.0, watched: false }),
            Some(px) => {
                let cur = if is_bid { self.bid.as_mut() } else { self.ask.as_mut() };
                if let Some(cur) = cur {
                    if cur.px == px && sz <= cur.rem + EPS {
                        cur.rem = sz; // amend down: keep queue position (and watch state)
                        return;
                    }
                }
                let book = if is_bid { &self.bids } else { &self.asks };
                Some(Order { px, rem: sz, queue: level(px, book), traded: 0.0, watched: true })
            }
        };
        // Replacing the Option drops the old order, which also "unwatches" it.
        if is_bid { self.bid = new } else { self.ask = new }
    }

    /// Python `Engine._activate`: the pending quote goes live (post-only check).
    fn activate(&mut self) {
        let Some(qt) = self.pending.take() else { return };
        let (bb, ba) = (self.best_bid, self.best_ask);
        let bid_ok = matches!(qt.bid_px, Some(p) if p < ba);
        let ask_ok = matches!(qt.ask_px, Some(p) if p > bb);
        self.n_rejects += (qt.bid_px.is_some() && !bid_ok) as u64 + (qt.ask_px.is_some() && !ask_ok) as u64;
        self.place(true, if bid_ok { qt.bid_px } else { None }, qt.bid_sz);
        self.place(false, if ask_ok { qt.ask_px } else { None }, qt.ask_sz);
    }

    fn event_start(&mut self, ts: i64) {
        if matches!(self.pending_ts, Some(p) if p <= ts) && self.mid.is_some() {
            self.pending_ts = None;
            self.activate();
        }
        self.last_ts = Some(ts);
    }

    // ------------------------------------------------- level updates (queue estimate)

    /// Python `Engine._level_change` for our order on one side (if watched at `px`).
    fn level_change(order: &mut Option<Order>, px: i64, old_lots: i64, new_lots: i64, lot: f64) {
        let Some(o) = order.as_mut() else { return };
        if !o.watched || o.px != px {
            return;
        }
        let (old, new) = (old_lots as f64 * lot, new_lots as f64 * lot);
        let traded = o.traded;
        o.traded = 0.0;
        let others = old - traded;
        let cancels = (old - new) - traded;
        if cancels > EPS && others > EPS {
            o.queue -= cancels * o.queue / others;
        }
        o.queue = py_min(py_max(o.queue, 0.0), new);
    }

    fn apply_book(&mut self, m: &BookMsg) {
        let lot = self.lot;
        if m.is_snapshot {
            let new_b: BTreeMap<i64, i64> = m.bids.iter().copied().filter(|&(_, s)| s != 0).collect();
            let new_a: BTreeMap<i64, i64> = m.asks.iter().copied().filter(|&(_, s)| s != 0).collect();
            let old_b = std::mem::replace(&mut self.bids, new_b);
            let old_a = std::mem::replace(&mut self.asks, new_a);
            for (order, old, new) in [(&mut self.bid, &old_b, &self.bids), (&mut self.ask, &old_a, &self.asks)] {
                if let Some(px) = order.as_ref().filter(|o| o.watched).map(|o| o.px) {
                    let (o_sz, n_sz) = (*old.get(&px).unwrap_or(&0), *new.get(&px).unwrap_or(&0));
                    if o_sz != n_sz {
                        Self::level_change(order, px, o_sz, n_sz, lot);
                    }
                }
            }
            return;
        }
        for (book, order, levels) in [(&mut self.bids, &mut self.bid, &m.bids), (&mut self.asks, &mut self.ask, &m.asks)] {
            for &(p, s) in levels {
                let old = if s != 0 { book.insert(p, s) } else { book.remove(&p) }.unwrap_or(0);
                if old != s {
                    Self::level_change(order, p, old, s, lot);
                }
            }
        }
    }

    // --------------------------------------------------------------------- events

    pub fn on_book(&mut self, m: &BookMsg) -> anyhow::Result<()> {
        let ts = m.ts;
        let idx = self.n_book;
        self.n_book += 1;
        self.event_start(ts);
        self.apply_book(m);
        let (Some((&bb, _)), Some((&ba, _))) = (self.bids.last_key_value(), self.asks.first_key_value()) else {
            return Ok(()); // one-sided book: keep the last mid
        };
        self.best_bid = bb;
        self.best_ask = ba;
        let mid_ticks = (bb + ba) as f64 / 2.0;
        let mid = mid_ticks * self.tick;
        self.mid = Some(mid);
        if let Some(qt) = self.quotes.get(self.cursor).copied() {
            if qt.msg_index == idx {
                anyhow::ensure!(qt.ts == ts, "quote schedule ts {} != book ts {ts} at message {idx}", qt.ts);
                self.pending = Some(qt);
                self.pending_ts = Some(ts + self.cfg.latency_ms);
                self.cursor += 1;
            }
        }
        if ts >= self.next_sample {
            self.sample(ts, mid);
            self.next_sample = ts - ts.rem_euclid(self.cfg.sample_ms) + self.cfg.sample_ms;
        }
        Ok(())
    }

    pub fn on_trade(&mut self, t: &Trade) {
        self.n_trades += 1;
        self.event_start(t.ts);
        let Some(mid) = self.mid else { return };
        let side = t.side as i64;
        let sz = t.sz as f64 * self.lot;
        let l2 = self.cfg.fill_model == FillModel::L2Queue;
        let ratio = self.cfg.touch_fill_ratio;
        // a seller-initiated print (-1) can only hit our bid, a buyer-initiated one our ask
        let order = if side == -1 { &mut self.bid } else { &mut self.ask };
        let Some(o) = order.as_mut() else { return };
        if o.rem <= EPS || (side == -1 && t.px > o.px) || (side == 1 && t.px < o.px) {
            return;
        }
        let through = t.px != o.px;
        let qty = if through {
            o.rem
        } else if l2 {
            let q = py_min(o.rem, py_max(0.0, sz - o.queue));
            o.queue = py_max(0.0, o.queue - sz);
            o.traded += sz;
            q
        } else {
            py_min(o.rem, sz * ratio)
        };
        if qty > EPS {
            let px = o.px as f64 * self.tick;
            o.rem -= qty;
            if o.rem <= EPS {
                o.watched = false; // fully filled: no longer in the queue
            }
            self.fill(t.ts, -side, px, qty, mid, through);
        }
    }

    pub fn finish(&mut self) {
        if let (Some(ts), Some(mid)) = (self.last_ts, self.mid) {
            self.sample(ts, mid);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(ts: i64, u: i64, snap: bool, bids: &[(i64, i64)], asks: &[(i64, i64)]) -> BookMsg {
        BookMsg { ts, seq: u, update_id: u, is_snapshot: snap, bids: bids.to_vec(), asks: asks.to_vec() }
    }

    fn quote(i: u64, ts: i64, bid: i64, ask: i64, sz: f64) -> QuoteRow {
        QuoteRow { msg_index: i, ts, bid_px: Some(bid), ask_px: Some(ask), bid_sz: sz, ask_sz: sz }
    }

    fn cfg(model: FillModel) -> SimConfig {
        SimConfig { latency_ms: 10, maker_fee: 0.0, touch_fill_ratio: 0.25, fill_model: model, sample_ms: 1000 }
    }

    fn trade(ts: i64, side: i8, px: i64, sz: i64) -> Trade {
        Trade { ts, side, px, sz }
    }

    #[test]
    fn queue_ahead_consumed_before_we_fill() {
        // same scenario as tests/test_simulator.py::test_queue_ahead_is_consumed_before_we_fill
        let mut e = Engine::new(cfg(FillModel::L2Queue), 1.0, 1.0, vec![quote(0, 0, 98, 110, 3.0)]);
        e.on_book(&msg(0, 1, true, &[(98, 5), (99, 1)], &[(101, 5)])).unwrap();
        e.on_trade(&trade(100, -1, 98, 3));
        e.on_trade(&trade(200, -1, 98, 4));
        assert_eq!(e.fills.len(), 1);
        assert_eq!(e.fills[0].qty, 2.0);
        assert!(!e.fills[0].through);
    }

    #[test]
    fn cancels_reduce_queue_pro_rata_and_prints_are_not_cancels() {
        let mut e = Engine::new(cfg(FillModel::L2Queue), 1.0, 1.0, vec![quote(0, 0, 98, 110, 3.0)]);
        e.on_book(&msg(0, 1, true, &[(98, 10), (99, 1)], &[(101, 5)])).unwrap();
        e.on_trade(&trade(100, -1, 98, 4)); // activates (queue 10) then print: queue 6
        e.on_book(&msg(200, 2, false, &[(98, 6)], &[])).unwrap(); // explained by the print
        assert_eq!(e.bid.as_ref().unwrap().queue, 6.0);
        e.on_book(&msg(300, 3, false, &[(98, 3)], &[])).unwrap(); // 3 cancels out of 6: queue 3
        assert_eq!(e.bid.as_ref().unwrap().queue, 3.0);
    }

    #[test]
    fn through_print_fills_full_size_and_touch_ratio() {
        let mut e = Engine::new(cfg(FillModel::TouchRatio), 1.0, 1.0, vec![quote(0, 0, 98, 102, 1.0)]);
        e.on_book(&msg(0, 1, true, &[(98, 5)], &[(101, 5)])).unwrap();
        e.on_trade(&trade(500, -1, 98, 2)); // at our price: 25% of 2
        assert_eq!(e.fills[0].qty, 0.5);
        e.on_trade(&trade(600, -1, 97, 1)); // through: remaining 0.5
        assert_eq!((e.fills[1].qty, e.fills[1].through), (0.5, true));
    }

    #[test]
    fn post_only_rejection_and_latency() {
        let mut e = Engine::new(cfg(FillModel::L2Queue), 1.0, 1.0, vec![quote(0, 0, 98, 102, 1.0)]);
        e.on_book(&msg(0, 1, true, &[(99, 5)], &[(101, 5)])).unwrap();
        e.on_trade(&trade(5, -1, 97, 1)); // before latency: not live
        assert!(e.fills.is_empty());
        e.on_book(&msg(8, 2, true, &[(96, 5)], &[(97, 5)])).unwrap(); // ask drops below our bid
        e.on_trade(&trade(20, -1, 95, 1)); // activation now: bid 98 >= ask 97 -> rejected
        assert!(e.fills.is_empty());
        assert_eq!(e.n_rejects, 1);
    }

    #[test]
    fn accounting_identity() {
        let mut e = Engine::new(SimConfig { maker_fee: 0.0002, ..cfg(FillModel::L2Queue) }, 0.1, 0.001,
                                vec![quote(0, 0, 980, 1020, 0.5)]);
        e.on_book(&msg(0, 1, true, &[(990, 5000)], &[(1010, 5000)])).unwrap();
        e.on_trade(&trade(100, -1, 979, 1));
        e.on_book(&msg(1000, 2, false, &[(995, 100)], &[(1005, 100)])).unwrap();
        e.finish();
        let (q, cash, edge, inv, fees) = e.totals();
        let mid = *e.states.mid.last().unwrap();
        assert!(((cash + q * mid) - (edge + inv - fees)).abs() < 1e-9);
    }
}
