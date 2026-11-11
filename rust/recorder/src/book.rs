//! Local L2 book maintained from Bybit snapshot + delta messages.
//!
//! Rules (Bybit v5 docs, websocket/public/orderbook): size 0 deletes a level, unknown
//! price inserts, known price overwrites; a snapshot resets the book; `u == 1` is a
//! snapshot sent after a service restart.
//!
//! Gap rule: a delta must have `u == prev_u + 1`. This is EMPIRICAL (verified on the
//! historical archive and on live data), not a documented guarantee. On a gap the book
//! is marked invalid and further deltas are not applied until a fresh snapshot.

use std::collections::BTreeMap;

pub type Level = (i64, i64); // (price_ticks, size_lots)

#[derive(Debug, PartialEq)]
pub enum Outcome {
    /// Delta applied cleanly.
    Applied,
    /// Snapshot started a new book (first one, after a gap/reconnect, or a restart).
    Reset { service_restart: bool },
    /// Snapshot repeated the last `u`: compared with the local book, then adopted.
    SnapshotCheck { matches: bool },
    /// Delta did not follow `prev_u + 1`. Book is now invalid.
    Gap { prev_u: i64, u: i64 },
    /// Delta received while waiting for a snapshot; not applied.
    NotReady,
}

#[derive(Default)]
pub struct Book {
    pub bids: BTreeMap<i64, i64>,
    pub asks: BTreeMap<i64, i64>,
    pub last_u: Option<i64>,
    pub last_seq: Option<i64>,
    pub valid: bool,
}

impl Book {
    /// Forget everything (used on disconnect / resubscribe).
    pub fn invalidate(&mut self) {
        self.valid = false;
    }

    pub fn on_snapshot(&mut self, u: i64, seq: i64, bids: &[Level], asks: &[Level]) -> Outcome {
        let outcome = if self.valid && self.last_u == Some(u) && u != 1 {
            let same = levels_equal(&self.bids, bids) && levels_equal(&self.asks, asks);
            Outcome::SnapshotCheck { matches: same }
        } else {
            Outcome::Reset { service_restart: u == 1 }
        };
        self.bids = bids.iter().copied().filter(|&(_, s)| s != 0).collect();
        self.asks = asks.iter().copied().filter(|&(_, s)| s != 0).collect();
        self.last_u = Some(u);
        self.last_seq = Some(seq);
        self.valid = true;
        outcome
    }

    pub fn on_delta(&mut self, u: i64, seq: i64, bids: &[Level], asks: &[Level]) -> Outcome {
        if !self.valid {
            return Outcome::NotReady;
        }
        if let Some(prev) = self.last_u {
            if u != prev + 1 {
                self.valid = false;
                return Outcome::Gap { prev_u: prev, u };
            }
        }
        apply(&mut self.bids, bids);
        apply(&mut self.asks, asks);
        self.last_u = Some(u);
        self.last_seq = Some(seq);
        Outcome::Applied
    }

    pub fn best_bid(&self) -> Option<i64> {
        self.bids.keys().next_back().copied()
    }

    pub fn best_ask(&self) -> Option<i64> {
        self.asks.keys().next().copied()
    }

    /// True if best bid >= best ask (crossed or locked).
    pub fn is_crossed(&self) -> bool {
        matches!((self.best_bid(), self.best_ask()), (Some(b), Some(a)) if b >= a)
    }

    /// Top `n` levels: bids best-first (descending), asks best-first (ascending).
    pub fn top(&self, n: usize) -> (Vec<Level>, Vec<Level>) {
        let b = self.bids.iter().rev().take(n).map(|(&p, &s)| (p, s)).collect();
        let a = self.asks.iter().take(n).map(|(&p, &s)| (p, s)).collect();
        (b, a)
    }
}

fn apply(side: &mut BTreeMap<i64, i64>, levels: &[Level]) {
    for &(p, s) in levels {
        if s == 0 {
            side.remove(&p);
        } else {
            side.insert(p, s);
        }
    }
}

fn levels_equal(have: &BTreeMap<i64, i64>, snap: &[Level]) -> bool {
    let nonzero = snap.iter().filter(|&&(_, s)| s != 0);
    nonzero.clone().count() == have.len() && nonzero.into_iter().all(|(p, s)| have.get(p) == Some(s))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book() -> Book {
        let mut b = Book::default();
        b.on_snapshot(10, 100, &[(100, 5), (99, 3)], &[(101, 2), (102, 7)]);
        b
    }

    #[test]
    fn delta_insert_update_delete() {
        let mut b = book();
        let o = b.on_delta(11, 101, &[(100, 0), (99, 4), (98, 1)], &[(101, 9)]);
        assert_eq!(o, Outcome::Applied);
        assert_eq!(b.top(10), (vec![(99, 4), (98, 1)], vec![(101, 9), (102, 7)]));
        assert_eq!((b.best_bid(), b.best_ask()), (Some(99), Some(101)));
        assert!(!b.is_crossed());
    }

    #[test]
    fn gap_invalidates_until_snapshot() {
        let mut b = book();
        assert_eq!(b.on_delta(13, 103, &[], &[]), Outcome::Gap { prev_u: 10, u: 13 });
        assert!(!b.valid);
        assert_eq!(b.on_delta(14, 104, &[(50, 1)], &[]), Outcome::NotReady);
        assert!(!b.bids.contains_key(&50));
        let o = b.on_snapshot(20, 120, &[(100, 1)], &[(101, 1)]);
        assert_eq!(o, Outcome::Reset { service_restart: false });
        assert_eq!(b.on_delta(21, 121, &[], &[]), Outcome::Applied);
    }

    #[test]
    fn snapshot_check_and_restart() {
        let mut b = book();
        b.on_delta(11, 101, &[(100, 6)], &[]);
        let o = b.on_snapshot(11, 101, &[(100, 6), (99, 3)], &[(101, 2), (102, 7)]);
        assert_eq!(o, Outcome::SnapshotCheck { matches: true });
        let o = b.on_snapshot(11, 101, &[(100, 1)], &[(101, 2)]);
        assert_eq!(o, Outcome::SnapshotCheck { matches: false });
        let o = b.on_snapshot(1, 5, &[(100, 1)], &[(101, 2)]);
        assert_eq!(o, Outcome::Reset { service_restart: true });
    }

    #[test]
    fn crossed_detected() {
        let mut b = book();
        b.on_delta(11, 101, &[(101, 1)], &[]);
        assert!(b.is_crossed());
    }
}
