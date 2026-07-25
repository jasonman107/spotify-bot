//! Polymarket market-channel WebSocket feed.
//!
//! Subscribes to the YES token of every bucket market across all active
//! events (subscriptions chunked across connections), maintains a small
//! per-token book to derive top-of-book with sizes, and emits one
//! `MarketTickRow` per change. Reconnects with backoff and resubscribes
//! whenever discovery changes the token set.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::watch;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

use crate::config::{Config, PM_MARKET_WS};
use crate::discovery::Registry;
use crate::now_ms;
use crate::recorder::{MarketTickRow, RecorderEvent, RecorderHandle};

/// Integer price key scale (`price * 1e6`); all Polymarket tick sizes are
/// exactly representable at 1e6.
const PRICE_SCALE: f64 = 1_000_000.0;
const PING_INTERVAL: Duration = Duration::from_secs(10);

// ── Wire types (market channel) ─────────────────────────────────────────────

fn de_str_f64<'de, D: serde::Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
    let s = String::deserialize(d)?;
    Ok(s.parse().unwrap_or(0.0))
}

fn de_opt_str_f64<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<f64>, D::Error> {
    let s: Option<String> = Option::deserialize(d)?;
    Ok(s.and_then(|v| v.parse().ok()))
}

#[derive(Deserialize, Debug, Default)]
pub struct WireLevel {
    #[serde(deserialize_with = "de_str_f64")]
    pub price: f64,
    #[serde(default, deserialize_with = "de_str_f64")]
    pub size: f64,
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireSide {
    #[serde(rename = "BUY")]
    Buy,
    #[serde(rename = "SELL")]
    Sell,
    #[serde(other)]
    Unknown,
}

#[derive(Deserialize, Debug)]
pub struct WirePriceChange {
    pub asset_id: String,
    #[serde(default, deserialize_with = "de_opt_str_f64")]
    pub price: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_str_f64")]
    pub size: Option<f64>,
    #[serde(default)]
    pub side: Option<WireSide>,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "event_type")]
pub enum WireMsg {
    #[serde(rename = "book")]
    Book {
        asset_id: String,
        #[serde(default)]
        timestamp: Option<String>,
        bids: Vec<WireLevel>,
        asks: Vec<WireLevel>,
    },
    #[serde(rename = "price_change")]
    PriceChange {
        #[serde(default)]
        timestamp: Option<String>,
        price_changes: Vec<WirePriceChange>,
    },
    #[serde(rename = "last_trade_price")]
    LastTradePrice {
        asset_id: String,
        #[serde(deserialize_with = "de_str_f64")]
        price: f64,
        #[serde(default)]
        timestamp: Option<String>,
    },
    #[serde(other)]
    Unknown,
}

/// The transport may deliver a single object or a JSON array of messages.
#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum WireEnvelope {
    Batch(Vec<WireMsg>),
    Single(WireMsg),
}

// ── Per-token book state ────────────────────────────────────────────────────

#[derive(Default)]
struct TokenBook {
    bids: BTreeMap<i64, f64>,
    asks: BTreeMap<i64, f64>,
    last_trade_price: f64,
    /// Fingerprint of the last emitted row, to suppress no-op rows.
    last_emit_fp: (i64, u64, i64, u64, i64),
}

fn price_key(p: f64) -> i64 {
    (p * PRICE_SCALE).round() as i64
}

fn key_price(k: i64) -> f64 {
    k as f64 / PRICE_SCALE
}

impl TokenBook {
    fn snapshot(&mut self, bids: &[WireLevel], asks: &[WireLevel]) {
        self.bids.clear();
        self.asks.clear();
        for l in bids {
            if l.size > 0.0 && l.price > 0.0 && l.price < 1.0 {
                self.bids.insert(price_key(l.price), l.size);
            }
        }
        for l in asks {
            if l.size > 0.0 && l.price > 0.0 && l.price < 1.0 {
                self.asks.insert(price_key(l.price), l.size);
            }
        }
    }

    fn apply_delta(&mut self, side: WireSide, price: f64, size: f64) {
        if !(price > 0.0 && price < 1.0) {
            return;
        }
        let book = match side {
            WireSide::Buy => &mut self.bids,
            WireSide::Sell => &mut self.asks,
            WireSide::Unknown => return,
        };
        let key = price_key(price);
        if size > 0.0 {
            book.insert(key, size);
        } else {
            book.remove(&key);
        }
    }

    fn top(&self) -> (f64, f64, f64, f64) {
        let (bid, bid_sz) = self
            .bids
            .iter()
            .next_back()
            .map(|(k, s)| (key_price(*k), *s))
            .unwrap_or((0.0, 0.0));
        let (ask, ask_sz) = self
            .asks
            .iter()
            .next()
            .map(|(k, s)| (key_price(*k), *s))
            .unwrap_or((0.0, 0.0));
        (bid, bid_sz, ask, ask_sz)
    }

    /// True when the top-of-book or last-trade picture changed since the
    /// previous emit — the caller should write a row.
    fn should_emit(&mut self) -> bool {
        let (bid, bid_sz, ask, ask_sz) = self.top();
        let fp = (
            price_key(bid),
            bid_sz.to_bits(),
            price_key(ask),
            ask_sz.to_bits(),
            price_key(self.last_trade_price),
        );
        if fp == self.last_emit_fp {
            return false;
        }
        self.last_emit_fp = fp;
        true
    }
}

fn parse_pm_ts(ts: Option<&str>) -> i64 {
    ts.and_then(|s| s.parse().ok()).unwrap_or(0)
}

// ── Connection handling ─────────────────────────────────────────────────────

struct ConnState {
    books: HashMap<String, TokenBook>,
    registry: Arc<Registry>,
    recorder: RecorderHandle,
}

impl ConnState {
    fn emit(&mut self, token_id: &str, changed: &'static str, pm_ts_ms: i64, ts_ms: i64) {
        let Some(market) = self.registry.market_for_token(token_id) else {
            return;
        };
        let outcome = if market.yes_token == token_id {
            "Yes"
        } else {
            "No"
        };
        let (event_slug, series, bucket) = (
            market.event_slug.clone(),
            market.series.clone(),
            market.bucket.clone(),
        );
        let Some(book) = self.books.get_mut(token_id) else {
            return;
        };
        if !book.should_emit() {
            return;
        }
        let (bid, bid_sz, ask, ask_sz) = book.top();
        let last_trade = book.last_trade_price;
        self.recorder
            .record(RecorderEvent::MarketTick(MarketTickRow {
                timestamp_ms: ts_ms,
                event_slug,
                series,
                bucket,
                token_id: token_id.to_string(),
                outcome: outcome.to_string(),
                best_bid: bid,
                best_ask: ask,
                bid_size: bid_sz,
                ask_size: ask_sz,
                last_trade_price: last_trade,
                book_ts_ms: pm_ts_ms,
                changed,
            }));
    }

    fn handle(&mut self, msg: WireMsg, recv_ts: i64) {
        match msg {
            WireMsg::Book {
                asset_id,
                timestamp,
                bids,
                asks,
            } => {
                let pm_ts = parse_pm_ts(timestamp.as_deref());
                self.books
                    .entry(asset_id.clone())
                    .or_default()
                    .snapshot(&bids, &asks);
                self.emit(&asset_id, "book", pm_ts, recv_ts);
            }
            WireMsg::PriceChange {
                timestamp,
                price_changes,
            } => {
                let pm_ts = parse_pm_ts(timestamp.as_deref());
                for pc in price_changes {
                    let (Some(price), Some(size), Some(side)) = (pc.price, pc.size, pc.side) else {
                        continue;
                    };
                    self.books
                        .entry(pc.asset_id.clone())
                        .or_default()
                        .apply_delta(side, price, size);
                    self.emit(&pc.asset_id, "price_change", pm_ts, recv_ts);
                }
            }
            WireMsg::LastTradePrice {
                asset_id,
                price,
                timestamp,
            } => {
                let pm_ts = parse_pm_ts(timestamp.as_deref());
                if let Some(book) = self.books.get_mut(&asset_id) {
                    book.last_trade_price = price;
                } else {
                    self.books
                        .entry(asset_id.clone())
                        .or_default()
                        .last_trade_price = price;
                }
                self.emit(&asset_id, "last_trade_price", pm_ts, recv_ts);
            }
            WireMsg::Unknown => {}
        }
    }
}

async fn run_connection(
    conn_id: usize,
    tokens: Vec<String>,
    registry: Arc<Registry>,
    recorder: RecorderHandle,
) -> Result<()> {
    info!(
        "Polymarket WS #{}: connecting with {} tokens",
        conn_id,
        tokens.len()
    );
    let (mut ws, _) = connect_async(PM_MARKET_WS).await?;
    let sub = json!({
        "assets_ids": tokens,
        "type": "market",
        "custom_feature_enabled": true,
    });
    ws.send(Message::Text(sub.to_string().into())).await?;
    info!("Polymarket WS #{}: subscribed", conn_id);

    let mut state = ConnState {
        books: HashMap::new(),
        registry,
        recorder,
    };
    let mut ping = tokio::time::interval(PING_INTERVAL);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = ping.tick() => {
                ws.send(Message::Text("PING".into())).await?;
            }
            frame = ws.next() => {
                let msg = frame.ok_or_else(|| anyhow!("Polymarket WS stream ended"))??;
                let recv_ts = now_ms();
                let text = match msg {
                    Message::Text(t) => t.to_string(),
                    Message::Ping(_) | Message::Pong(_) => continue,
                    Message::Close(f) => return Err(anyhow!("Polymarket WS closed: {:?}", f)),
                    _ => continue,
                };
                if text == "PONG" {
                    continue;
                }
                let envelope: WireEnvelope = match serde_json::from_str(&text) {
                    Ok(e) => e,
                    Err(e) => {
                        debug!("Polymarket WS #{}: unparseable message ({}): {:.200}", conn_id, e, text);
                        continue;
                    }
                };
                match envelope {
                    WireEnvelope::Batch(msgs) => {
                        for m in msgs {
                            state.handle(m, recv_ts);
                        }
                    }
                    WireEnvelope::Single(m) => state.handle(m, recv_ts),
                }
            }
        }
    }
}

/// One reconnect-forever task per token chunk. Returns only when aborted.
async fn connection_task(
    conn_id: usize,
    tokens: Vec<String>,
    registry: Arc<Registry>,
    recorder: RecorderHandle,
) {
    let mut backoff = 1.0f64;
    loop {
        let started = std::time::Instant::now();
        match run_connection(conn_id, tokens.clone(), registry.clone(), recorder.clone()).await {
            Ok(()) => backoff = 1.0,
            Err(e) => {
                if started.elapsed() > Duration::from_secs(60) {
                    backoff = 1.0;
                }
                warn!(
                    "Polymarket WS #{} disconnected: {} — retry in {:.0}s",
                    conn_id, e, backoff
                );
                tokio::time::sleep(Duration::from_secs_f64(backoff)).await;
                backoff = (backoff * 2.0).min(30.0);
            }
        }
    }
}

/// Manager: (re)spawns connection tasks whenever the registry token set changes.
pub async fn pm_feed_task(
    cfg: Config,
    mut registry_rx: watch::Receiver<Arc<Registry>>,
    recorder: RecorderHandle,
) {
    let mut current_tokens: Vec<String> = Vec::new();
    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    loop {
        let registry = registry_rx.borrow_and_update().clone();
        let tokens = registry.subscribe_tokens();

        if tokens != current_tokens {
            for h in handles.drain(..) {
                h.abort();
            }
            if tokens.is_empty() {
                info!("Polymarket feed: no tokens yet, waiting for discovery");
            } else {
                let chunks: Vec<Vec<String>> = tokens
                    .chunks(cfg.pm_ws_tokens_per_conn)
                    .map(|c| c.to_vec())
                    .collect();
                info!(
                    "Polymarket feed: subscribing {} tokens over {} connection(s)",
                    tokens.len(),
                    chunks.len()
                );
                for (i, chunk) in chunks.into_iter().enumerate() {
                    handles.push(tokio::spawn(connection_task(
                        i + 1,
                        chunk,
                        registry.clone(),
                        recorder.clone(),
                    )));
                }
            }
            current_tokens = tokens;
        }

        if registry_rx.changed().await.is_err() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_book_snapshot_top_and_dedupe() {
        let mut b = TokenBook::default();
        b.snapshot(
            &[
                WireLevel {
                    price: 0.18,
                    size: 100.0,
                },
                WireLevel {
                    price: 0.185,
                    size: 50.0,
                },
            ],
            &[
                WireLevel {
                    price: 0.19,
                    size: 40.0,
                },
                WireLevel {
                    price: 0.20,
                    size: 90.0,
                },
            ],
        );
        assert!(b.should_emit());
        let (bid, bid_sz, ask, ask_sz) = b.top();
        assert!((bid - 0.185).abs() < 1e-9);
        assert!((bid_sz - 50.0).abs() < 1e-9);
        assert!((ask - 0.19).abs() < 1e-9);
        assert!((ask_sz - 40.0).abs() < 1e-9);
        assert!(!b.should_emit(), "unchanged book must not re-emit");

        // Remove the best ask level — top changes, emit again.
        b.apply_delta(WireSide::Sell, 0.19, 0.0);
        assert!(b.should_emit());
        let (_, _, ask, _) = b.top();
        assert!((ask - 0.20).abs() < 1e-9);
    }

    #[test]
    fn test_last_trade_changes_trigger_emit() {
        let mut b = TokenBook::default();
        b.snapshot(
            &[WireLevel {
                price: 0.5,
                size: 10.0,
            }],
            &[],
        );
        assert!(b.should_emit());
        b.last_trade_price = 0.51;
        assert!(b.should_emit());
        assert!(!b.should_emit());
    }

    #[test]
    fn test_wire_parse_book_and_batch() {
        let json = r#"[
            {"event_type":"book","asset_id":"tok","timestamp":"1700000099800",
             "bids":[{"price":"0.55","size":"100"}],"asks":[{"price":"0.57","size":"50"}]},
            {"event_type":"last_trade_price","asset_id":"tok","price":"0.56"}
        ]"#;
        let env: WireEnvelope = serde_json::from_str(json).unwrap();
        let WireEnvelope::Batch(msgs) = env else {
            panic!("expected batch");
        };
        assert_eq!(msgs.len(), 2);
        assert!(matches!(msgs[0], WireMsg::Book { .. }));
        assert!(matches!(msgs[1], WireMsg::LastTradePrice { .. }));
    }

    #[test]
    fn test_wire_parse_price_change_and_unknown() {
        let json = r#"{"event_type":"price_change","timestamp":"1700000099900",
            "price_changes":[{"asset_id":"tok","price":"0.50","size":"200","side":"BUY"}]}"#;
        let msg: WireMsg = serde_json::from_str(json).unwrap();
        let WireMsg::PriceChange { price_changes, .. } = msg else {
            panic!("expected price_change");
        };
        assert_eq!(price_changes[0].side, Some(WireSide::Buy));

        let unknown: WireMsg =
            serde_json::from_str(r#"{"event_type":"tick_size_change"}"#).unwrap();
        assert!(matches!(unknown, WireMsg::Unknown));
    }
}
