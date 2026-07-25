//! Daily-rotating tick recorder, ported from elon-tweets-bot: feed tasks hand
//! off typed events with `try_send` (never blocking); a dedicated OS thread
//! owns all file I/O, rotation, buffering, and flushing.
//!
//! One sink, rotated at UTC midnight:
//! - `market_ticks_YYYYMMDD.csv` — one row per top-of-book / trade change
//!
//! Spotify chart + listener observations are collected by the Python cron
//! collector (`scripts/collect_snapshots.py`), not this process.

use std::fs::{self, File, OpenOptions};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use tracing::{debug, info, warn};

pub const MARKET_TICKS_HEADER: [&str; 13] = [
    "timestamp_ms",
    "event_slug",
    "series",
    "bucket",
    "token_id",
    "outcome",
    "best_bid",
    "best_ask",
    "bid_size",
    "ask_size",
    "last_trade_price",
    "book_ts_ms",
    "changed",
];

#[derive(Debug, Clone)]
pub struct MarketTickRow {
    pub timestamp_ms: i64,
    pub event_slug: String,
    pub series: String,
    pub bucket: String,
    pub token_id: String,
    pub outcome: String,
    pub best_bid: f64,
    pub best_ask: f64,
    pub bid_size: f64,
    pub ask_size: f64,
    pub last_trade_price: f64,
    /// Polymarket-embedded message timestamp (ms); 0 when absent.
    pub book_ts_ms: i64,
    /// Which WS event produced this row: book | price_change | last_trade_price.
    pub changed: &'static str,
}

#[derive(Debug, Clone)]
pub enum RecorderEvent {
    MarketTick(MarketTickRow),
}

enum Command {
    Event(RecorderEvent),
    Shutdown,
}

#[derive(Debug, Clone)]
pub struct RecorderConfig {
    pub dir: PathBuf,
    pub queue_capacity: usize,
    pub flush_interval: Duration,
}

#[derive(Clone)]
pub struct RecorderHandle {
    tx: SyncSender<Command>,
    dropped: Arc<AtomicU64>,
    join: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl RecorderHandle {
    pub fn start(cfg: RecorderConfig) -> Result<Self> {
        fs::create_dir_all(&cfg.dir)
            .with_context(|| format!("create data directory {}", cfg.dir.display()))?;
        let (tx, rx) = sync_channel(cfg.queue_capacity.max(1));
        let dropped = Arc::new(AtomicU64::new(0));
        let writer_dropped = dropped.clone();
        let join = thread::Builder::new()
            .name("recorder-writer".to_string())
            .spawn(move || writer_loop(cfg, rx, writer_dropped))
            .context("spawn recorder writer thread")?;
        Ok(Self {
            tx,
            dropped,
            join: Arc::new(Mutex::new(Some(join))),
        })
    }

    pub fn record(&self, event: RecorderEvent) {
        match self.tx.try_send(Command::Event(event)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn dropped_rows(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    pub fn shutdown(&self) {
        if let Err(e) = self.tx.try_send(Command::Shutdown) {
            debug!("recorder shutdown signal not queued: {}", e);
        }
        match self.join.lock() {
            Ok(mut guard) => {
                if let Some(join) = guard.take() {
                    if let Err(e) = join.join() {
                        warn!("recorder writer thread panicked: {:?}", e);
                    }
                }
            }
            Err(e) => warn!("recorder join lock poisoned: {}", e),
        }
    }
}

fn writer_loop(cfg: RecorderConfig, rx: Receiver<Command>, dropped: Arc<AtomicU64>) {
    let mut sinks = match Sinks::open(&cfg.dir) {
        Ok(s) => s,
        Err(e) => {
            warn!("recorder disabled: {}", e);
            return;
        }
    };
    let mut last_drop_log: u64 = 0;
    info!(
        "recorder writer started: dir={} flush_ms={}",
        cfg.dir.display(),
        cfg.flush_interval.as_millis()
    );

    loop {
        match rx.recv_timeout(cfg.flush_interval) {
            Ok(Command::Event(event)) => {
                if let Err(e) = sinks.write(&event) {
                    warn!("recorder write failed: {}", e);
                }
            }
            Ok(Command::Shutdown) => break,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                if let Err(e) = sinks.flush() {
                    warn!("recorder flush failed: {}", e);
                }
                let d = dropped.load(Ordering::Relaxed);
                if d > last_drop_log {
                    warn!("recorder dropped_rows={}", d);
                    last_drop_log = d;
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    if let Err(e) = sinks.flush() {
        warn!("recorder final flush failed: {}", e);
    }
    info!("recorder writer stopped");
}

struct Sinks {
    dir: PathBuf,
    current_date: String,
    market_ticks: csv::Writer<BufWriter<File>>,
}

impl Sinks {
    fn open(dir: &Path) -> Result<Self> {
        let date = Utc::now().format("%Y%m%d").to_string();
        Ok(Self {
            dir: dir.to_path_buf(),
            market_ticks: open_csv(dir, "market_ticks", &date, &MARKET_TICKS_HEADER)?,
            current_date: date,
        })
    }

    fn rotate_if_needed(&mut self) -> Result<()> {
        let today = Utc::now().format("%Y%m%d").to_string();
        if today == self.current_date {
            return Ok(());
        }
        self.flush()?;
        self.market_ticks = open_csv(&self.dir, "market_ticks", &today, &MARKET_TICKS_HEADER)?;
        info!("recorder daily rollover to {}", today);
        self.current_date = today;
        Ok(())
    }

    fn write(&mut self, event: &RecorderEvent) -> Result<()> {
        self.rotate_if_needed()?;
        match event {
            RecorderEvent::MarketTick(r) => self
                .market_ticks
                .write_record([
                    r.timestamp_ms.to_string(),
                    r.event_slug.clone(),
                    r.series.clone(),
                    r.bucket.clone(),
                    r.token_id.clone(),
                    r.outcome.clone(),
                    fmt_f64(r.best_bid),
                    fmt_f64(r.best_ask),
                    fmt_f64(r.bid_size),
                    fmt_f64(r.ask_size),
                    fmt_f64(r.last_trade_price),
                    fmt_i64(r.book_ts_ms),
                    r.changed.to_string(),
                ])
                .context("write market tick row"),
        }
    }

    fn flush(&mut self) -> Result<()> {
        self.market_ticks.flush().context("flush market ticks")
    }
}

fn open_csv(
    dir: &Path,
    prefix: &str,
    date: &str,
    header: &[&str],
) -> Result<csv::Writer<BufWriter<File>>> {
    fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    let path = dir.join(format!("{}_{}.csv", prefix, date));
    let is_new = !path.exists() || path.metadata().map(|m| m.len() == 0).unwrap_or(true);
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("open {}", path.display()))?;
    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(BufWriter::new(file));
    if is_new {
        writer.write_record(header).context("write CSV header")?;
        writer.flush().context("flush CSV header")?;
    }
    Ok(writer)
}

/// Empty string for unset (0.0) floats, matching elon-tweets-bot conventions.
fn fmt_f64(v: f64) -> String {
    if v == 0.0 {
        String::new()
    } else {
        v.to_string()
    }
}

fn fmt_i64(v: i64) -> String {
    if v == 0 {
        String::new()
    } else {
        v.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tick_row() -> MarketTickRow {
        MarketTickRow {
            timestamp_ms: 1_778_000_000_000,
            event_slug: "1-song-this-week-july-31".to_string(),
            series: "1-spotify-song".to_string(),
            bucket: "Dai Dai - Shakira, Burna Boy".to_string(),
            token_id: "12345".to_string(),
            outcome: "Yes".to_string(),
            best_bid: 0.185,
            best_ask: 0.19,
            bid_size: 1000.0,
            ask_size: 500.0,
            last_trade_price: 0.187,
            book_ts_ms: 1_777_999_999_990,
            changed: "book",
        }
    }

    #[test]
    fn test_sinks_write_and_reopen_headers_once() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut sinks = Sinks::open(tmp.path()).unwrap();
            sinks.write(&RecorderEvent::MarketTick(tick_row())).unwrap();
            sinks.flush().unwrap();
        }
        // Re-open: header must not be duplicated on append.
        {
            let mut sinks = Sinks::open(tmp.path()).unwrap();
            sinks.write(&RecorderEvent::MarketTick(tick_row())).unwrap();
            sinks.flush().unwrap();
        }

        let date = Utc::now().format("%Y%m%d").to_string();
        let ticks =
            std::fs::read_to_string(tmp.path().join(format!("market_ticks_{}.csv", date))).unwrap();
        assert_eq!(ticks.lines().count(), 3, "header + two rows");
        assert!(ticks.starts_with("timestamp_ms,event_slug,series,bucket"));
    }

    #[test]
    fn test_handle_records_without_blocking() {
        let tmp = tempfile::tempdir().unwrap();
        let handle = RecorderHandle::start(RecorderConfig {
            dir: tmp.path().to_path_buf(),
            queue_capacity: 16,
            flush_interval: Duration::from_millis(10),
        })
        .unwrap();
        handle.record(RecorderEvent::MarketTick(tick_row()));
        handle.shutdown();

        let date = Utc::now().format("%Y%m%d").to_string();
        let ticks =
            std::fs::read_to_string(tmp.path().join(format!("market_ticks_{}.csv", date))).unwrap();
        assert_eq!(ticks.lines().count(), 2);
        assert_eq!(handle.dropped_rows(), 0);
    }
}
