//! `.env`-driven configuration, mirroring elon-tweets-bot conventions:
//! every knob has a default and is overridable via environment variable.

use std::path::PathBuf;
use std::time::Duration;

pub const GAMMA_API: &str = "https://gamma-api.polymarket.com";
pub const PM_MARKET_WS: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

#[derive(Debug, Clone)]
pub struct Config {
    /// Gamma series slugs grouping the recurring Spotify events.
    pub series_slugs: Vec<String>,
    /// Gamma tag id catching one-off Spotify events outside the series
    /// (`spotify` = 102851). 0 disables tag discovery.
    pub spotify_tag_id: u64,
    pub data_dir: PathBuf,
    pub gamma_poll: Duration,
    pub record_queue_capacity: usize,
    pub record_flush: Duration,
    /// Max asset ids per Polymarket market-WS connection.
    pub pm_ws_tokens_per_conn: usize,
}

fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            series_slugs: env_str(
                "SERIES_SLUG",
                "1-spotify-song,top-us-spotify-song,monthly-listeners",
            )
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
            spotify_tag_id: env_u64("SPOTIFY_TAG_ID", 102_851),
            data_dir: PathBuf::from(env_str("DATA_DIR", "data")),
            gamma_poll: Duration::from_secs(env_u64("GAMMA_POLL_SECS", 300)),
            record_queue_capacity: env_u64("RECORD_QUEUE_CAPACITY", 100_000) as usize,
            record_flush: Duration::from_millis(env_u64("RECORD_FLUSH_MS", 200).max(1)),
            pm_ws_tokens_per_conn: (env_u64("PM_WS_TOKENS_PER_CONN", 100) as usize).max(1),
        }
    }

    pub fn http_client(&self) -> anyhow::Result<reqwest::Client> {
        Ok(reqwest::Client::builder()
            .tcp_nodelay(true)
            .timeout(Duration::from_secs(30))
            .build()?)
    }
}
