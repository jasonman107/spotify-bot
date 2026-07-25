pub mod config;
pub mod discovery;
pub mod pm_feed;
pub mod recorder;

use std::time::{SystemTime, UNIX_EPOCH};

/// Wall-clock Unix milliseconds.
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
