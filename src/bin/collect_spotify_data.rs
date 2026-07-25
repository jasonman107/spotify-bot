//! Spotify-market data collector.
//!
//! Runs two concurrent tasks and one recorder:
//! - Gamma discovery (active events from the Spotify series + `spotify` tag)
//! - Polymarket market WS (top-of-book for every outcome token)
//!
//! Read-only: no private key required. Output lands in `DATA_DIR` (daily
//! rotation): market_ticks_*.csv. Chart + listener snapshots come from the
//! Python cron collector, not this process.

use std::sync::Arc;

use anyhow::Result;
use rustls::crypto::ring as ring_provider;
use tokio::sync::watch;
use tracing::info;

use spotify_bot::config::Config;
use spotify_bot::discovery::{self, Registry};
use spotify_bot::pm_feed;
use spotify_bot::recorder::{RecorderConfig, RecorderHandle};

#[tokio::main]
async fn main() -> Result<()> {
    // Install ring as the global rustls CryptoProvider before any TLS
    // connection — required when multiple provider features are compiled in
    // through different transitive dependencies (reqwest, tungstenite).
    let _ = ring_provider::default_provider().install_default();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let _ = dotenvy::dotenv();
    let cfg = Config::from_env();

    info!("collect-spotify-data starting");
    info!("  series slugs    : {}", cfg.series_slugs.join(", "));
    info!("  spotify tag id  : {}", cfg.spotify_tag_id);
    info!("  data dir        : {}", cfg.data_dir.display());
    info!("  gamma poll      : {:?}", cfg.gamma_poll);

    let recorder = RecorderHandle::start(RecorderConfig {
        dir: cfg.data_dir.clone(),
        queue_capacity: cfg.record_queue_capacity,
        flush_interval: cfg.record_flush,
    })?;

    let (registry_tx, registry_rx) = watch::channel(Arc::new(Registry::default()));

    let discovery_h = tokio::spawn(discovery::discovery_task(cfg.clone(), registry_tx));
    let pm_h = tokio::spawn(pm_feed::pm_feed_task(
        cfg.clone(),
        registry_rx,
        recorder.clone(),
    ));

    // Wait for SIGINT (Ctrl-C) or SIGTERM.
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate())?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;

    info!("shutdown signal received — flushing and exiting");
    discovery_h.abort();
    pm_h.abort();
    recorder.shutdown();
    info!("done (dropped_rows={})", recorder.dropped_rows());
    Ok(())
}
