//! Gamma market discovery.
//!
//! Polls the recurring Spotify series (`1-spotify-song`, `top-us-spotify-song`,
//! `monthly-listeners`) plus everything carrying the `spotify` tag for active
//! events, and publishes the result through a `watch` channel. New weekly
//! events are created every Friday, so consumers (the Polymarket WS feed)
//! must handle registry changes mid-run.
//!
//! Unlike the tweet-count buckets, outcome labels here are free text
//! (`"Dai Dai - Shakira, Burna Boy"`, listener strikes like `"↑ 58m"`,
//! `"Other"`); interpretation lives in the Python trader, the collector just
//! carries the label through.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::DateTime;
use serde_json::Value;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::config::{Config, GAMMA_API};

/// One outcome market (song / strike / Other) inside a Spotify event.
#[derive(Debug, Clone, PartialEq)]
pub struct BucketMarket {
    pub event_slug: String,
    /// Series slug the event came from; "tag" for tag-only discoveries.
    pub series: String,
    /// Raw outcome label from Gamma (`groupItemTitle`).
    pub bucket: String,
    pub yes_token: String,
    pub no_token: String,
    pub neg_risk: bool,
    pub end_date_ts: i64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Registry {
    pub markets: Vec<BucketMarket>,
}

impl Registry {
    /// Sorted YES+NO token ids across all bucket markets — the Polymarket WS
    /// subscription set. Both sides are subscribed because the market channel
    /// pushes `price_change` events for sibling tokens anyway; subscribing
    /// them gets proper `book` snapshots instead of partial delta-only books.
    /// Sorted so set comparison is order-independent.
    pub fn subscribe_tokens(&self) -> Vec<String> {
        let set: BTreeSet<&str> = self
            .markets
            .iter()
            .flat_map(|m| [m.yes_token.as_str(), m.no_token.as_str()])
            .filter(|t| !t.is_empty())
            .collect();
        set.into_iter().map(str::to_string).collect()
    }

    pub fn market_for_token(&self, token_id: &str) -> Option<&BucketMarket> {
        self.markets
            .iter()
            .find(|m| m.yes_token == token_id || m.no_token == token_id)
    }
}

/// Gamma sometimes encodes arrays as JSON strings (e.g. `clobTokenIds`).
fn parse_str_or_array(val: Option<&Value>) -> Vec<String> {
    match val {
        None => vec![],
        Some(Value::Array(arr)) => arr
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect(),
        Some(Value::String(s)) => serde_json::from_str::<Vec<String>>(s).unwrap_or_default(),
        _ => vec![],
    }
}

fn parse_ts(date_str: &str) -> i64 {
    DateTime::parse_from_rfc3339(&date_str.replace('Z', "+00:00"))
        .map(|dt| dt.timestamp())
        .unwrap_or(0)
}

fn bucket_markets_from_event(ev: &Value, series: &str) -> Vec<BucketMarket> {
    let slug = ev.get("slug").and_then(|v| v.as_str()).unwrap_or("");
    let Some(markets) = ev.get("markets").and_then(|m| m.as_array()) else {
        return vec![];
    };
    let mut out = Vec::with_capacity(markets.len());
    for m in markets {
        if m.get("closed").and_then(|v| v.as_bool()).unwrap_or(false) {
            continue;
        }
        let tokens = parse_str_or_array(m.get("clobTokenIds"));
        let outcomes = parse_str_or_array(m.get("outcomes"));
        if tokens.len() < 2 || outcomes.len() < 2 {
            continue;
        }
        // Map outcome labels to token ids; Yes/No ordering is not guaranteed.
        let yes = outcomes
            .iter()
            .zip(tokens.iter())
            .find(|(o, _)| o.eq_ignore_ascii_case("yes"))
            .map(|(_, t)| t.clone())
            .unwrap_or_else(|| tokens[0].clone());
        let no = outcomes
            .iter()
            .zip(tokens.iter())
            .find(|(o, _)| o.eq_ignore_ascii_case("no"))
            .map(|(_, t)| t.clone())
            .unwrap_or_else(|| tokens[1].clone());
        // Some single-market events have no groupItemTitle; fall back to the
        // market question so the label is never empty.
        let bucket = m
            .get("groupItemTitle")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .or_else(|| m.get("question").and_then(|v| v.as_str()))
            .unwrap_or("")
            .to_string();
        out.push(BucketMarket {
            event_slug: slug.to_string(),
            series: series.to_string(),
            bucket,
            yes_token: yes,
            no_token: no,
            neg_risk: m.get("negRisk").and_then(|v| v.as_bool()).unwrap_or(false),
            end_date_ts: parse_ts(m.get("endDate").and_then(|v| v.as_str()).unwrap_or("")),
        });
    }
    out
}

fn events_from_response(data: Value) -> Vec<Value> {
    match data {
        Value::Array(arr) => arr,
        obj @ Value::Object(_) => vec![obj],
        _ => vec![],
    }
}

pub async fn fetch_series_markets(
    client: &reqwest::Client,
    series_slug: &str,
) -> Result<Vec<BucketMarket>> {
    let resp = client
        .get(format!("{}/events", GAMMA_API))
        .query(&[
            ("series_slug", series_slug),
            ("closed", "false"),
            ("active", "true"),
            ("limit", "50"),
        ])
        .send()
        .await
        .context("gamma series events request")?;
    let data: Value = resp.json().await.context("gamma series events parse")?;
    let mut markets = Vec::new();
    for ev in &events_from_response(data) {
        markets.extend(bucket_markets_from_event(ev, series_slug));
    }
    Ok(markets)
}

pub async fn fetch_tag_markets(
    client: &reqwest::Client,
    tag_id: u64,
) -> Result<Vec<BucketMarket>> {
    let resp = client
        .get(format!("{}/events", GAMMA_API))
        .query(&[
            ("tag_id", tag_id.to_string().as_str()),
            ("closed", "false"),
            ("active", "true"),
            ("limit", "100"),
        ])
        .send()
        .await
        .context("gamma tag events request")?;
    let data: Value = resp.json().await.context("gamma tag events parse")?;
    let mut markets = Vec::new();
    for ev in &events_from_response(data) {
        markets.extend(bucket_markets_from_event(ev, "tag"));
    }
    Ok(markets)
}

async fn build_registry(client: &reqwest::Client, cfg: &Config) -> Result<Registry> {
    let mut markets = Vec::new();
    for slug in &cfg.series_slugs {
        markets.extend(fetch_series_markets(client, slug).await?);
    }
    // Tag discovery failure should not blank out series data — degrade and
    // let the next poll recover.
    if cfg.spotify_tag_id != 0 {
        match fetch_tag_markets(client, cfg.spotify_tag_id).await {
            Ok(tagged) => {
                for m in tagged {
                    let dup = markets
                        .iter()
                        .any(|x| x.event_slug == m.event_slug && x.bucket == m.bucket);
                    if !dup {
                        markets.push(m);
                    }
                }
            }
            Err(e) => warn!("tag discovery failed (keeping series markets): {}", e),
        }
    }
    Ok(Registry { markets })
}

/// Poll Gamma forever, publishing registry changes on `tx`.
pub async fn discovery_task(cfg: Config, tx: watch::Sender<Arc<Registry>>) {
    let client = match cfg.http_client() {
        Ok(c) => c,
        Err(e) => {
            warn!("discovery HTTP client build failed: {}", e);
            return;
        }
    };
    let mut backoff = Duration::from_secs(5);
    loop {
        match build_registry(&client, &cfg).await {
            Ok(reg) => {
                backoff = Duration::from_secs(5);
                let current = tx.borrow().clone();
                if *current != reg {
                    let n_events: BTreeSet<&str> =
                        reg.markets.iter().map(|m| m.event_slug.as_str()).collect();
                    info!(
                        "registry updated: {} bucket markets across {} events",
                        reg.markets.len(),
                        n_events.len(),
                    );
                    let _ = tx.send(Arc::new(reg));
                } else {
                    debug!("registry unchanged");
                }
                tokio::time::sleep(cfg.gamma_poll).await;
            }
            Err(e) => {
                warn!("discovery failed: {} — retry in {:?}", e, backoff);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(120));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_markets_from_event() {
        let ev: Value = serde_json::json!({
            "slug": "1-song-this-week-july-31",
            "markets": [
                {
                    "groupItemTitle": "Dai Dai - Shakira, Burna Boy",
                    "clobTokenIds": "[\"111\", \"222\"]",
                    "outcomes": "[\"Yes\", \"No\"]",
                    "negRisk": true,
                    "closed": false,
                    "endDate": "2026-07-31T23:59:00Z"
                },
                {
                    "groupItemTitle": "Golden - HUNTR/X",
                    "clobTokenIds": "[\"333\", \"444\"]",
                    "outcomes": "[\"Yes\", \"No\"]",
                    "closed": true
                }
            ]
        });
        let markets = bucket_markets_from_event(&ev, "1-spotify-song");
        assert_eq!(markets.len(), 1, "closed market skipped");
        let m = &markets[0];
        assert_eq!(m.bucket, "Dai Dai - Shakira, Burna Boy");
        assert_eq!(m.series, "1-spotify-song");
        assert_eq!(m.yes_token, "111");
        assert_eq!(m.no_token, "222");
        assert!(m.neg_risk);
        assert!(m.end_date_ts > 0);
    }

    #[test]
    fn test_bucket_falls_back_to_question() {
        let ev: Value = serde_json::json!({
            "slug": "justin-bieber-monthly-listeners-hits-by-august-31",
            "markets": [{
                "groupItemTitle": "",
                "question": "Will Justin Bieber hit 90M monthly listeners?",
                "clobTokenIds": ["1", "2"],
                "outcomes": ["Yes", "No"],
                "closed": false
            }]
        });
        let markets = bucket_markets_from_event(&ev, "monthly-listeners");
        assert_eq!(
            markets[0].bucket,
            "Will Justin Bieber hit 90M monthly listeners?"
        );
    }

    #[test]
    fn test_registry_subscribe_tokens_sorted_dedup_both_sides() {
        let mk = |yes: &str, no: &str| BucketMarket {
            event_slug: "e".into(),
            series: "s".into(),
            bucket: "b".into(),
            yes_token: yes.into(),
            no_token: no.into(),
            neg_risk: true,
            end_date_ts: 0,
        };
        let reg = Registry {
            markets: vec![mk("2", "3"), mk("1", "3")],
        };
        assert_eq!(
            reg.subscribe_tokens(),
            vec!["1".to_string(), "2".to_string(), "3".to_string()]
        );
    }
}
