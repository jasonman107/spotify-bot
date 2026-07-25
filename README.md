# spotify-bot

Trades Polymarket markets that resolve solely on Spotify data (weekly #1 song global/US, artist monthly-listener thresholds, monthly top artist). Sibling of `../elon-tweets-bot`; see [PLAN.md](PLAN.md) for the full design.

**Current phase: paper trading the weekly #1 markets** with the trajectory
model (research steps 01–06 validated it: LOWO Brier 0.096, backtest +101%
ROI over 11 events). Listener markets and live mode come after forward
calibration holds.

## Running

```sh
cargo run --release --bin collect-spotify-data   # tick collector (Gamma + market WS)
python3 scripts/trader.py --mode paper           # 60s-cycle paper trader
```

Both are currently running under nohup with logs in `logs/`. The trader
prints one line per event per cycle and logs fills to `data/paper/trades.csv`,
model-vs-market snapshots to `data/paper/snapshots.csv`.

## Data collection

```sh
python3 scripts/backfill_history.py            # Polymarket Gamma events + CLOB price history
python3 scripts/backfill_kworb.py              # current kworb charts + per-track weekly history
python3 scripts/backfill_wayback.py            # historical daily charts + listener counts via Wayback Machine
python3 scripts/collect_snapshots.py           # one forward snapshot (kworb charts + listeners); cron this daily
scripts/install_cron.sh                        # installs the daily snapshot cron on this machine
```

Everything lands in `data/` (gitignored):

- `data/backfill/markets.parquet`, `events.parquet` — every Spotify-tagged Polymarket market with tokens + resolution outcomes
- `data/backfill/prices/event_slug=*/prices.parquet` — CLOB YES-price history (~1 month retention upstream, so run early and often)
- `data/backfill/kworb/*.parquet` — chart snapshots, per-track weekly history panel, listener table
- `data/backfill/wayback/*.parquet` — historical daily charts (global + US) and daily listener counts reconstructed from Wayback snapshots of kworb
- `data/snapshots/` — forward daily collection (append-only CSV + gzipped raw HTML as as-published evidence)

## Data source notes

- kworb page formats and quirks: see docstring in `scripts/kworb_lib.py`.
- `open.spotify.com/artist/{id}` (the actual Polymarket resolution surface for listener markets) serves only a JS shell to plain HTTP clients from this network; the anonymous-token endpoint 403s. Live bot will need a headless-browser scrape. For research, kworb's listener table (exact counts, top ~2000 artists, daily) is the proxy, with Wayback for history.
- Resolution-semantics gotchas (barrier vs snapshot, chart week Fri–Thu UTC, restatement risk) are documented in PLAN.md — read before modeling.
