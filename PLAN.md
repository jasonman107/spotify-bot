# spotify-bot — Plan

One bot that discovers, prices, and trades every Polymarket market whose resolution depends solely on Spotify data. Architecture is a port of `../elon-tweets-bot` (Rust collector + Python trader sharing a `data/` directory), swapping the Twitter/XTracker layer for Spotify data feeds and the NegBinom tweet-count model for chart-rank / listener-threshold models.

## Context

- `../elon-tweets-bot` is proven: Rust collector (Gamma discovery → Polymarket market WS → CSV recorder) + Python trader (60s loop, paper/live FAK execution via `py-clob-client-v2`) + numbered research pipeline + Docker/EC2 deploy. Most of it is domain-agnostic.
- Polymarket's Spotify markets all carry tag `spotify` (tag id **102851**). Recurring series (Gamma `/series`):
  - `1-spotify-song` (id 10710) — #1 global song this week, weekly
  - `top-us-spotify-song` (id 10770) — #1 US song this week, weekly
  - `monthly-listeners` (id 12344) — per-artist listener-threshold ladders (Bieber, Madonna, Shakira, …), monthly, **barrier/touch semantics**
  - Plus non-series events: `top-spotify-artist-in-<month>`, `#2 song`, annual `top-song-2026` / `top-spotify-artist-2026` (resolves off Spotify Wrapped — out of scope for v1).
- Kalshi has a far deeper Spotify catalog (daily/weekly/city charts, ~$400M music volume YTD) — **phase 3**, after the Polymarket loop is profitable. Reason: we already own the whole Polymarket stack; Kalshi means new auth, new execution, new fee model (quadratic).

## Market types and how they resolve (encode these exactly)

| Type | Example | Resolution |
| --- | --- | --- |
| Weekly #1 song (global / US) | `1-song-this-week-july-31-*` | Spotify weekly chart, week = **Fri 00:00 UTC → Thu 23:59 UTC**, published Friday on **open.spotify.com Charts** (not charts.spotify.com — that's Kalshi's source). "Other" if not published by ~Sat 11:59 PM ET. |
| Monthly-listener threshold | `justin-bieber-monthly-listeners-hits-by-august-31-*` | YES if artist-page monthly listeners ≥ strike **at any point** before month-end 11:59 PM ET. Barrier option, not terminal snapshot. Listener count = rolling 28-day uniques, refreshes ~daily. |
| Monthly top artist | `top-spotify-artist-in-<month>` | Greatest monthly listeners **at a hard snapshot, 12:00 PM ET on the last day**; alphabetical tiebreak. |

Critical facts to bake into code, not comments:
- **Barrier markets require observation**: a touch we didn't record is a touch we can't prove. Persist every listener-count observation with both source and receipt timestamps.
- **Restatement risk**: Spotify culls artificial streams after publication (the June 2026 "Earrings" incident: >500k streams removed, chart #1 → #4 *after* Kalshi settled). Settlement value = chart **as first published**. Archive the as-published artifact.
- **Manipulation is adversarial flow**: longshot daily/weekly buckets can be pumped by bot-streams cheaper than the market payout. Sudden longshot repricing is information, not noise.
- All events are `negRisk: true` multi-outcome; tick size 0.001, min order 5; outcome labels in `groupItemTitle` as `"Title - Artist"`.

## Architecture

Same two-service shape as elon-tweets-bot. Copy these near-verbatim (paths relative to `../elon-tweets-bot`):

| Reuse as-is | What it does |
| --- | --- |
| `src/recorder.rs` | Bounded-channel writer thread, UTC daily rotation — only the Row structs change |
| `src/pm_feed.rs` | Polymarket market WS client (chunked subscribe, PING/PONG, book/price_change/last_trade_price) |
| `src/config.rs`, `src/lib.rs`, `src/bin/collect_elon_data.rs` | Env config + task orchestration + rustls ring-provider skeleton |
| `scripts/execution.py` | Paper/Live FAK executor, decimal cleaning, fill verification — zero domain content |
| `scripts/setup_live_wallet.py`, `scripts/redeem_positions.py`, `scripts/convert_to_parquet.py` | Wallet, redemption listing, parquet conversion |
| `Dockerfile`, `Dockerfile.trader`, both compose files, `.github/workflows/deploy.yml`, `deploy/provision_ec2.sh`, `scripts/bot_config.sh` | Whole deploy stack (rename paths/containers) |
| `.cursor/skills/polymarket/**`, `.cursor/rules/*.mdc` | Polymarket reference + house rules (timestamp discipline, decimal precision, etc.) |

Do **not** copy: `.env`, `deploy/*.pem` (elon repo has live secrets committed to the working tree — start this repo clean with `.env.example` only).

### New Rust modules (replace `x_stream.rs` / `xtracker_feed.rs`)

1. **`src/discovery.rs`** (adapt): poll Gamma `GET /series?slug=…` for the 3 series slugs (nested `events[]` gives new weeklies the moment they're created — empirically Friday ~18:15 UTC) + `GET /events?tag_id=102851&closed=false` to catch one-off events. Publish token registry via `watch` channel exactly like the elon bot. New outcome-label parser: `"Title - Artist"` strings and listener-strike labels (`↑ 58m`) instead of integer buckets.
2. **`src/spotify_charts_feed.rs`**: fetch daily + weekly charts.
   - Primary: charts.spotify.com internal API (`https://charts-spotify-com-service.spotify.com/auth/v0/charts/{chart-id}/latest`, needs the anonymous bearer the web app bootstraps). Unofficial → wrap in retries and treat as soft dependency.
   - Fallback: kworb.net (`kworb.net/spotify/country/{global,us}_{daily,weekly}.html`, plain HTML tables) — cross-check only, never resolution truth.
   - Cadence: daily charts land before 22:00 UTC next day → poll 18:00–23:00 UTC every 15 min until new data; weekly poll Friday from 10:00 UTC. Archive raw payloads (as-published evidence).
3. **`src/monthly_listeners_feed.rs`**: scrape `open.spotify.com/artist/{id}` (server-rendered; contains "monthly listeners" with a plain UA) for the roster of artists appearing in any open market. 4x/day per artist. This page **is** the Polymarket resolution surface — store raw HTML snapshot + parsed number + both timestamps.

Recorder sinks: `market_ticks_YYYYMMDD.csv` (same schema as elon bot), `chart_snapshots_YYYYMMDD.jsonl`, `listener_counts_YYYYMMDD.csv`, plus `data/raw_html/` archives. Tick volume will be smaller than elon's (fewer tokens), but keep the parquet conversion cron from day one.

### Python trader (`scripts/trader.py`, adapted)

Keep the elon cycle skeleton (refresh registry → build signal state → compute probs → `read_quotes()` 8MB-tail trick → gate → enter/exit/settle → snapshot) and `TraderState`, risk caps, Paper/Live split. Replace the model layer with three per-type models in `scripts/research/model_lib.py`:

1. **Weekly #1 song**: race between top candidates. Signal = daily-chart stream trajectory (Fri–Thu accumulation) from the charts feed + kworb daily streams. Mid-week, most of the week is already observed — model remaining days with per-track day-of-week profiles and compute P(track A total > all others) via simple Monte Carlo over daily-stream residuals. This is a *much* smoother process than tweet counts; expect the edge to be "market slow to update after each daily chart drop" (one repricing event per day, ~20:00–22:00 UTC).
2. **Listener-threshold (barrier)**: daily listener series per artist → drift + noise on the 28-day rolling count; P(max over remaining daily snapshots ≥ strike) via first-passage simulation. Known catalysts (album drop, tour, viral moment) dominate — start with the statistical model and flag high-vol artists for manual review.
3. **Monthly top artist (snapshot)**: rank-race on the same listener series, terminal snapshot at 12:00 ET last day.

Gates: reuse elon's structure (edge threshold, fresh-tick, min-time-left, ask band, per-key entry cap, portfolio cap ~$50, flat $5 sizing, dynamic exit on `model_p − bid < −0.05`). Recalibrate all thresholds in backtest — elon's numbers (0.15 edge, 12h) were tuned for a 10-second signal, not a daily one.

**Anomaly/manipulation guard** (new, from the Earrings post-mortem): deseasonalized day-of-week z-score per track on daily streams; flag geographic divergence (US-only vs global surge) and single-track-vs-catalog spikes. Any flagged track: no new longshot NO positions against it, and surface an alert. This is defense first, alpha later.

## Research pipeline (before live)

Reproduce the elon numbered-script pattern (`scripts/research/01_…`):
1. Backfill: Gamma series history (all past weekly events + outcomes), CLOB `/prices-history` for their tokens (~1 month retention — start collecting now, this data evaporates), kworb historical daily/weekly charts, kworb `listeners.html` history (back to 2023).
2. EDA: how early does the eventual #1 become obvious each week? How do prices track daily chart drops?
3. Walk-forward model fit + calibration *before* PnL (elon lesson: evaluate forecast quality first — backtest fills at printed prices overstate PnL during bursts).
4. Divergence backtest with elon's bootstrap-by-event methodology, then gate tuning.

## Phases

1. **Collector + paper trader** (v1): discovery for the 3 series, charts + listener feeds, recorder, backfill scripts running daily. Paper trade weekly #1 markets only.
2. **Barrier + snapshot markets**: listener feed hardened (4x/day, HTML archive), the two listener models, paper → live on weekly markets if calibration holds.
3. **Kalshi**: read-only mirror first (their daily markets are a leading indicator for Polymarket weeklies — the same chart prints there daily), then execution. Encode the date-label mapping: Kalshi weekly ticker `26JUL02` (Thursday chart-end) ≈ Polymarket "July 3" (Friday publish). Off-by-one here silently mis-maps the whole book.

## Verification

- `cargo test` with canned Gamma/charts/artist-page fixtures (house rule: never hit live APIs in tests).
- Run collector locally for 24h; confirm chart snapshot lands within the publish window, listener counts parse for the full roster, tick CSV rotates.
- Paper trader ≥ 2 full weekly cycles; check `snapshots.csv` calibration (model vs market vs realized outcome) before any live order.
- `analyze_bot_health.py` port for the ops loop.

## Open questions

- Charts internal-API token bootstrap: needs a spike — if it's brittle, fall back to Friday CSV download + kworb dailies.
- Whether to also archive Polymarket's exact "open.spotify.com Charts heading" surface (web-player charts view) separately from charts.spotify.com — they should agree, but disputes settle on the former.
