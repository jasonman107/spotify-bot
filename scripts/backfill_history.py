#!/usr/bin/env python3
"""Backfill Polymarket history for Spotify-resolved markets into parquet.

Sources (public, no auth):
- Gamma: all events in the recurring Spotify series plus everything tagged
  `spotify` (tag id 102851), closed + active, with buckets, tokens, and
  resolution outcomes -> data/backfill/{events,markets}.parquet
- CLOB prices-history: ~10-min YES price series per bucket market. Upstream
  retention only covers events ended within roughly the last month, so we
  fetch events ending within --price-days (default 45) plus active ones.
  -> data/backfill/prices/event_slug=<slug>/prices.parquet

Idempotent: existing outputs are skipped unless --force (markets/events are
always refreshed; per-event price files are skipped once written).

Usage:
    python3 scripts/backfill_history.py [--data-dir data] [--price-days 45] [--force]
"""

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from kworb_lib import get_json

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
SERIES = ("1-spotify-song", "top-us-spotify-song", "monthly-listeners")
SPOTIFY_TAG_ID = 102851


def parse_maybe_json_list(v):
    if isinstance(v, str):
        try:
            return json.loads(v)
        except json.JSONDecodeError:
            return []
    return v or []


def fetch_events_paged(query: str) -> list[dict]:
    events = []
    offset = 0
    while True:
        page = get_json(f"{GAMMA}/events?{query}&limit=100&offset={offset}")
        if not page:
            break
        events.extend(page)
        if len(page) < 100:
            break
        offset += 100
        time.sleep(0.2)
    return events


def fetch_all_events() -> list[dict]:
    seen: dict[str, dict] = {}
    for series in SERIES:
        for closed in ("true", "false"):
            for ev in fetch_events_paged(f"series_slug={series}&closed={closed}"):
                seen[ev.get("slug", "")] = ev
    for closed in ("true", "false"):
        for ev in fetch_events_paged(f"tag_id={SPOTIFY_TAG_ID}&closed={closed}"):
            seen.setdefault(ev.get("slug", ""), ev)
    seen.pop("", None)
    return list(seen.values())


def events_frame(events: list[dict]) -> pd.DataFrame:
    rows = []
    for ev in events:
        series = ev.get("series") or []
        rows.append({
            "event_slug": ev.get("slug", ""),
            "title": ev.get("title", ""),
            "series_slug": series[0].get("slug", "") if series else "",
            "event_start": ev.get("startDate", ""),
            "event_end": ev.get("endDate", ""),
            "created_at": ev.get("createdAt", ""),
            "closed": bool(ev.get("closed", False)),
            "neg_risk": bool(ev.get("negRisk", False)),
            "volume": float(ev.get("volume") or 0),
            "description": (ev.get("description") or ""),
        })
    return pd.DataFrame(rows)


def markets_frame(events: list[dict]) -> pd.DataFrame:
    rows = []
    for ev in events:
        for m in ev.get("markets", []):
            tokens = parse_maybe_json_list(m.get("clobTokenIds"))
            outcomes = parse_maybe_json_list(m.get("outcomes"))
            outcome_prices = parse_maybe_json_list(m.get("outcomePrices"))
            tok_by_outcome = dict(zip([o.lower() for o in outcomes], tokens))
            price_by_outcome = dict(zip([o.lower() for o in outcomes], outcome_prices))
            rows.append({
                "event_slug": ev.get("slug", ""),
                "event_end": ev.get("endDate", ""),
                "closed": bool(ev.get("closed", False)),
                # outcome label, e.g. "Earrings - Malcolm Todd" or a strike
                "bucket": m.get("groupItemTitle", "") or m.get("question", ""),
                "question": m.get("question", ""),
                "condition_id": m.get("conditionId", ""),
                "yes_token": tok_by_outcome.get("yes", ""),
                "no_token": tok_by_outcome.get("no", ""),
                "market_closed": bool(m.get("closed", False)),
                # "1" when the bucket resolved YES.
                "resolved_yes_price": price_by_outcome.get("yes", ""),
                "volume": float(m.get("volumeNum") or 0),
                "neg_risk": bool(m.get("negRisk", False)),
            })
    return pd.DataFrame(rows)


def backfill_markets(out_dir: Path) -> pd.DataFrame:
    events = fetch_all_events()
    ev_df = events_frame(events)
    mk_df = markets_frame(events)
    out_dir.mkdir(parents=True, exist_ok=True)
    ev_df.to_parquet(out_dir / "events.parquet", compression="zstd", index=False)
    mk_df.to_parquet(out_dir / "markets.parquet", compression="zstd", index=False)
    print(f"events: {len(ev_df)} -> {out_dir/'events.parquet'}")
    print(f"markets: {len(mk_df)} bucket markets across "
          f"{mk_df['event_slug'].nunique()} events -> {out_dir/'markets.parquet'}")
    return mk_df


def backfill_prices(out_dir: Path, markets: pd.DataFrame, price_days: int,
                    force: bool) -> None:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=price_days)).isoformat()
    recent = markets[(markets["event_end"] >= cutoff) & (markets["yes_token"] != "")]
    slugs = recent["event_slug"].unique()
    print(f"prices: {len(slugs)} events ended/ending after {cutoff[:10]}")
    for slug in slugs:
        out = out_dir / "prices" / f"event_slug={slug}" / "prices.parquet"
        if out.exists() and not force:
            continue
        frames = []
        for _, m in recent[recent["event_slug"] == slug].iterrows():
            body = get_json(
                f"{CLOB}/prices-history?market={m.yes_token}&interval=max&fidelity=1"
            )
            hist = (body or {}).get("history", [])
            if not hist:
                continue
            df = pd.DataFrame(hist).rename(columns={"t": "timestamp_s", "p": "price"})
            df["bucket"] = m.bucket
            df["token_id"] = m.yes_token
            frames.append(df)
            time.sleep(0.15)
        if not frames:
            print(f"  {slug}: no price history (outside retention)")
            continue
        df = pd.concat(frames, ignore_index=True)
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, compression="zstd", index=False)
        print(f"  {slug}: {len(df)} points across "
              f"{df['bucket'].nunique()} buckets -> {out}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-dir", default="data", type=Path)
    ap.add_argument("--price-days", default=45, type=int,
                    help="fetch price history for events ending within N days")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    out_dir = args.data_dir / "backfill"
    markets = backfill_markets(out_dir)
    backfill_prices(out_dir, markets, args.price_days, args.force)
    print("backfill done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
