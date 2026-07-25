#!/usr/bin/env python3
"""Step 1: validate that our data joins to market resolutions.

Checks, for every CLOSED Polymarket weekly #1 event (global + US):
- exactly one bucket resolved YES
- the kworb weekly chart #1 (Global / US) for the matching chart week agrees
  with the resolved bucket

Week mapping (encode once, reuse everywhere): Polymarket labels weekly events
by the Friday PUBLISH date; the chart week ends the preceding Thursday, which
is how kworb dates its weekly rows. So kworb week_end = event label date - 1.

Usage: python3 scripts/research/01_validate_data.py [--data-dir data]
"""

import argparse
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

WEEKLY_SERIES = {"1-spotify-song": "Global", "top-us-spotify-song": "US"}


def norm(s: str) -> set[str]:
    """Token set for fuzzy title/artist matching across label formats."""
    s = re.sub(r"\(w/[^)]*\)", " ", s)     # kworb feature credits
    s = re.sub(r"\(feat[^)]*\)", " ", s, flags=re.I)
    s = re.sub(r"[^a-z0-9 ]", " ", s.lower())
    return {t for t in s.split() if t not in {"the", "a", "with", "and"}}


def label_date(event_slug: str, event_end: str) -> datetime:
    # slugs look like 1-song-this-week-july-31-2026...; trust event_end instead
    return datetime.fromisoformat(event_end.replace("Z", "+00:00"))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-dir", default="data", type=Path)
    args = ap.parse_args()
    bf = args.data_dir / "backfill"

    ev = pd.read_parquet(bf / "events.parquet")
    mk = pd.read_parquet(bf / "markets.parquet")
    hist = pd.read_parquet(bf / "kworb" / "track_weekly_history.parquet")

    # kworb weekly #1 per (country, week), from per-track pages...
    k1 = hist[hist.pos == 1][["week_end", "country", "artist_title"]]
    # ...plus Wayback captures of the weekly chart pages (fills weeks whose
    # winner has since left the charts entirely)
    wb = bf / "wayback" / "weekly_charts.parquet"
    if wb.exists():
        w = pd.read_parquet(wb)
        w = w[w.pos == 1].rename(columns={"chart_date": "week_end"})
        w["country"] = w.country.map({"global": "Global", "us": "US"})
        k1 = pd.concat(
            [k1, w[["week_end", "country", "artist_title"]]], ignore_index=True
        ).drop_duplicates(subset=["week_end", "country"])

    ok = mismatch = missing = 0
    for _, e in ev[ev.closed & ev.series_slug.isin(WEEKLY_SERIES)].iterrows():
        country = WEEKLY_SERIES[e.series_slug]
        buckets = mk[mk.event_slug == e.event_slug]
        winners = buckets[buckets.resolved_yes_price == "1"]
        if len(winners) != 1:
            print(f"WARN {e.event_slug}: {len(winners)} YES buckets")
            continue
        winner = winners.iloc[0].bucket
        week_end = (label_date(e.event_slug, e.event_end) - timedelta(days=1)).date().isoformat()
        row = k1[(k1.country == country) & (k1.week_end == week_end)]
        if row.empty:
            missing += 1
            print(f"MISS {e.event_slug}: no kworb {country} row for week_end {week_end}")
            continue
        kworb_top = row.iloc[0].artist_title
        overlap = norm(winner) & norm(kworb_top)
        if len(overlap) >= 2 or norm(winner) <= norm(kworb_top):
            ok += 1
        else:
            mismatch += 1
            print(f"DIFF {e.event_slug} ({week_end}): market='{winner}' kworb='{kworb_top}'")

    print(f"\nweekly #1 events: {ok} matched, {mismatch} mismatched, {missing} missing kworb week")
    if mismatch or missing:
        print("NOTE: 'Other' resolutions and pre-kworb-coverage weeks land here; inspect before modeling.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
