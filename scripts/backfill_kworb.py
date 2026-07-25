#!/usr/bin/env python3
"""Backfill kworb.net Spotify data into parquet.

- Current chart pages (global/us x daily/weekly) -> kworb/charts_current.parquet
- Listener tables (listeners.html + listeners2.html, exact monthly-listener
  counts for the top ~2000 artists) -> kworb/listeners_current.parquet
- Per-track WEEKLY chart history for every track on the current chart pages
  (kworb keeps no dated chart archives; track pages are the only on-site
  history) -> kworb/track_weekly_history.parquet

Raw track HTML is cached in data/backfill/kworb/track_html/ and refetched only
if older than --max-age-hours (default 20), so reruns are cheap and polite.

Usage:
    python3 scripts/backfill_kworb.py [--data-dir data] [--max-age-hours 20]
"""

import argparse
import gzip
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from kworb_lib import KWORB, get, parse_chart_page, parse_listeners_page, parse_track_page

CHARTS = [
    ("global", "daily"), ("us", "daily"),
    ("global", "weekly"), ("us", "weekly"),
]


def backfill_charts(out_dir: Path) -> pd.DataFrame:
    frames = []
    for country, cadence in CHARTS:
        html = get(f"{KWORB}/spotify/country/{country}_{cadence}.html")
        if not html:
            continue
        df = pd.DataFrame(parse_chart_page(html, cadence))
        df["country"] = country
        df["cadence"] = cadence
        frames.append(df)
        time.sleep(0.3)
    df = pd.concat(frames, ignore_index=True)
    df.to_parquet(out_dir / "charts_current.parquet", compression="zstd", index=False)
    print(f"charts: {len(df)} rows ({df['chart_date'].min()}..{df['chart_date'].max()})"
          f" -> {out_dir/'charts_current.parquet'}")
    return df


def backfill_listeners(out_dir: Path) -> None:
    frames = []
    for page in ("listeners.html", "listeners2.html"):
        html = get(f"{KWORB}/spotify/{page}")
        if html:
            frames.append(pd.DataFrame(parse_listeners_page(html)))
        time.sleep(0.3)
    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset="artist_id")
    df["fetched_at"] = datetime.now(timezone.utc).isoformat()
    df.to_parquet(out_dir / "listeners_current.parquet", compression="zstd", index=False)
    print(f"listeners: {len(df)} artists -> {out_dir/'listeners_current.parquet'}")


def backfill_track_histories(out_dir: Path, charts: pd.DataFrame,
                             max_age_hours: float) -> None:
    cache = out_dir / "track_html"
    cache.mkdir(parents=True, exist_ok=True)
    tracks = (charts[charts["track_id"] != ""]
              .drop_duplicates(subset="track_id")[["track_id", "artist_title"]])
    print(f"track histories: {len(tracks)} unique tracks on current charts")
    now = time.time()
    frames = {"weekly": [], "daily": []}
    fetched = 0
    for _, t in tracks.iterrows():
        path = cache / f"{t.track_id}.html.gz"
        if path.exists() and (now - path.stat().st_mtime) < max_age_hours * 3600:
            html = gzip.decompress(path.read_bytes()).decode("utf-8", "replace")
        else:
            html = get(f"{KWORB}/spotify/track/{t.track_id}.html")
            if not html:
                continue
            path.write_bytes(gzip.compress(html.encode("utf-8")))
            fetched += 1
            time.sleep(0.3)
        for cadence, rows in parse_track_page(html).items():
            if not rows:
                continue
            df = pd.DataFrame(rows)
            df["track_id"] = t.track_id
            df["artist_title"] = t.artist_title
            frames[cadence].append(df)
    for cadence, name in (("weekly", "track_weekly_history"),
                          ("daily", "track_daily_history")):
        df = pd.concat(frames[cadence], ignore_index=True)
        if cadence == "weekly":
            df = df.rename(columns={"date": "week_end"})
        df.to_parquet(out_dir / f"{name}.parquet", compression="zstd", index=False)
        date_col = "week_end" if cadence == "weekly" else "date"
        print(f"track {cadence}: {len(frames[cadence])} tracks, {len(df)} rows "
              f"({df[date_col].min()}..{df[date_col].max()}) -> {out_dir/name}.parquet")
    print(f"track histories: {fetched} fetched")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-dir", default="data", type=Path)
    ap.add_argument("--max-age-hours", default=20, type=float)
    args = ap.parse_args()

    out_dir = args.data_dir / "backfill" / "kworb"
    out_dir.mkdir(parents=True, exist_ok=True)
    charts = backfill_charts(out_dir)
    backfill_listeners(out_dir)
    backfill_track_histories(out_dir, charts, args.max_age_hours)
    print("kworb backfill done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
