#!/usr/bin/env python3
"""Backfill historical kworb pages from the Wayback Machine.

kworb keeps no dated archives, but web.archive.org snapshots its pages
near-daily. This reconstructs:

- daily chart history (global + US): wayback/daily_charts.parquet
  (one row per (chart_date, pos); deduped to the latest snapshot per chart_date)
- weekly chart history (global + US): wayback/weekly_charts.parquet
  (complements per-track pages, which only cover currently-charting tracks)
- monthly-listener history: wayback/listeners_history.parquet
  (one row per (snapshot_date, artist))

Raw snapshot HTML is cached under data/backfill/wayback/html/ so reruns only
fetch new snapshots. Wayback is slow and rate-limited: full first run takes
~30-60 min; run it in the background.

Usage:
    python3 scripts/backfill_wayback.py [--data-dir data] [--from 20240101]
        [--pages us_daily global_daily listeners ...] [--limit N]
"""

import argparse
import gzip
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from kworb_lib import get, get_json, parse_chart_page, parse_listeners_page

CDX = "http://web.archive.org/cdx/search/cdx"
PAGES = {
    # key: (original URL, parser kind, cdx collapse)
    "global_daily": ("https://kworb.net/spotify/country/global_daily.html", "chart_daily", "timestamp:8"),
    "us_daily": ("https://kworb.net/spotify/country/us_daily.html", "chart_daily", "timestamp:8"),
    "global_weekly": ("https://kworb.net/spotify/country/global_weekly.html", "chart_weekly", "digest"),
    "us_weekly": ("https://kworb.net/spotify/country/us_weekly.html", "chart_weekly", "digest"),
    "listeners": ("https://kworb.net/spotify/listeners.html", "listeners", "timestamp:8"),
}


def list_snapshots(original: str, collapse: str, since: str) -> list[str]:
    body = get_json(
        f"{CDX}?url={original}&output=json&filter=statuscode:200"
        f"&from={since}&collapse={collapse}", timeout=90
    )
    if not body or len(body) < 2:
        return []
    cols = body[0]
    ts_i = cols.index("timestamp")
    return [row[ts_i] for row in body[1:]]


def fetch_snapshot(cache_dir: Path, original: str, ts: str) -> str | None:
    path = cache_dir / f"{ts}.html.gz"
    if path.exists():
        return gzip.decompress(path.read_bytes()).decode("utf-8", "replace")
    # id_ suffix returns the original page bytes without wayback chrome
    html = get(f"https://web.archive.org/web/{ts}id_/{original}", timeout=90)
    if html is None:
        return None
    path.write_bytes(gzip.compress(html.encode("utf-8")))
    time.sleep(1.0)
    return html


def run_page(key: str, out_dir: Path, since: str, limit: int | None) -> pd.DataFrame:
    original, kind, collapse = PAGES[key]
    cache_dir = out_dir / "html" / key
    cache_dir.mkdir(parents=True, exist_ok=True)
    stamps = list_snapshots(original, collapse, since)
    if limit:
        stamps = stamps[-limit:]
    cached = sum((cache_dir / f"{ts}.html.gz").exists() for ts in stamps)
    print(f"{key}: {len(stamps)} snapshots ({cached} cached)")
    frames = []
    for i, ts in enumerate(stamps):
        html = fetch_snapshot(cache_dir, original, ts)
        if html is None:
            continue
        if kind == "listeners":
            rows = parse_listeners_page(html)
            df = pd.DataFrame(rows)
            if df.empty:
                continue
            df["snapshot_date"] = f"{ts[:4]}-{ts[4:6]}-{ts[6:8]}"
        else:
            cadence = "daily" if kind == "chart_daily" else "weekly"
            rows = parse_chart_page(html, cadence)
            df = pd.DataFrame(rows)
            if df.empty:
                continue
            df["country"] = key.split("_")[0]
        df["snapshot_ts"] = ts
        frames.append(df)
        if (i + 1) % 25 == 0:
            print(f"  {key}: {i+1}/{len(stamps)}")
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if kind == "listeners":
        df = (df.sort_values("snapshot_ts")
                .drop_duplicates(subset=["snapshot_date", "artist_id"], keep="last"))
    else:
        # multiple snapshots can capture the same chart date; keep the latest
        df = (df.sort_values("snapshot_ts")
                .drop_duplicates(subset=["chart_date", "pos"], keep="last"))
    return df.reset_index(drop=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-dir", default="data", type=Path)
    ap.add_argument("--from", dest="since", default="20240101")
    ap.add_argument("--pages", nargs="*", default=list(PAGES))
    ap.add_argument("--limit", type=int, default=None,
                    help="only the N most recent snapshots per page (for testing)")
    args = ap.parse_args()

    out_dir = args.data_dir / "backfill" / "wayback"
    out_dir.mkdir(parents=True, exist_ok=True)

    daily, weekly = [], []
    for key in args.pages:
        df = run_page(key, out_dir, args.since, args.limit)
        if df.empty:
            print(f"{key}: nothing parsed")
            continue
        if key == "listeners":
            df.to_parquet(out_dir / "listeners_history.parquet",
                          compression="zstd", index=False)
            print(f"listeners: {len(df)} rows, "
                  f"{df['snapshot_date'].nunique()} days -> listeners_history.parquet")
        elif key.endswith("_daily"):
            daily.append(df)
        else:
            weekly.append(df)
    for frames, name in ((daily, "daily_charts"), (weekly, "weekly_charts")):
        if frames:
            df = pd.concat(frames, ignore_index=True)
            df.to_parquet(out_dir / f"{name}.parquet", compression="zstd", index=False)
            print(f"{name}: {len(df)} rows, {df['chart_date'].nunique()} chart dates "
                  f"-> {name}.parquet")
    print("wayback backfill done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
