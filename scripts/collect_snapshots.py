#!/usr/bin/env python3
"""Forward daily snapshot collector. Run from cron (see install_cron.sh).

Each run appends to data/snapshots/:
- charts.csv     — kworb global/us daily+weekly chart rows (top 200 each)
- listeners.csv  — kworb exact monthly-listener counts (top ~2000 artists)
- raw/YYYYMMDD/  — gzipped raw HTML of every page fetched (as-published
                   evidence; barrier markets settle on "at any point", so an
                   observation we didn't archive is one we can't prove)

Both CSVs carry fetched_at (UTC, this machine) alongside any source-implied
date — never substitute one for the other.

Usage:
    python3 scripts/collect_snapshots.py [--data-dir data]
"""

import argparse
import csv
import gzip
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from kworb_lib import KWORB, get, parse_chart_page, parse_listeners_page

CHART_PAGES = [("global", "daily"), ("us", "daily"),
               ("global", "weekly"), ("us", "weekly")]
LISTENER_PAGES = ["listeners.html", "listeners2.html"]

CHART_FIELDS = ["fetched_at", "country", "cadence", "chart_date", "pos",
                "artist_title", "artist_id", "track_id", "periods_on_chart",
                "peak", "streams", "streams_delta", "streams_7day", "total"]
LISTENER_FIELDS = ["fetched_at", "rank", "artist", "artist_id", "listeners",
                   "daily_delta", "peak_rank", "peak_listeners"]


def append_rows(path: Path, fields: list[str], rows: list[dict]) -> None:
    new = not path.exists()
    with path.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if new:
            w.writeheader()
        w.writerows(rows)


def archive_raw(raw_dir: Path, name: str, html: str) -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%H%M%S")
    (raw_dir / f"{name}.{ts}.html.gz").write_bytes(gzip.compress(html.encode("utf-8")))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-dir", default="data", type=Path)
    args = ap.parse_args()

    out_dir = args.data_dir / "snapshots"
    out_dir.mkdir(parents=True, exist_ok=True)
    fetched_at = datetime.now(timezone.utc).isoformat()
    raw_dir = out_dir / "raw" / datetime.now(timezone.utc).strftime("%Y%m%d")

    n_charts = 0
    for country, cadence in CHART_PAGES:
        html = get(f"{KWORB}/spotify/country/{country}_{cadence}.html")
        if not html:
            continue
        archive_raw(raw_dir, f"{country}_{cadence}", html)
        rows = parse_chart_page(html, cadence)
        for r in rows:
            r.update(fetched_at=fetched_at, country=country, cadence=cadence)
        append_rows(out_dir / "charts.csv", CHART_FIELDS, rows)
        n_charts += len(rows)

    n_listeners = 0
    for page in LISTENER_PAGES:
        html = get(f"{KWORB}/spotify/{page}")
        if not html:
            continue
        archive_raw(raw_dir, page.replace(".html", ""), html)
        rows = parse_listeners_page(html)
        for r in rows:
            r["fetched_at"] = fetched_at
        append_rows(out_dir / "listeners.csv", LISTENER_FIELDS, rows)
        n_listeners += len(rows)

    print(f"{fetched_at}: {n_charts} chart rows, {n_listeners} listener rows")
    if n_charts == 0 or n_listeners == 0:
        print("WARN: a source returned nothing", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
