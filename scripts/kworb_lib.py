"""Fetch + parse helpers for kworb.net Spotify pages.

Page formats (verified 2026-07-25):

- Chart pages `spotify/country/{cc}_{daily,weekly}.html` — one table, latest
  chart only (no dated archives on kworb; history comes from per-track pages
  or Wayback). Columns:
    daily:  Pos, P+, Artist and Title, Days, Pk, (x?), Streams, Streams+, 7Day, 7Day+, Total
    weekly: Pos, P+, Artist and Title, Wks, Pk, (x?), Streams, Streams+, Total
  The title cell links `../artist/{artist_id}.html` and `../track/{track_id}.html`.
  The chart date appears in the page as YYYY/MM/DD.

- `spotify/listeners.html` (+ listeners2.html) — top artists by monthly
  listeners: #, Artist, Listeners, Daily +/-, Peak, PkListeners. Exact counts.

- `spotify/track/{track_id}.html` — TWO tables, each with its own header row
  of country codes (Date, Global, US, CA, ...) and "pos (streams)" cells,
  first rows being Total/Peak aggregates:
    table 0: weekly history (rows dated by the Thursday week-END, matching
             Spotify's Fri-Thu chart week), full lifetime
    table 1: daily history, roughly the trailing ~35 days
  Column sets differ per track (only countries where it charted).
"""

import json
import re
import subprocess
import sys
import time
from html import unescape

KWORB = "https://kworb.net"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")


def get(url: str, retries: int = 3, timeout: int = 45) -> str | None:
    """GET via curl (system Python lacks CA certs for urllib on this machine).

    Decodes leniently: old kworb pages and Wayback captures are not always
    valid UTF-8, so we never let a stray byte kill a long backfill.
    """
    for attempt in range(retries):
        out = subprocess.run(
            ["curl", "-s", "--compressed", "-m", str(timeout), "-A", UA, url],
            capture_output=True,
        )
        if out.returncode == 0 and out.stdout:
            return out.stdout.decode("utf-8", "replace")
        time.sleep(2 * (attempt + 1))
    print(f"  WARN: failed to fetch {url}", file=sys.stderr)
    return None


def get_json(url: str, retries: int = 3, timeout: int = 45):
    body = get(url, retries=retries, timeout=timeout)
    if body is None:
        return None
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        print(f"  WARN: non-JSON from {url}: {body[:120]!r}", file=sys.stderr)
        return None


def _strip(cell_html: str) -> str:
    return unescape(re.sub(r"<[^>]+>", "", cell_html)).strip()


def _num(s: str) -> int | None:
    s = s.replace(",", "").replace("+", "").strip()
    if not s or s in ("--", "-", "="):
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _rows(html: str) -> list[str]:
    return re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S)


def _cells(row_html: str) -> list[str]:
    return re.findall(r"<td[^>]*>(.*?)</td>", row_html, re.S)


def page_chart_date(html: str) -> str | None:
    """Chart date shown on kworb chart pages, as YYYY-MM-DD."""
    m = re.search(r"(\d{4})/(\d{2})/(\d{2})", html)
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else None


def parse_chart_page(html: str, cadence: str) -> list[dict]:
    """Parse a country daily/weekly chart page into row dicts.

    cadence: "daily" | "weekly" (daily has two extra 7Day columns).
    """
    chart_date = page_chart_date(html)
    out = []
    for row in _rows(html):
        cells = _cells(row)
        min_cols = 11 if cadence == "daily" else 9
        if len(cells) < min_cols:
            continue
        title_html = cells[2]
        artist_m = re.search(r"artist/([A-Za-z0-9]+)\.html", title_html)
        track_m = re.search(r"track/([A-Za-z0-9]+)\.html", title_html)
        rec = {
            "chart_date": chart_date,
            "pos": _num(_strip(cells[0])),
            "artist_title": _strip(title_html),
            "artist_id": artist_m.group(1) if artist_m else "",
            "track_id": track_m.group(1) if track_m else "",
            "periods_on_chart": _num(_strip(cells[3])),
            "peak": _num(_strip(cells[4])),
            "streams": _num(_strip(cells[6])),
            "streams_delta": _num(_strip(cells[7])),
        }
        if cadence == "daily":
            rec["streams_7day"] = _num(_strip(cells[8]))
            rec["total"] = _num(_strip(cells[10]))
        else:
            rec["streams_7day"] = None
            rec["total"] = _num(_strip(cells[8]))
        if rec["pos"] is None:
            continue
        out.append(rec)
    return out


def parse_listeners_page(html: str) -> list[dict]:
    """Parse listeners.html / listeners2.html: exact monthly-listener counts."""
    out = []
    for row in _rows(html):
        cells = _cells(row)
        if len(cells) < 6:
            continue
        artist_m = re.search(r"artist/([A-Za-z0-9]+)", cells[1])
        rec = {
            "rank": _num(_strip(cells[0])),
            "artist": _strip(cells[1]),
            "artist_id": artist_m.group(1) if artist_m else "",
            "listeners": _num(_strip(cells[2])),
            "daily_delta": _num(_strip(cells[3])),
            "peak_rank": _num(_strip(cells[4])),
            "peak_listeners": _num(_strip(cells[5])),
        }
        if rec["rank"] is None or rec["listeners"] is None:
            continue
        out.append(rec)
    return out


def parse_track_page(html: str) -> dict[str, list[dict]]:
    """Parse a per-track history page into {"weekly": [...], "daily": [...]}.

    Each row dict is {date, country, pos, streams}; Total/Peak aggregate rows
    are skipped. Tables are classified weekly vs daily by their median gap
    between consecutive dates (7 days vs 1), since page order is unlabeled.
    """
    from datetime import date as _date

    out: dict[str, list[dict]] = {"weekly": [], "daily": []}
    for chunk in re.split(r"<table[^>]*>", html)[1:]:
        chunk = chunk.split("</table>")[0]
        headers = [_strip(h) for h in re.findall(r"<th[^>]*>(.*?)</th>", chunk)]
        if not headers or headers[0] != "Date":
            continue
        countries = headers[1:]
        rows_out, dates = [], []
        for row in _rows(chunk):
            cells = _cells(row)
            if len(cells) < 2:
                continue
            m = re.match(r"(\d{4})/(\d{2})/(\d{2})", _strip(cells[0]))
            if not m:
                continue  # Total / Peak rows
            d = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
            dates.append(_date(int(m.group(1)), int(m.group(2)), int(m.group(3))))
            for country, cell in zip(countries, cells[1:]):
                pm = re.match(r"(\d+)\s*\(([\d,]+)\)", _strip(cell))
                if not pm:
                    continue
                rows_out.append({
                    "date": d,
                    "country": country,
                    "pos": int(pm.group(1)),
                    "streams": int(pm.group(2).replace(",", "")),
                })
        if len(dates) < 2:
            continue
        gaps = sorted((b - a).days for a, b in zip(dates, dates[1:]))
        cadence = "weekly" if gaps[len(gaps) // 2] >= 7 else "daily"
        out[cadence].extend(rows_out)
    return out
