#!/usr/bin/env python3
"""Step 2: EDA on the weekly #1 race — is there a tradeable edge window?

Two questions:
1. From Wayback daily charts: for each chart week (Fri..Thu), accumulate daily
   streams per track. On which day does the eventual weekly #1 first take an
   insurmountable cumulative lead? (If it's decided by Sunday, the last 4 days
   of market trading are pure convergence — the elon-bot regime.)
2. From CLOB price history: for resolved weekly events, what YES price was the
   eventual winner trading at, per day of the chart week? Slow convergence
   after the outcome is statistically decided = the edge.

Usage: python3 scripts/research/02_eda_weekly_race.py [--data-dir data]
"""

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

WEEKLY_SERIES = {"1-spotify-song": "global", "top-us-spotify-song": "us"}


def chart_week_of(d: pd.Timestamp) -> pd.Timestamp:
    """Return the Thursday week_end of the chart week containing day d.
    Chart weeks run Friday..Thursday."""
    # weekday(): Mon=0..Sun=6; Thursday=3
    offset = (3 - d.weekday()) % 7
    return d + timedelta(days=offset)


def q1_decided_day(bf: Path) -> None:
    daily = pd.read_parquet(bf / "wayback" / "daily_charts.parquet")
    daily["chart_date"] = pd.to_datetime(daily["chart_date"])
    daily["week_end"] = daily["chart_date"].map(chart_week_of)
    weekly = pd.read_parquet(bf / "kworb" / "track_weekly_history.parquet")
    weekly["week_end"] = pd.to_datetime(weekly["week_end"])

    print("=== Q1: day the weekly #1 was decided (cumulative-lead analysis) ===")
    for country in ("global", "us"):
        cc = "Global" if country == "global" else "US"
        w1 = weekly[(weekly.country == cc) & (weekly.pos == 1)]
        rows = []
        for week_end, grp in daily[daily.country == country].groupby("week_end"):
            days_have = grp.chart_date.nunique()
            if days_have < 5:
                continue  # too many wayback gaps to call it
            top = w1[w1.week_end == week_end]
            if top.empty:
                continue
            # winner's kworb track_id via the weekly panel join on title
            cum = (grp.groupby(["track_id", "artist_title", "chart_date"])["streams"]
                      .sum().groupby(level=[0, 1]).cumsum().reset_index(name="cum"))
            week_start = week_end - timedelta(days=6)
            decided = None
            for day_i in range(7):
                d = week_start + timedelta(days=day_i)
                snap = cum[cum.chart_date == d]
                if snap.empty:
                    continue
                snap = snap.sort_values("cum", ascending=False)
                leader = snap.iloc[0]
                lead = leader.cum - (snap.iloc[1].cum if len(snap) > 1 else 0)
                remaining_days = 6 - day_i
                # insurmountable if lead > remaining days * runner-up's best day
                runner_best = (grp[grp.artist_title == snap.iloc[1].artist_title]
                               .streams.max() if len(snap) > 1 else 0)
                if remaining_days * runner_best < lead:
                    decided = day_i
                    break
            rows.append({"week_end": week_end.date(), "days_covered": days_have,
                         "decided_day": decided})
        df = pd.DataFrame(rows)
        if df.empty:
            print(f"{country}: no overlapping weeks yet")
            continue
        print(f"\n{country}: {len(df)} weeks with >=5 wayback days")
        print(df.decided_day.value_counts(dropna=False).sort_index()
              .rename_axis("decided_on_day (0=Fri..6=Thu, NaN=never)").to_string())


def q2_winner_price_path(bf: Path) -> None:
    ev = pd.read_parquet(bf / "events.parquet")
    mk = pd.read_parquet(bf / "markets.parquet")
    print("\n=== Q2: eventual winner's YES price by day of chart week ===")
    rows = []
    for _, e in ev[ev.closed & ev.series_slug.isin(WEEKLY_SERIES)].iterrows():
        pf = bf / "prices" / f"event_slug={e.event_slug}" / "prices.parquet"
        if not pf.exists():
            continue
        buckets = mk[mk.event_slug == e.event_slug]
        winners = buckets[buckets.resolved_yes_price == "1"]
        if len(winners) != 1:
            continue
        prices = pd.read_parquet(pf)
        wp = prices[prices.bucket == winners.iloc[0].bucket].copy()
        if wp.empty:
            continue
        wp["ts"] = pd.to_datetime(wp.timestamp_s, unit="s", utc=True)
        week_end = datetime.fromisoformat(
            e.event_end.replace("Z", "+00:00")) - timedelta(days=1)
        week_start = week_end - timedelta(days=6)
        wp["day_i"] = (wp.ts.dt.normalize() -
                       pd.Timestamp(week_start.date(), tz=timezone.utc)).dt.days
        for day_i, grp in wp.groupby("day_i"):
            rows.append({"event": e.event_slug[:40], "day_i": day_i,
                         "close": grp.sort_values("ts").price.iloc[-1]})
    df = pd.DataFrame(rows)
    if df.empty:
        print("no priced resolved weekly events yet")
        return
    piv = df.pivot_table(index="event", columns="day_i", values="close")
    piv.columns = [f"d{c}" for c in piv.columns]
    print("winner YES close by day (0=Fri week start .. 6=Thu week end):")
    print(piv.round(3).to_string())
    in_week = df[(df.day_i >= 0) & (df.day_i <= 6)]
    print("\nmean winner close by day:",
          in_week.groupby("day_i").close.mean().round(3).to_dict())


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-dir", default="data", type=Path)
    args = ap.parse_args()
    bf = args.data_dir / "backfill"
    if (bf / "wayback" / "daily_charts.parquet").exists():
        q1_decided_day(bf)
    else:
        print("wayback daily charts not built yet; skipping Q1")
    q2_winner_price_path(bf)
    return 0


if __name__ == "__main__":
    sys.exit(main())
