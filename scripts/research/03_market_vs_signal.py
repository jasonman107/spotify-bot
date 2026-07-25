#!/usr/bin/env python3
"""Step 3: quantify the edge — daily-chart signal vs market price, per day.

Uses kworb per-track DAILY history (trailing ~35 days; Global + US) to
reconstruct each chart week's cumulative-stream race day by day, then joins
the eventual winner's CLOB YES price to ask, for each day of the week:

- share of the winner's final weekly streams already banked ("determinism")
- the winner's cumulative lead over the runner-up, in units of the
  runner-up's best single day x days remaining ("insurmountability", >1 =
  mathematically-ish decided)
- what the market was charging for the winner at that day's close

If insurmountability crosses 1 while price is still well below 1, the gap is
the gross edge per week. Prints one row per (event, day).

Usage: python3 scripts/research/03_market_vs_signal.py [--data-dir data]
"""

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

WEEKLY_SERIES = {"1-spotify-song": "Global", "top-us-spotify-song": "US"}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-dir", default="data", type=Path)
    args = ap.parse_args()
    bf = args.data_dir / "backfill"

    ev = pd.read_parquet(bf / "events.parquet")
    mk = pd.read_parquet(bf / "markets.parquet")
    daily = pd.read_parquet(bf / "kworb" / "track_daily_history.parquet")
    daily["date"] = pd.to_datetime(daily["date"])

    out_rows = []
    weekly_events = ev[ev.closed & ev.series_slug.isin(WEEKLY_SERIES)]
    for _, e in weekly_events.iterrows():
        country = WEEKLY_SERIES[e.series_slug]
        week_end = (datetime.fromisoformat(e.event_end.replace("Z", "+00:00"))
                    - timedelta(days=1))
        week_start = week_end - timedelta(days=6)
        wk = daily[(daily.country == country) &
                   (daily.date >= pd.Timestamp(week_start.date())) &
                   (daily.date <= pd.Timestamp(week_end.date()))]
        if wk.date.nunique() < 7:
            continue  # outside the ~35-day daily-history window
        # daily tables only exist for tracks on CURRENT charts, so older weeks
        # have partial panels; require near-full top-200 coverage every day
        if wk.groupby("date").track_id.nunique().min() < 50:
            continue

        winners = mk[(mk.event_slug == e.event_slug) &
                     (mk.resolved_yes_price == "1")]
        if len(winners) != 1:
            continue
        winner_bucket = winners.iloc[0].bucket

        pf = bf / "prices" / f"event_slug={e.event_slug}" / "prices.parquet"
        prices = pd.read_parquet(pf) if pf.exists() else pd.DataFrame()
        wp = prices[prices.bucket == winner_bucket].copy() if not prices.empty else pd.DataFrame()
        if not wp.empty:
            wp["ts"] = pd.to_datetime(wp.timestamp_s, unit="s", utc=True)

        # cumulative race, day by day
        panel = (wk.groupby(["track_id", "artist_title", "date"])["streams"]
                   .sum().unstack(fill_value=0).cumsum(axis=1))
        final = panel.iloc[:, -1].sort_values(ascending=False)
        true_top_title = final.index[0][1]
        for day_i, day in enumerate(sorted(panel.columns)):
            snap = panel[day].sort_values(ascending=False)
            leader_title = snap.index[0][1]
            lead = snap.iloc[0] - (snap.iloc[1] if len(snap) > 1 else 0)
            runner_title = snap.index[1][1] if len(snap) > 1 else ""
            runner_best_day = (wk[wk.artist_title == runner_title]
                               .groupby("date").streams.sum().max() or 0)
            days_left = 6 - day_i
            insurm = (lead / (runner_best_day * days_left)
                      if runner_best_day * days_left > 0 else float("inf"))
            price = None
            if not wp.empty:
                upto = wp[wp.ts <= pd.Timestamp(day, tz=timezone.utc) + timedelta(days=1)]
                if not upto.empty:
                    price = upto.sort_values("ts").price.iloc[-1]
            out_rows.append({
                "event": e.event_slug[:44], "country": country,
                "day_i": day_i, "date": day.date().isoformat(),
                "leader": leader_title[:34],
                "leader_is_winner": leader_title == true_top_title,
                "insurmountability": round(insurm, 2),
                "winner_close": price,
            })

    df = pd.DataFrame(out_rows)
    if df.empty:
        print("no weeks with full daily coverage + resolution; "
              "wait for more track_daily history")
        return 0
    pd.set_option("display.width", 200)
    for event, grp in df.groupby("event"):
        print(f"\n{event} [{grp.country.iloc[0]}]")
        print(grp[["day_i", "date", "leader", "leader_is_winner",
                   "insurmountability", "winner_close"]].to_string(index=False))

    dec = df[(df.insurmountability >= 1.0) & df.leader_is_winner]
    if not dec.empty:
        first_dec = dec.groupby("event").first()
        print("\n=== summary: first day the race was decided (insurm >= 1) ===")
        print(first_dec[["day_i", "date", "winner_close"]].to_string())
        priced = first_dec.dropna(subset=["winner_close"])
        if not priced.empty:
            print(f"\nmean winner price when decided: {priced.winner_close.mean():.3f} "
                  f"(gross edge {1 - priced.winner_close.mean():.3f} per $1, "
                  f"{len(priced)} events)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
