#!/usr/bin/env python3
"""Step 4: empirical base rates — P(weekly #1 | leading the race on day k).

Wayback daily charts have gaps (~54% of days), so full-week cumulative sums
are rare. Instead, for every (country, chart week) whose weekly winner we
know, and every OBSERVED day k of that week, compute the leader by cumulative
streams over the days observed so far and ask whether that leader went on to
win the week. Margin (leader cume / runner-up cume) and observed-day count are
kept as covariates; gaps add noise but not directional bias to leader identity.

Output: hit-rate tables by day-of-week and by margin — the raw material for
the pricing model. Compare these to the winner-price-by-day table from step 2:
edge = base rate minus market price at the same point.

Usage: python3 scripts/research/04_leader_calibration.py [--data-dir data]
"""

import argparse
import sys
from datetime import timedelta
from pathlib import Path

import pandas as pd


def chart_week_of(d: pd.Timestamp) -> pd.Timestamp:
    """Thursday week_end of the Fri..Thu chart week containing day d."""
    return d + timedelta(days=(3 - d.weekday()) % 7)


def winners_by_week(bf: Path) -> pd.DataFrame:
    """(country[global|us], week_end) -> winning artist_title, from per-track
    weekly pages plus Wayback weekly chart captures."""
    frames = []
    hist = pd.read_parquet(bf / "kworb" / "track_weekly_history.parquet")
    h1 = hist[hist.pos == 1][["week_end", "country", "artist_title"]].copy()
    h1["country"] = h1.country.map({"Global": "global", "US": "us"})
    frames.append(h1.dropna(subset=["country"]))
    wb = bf / "wayback" / "weekly_charts.parquet"
    if wb.exists():
        w = pd.read_parquet(wb)
        w1 = w[w.pos == 1].rename(columns={"chart_date": "week_end"})
        frames.append(w1[["week_end", "country", "artist_title"]])
    df = pd.concat(frames, ignore_index=True)
    df["week_end"] = pd.to_datetime(df.week_end)
    return df.drop_duplicates(subset=["week_end", "country"])


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-dir", default="data", type=Path)
    ap.add_argument("--min-days", default=1, type=int,
                    help="min observed days before scoring a leader")
    args = ap.parse_args()
    bf = args.data_dir / "backfill"

    daily = pd.read_parquet(bf / "wayback" / "daily_charts.parquet")
    daily["chart_date"] = pd.to_datetime(daily.chart_date)
    daily["week_end"] = daily.chart_date.map(chart_week_of)
    winners = winners_by_week(bf)

    obs = []
    for (country, week_end), grp in daily.groupby(["country", "week_end"]):
        w = winners[(winners.country == country) & (winners.week_end == week_end)]
        if w.empty:
            continue
        winner_title = w.iloc[0].artist_title
        days = sorted(grp.chart_date.unique())
        cume = None
        for day in days:
            day_df = grp[grp.chart_date == day].groupby("artist_title").streams.sum()
            cume = day_df if cume is None else cume.add(day_df, fill_value=0)
            n_obs = days.index(day) + 1
            if n_obs < args.min_days or len(cume) < 2:
                continue
            top2 = cume.sort_values(ascending=False).iloc[:2]
            day_i = 6 - (week_end - day).days
            obs.append({
                "country": country,
                "week_end": week_end.date().isoformat(),
                "day_i": day_i,
                "n_obs_days": n_obs,
                "leader": top2.index[0],
                "margin": top2.iloc[0] / max(top2.iloc[1], 1),
                "leader_wins": top2.index[0] == winner_title,
                # same-day-only variant: who won just this day
                "day_top": day_df.idxmax(),
                "day_top_wins": day_df.idxmax() == winner_title,
            })

    df = pd.DataFrame(obs)
    if df.empty:
        print("no overlapping (daily, weekly-outcome) weeks")
        return 1
    df.to_parquet(bf / "leader_calibration_obs.parquet", index=False)
    n_weeks = df.groupby(["country", "week_end"]).ngroups
    print(f"{len(df)} (week,day) observations across {n_weeks} country-weeks\n")

    print("=== P(cumulative leader on day k wins the week) ===")
    t = (df.groupby("day_i").agg(n=("leader_wins", "size"),
                                 p_win=("leader_wins", "mean")).round(3))
    print(t.to_string())

    print("\n=== same, but only days with >= 3 observed days of cume ===")
    t = (df[df.n_obs_days >= 3].groupby("day_i")
           .agg(n=("leader_wins", "size"), p_win=("leader_wins", "mean")).round(3))
    print(t.to_string())

    print("\n=== P(leader wins) by margin bucket (all days) ===")
    df["margin_bin"] = pd.cut(df.margin, [1, 1.05, 1.1, 1.2, 1.5, 2, 100])
    t = (df.groupby("margin_bin", observed=True)
           .agg(n=("leader_wins", "size"), p_win=("leader_wins", "mean")).round(3))
    print(t.to_string())

    print("\n=== P(single-day #1 wins the week) by day ===")
    t = (df.groupby("day_i").agg(n=("day_top_wins", "size"),
                                 p_win=("day_top_wins", "mean")).round(3))
    print(t.to_string())

    print("\n=== margin x day (n>=8 cells) ===")
    piv = df.pivot_table(index="margin_bin", columns="day_i",
                         values="leader_wins", aggfunc=["mean", "size"],
                         observed=True)
    mean, size = piv["mean"].round(2), piv["size"]
    print(mean.where(size >= 8).to_string())
    return 0


if __name__ == "__main__":
    sys.exit(main())
