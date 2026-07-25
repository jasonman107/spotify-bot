#!/usr/bin/env python3
"""Step 5 (v2): proof-of-concept backtest on all priced weekly events.

Consumes step 6's trajectory_obs.parquet — one row per (country, week,
observed day) with the cumulative leader and leave-one-week-out win
probabilities (p_m0 margin bins, p_m2 trajectory logistic; --model picks).

Per tradeable observation (day_i < 6): find the leader's bucket by BEST
title-token overlap (unique argmax — a plain threshold once bought the wrong
Bad Bunny song), falling back to the 'Other' bucket when the leader isn't
listed (P(Other) >= P(unlisted leader wins), so the entry is conservative).
Price from CLOB prices-history or subgraph fills (fills staler than 36h are
discarded). Buy $1 notional when p_model - price > --edge, hold to
resolution. Still deliberately crude: closing prints, no spread/fees.

Usage: python3 scripts/research/05_backtest_lite.py [--data-dir data]
           [--edge 0.10] [--model p_m2]
"""

import argparse
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

WEEKLY_SERIES = {"1-spotify-song": "global", "top-us-spotify-song": "us"}


def norm(s: str) -> set[str]:
    s = re.sub(r"\(w/[^)]*\)", " ", s)
    s = re.sub(r"\(feat[^)]*\)", " ", s, flags=re.I)
    s = re.sub(r"[^a-z0-9 ]", " ", s.lower())
    return {t for t in s.split() if t not in {"the", "a", "with", "and"}}


def match_bucket(buckets: pd.DataFrame, leader: str):
    """(bucket_row, is_other). Unique best token overlap >= 2, else Other."""
    lt = norm(leader)
    scored = sorted(((len(norm(b.bucket) & lt), i)
                     for i, b in buckets.iterrows()), reverse=True)
    if scored and scored[0][0] >= 2 and \
            (len(scored) == 1 or scored[0][0] > scored[1][0]):
        return buckets.loc[scored[0][1]], False
    other = buckets[buckets.bucket.str.strip().str.lower() == "other"]
    if not other.empty:
        return other.iloc[0], True
    return None, False


def price_at(bf: Path, event_slug: str, bucket: str, cutoff) -> float | None:
    pf = bf / "prices" / f"event_slug={event_slug}" / "prices.parquet"
    if pf.exists():
        p = pd.read_parquet(pf)
        p = p[(p.bucket == bucket) & (p.timestamp_s <= cutoff.timestamp())]
        return None if p.empty else p.sort_values("timestamp_s").price.iloc[-1]
    ff = bf / "fills" / f"event_slug={event_slug}" / "fills.parquet"
    if ff.exists():
        f = pd.read_parquet(ff)
        f = f[(f.bucket == bucket) & (f.timestamp_s <= cutoff.timestamp()) &
              (f.timestamp_s > cutoff.timestamp() - 36 * 3600)]
        return None if f.empty else f.sort_values("timestamp_s").price_yes.iloc[-1]
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-dir", default="data", type=Path)
    ap.add_argument("--edge", default=0.10, type=float)
    ap.add_argument("--model", default="p_m2", choices=["p_m0", "p_m1", "p_m2"])
    args = ap.parse_args()
    bf = args.data_dir / "backfill"

    ev = pd.read_parquet(bf / "events.parquet")
    mk = pd.read_parquet(bf / "markets.parquet")
    obs = pd.read_parquet(bf / "trajectory_obs.parquet")

    trades = []
    for _, e in ev[ev.closed & ev.series_slug.isin(WEEKLY_SERIES)].iterrows():
        country = WEEKLY_SERIES[e.series_slug]
        buckets = mk[mk.event_slug == e.event_slug]
        if buckets[buckets.resolved_yes_price == "1"].empty:
            continue
        week_end = (datetime.fromisoformat(e.event_end.replace("Z", "+00:00"))
                    - timedelta(days=1))
        wk_obs = obs[(obs.country == country) &
                     (obs.week_end == week_end.date().isoformat()) &
                     (obs.day_i < 6)]
        for _, o in wk_obs.iterrows():
            p_model = float(o[args.model])
            b, is_other = match_bucket(buckets, o.leader)
            if b is None:
                continue
            day = pd.Timestamp(week_end.date(), tz=timezone.utc) \
                - timedelta(days=6 - int(o.day_i))
            price = price_at(bf, e.event_slug, b.bucket, day + timedelta(days=1))
            if price is None or not (0.02 <= price <= 0.95) \
                    or p_model - price <= args.edge:
                continue
            won = b.resolved_yes_price == "1"
            trades.append({
                "event": e.event_slug[:40], "day_i": int(o.day_i),
                "leader": o.leader[:28], "bucket": b.bucket[:24],
                "other": is_other, "p_model": round(p_model, 3),
                "price": round(float(price), 3), "won": won,
                "pnl": (1 - price) if won else -price,
            })

    df = pd.DataFrame(trades)
    if df.empty:
        print("no trades passed the gates")
        return 0
    pd.set_option("display.width", 190)
    print(df.to_string(index=False))
    print(f"\nmodel={args.model} edge>{args.edge}: "
          f"{len(df)} trades ({df.event.nunique()} events), "
          f"hit rate {df.won.mean():.2f}, "
          f"total PnL ${df.pnl.sum():.2f} on ${df.price.sum():.2f} at risk "
          f"(ROI {df.pnl.sum() / df.price.sum():+.1%})")
    print(f"mean model prob {df.p_model.mean():.3f} vs realized {df.won.mean():.3f}")
    ev_pnl = df.groupby("event").pnl.sum()
    print(f"per-event PnL: {ev_pnl.lt(0).sum()} losing / {len(ev_pnl)} events, "
          f"worst {ev_pnl.min():+.2f}, best {ev_pnl.max():+.2f}")
    print("\nby day_i:")
    print(df.groupby("day_i").agg(n=("won", "size"), hit=("won", "mean"),
                                  pnl=("pnl", "sum")).round(3).to_string())
    return 0


if __name__ == "__main__":
    sys.exit(main())
