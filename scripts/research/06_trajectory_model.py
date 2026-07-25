#!/usr/bin/env python3
"""Step 6: does trajectory improve the forecast? (quality before PnL)

Compares three leader-win probability models, all leave-one-week-out:
  M0  empirical P(win | day_i, cume-margin bin)        — the step-4 baseline
  M1  empirical P(win | day_i, PROJECTED-margin bin)   — trajectory via bins
  M2  logistic on [day_i, log margin, log proj_margin, leader_drift,
      runner_drift, proj_top_is_leader]                — smooth combination

Metrics: Brier score and log-loss over all observations (lower = better),
plus a reliability table and the decay-trap case studies from step 5's
losing trades. Saves observations + LOWO probabilities for step 5 to trade.

Usage: python3 scripts/research/06_trajectory_model.py [--data-dir data]
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from weekly_lib import build_observations, fit_logistic, predict_logistic

MBINS = [0, 1.05, 1.1, 1.2, 1.5, 2.0, np.inf]
CASE_STUDIES = [("global", "2026-02-12"), ("global", "2026-03-05"),
                ("us", "2026-03-26")]


def bin_probs_lowo(df, feature, label="leader_wins", min_n=8):
    """LOWO empirical probability per (day_i, feature-bin) cell, computed by
    removing the observation's own week from the cell counts."""
    df = df.copy()
    df["bin"] = pd.cut(df[feature], MBINS)
    out = np.empty(len(df))
    cell = df.groupby(["day_i", "bin"], observed=True)[label].agg(["sum", "size"])
    day = df.groupby("day_i")[label].agg(["sum", "size"])
    wk_cell = df.groupby(["country", "week_end", "day_i", "bin"],
                         observed=True)[label].agg(["sum", "size"])
    wk_day = df.groupby(["country", "week_end", "day_i"])[label].agg(["sum", "size"])
    for i, (_, r) in enumerate(df.iterrows()):
        ck, wk = (r.day_i, r["bin"]), (r.country, r.week_end, r.day_i, r["bin"])
        cs, cn = cell.loc[ck, "sum"], cell.loc[ck, "size"]
        ws, wn = (wk_cell.loc[wk] if wk in wk_cell.index else (0, 0))
        s, n = cs - ws, cn - wn
        if n < min_n:
            dk, dwk = r.day_i, (r.country, r.week_end, r.day_i)
            ds, dn = day.loc[dk, "sum"], day.loc[dk, "size"]
            dws, dwn = (wk_day.loc[dwk] if dwk in wk_day.index else (0, 0))
            s, n = ds - dws, dn - dwn
        out[i] = (s + 0.5) / (n + 1.0)  # light smoothing, avoids 0/1
    return out


def logistic_lowo(df):
    """LOWO logistic regression (weekly_lib implementation); refit once per
    held-out week."""
    weeks = (df.country + "|" + df.week_end).to_numpy()
    out = np.empty(len(df))
    for wk in np.unique(weeks):
        tr, te = weeks != wk, weeks == wk
        w = fit_logistic(df[tr])
        out[te] = predict_logistic(w, df[te])
    return out


def scores(y, p):
    return ((p - y) ** 2).mean(), \
        -(y * np.log(p + 1e-12) + (1 - y) * np.log(1 - p + 1e-12)).mean()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-dir", default="data", type=Path)
    args = ap.parse_args()
    bf = args.data_dir / "backfill"

    df = build_observations(bf)
    n_weeks = df.groupby(["country", "week_end"]).ngroups
    print(f"{len(df)} observations across {n_weeks} country-weeks")

    df["p_m0"] = bin_probs_lowo(df, "margin")
    df["p_m1"] = bin_probs_lowo(df, "proj_margin")
    df["p_m2"] = logistic_lowo(df)
    y = df.leader_wins.to_numpy(float)

    print("\n=== LOWO forecast quality (lower = better) ===")
    print(f"{'model':<28}{'Brier':>8}{'log-loss':>10}")
    for name, col in [("M0 day x margin", "p_m0"),
                      ("M1 day x projected margin", "p_m1"),
                      ("M2 logistic trajectory", "p_m2")]:
        b, ll = scores(y, df[col].to_numpy())
        print(f"{name:<28}{b:>8.4f}{ll:>10.4f}")

    print("\n=== reliability (M2): predicted decile vs realized ===")
    df["dec"] = pd.qcut(df.p_m2, 8, duplicates="drop")
    print(df.groupby("dec", observed=True)
            .agg(n=("leader_wins", "size"), pred=("p_m2", "mean"),
                 real=("leader_wins", "mean")).round(3).to_string())

    print("\n=== decay-trap case studies (step-5 losing weeks) ===")
    cols = ["day_i", "leader", "margin", "proj_margin", "leader_drift",
            "proj_top_is_leader", "p_m0", "p_m2", "leader_wins"]
    for country, week_end in CASE_STUDIES:
        cs = df[(df.country == country) & (df.week_end == week_end)]
        if cs.empty:
            continue
        print(f"\n{country} week ending {week_end}:")
        print(cs[cols].round(3).to_string(index=False))

    out = bf / "trajectory_obs.parquet"
    df.drop(columns=["bin", "dec"], errors="ignore").to_parquet(out, index=False)
    print(f"\nsaved -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
