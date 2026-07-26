"""Shared feature engineering for the weekly #1 race.

Builds one long daily panel (country, date, artist_title, streams) from the
two sources — kworb per-track daily tables (full top-200 coverage, trailing
~35 days) and Wayback captures of kworb daily chart pages (~54% of days,
back to 2023) — preferring kworb where both cover a (country, date).

Produces per-(country, week, day) observations for the CUMULATIVE LEADER with
trajectory features, computed only from days observed up to that point
(walk-forward safe):

- margin:       leader cume / runner-up cume
- proj_margin:  leader projected 7-day total / best rival's projected total,
                where each track's unobserved days are filled with its last
                deseasonalized daily rate carried forward with a clipped
                drift ratio (its last-vs-previous observed day) and the
                weekday factor restored. Rising songs project up, decaying
                songs project down — the feature margin-only models miss.
- leader_drift / runner_drift: those clipped drift ratios
- proj_top_is_leader: whether the projected winner is still the cume leader
- leader_wins:  label (did the cume leader win the week)
"""

import re
import sys
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd

DRIFT_CLIP = (0.70, 1.25)


# ── Leader-win logistic (single implementation for research AND trading) ────

def logistic_design(df: pd.DataFrame) -> np.ndarray:
    """Feature matrix for the leader-win logistic. Any change here changes
    the production model — rerun 06 and the backtest together."""
    return np.column_stack([
        df.day_i / 6.0,
        np.log(df.margin.clip(lower=1.0)),
        np.log(df.proj_margin.clip(1e-3)),
        df.leader_drift - 1.0,
        df.runner_drift - 1.0,
        df.proj_top_is_leader.astype(float),
        np.ones(len(df)),
    ])


def fit_logistic(df: pd.DataFrame, label: str = "leader_wins") -> np.ndarray:
    """Ridge-regularized logistic weights via scipy L-BFGS."""
    from scipy.optimize import minimize

    X, y = logistic_design(df), df[label].to_numpy(float)

    def nll(w):
        z = np.clip(X @ w, -30, 30)
        p = 1 / (1 + np.exp(-z))
        return -(y * np.log(p + 1e-12) + (1 - y) * np.log(1 - p + 1e-12)).sum() \
            + 0.5 * (w[:-1] ** 2).sum()

    return minimize(nll, np.zeros(X.shape[1]), method="L-BFGS-B").x


def predict_logistic(w: np.ndarray, df: pd.DataFrame) -> np.ndarray:
    z = np.clip(logistic_design(df) @ w, -30, 30)
    return 1 / (1 + np.exp(-z))


# ── Bucket matching (shared by backtest and trader) ─────────────────────────

def norm_tokens(s: str) -> set:
    """Token set for fuzzy title/artist matching across label formats.

    Unicode-aware: stripping to [a-z0-9] would reduce a fully non-Latin
    title (K-pop charts weekly) to an empty set, which best_bucket would
    silently mis-route to 'Other'."""
    s = re.sub(r"\(w/[^)]*\)", " ", s)
    s = re.sub(r"\(feat[^)]*\)", " ", s, flags=re.I)
    s = re.sub(r"[^\w ]", " ", s.lower(), flags=re.UNICODE)
    return {t for t in s.split() if t not in {"the", "a", "with", "and"}}


def best_bucket(labels: list, leader: str) -> str | None:
    """Unique best token-overlap label with >=2 shared tokens, else the
    'Other' label if present, else None. A plain >=2 threshold once matched
    the wrong Bad Bunny song — argmax with a uniqueness check is required."""
    lt = norm_tokens(leader)
    scored = sorted(((len(norm_tokens(lb) & lt), lb) for lb in labels),
                    key=lambda x: -x[0])
    if scored and scored[0][0] >= 2 and \
            (len(scored) == 1 or scored[0][0] > scored[1][0]):
        return scored[0][1]
    for lb in labels:
        if lb.strip().lower() == "other":
            return lb
    return None


def chart_week_of(d: pd.Timestamp) -> pd.Timestamp:
    """Thursday week_end of the Fri..Thu chart week containing day d."""
    return d + timedelta(days=(3 - d.weekday()) % 7)


def load_daily_panel(bf: Path) -> pd.DataFrame:
    """(country[global|us], date, artist_title, streams), kworb preferred."""
    kw = pd.read_parquet(bf / "kworb" / "track_daily_history.parquet")
    kw = kw[kw.country.isin(["Global", "US"])].copy()
    kw["country"] = kw.country.map({"Global": "global", "US": "us"})
    kw["date"] = pd.to_datetime(kw.date)
    kw = kw.groupby(["country", "date", "artist_title"], as_index=False).streams.sum()

    wb = pd.read_parquet(bf / "wayback" / "daily_charts.parquet")
    wb["date"] = pd.to_datetime(wb.chart_date)
    wb = wb.groupby(["country", "date", "artist_title"], as_index=False).streams.sum()

    # kworb wins where it has real coverage (>=50 tracks that day)
    cov = kw.groupby(["country", "date"]).artist_title.nunique()
    good = set(cov[cov >= 50].index)
    wb_keep = wb[~wb.set_index(["country", "date"]).index.isin(good)]
    kw_keep = kw[kw.set_index(["country", "date"]).index.isin(good)]
    return pd.concat([kw_keep, wb_keep], ignore_index=True)


def winners_by_week(bf: Path) -> pd.DataFrame:
    """(country, week_end) -> winning artist_title."""
    frames = []
    hist = pd.read_parquet(bf / "kworb" / "track_weekly_history.parquet")
    h1 = hist[hist.pos == 1][["week_end", "country", "artist_title"]].copy()
    h1["country"] = h1.country.map({"Global": "global", "US": "us"})
    frames.append(h1.dropna(subset=["country"]))
    wbp = bf / "wayback" / "weekly_charts.parquet"
    if wbp.exists():
        w = pd.read_parquet(wbp)
        w1 = w[w.pos == 1].rename(columns={"chart_date": "week_end"})
        frames.append(w1[["week_end", "country", "artist_title"]])
    df = pd.concat(frames, ignore_index=True)
    df["week_end"] = pd.to_datetime(df.week_end)
    return df.drop_duplicates(subset=["week_end", "country"])


def weekday_factors(panel: pd.DataFrame) -> dict:
    """(country, weekday) -> mean-1 multiplicative seasonality, from the
    top-50 tracks per day (stable across chart membership churn)."""
    top = (panel.sort_values("streams", ascending=False)
                .groupby(["country", "date"]).head(50))
    daily_mean = top.groupby(["country", "date"]).streams.mean().reset_index()
    daily_mean["weekday"] = daily_mean.date.dt.weekday
    out = {}
    for country, grp in daily_mean.groupby("country"):
        f = grp.groupby("weekday").streams.mean()
        f = f / f.mean()
        for wd, v in f.items():
            out[(country, wd)] = v
    return out


def _project_total(track_days: pd.Series, week_days: list, factors: dict,
                   country: str) -> float:
    """Projected 7-day total for one track. track_days: date -> streams
    (observed days only, within the week)."""
    obs_dates = sorted(track_days.index)
    deseas = {d: track_days[d] / factors.get((country, d.weekday()), 1.0)
              for d in obs_dates}
    last = obs_dates[-1]
    if len(obs_dates) >= 2:
        prev = obs_dates[-2]
        gap = (last - prev).days
        drift = float(np.clip((deseas[last] / max(deseas[prev], 1.0))
                              ** (1.0 / max(gap, 1)), *DRIFT_CLIP))
    else:
        drift = 1.0
    total = float(track_days.sum())
    for d in week_days:
        if d in track_days.index:
            continue
        k = (d - last).days
        rate = deseas[last] * (drift ** min(abs(k), 4))
        total += rate * factors.get((country, d.weekday()), 1.0)
    return total


def week_observations(panel: pd.DataFrame, winners: pd.DataFrame,
                      factors: dict) -> pd.DataFrame:
    """One row per (country, week, observed day): leader + features + label."""
    panel = panel.copy()
    panel["week_end"] = panel.date.map(chart_week_of)
    obs = []
    for (country, week_end), grp in panel.groupby(["country", "week_end"]):
        w = winners[(winners.country == country) & (winners.week_end == week_end)]
        if w.empty:
            continue
        winner_title = w.iloc[0].artist_title
        week_days = [week_end - timedelta(days=6 - i) for i in range(7)]
        days = sorted(grp.date.unique())
        cume = None
        for day in days:
            day_df = grp[grp.date == day].groupby("artist_title").streams.sum()
            cume = day_df if cume is None else cume.add(day_df, fill_value=0)
            if len(cume) < 2:
                continue
            day_i = 6 - (week_end - day).days
            top = cume.sort_values(ascending=False)
            leader, runner = top.index[0], top.index[1]
            margin = top.iloc[0] / max(top.iloc[1], 1)

            upto = grp[grp.date <= day]
            proj, drifts = {}, {}
            for t in top.index[:5]:
                td = upto[upto.artist_title == t].set_index("date").streams
                proj[t] = _project_total(td, week_days, factors, country)
                od = sorted(td.index)
                if len(od) >= 2:
                    de = [td[d_] / factors.get((country, d_.weekday()), 1.0)
                          for d_ in od[-2:]]
                    gap = (od[-1] - od[-2]).days
                    drifts[t] = float(np.clip((de[1] / max(de[0], 1.0))
                                              ** (1.0 / max(gap, 1)), *DRIFT_CLIP))
                else:
                    drifts[t] = 1.0
            best_rival = max((v for k, v in proj.items() if k != leader),
                             default=1.0)
            proj_top = max(proj, key=proj.get)
            obs.append({
                "country": country,
                "week_end": week_end.date().isoformat(),
                "day_i": day_i,
                "n_obs_days": days.index(day) + 1,
                "leader": leader,
                "margin": margin,
                "proj_margin": proj[leader] / max(best_rival, 1.0),
                "leader_drift": drifts.get(leader, 1.0),
                "runner_drift": drifts.get(runner, 1.0),
                "proj_top_is_leader": proj_top == leader,
                "leader_wins": leader == winner_title,
            })
    return pd.DataFrame(obs)


def build_observations(bf: Path) -> pd.DataFrame:
    panel = load_daily_panel(bf)
    winners = winners_by_week(bf)
    factors = weekday_factors(panel)
    return week_observations(panel, winners, factors)
