#!/usr/bin/env python3
"""Weekly-#1 trajectory trader: quotes the leader-win model against live books.

TRADING_MODE=paper (default) logs hypothetical fills; TRADING_MODE=live places
real FAK orders through the CLOB (see scripts/execution.py). v1 trades the
two weekly series only (global + US #1 song); listener markets come later.

Every cycle (default 60s):
  1. refresh active weekly events + outcome buckets (Gamma)
  2. refresh the daily-chart panel (kworb daily pages, hourly; plus the
     backfill/cron history) and build the current chart week's cume race
  3. compute P(leader wins) with the trajectory logistic fit on
     backfill/trajectory_obs.parquet (weights cached, refit daily)
  4. read live top-of-book from the collector's market_ticks CSV (tail)
  5. buy the leader's bucket (or 'Other' when unlisted) when model - ask
     exceeds the threshold; flat ORDER_SIZE_USDC, capped exposure
  6. dynamic exit: sell when model prob drops below bid by EXIT_MARGIN
  7. settle ended events against the kworb weekly chart #1
  8. snapshot model vs market every few minutes for forward-test analysis

State lives in data/paper/ or data/live/ per mode. Safe to restart any time.

Usage: python3 scripts/trader.py [--mode paper|live] [--once] [--threshold 0.10]
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent / "research"))
from execution import make_executor  # noqa: E402
from kworb_lib import KWORB, get, get_json, parse_chart_page  # noqa: E402
from weekly_lib import (  # noqa: E402
    DRIFT_CLIP, best_bucket, chart_week_of, fit_logistic, load_daily_panel,
    predict_logistic, weekday_factors, week_observations, winners_by_week,
)

GAMMA = "https://gamma-api.polymarket.com"
WEEKLY_SERIES = {"1-spotify-song": "global", "top-us-spotify-song": "us"}
TICK_TAIL_BYTES = 8_000_000
MAX_TICK_AGE_S = 600
PRICE_LO, PRICE_HI = 0.02, 0.95
# No entries once the chart week is over (Thu 23:59 UTC) — after that only
# the published chart matters and we hold/exit into resolution.
KWORB_REFRESH_S = 3600

TRADE_COLS = ["ts", "action", "event_slug", "bucket", "side", "price",
              "shares", "cost", "model_p", "leader", "day_i", "margin",
              "proj_margin", "pnl"]


def now_utc():
    return pd.Timestamp.now(tz="UTC")


class Registry:
    """Active weekly events + their outcome buckets. Refreshed periodically."""

    def __init__(self):
        self.events = []  # dicts: slug, country, week_end, buckets{label: q}
        self.fetched_at = None

    def refresh(self):
        events = []
        for series, country in WEEKLY_SERIES.items():
            body = get_json(
                f"{GAMMA}/events?series_slug={series}&closed=false"
                "&active=true&limit=50")
            for ev in body or []:
                end = ev.get("endDate", "")
                if not end:
                    continue
                buckets = {}
                for m in ev.get("markets", []):
                    if m.get("closed"):
                        continue
                    label = m.get("groupItemTitle") or m.get("question") or ""
                    toks = m.get("clobTokenIds")
                    outs = m.get("outcomes")
                    toks = json.loads(toks) if isinstance(toks, str) else toks or []
                    outs = json.loads(outs) if isinstance(outs, str) else outs or []
                    by = dict(zip([o.lower() for o in outs], toks))
                    if label and by.get("yes"):
                        buckets[label] = {"yes_token": by["yes"]}
                if not buckets:
                    continue
                end_ts = pd.Timestamp(end)
                events.append({
                    "slug": ev.get("slug", ""),
                    "country": country,
                    "end": end_ts,
                    # Polymarket labels by Friday publish; chart week ends Thu.
                    # Kept tz-naive UTC to match the chart panel's dates.
                    "week_end": (end_ts.tz_convert(None)
                                 - pd.Timedelta(days=1)).normalize(),
                    "buckets": buckets,
                })
        if events:
            self.events = events
            self.fetched_at = now_utc()

    def stale(self):
        return (self.fetched_at is None
                or now_utc() - self.fetched_at > pd.Timedelta(minutes=15))


class ChartPanel:
    """Daily-stream panel: backfill history + live kworb daily-page pulls.

    Fetched rows are persisted to data/snapshots/trader_daily.csv and
    reloaded at startup — kworb daily pages only show the LATEST chart day,
    so a restart mid-week would otherwise lose the earlier days of the
    in-progress chart week (fatal on a redeployed box)."""

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.live_file = data_dir / "snapshots" / "trader_daily.csv"
        self.live_rows = []
        self.last_fetch = 0.0
        if self.live_file.exists():
            df = pd.read_csv(self.live_file, dtype={"streams": "int64"})
            self.live_rows = df.to_dict("records")
            print(f"chart panel: reloaded {len(self.live_rows)} persisted "
                  f"daily rows from {self.live_file}")
        base = load_daily_panel(data_dir / "backfill")
        self.base = base
        self.factors = weekday_factors(base)

    def refresh(self):
        if time.time() - self.last_fetch < KWORB_REFRESH_S:
            return
        new_rows = []
        for country in ("global", "us"):
            html = get(f"{KWORB}/spotify/country/{country}_daily.html")
            if not html:
                continue
            for r in parse_chart_page(html, "daily"):
                new_rows.append({
                    "country": country, "date": r["chart_date"],
                    "artist_title": r["artist_title"], "streams": r["streams"],
                })
        if new_rows:
            self.live_rows.extend(new_rows)
            self.live_file.parent.mkdir(parents=True, exist_ok=True)
            new = not self.live_file.exists()
            pd.DataFrame(new_rows).to_csv(self.live_file, mode="a",
                                          index=False, header=new)
        self.last_fetch = time.time()

    def panel(self) -> pd.DataFrame:
        frames = [self.base]
        if self.live_rows:
            live = pd.DataFrame(self.live_rows)
            live["date"] = pd.to_datetime(live.date)
            live = (live.groupby(["country", "date", "artist_title"],
                                 as_index=False).streams.last())
            frames.append(live)
        df = pd.concat(frames, ignore_index=True)
        return df.drop_duplicates(subset=["country", "date", "artist_title"],
                                  keep="last")


def week_state(panel: pd.DataFrame, factors: dict, country: str,
               week_end: pd.Timestamp) -> dict | None:
    """Latest (day_i, leader, features) for the in-progress chart week, via
    the same observation builder research uses."""
    week_start = week_end - pd.Timedelta(days=6)
    wk = panel[(panel.country == country) & (panel.date >= week_start)
               & (panel.date <= week_end)]
    if wk.empty:
        return None
    winners = pd.DataFrame([{  # dummy winner so the builder yields rows
        "country": country, "week_end": week_end, "artist_title": ""}])
    obs = week_observations(wk, winners, factors)
    if obs.empty:
        return None
    return obs.sort_values("day_i").iloc[-1].to_dict()


def read_quotes(data_dir: Path) -> dict:
    """Latest top-of-book per (event_slug, bucket, outcome) from the tick CSV."""
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    path = data_dir / f"market_ticks_{today}.csv"
    if not path.exists():
        yest = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y%m%d")
        path = data_dir / f"market_ticks_{yest}.csv"
        if not path.exists():
            return {}
    with open(path, "rb") as f:
        f.seek(max(f.seek(0, 2) - TICK_TAIL_BYTES, 0))
        chunk = f.read().decode(errors="replace")
    quotes = {}
    for line in chunk.splitlines()[1:]:
        p = line.split(",")
        if len(p) != 13 or p[0] == "timestamp_ms":
            continue
        try:
            ts, bid, ask = int(p[0]), float(p[6] or 0), float(p[7] or 0)
        except ValueError:
            continue
        if bid > 0 and ask > 0:
            quotes[(p[1], p[3], p[5])] = {
                "ts_ms": ts, "bid": bid, "ask": ask, "token_id": p[4]}
    return quotes


class TraderState:
    """Positions + per-key entry counts, persisted as JSON; trade log CSV."""

    def __init__(self, state_dir: Path):
        self.dir = state_dir
        self.dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.dir / "state.json"
        self.trades_file = self.dir / "trades.csv"
        self.snap_file = self.dir / "snapshots.csv"
        self.positions = {}
        self.entries = {}
        if self.state_file.exists():
            raw = json.loads(self.state_file.read_text())
            self.positions = {
                tuple(k.split("|")): v for k, v in raw["positions"].items()}
            self.entries = {
                tuple(k.split("|")): v
                for k, v in raw.get("entries", {}).items()}
        self._rotate_trades_if_schema_changed()

    def _rotate_trades_if_schema_changed(self):
        if not self.trades_file.exists():
            return
        with open(self.trades_file) as f:
            header = f.readline().strip()
        if header != ",".join(TRADE_COLS):
            legacy = self.dir / "trades_v1.csv"
            self.trades_file.rename(legacy)
            print(f"trades.csv schema changed; old log moved to {legacy}")

    def save(self):
        self.state_file.write_text(json.dumps({
            "positions": {"|".join(k): v for k, v in self.positions.items()},
            "entries": {"|".join(k): v for k, v in self.entries.items()},
        }, indent=1))

    def open_cost(self) -> float:
        return sum(p["cost"] for p in self.positions.values())

    def append_trade(self, row: dict):
        new = not self.trades_file.exists()
        with open(self.trades_file, "a") as f:
            if new:
                f.write(",".join(TRADE_COLS) + "\n")
            f.write(",".join(str(row.get(c, "")) for c in TRADE_COLS) + "\n")

    def append_snapshots(self, rows: list):
        if not rows:
            return
        df = pd.DataFrame(rows)
        df.to_csv(self.snap_file, mode="a", index=False,
                  header=not self.snap_file.exists())


def load_model_weights(bf_dir: Path, cache: Path) -> np.ndarray:
    """Trajectory-logistic weights fit on all research observations; cached
    for a day so restarts are cheap."""
    if cache.exists():
        c = json.loads(cache.read_text())
        if now_utc() - pd.Timestamp(c["at"]) < pd.Timedelta(days=1):
            return np.array(c["w"])
    obs = pd.read_parquet(bf_dir / "trajectory_obs.parquet")
    w = fit_logistic(obs)
    cache.write_text(json.dumps({"w": list(w), "at": str(now_utc())}))
    return w


def kworb_weekly_top(country: str) -> str | None:
    """Current #1 on the kworb weekly page (resolution proxy for settling
    paper positions; live-mode redemption uses the real resolution)."""
    html = get(f"{KWORB}/spotify/country/{country}_weekly.html")
    if not html:
        return None
    rows = parse_chart_page(html, "weekly")
    top = [r for r in rows if r["pos"] == 1]
    return top[0]["artist_title"] if top else None


def settle(state: TraderState, ev):
    winner = kworb_weekly_top(ev["country"])
    if winner is None:
        return  # kworb unreachable; retry next cycle
    win_bucket = best_bucket(list(ev["buckets"]), winner)
    for key in [k for k in state.positions if k[0] == ev["slug"]]:
        pos = state.positions.pop(key)
        _, bucket, _ = key
        won = bucket == win_bucket
        pnl = pos["shares"] * (1.0 if won else 0.0) - pos["cost"]
        state.append_trade({
            "ts": str(now_utc()), "action": "settle", "event_slug": ev["slug"],
            "bucket": bucket, "side": "yes", "price": pos["price"],
            "shares": pos["shares"], "cost": round(pos["cost"], 4),
            "pnl": round(pnl, 4),
        })
        print(f"SETTLE {ev['slug']} {bucket}: kworb_winner={winner} "
              f"pnl={pnl:+.3f}")
    state.entries = {k: v for k, v in state.entries.items()
                     if k[0] != ev["slug"]}
    state.save()


def should_exit(prob: float, bid: float, exit_margin: float) -> bool:
    return bid > 0 and prob - bid < -exit_margin


def cycle(args, registry, chart, state, executor, weights, snap_due):
    if registry.stale():
        registry.refresh()
        print(f"registry: {len(registry.events)} active weekly events")
    chart.refresh()
    panel = chart.panel()

    quotes = read_quotes(args.data_dir)
    now = now_utc()
    fresh_ms = max((q["ts_ms"] for q in quotes.values()), default=0)
    ticks_fresh = (now.timestamp() * 1000 - fresh_ms) < MAX_TICK_AGE_S * 1000

    snaps = []
    for ev in registry.events:
        if now > ev["end"]:
            settle(state, ev)
            continue
        st = week_state(panel, chart.factors, ev["country"], ev["week_end"])
        if st is None:
            continue
        p_model = predict_logistic(
            weights, pd.DataFrame([st]).astype(
                {"proj_top_is_leader": bool}))[0].item()
        leader = st["leader"]
        label = best_bucket(list(ev["buckets"]), leader)
        chart_week_open = now.tz_convert(None) <= \
            ev["week_end"] + pd.Timedelta(days=1)
        print(f"{ev['slug']}: day={int(st['day_i'])} leader='{leader[:40]}' "
              f"margin={st['margin']:.3f} proj={st['proj_margin']:.3f} "
              f"p={p_model:.3f} bucket='{label}' ticks_fresh={ticks_fresh}")

        for b in ev["buckets"]:
            q = quotes.get((ev["slug"], b, "Yes"))
            if snap_due and q:
                snaps.append({
                    "ts": str(now), "event_slug": ev["slug"], "bucket": b,
                    "model_p": round(p_model, 4) if b == label else "",
                    "leader": leader if b == label else "",
                    "day_i": int(st["day_i"]),
                    "yes_bid": q["bid"], "yes_ask": q["ask"],
                })

        if not ticks_fresh or args.observe or label is None:
            continue
        key = (ev["slug"], label, "yes")
        q = quotes.get((ev["slug"], label, "Yes"))
        pos = state.positions.get(key)
        if pos is not None:
            bid = q["bid"] if q else 0.0
            if should_exit(p_model, bid, args.exit_margin):
                fill = executor.sell(pos["token_id"], bid, pos["shares"])
                if fill:
                    cost_out = pos["cost"] * (fill["shares"] / pos["shares"])
                    pnl = fill["proceeds"] - cost_out
                    state.append_trade({
                        "ts": str(now), "action": "exit",
                        "event_slug": ev["slug"], "bucket": label,
                        "side": "yes", "price": round(fill["price"], 4),
                        "shares": fill["shares"], "cost": round(cost_out, 4),
                        "model_p": round(p_model, 4), "pnl": round(pnl, 4),
                    })
                    print(f"EXIT {ev['slug']} {label} @ {fill['price']:.3f} "
                          f"(model {p_model:.3f}, pnl {pnl:+.3f})")
                    remaining = pos["shares"] - fill["shares"]
                    if remaining >= 0.01:
                        pos["shares"] = remaining
                        pos["cost"] -= cost_out
                    else:
                        del state.positions[key]
                    state.save()
            continue
        if not chart_week_open or q is None:
            continue
        if not (PRICE_LO <= q["ask"] <= PRICE_HI):
            continue
        edge = p_model - q["ask"]
        if edge <= args.threshold:
            continue
        if state.entries.get(key, 0) >= args.max_entries:
            continue
        if state.open_cost() + args.order_size > args.max_exposure + 1e-9:
            continue
        fill = executor.buy(q["token_id"], q["ask"], args.order_size)
        if fill is None:
            continue
        state.positions[key] = {
            "price": fill["price"], "shares": fill["shares"],
            "cost": fill["cost"], "ts": str(now), "model_p": p_model,
            "token_id": q["token_id"],
        }
        state.entries[key] = state.entries.get(key, 0) + 1
        state.append_trade({
            "ts": str(now), "action": "open", "event_slug": ev["slug"],
            "bucket": label, "side": "yes",
            "price": round(fill["price"], 4), "shares": fill["shares"],
            "cost": round(fill["cost"], 4), "model_p": round(p_model, 4),
            "leader": leader, "day_i": int(st["day_i"]),
            "margin": round(st["margin"], 4),
            "proj_margin": round(st["proj_margin"], 4),
        })
        state.save()
        print(f"OPEN {ev['slug']} {label} {fill['shares']} @ "
              f"{fill['price']:.3f} (model {p_model:.3f}, edge {edge:+.3f})")
    state.append_snapshots(snaps)


def env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, ""))
    except ValueError:
        return default


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data", type=Path)
    ap.add_argument("--mode", default=None, choices=["paper", "live"],
                    help="overrides TRADING_MODE env (default paper)")
    ap.add_argument("--threshold", default=env_float("EDGE_THRESHOLD", 0.10),
                    type=float)
    ap.add_argument("--order-size", default=env_float("ORDER_SIZE_USDC", 5.0),
                    type=float, help="flat USDC stake per signal")
    ap.add_argument("--max-exposure",
                    default=env_float("MAX_EXPOSURE_USDC", 50.0), type=float)
    ap.add_argument("--exit-margin", default=env_float("EXIT_MARGIN", 0.05),
                    type=float)
    ap.add_argument("--max-entries", default=3, type=int,
                    help="entries per bucket per event")
    ap.add_argument("--interval", default=60, type=float, help="cycle seconds")
    ap.add_argument("--snapshot-interval", default=300, type=float)
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--observe", action="store_true",
                    help="snapshot only, never trade")
    args = ap.parse_args()

    executor = make_executor(args.mode)
    mode = "live" if executor.live else "paper"
    state = TraderState(args.data_dir / mode)
    print(f"trader starting: mode={mode} threshold={args.threshold} "
          f"order_size=${args.order_size} max_exposure=${args.max_exposure} "
          f"open_positions={len(state.positions)} observe={args.observe}")

    weights = load_model_weights(args.data_dir / "backfill",
                                 state.dir / "model_weights.json")
    print(f"trajectory logistic weights: {np.round(weights, 3).tolist()}")
    chart = ChartPanel(args.data_dir)

    registry = Registry()
    last_snap = 0.0
    while True:
        snap_due = time.time() - last_snap >= args.snapshot_interval
        try:
            cycle(args, registry, chart, state, executor, weights, snap_due)
            if snap_due:
                last_snap = time.time()
        except Exception as e:
            print(f"cycle error: {type(e).__name__}: {e}", file=sys.stderr)
        if args.once:
            return 0
        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())
