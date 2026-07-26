#!/usr/bin/env python3
"""Fetch trader state + container logs from the EC2 box and report bot health.

The analogue of ../elon-tweets-bot's analyze_bot_health.py for this repo:
trading activity and PnL come from the trader's structured outputs
(trades.csv / state.json / snapshots.csv), operational health from the
container logs, claimables from the public Data API.

Sections: trading activity (window), realized PnL per event, open positions
with marks, model-vs-market per event now, cycle/feed health, claimables.

Examples:
  python3 scripts/analyze_bot_health.py                      # paper, last 24h
  python3 scripts/analyze_bot_health.py --mode live --hours 48
  python3 scripts/analyze_bot_health.py --no-download        # reuse cache
  python3 scripts/analyze_bot_health.py --local --data-dir data

Remote fetch uses ssh/rsync (host and key overridable); results are cached
under --dest (default analysis/bot_health/<mode>/).
"""

import argparse
import csv as csv_mod
import json
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

DEFAULT_HOST = "ubuntu@3.249.34.159"
DEFAULT_KEY = "deploy/spotify-bot-paper-key.pem"
REMOTE_DIR = "/opt/spotify-bot"
CONTAINER = "spotify-bot-trader"
DATA_API = "https://data-api.polymarket.com"
CYCLE_GAP_WARN_S = 300
TICK_STALE_WARN_S = 600

LOG_TS = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.\d+)?Z?\s?")
ERROR_PATTERNS = ("order rejected", "order failed", "cycle error",
                  "get_order fallback failed")


def sh(cmd: list, **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def parse_key(k: str) -> tuple:
    """State keys are JSON-encoded tuples (legacy pipe format still parses)."""
    try:
        parsed = json.loads(k)
        if isinstance(parsed, list):
            return tuple(parsed)
    except json.JSONDecodeError:
        pass
    return tuple(k.split("|"))


# ── Fetch ────────────────────────────────────────────────────────────────────

def fetch_remote(args, dest: Path):
    ssh_base = ["ssh", "-i", args.key, "-o", "ConnectTimeout=10", args.host]
    dest.mkdir(parents=True, exist_ok=True)

    print(f"fetching {args.mode} state from {args.host} ...", file=sys.stderr)
    rsync = sh(["rsync", "-az", "-e", f"ssh -i {args.key}",
                f"{args.host}:{REMOTE_DIR}/data/{args.mode}/", str(dest)])
    if rsync.returncode != 0:
        print(f"rsync warning: {rsync.stderr.strip()}", file=sys.stderr)

    logs = sh(ssh_base + [f"docker logs --since {args.hours}h --timestamps "
                          f"{CONTAINER} 2>&1"])
    (dest / "trader.log").write_text(logs.stdout)

    ticks = sh(ssh_base + [
        f"f=$(ls -t {REMOTE_DIR}/data/market_ticks_*.csv 2>/dev/null | head -1);"
        f' [ -n "$f" ] && tail -c 200000 "$f"'])
    (dest / "ticks_tail.csv").write_text(ticks.stdout)

    # the trader's persisted daily-chart rows — the leader-race input
    daily = sh(ssh_base + [f"cat {REMOTE_DIR}/data/snapshots/trader_daily.csv "
                           "2>/dev/null"])
    (dest / "trader_daily.csv").write_text(daily.stdout)


# ── Loaders ──────────────────────────────────────────────────────────────────

def load_trades(dest: Path, since: datetime) -> pd.DataFrame:
    f = dest / "trades.csv"
    if not f.exists():
        return pd.DataFrame()
    df = pd.read_csv(f)
    df["ts"] = pd.to_datetime(df["ts"], utc=True, format="ISO8601")
    return df[df["ts"] >= since]


def load_state(dest: Path) -> dict:
    f = dest / "state.json"
    if not f.exists():
        return {"positions": {}, "entries": {}}
    raw = json.loads(f.read_text())
    return {"positions": {parse_key(k): v
                          for k, v in raw.get("positions", {}).items()},
            "entries": {parse_key(k): v
                        for k, v in raw.get("entries", {}).items()}}


def load_snapshots(dest: Path) -> pd.DataFrame:
    f = dest / "snapshots.csv"
    if not f.exists():
        return pd.DataFrame()
    df = pd.read_csv(f)
    df["ts"] = pd.to_datetime(df["ts"], utc=True, format="ISO8601")
    return df


def latest_marks(snaps: pd.DataFrame) -> pd.DataFrame:
    """Most recent snapshot row per (event, bucket)."""
    if snaps.empty:
        return snaps
    return (snaps.sort_values("ts")
            .groupby(["event_slug", "bucket"], as_index=False).last())


# ── Log health ───────────────────────────────────────────────────────────────

def parse_logs(dest: Path) -> dict:
    f = dest / "trader.log"
    out = {"lines": 0, "cycle_ts": [], "stale_ticks": 0, "errors": {},
           "restarts": [], "gaps": []}
    if not f.exists():
        return out
    for line in f.read_text().splitlines():
        out["lines"] += 1
        m = LOG_TS.match(line)
        ts = (datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S")
              .replace(tzinfo=timezone.utc) if m else None)
        body = line[m.end():].strip() if m else line
        if "ticks_fresh=" in body and ts:
            out["cycle_ts"].append(ts)
        if "ticks_fresh=False" in body:
            out["stale_ticks"] += 1
        if "trader starting:" in body:
            mode = re.search(r"mode=(\w+)", body)
            out["restarts"].append((ts, mode.group(1) if mode else "?"))
        for pat in ERROR_PATTERNS:
            if pat in body:
                key = body[:120]
                out["errors"][key] = out["errors"].get(key, 0) + 1
    ts_list = sorted(set(out["cycle_ts"]))
    for a, b in zip(ts_list, ts_list[1:]):
        gap = (b - a).total_seconds()
        if gap > CYCLE_GAP_WARN_S:
            out["gaps"].append((a, gap))
    out["cycle_ts"] = ts_list
    return out


def tick_age_s(dest: Path) -> float | None:
    """Newest tick timestamp in the fetched tail (13-col CSV, quoted labels)."""
    f = dest / "ticks_tail.csv"
    if not f.exists() or not f.stat().st_size:
        return None
    text = f.read_text(errors="replace")
    text = text[text.find("\n") + 1:]  # tail -c can cut mid-row
    last_ms = 0
    for p in csv_mod.reader(text.splitlines()):
        if len(p) == 13 and p[0].isdigit():
            last_ms = max(last_ms, int(p[0]))
    if not last_ms:
        return None
    return datetime.now(timezone.utc).timestamp() - last_ms / 1000


# ── Claimables ───────────────────────────────────────────────────────────────

def fetch_claimables(funder: str) -> tuple[int, float]:
    out = sh(["curl", "-s", "-m", "30",
              f"{DATA_API}/positions?user={funder}&limit=100"
              f"&redeemable=true&sizeThreshold=0.01"])
    try:
        rows = json.loads(out.stdout)
    except json.JSONDecodeError:
        return 0, 0.0
    rows = [p for p in rows
            if p.get("redeemable") and float(p.get("currentValue") or 0) > 0.01]
    return len(rows), sum(float(p.get("currentValue") or 0) for p in rows)


# ── Report ───────────────────────────────────────────────────────────────────

def hdr(title: str):
    print(f"\n{'=' * 64}\n{title}\n{'=' * 64}")


def report_trades(trades: pd.DataFrame, hours: float):
    hdr(f"Trading activity (last {hours:.0f}h)")
    if trades.empty:
        print("no trades in window")
        return
    for action in ("open", "exit", "settle"):
        sub = trades[trades["action"] == action]
        if sub.empty:
            continue
        print(f"\n{action.upper()} ({len(sub)}):")
        for _, r in sub.iterrows():
            pnl = f" pnl={r['pnl']:+.3f}" if pd.notna(r.get("pnl")) else ""
            shares = f" x{r['shares']}" if pd.notna(r.get("shares")) else ""
            extra = ""
            if action == "open" and pd.notna(r.get("day_i")):
                extra = (f" [day {int(r['day_i'])}, model {r['model_p']:.2f},"
                         f" margin {r['margin']:.2f}]")
            print(f"  {str(r['ts'])[:19]}  {r['event_slug']}  "
                  f"{r['bucket']} {r['side']} @ {r['price']}{shares}{pnl}{extra}")
    closed = trades[trades["action"].isin(["exit", "settle"])].dropna(
        subset=["pnl"])
    if not closed.empty:
        wins = (closed["pnl"] > 0).sum()
        print(f"\nrealized pnl: {closed['pnl'].sum():+.2f} over "
              f"{len(closed)} closed ({wins} wins, "
              f"hit {wins / len(closed):.0%})")
        by_ev = closed.groupby("event_slug")["pnl"].sum()
        for slug, pnl in by_ev.items():
            print(f"  {slug}: {pnl:+.2f}")


def report_positions(state: dict, marks: pd.DataFrame, max_exposure: float):
    hdr("Open positions")
    positions = state["positions"]
    if not positions:
        print("none")
        return
    total_cost = total_mark = 0.0
    for key, pos in positions.items():
        slug, bucket, side = key
        cost = pos.get("cost", pos.get("price", 0.0))
        shares = pos.get("shares", 1.0)
        row = marks[(marks["event_slug"] == slug)
                    & (marks["bucket"] == bucket)] if not marks.empty else None
        bid = None
        if row is not None and len(row):
            bid = pd.to_numeric(row.iloc[0]["yes_bid"], errors="coerce")
        mark_val = shares * bid if bid is not None and pd.notna(bid) else None
        total_cost += cost
        total_mark += mark_val if mark_val is not None else cost
        upnl = (f"upnl={mark_val - cost:+.2f}"
                if mark_val is not None else "upnl=?")
        model_p = pos.get("model_p")
        mp = f" model={model_p:.2f}" if model_p is not None else ""
        print(f"  {slug} {bucket} {side}: {shares} @ "
              f"{pos.get('price', 0):.3f} cost={cost:.2f} "
              f"bid={bid if bid is None or pd.isna(bid) else round(float(bid), 3)} "
              f"{upnl}{mp}")
    print(f"\nopen cost: ${total_cost:.2f} / ${max_exposure:.0f} cap "
          f"(headroom ${max_exposure - total_cost:.2f}); "
          f"mark value ${total_mark:.2f}")


def report_divergence(snaps: pd.DataFrame):
    hdr("Model vs market (latest snapshot per event)")
    if snaps.empty:
        print("no snapshots")
        return
    last_ts = snaps.groupby("event_slug")["ts"].max()
    for slug, ts in last_ts.items():
        ev = snaps[(snaps["event_slug"] == slug) & (snaps["ts"] == ts)].copy()
        ev["model_p"] = pd.to_numeric(ev["model_p"], errors="coerce")
        age_min = (datetime.now(timezone.utc) - ts).total_seconds() / 60
        day = ev["day_i"].dropna()
        day_s = f"day {int(day.iloc[0])}" if len(day) else "day ?"
        print(f"\n{slug}  (snapshot {age_min:.0f} min ago, {day_s})")
        # the leader row carries model_p; everything else is market-only
        leader_rows = ev.dropna(subset=["model_p"])
        for _, r in leader_rows.iterrows():
            edge = r["model_p"] - r["yes_ask"]
            flag = "  <-- edge" if edge > 0.10 else ""
            print(f"  LEADER {str(r['leader'])[:34]:<36} "
                  f"model={r['model_p']:.2f} "
                  f"bid/ask={r['yes_bid']:.2f}/{r['yes_ask']:.2f} "
                  f"edge={edge:+.2f}{flag}")
        rest = (ev[ev["model_p"].isna()]
                .sort_values("yes_ask", ascending=False).head(3))
        for _, r in rest.iterrows():
            print(f"  {str(r['bucket'])[:36]:<43} "
                  f"bid/ask={r['yes_bid']:.2f}/{r['yes_ask']:.2f}")


def report_leader_race(dest: Path, data_dir: Path):
    """Reconstruct the cumulative-stream race the trader's leader call is
    based on: chart week = Fri..Thu, leader = highest cume over the days
    observed so far (chart POSITION is irrelevant, only summed streams).
    Uses the same weekly_lib code path as the trader itself."""
    hdr("Leader race (current chart week, Fri..Thu)")
    sys.path.insert(0, str(Path(__file__).parent / "research"))
    try:
        from weekly_lib import (chart_week_of, load_daily_panel,
                                week_observations, weekday_factors)
        base = load_daily_panel(data_dir / "backfill")
    except (ImportError, FileNotFoundError) as e:
        print(f"(cannot rebuild race: {e})")
        return
    frames = [base]
    live_f = dest / "trader_daily.csv"
    if live_f.exists() and live_f.stat().st_size:
        live = pd.read_csv(live_f)
        live["date"] = pd.to_datetime(live.date)
        frames.append(live.groupby(["country", "date", "artist_title"],
                                   as_index=False).streams.last())
    panel = (pd.concat(frames, ignore_index=True)
             .drop_duplicates(subset=["country", "date", "artist_title"],
                              keep="last"))
    factors = weekday_factors(base)
    week_end = chart_week_of(pd.Timestamp(datetime.now(timezone.utc).date()))
    week_start = week_end - timedelta(days=6)
    print(f"week: {week_start.date()} .. {week_end.date()}")
    for country in ("global", "us"):
        wk = panel[(panel.country == country) & (panel.date >= week_start)
                   & (panel.date <= week_end)]
        if wk.empty:
            print(f"\n{country}: no chart days observed yet")
            continue
        days = sorted(wk.date.unique())
        cume = (wk.groupby("artist_title").streams.sum()
                .sort_values(ascending=False))
        print(f"\n{country}: {len(days)} day(s) observed "
              f"({', '.join(str(d.date()) for d in days)})")
        piv = (wk[wk.artist_title.isin(cume.head(5).index)]
               .pivot_table(index="artist_title", columns="date",
                            values="streams", aggfunc="sum")
               .reindex(cume.head(5).index))
        piv.columns = [f"{c:%a %d}" for c in piv.columns]
        piv["cume"] = cume.head(5)
        piv["vs_#1"] = (cume.head(5) / cume.iloc[0]).round(3)
        with pd.option_context("display.width", 140,
                               "display.float_format", "{:,.0f}".format):
            out = piv.copy()
            out["vs_#1"] = piv["vs_#1"].map("{:.3f}".format)
            print(out.to_string())
        winners = pd.DataFrame([{"country": country, "week_end": week_end,
                                 "artist_title": ""}])
        obs = week_observations(wk, winners, factors)
        if not obs.empty:
            o = obs.sort_values("day_i").iloc[-1]
            print(f"  features: day_i={int(o.day_i)} margin={o.margin:.3f} "
                  f"proj_margin={o.proj_margin:.3f} "
                  f"leader_drift={o.leader_drift:.3f} "
                  f"runner_drift={o.runner_drift:.3f} "
                  f"proj_top_is_leader={bool(o.proj_top_is_leader)}")


def report_health(logh: dict, age_s: float | None, hours: float):
    hdr("Health")
    cycles = logh["cycle_ts"]
    expected = hours * 60
    if cycles:
        span_min = (cycles[-1] - cycles[0]).total_seconds() / 60
        print(f"cycles logged : {len(cycles)} over {span_min:.0f} min "
              f"(~{expected:.0f} expected for {hours:.0f}h)")
        print(f"last cycle    : {cycles[-1]:%Y-%m-%d %H:%M:%S} UTC")
    else:
        print("cycles logged : NONE — is the container running?")
    for ts, gap in logh["gaps"][-5:]:
        print(f"  gap: {gap / 60:.1f} min after {ts:%H:%M:%S}")
    print(f"stale-tick cycles: {logh['stale_ticks']}")
    if age_s is not None:
        flag = "  STALE" if age_s > TICK_STALE_WARN_S else ""
        print(f"collector tick age: {age_s:.0f}s{flag}")
    for ts, mode in logh["restarts"]:
        t = f"{ts:%m-%d %H:%M}" if ts else "?"
        print(f"restart: {t} (mode={mode})")
    if logh["errors"]:
        print("\nerrors (deduped):")
        for msg, n in sorted(logh["errors"].items(), key=lambda x: -x[1]):
            print(f"  {n:>3}x {msg}")
    else:
        print("errors: none")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", default="paper", choices=["live", "paper"])
    ap.add_argument("--hours", default=24, type=float)
    ap.add_argument("--host", default=DEFAULT_HOST)
    ap.add_argument("--key", default=DEFAULT_KEY)
    ap.add_argument("--dest", default=Path("analysis/bot_health"), type=Path)
    ap.add_argument("--no-download", action="store_true",
                    help="reuse cached fetch in --dest")
    ap.add_argument("--local", action="store_true",
                    help="analyze the local --data-dir instead of the box")
    ap.add_argument("--data-dir", default=Path("data"), type=Path)
    ap.add_argument("--max-exposure", default=50.0, type=float)
    ap.add_argument("--funder",
                    default="0x0345586486a69c6206c2b78c69b601af537d584c")
    args = ap.parse_args()

    if args.local:
        dest = args.data_dir / args.mode
    else:
        dest = args.dest / args.mode
        if not args.no_download:
            fetch_remote(args, dest)

    since = datetime.now(timezone.utc) - timedelta(hours=args.hours)
    trades = load_trades(dest, since)
    state = load_state(dest)
    snaps = load_snapshots(dest)
    marks = latest_marks(snaps)

    print(f"bot health report — mode={args.mode} "
          f"({datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC)")
    report_trades(trades, args.hours)
    report_positions(state, marks, args.max_exposure)
    report_leader_race(dest, args.data_dir)
    report_divergence(snaps[snaps["ts"] >= since] if not snaps.empty else snaps)
    report_health(parse_logs(dest), tick_age_s(dest), args.hours)

    if args.mode == "live":
        n, value = fetch_claimables(args.funder)
        hdr("Claimables (Data API)")
        print(f"{n} redeemable positions, ${value:.2f} — claim in the "
              f"Polymarket UI" if n else "nothing to redeem")
    return 0


if __name__ == "__main__":
    sys.exit(main())
