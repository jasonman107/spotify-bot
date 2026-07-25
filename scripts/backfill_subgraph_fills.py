#!/usr/bin/env python3
"""Backfill trade-by-trade fills from Polymarket's public orderbook subgraph.

The Goldsky orderbook subgraph has every order fill since June 2024 with no
retention limit — but it stopped indexing 2026-04-28, so it complements (not
replaces) CLOB prices-history: fills cover the older weekly Spotify events
that CLOB's ~45-day window has already dropped.

For each closed event with a resolved bucket that ended before the subgraph
cutoff, fetches all fills for all bucket tokens (YES and NO) into
data/backfill/fills/event_slug=<slug>/fills.parquet with:
    timestamp_s, bucket, token_id, is_yes, price_yes, size_tokens
price_yes is the YES-equivalent price (NO fills stored as 1 - price).

Idempotent: events with an existing output file are skipped.

Usage: python3 scripts/backfill_subgraph_fills.py [--data-dir data]
"""

import argparse
import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

SUBGRAPH = ("https://api.goldsky.com/api/public/"
            "project_cl6mb8i9h0003e201j6li0diw/subgraphs/"
            "orderbook-subgraph/prod/gn")
PAGE = 1000
INDEX_CUTOFF = "2026-04-29"  # subgraph stopped indexing 2026-04-28


def gql(query: str, retries: int = 6):
    for attempt in range(retries):
        out = subprocess.run(
            ["curl", "-s", "-m", "60", "-X", "POST",
             "-H", "Content-Type: application/json",
             "-d", json.dumps({"query": query}), SUBGRAPH],
            capture_output=True, text=True)
        try:
            body = json.loads(out.stdout)
            if "data" in body:
                return body["data"]
        except json.JSONDecodeError:
            pass
        time.sleep(min(3 * 2 ** attempt, 60))
    raise RuntimeError(f"subgraph query failed: {query[:120]}...")


def fetch_side(field: str, tokens: list, start_ts: int, end_ts: int):
    """All fills where `field` (maker/taker asset) is one of `tokens`."""
    toks = json.dumps(tokens)
    cursor, rows = "", []
    while True:
        q = f'''{{ orderFilledEvents(first: {PAGE}, orderBy: id,
            where: {{ {field}_in: {toks}, id_gt: "{cursor}",
                      timestamp_gte: "{start_ts}", timestamp_lte: "{end_ts}" }})
            {{ id timestamp makerAssetId takerAssetId
               makerAmountFilled takerAmountFilled }} }}'''
        page = gql(q)["orderFilledEvents"]
        rows.extend(page)
        if len(page) < PAGE:
            return rows
        cursor = page[-1]["id"]


def fills_for_event(tokens_meta: dict, start_ts: int, end_ts: int) -> pd.DataFrame:
    tokens = list(tokens_meta)
    seen, out = set(), []
    for field in ("makerAssetId", "takerAssetId"):
        for raw in fetch_side(field, tokens, start_ts, end_ts):
            if raw["id"] in seen:
                continue
            seen.add(raw["id"])
            maker_amt = float(raw["makerAmountFilled"])
            taker_amt = float(raw["takerAmountFilled"])
            if maker_amt <= 0 or taker_amt <= 0:
                continue
            if raw["makerAssetId"] in tokens_meta:
                tok, price = raw["makerAssetId"], taker_amt / maker_amt
                size = maker_amt / 1e6
            else:
                tok, price = raw["takerAssetId"], maker_amt / taker_amt
                size = taker_amt / 1e6
            if not 0 < price < 1:
                continue
            bucket, is_yes = tokens_meta[tok]
            out.append({
                "timestamp_s": int(raw["timestamp"]), "bucket": bucket,
                "token_id": tok, "is_yes": is_yes,
                "price_yes": price if is_yes else 1 - price,
                "size_tokens": size,
            })
    return pd.DataFrame(out)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-dir", default="data", type=Path)
    args = ap.parse_args()
    bf = args.data_dir / "backfill"

    ev = pd.read_parquet(bf / "events.parquet")
    markets = pd.read_parquet(bf / "markets.parquet")
    resolved = set(markets[markets["resolved_yes_price"] == "1"]["event_slug"])
    todo = ev[ev.closed & (ev.event_end < INDEX_CUTOFF)
              & ev.event_slug.isin(resolved)]
    print(f"{len(todo)} resolved events ended before subgraph cutoff {INDEX_CUTOFF}")

    t0 = time.time()

    def fetch_one(e):
        out = bf / "fills" / f"event_slug={e.event_slug}" / "fills.parquet"
        if out.exists():
            return e.event_slug, -1
        tokens_meta = {}
        for _, m in markets[markets["event_slug"] == e.event_slug].iterrows():
            if m.yes_token:
                tokens_meta[m.yes_token] = (m.bucket, True)
            if m.no_token:
                tokens_meta[m.no_token] = (m.bucket, False)
        if not e.event_end:
            return e.event_slug, -2  # Gamma sometimes omits endDate
        end_ts = int(pd.Timestamp(e.event_end).timestamp())
        # markets open days before the chart week; include the run-up
        start_ts = end_ts - 21 * 86400
        df = fills_for_event(tokens_meta, start_ts, end_ts)
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, compression="zstd", index=False)
        return e.event_slug, len(df)

    done_n, failed = 0, []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futs = {pool.submit(fetch_one, e): e.event_slug
                for _, e in todo.iterrows()}
        for fut in as_completed(futs):
            done_n += 1
            try:
                slug, n = fut.result()
            except Exception as exc:
                failed.append(futs[fut])
                print(f"[{done_n}/{len(todo)}] {futs[fut]}: FAILED {exc}", flush=True)
                continue
            if n >= 0:
                print(f"[{done_n}/{len(todo)}] {slug}: {n} fills "
                      f"({time.time() - t0:.0f}s elapsed)", flush=True)
    if failed:
        print(f"failed events (re-run to retry): {failed}")
    print("done")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
