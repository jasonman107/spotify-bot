#!/usr/bin/env bash
# One-time data bootstrap for a freshly provisioned box: ships the .env
# (paper mode) and the parquet files the trader needs to price markets
# (daily-panel history + the trajectory-model observations). Idempotent —
# rsync only sends what changed. Tick/backfill caches stay local.
#
# Usage: ./deploy/bootstrap_data.sh <host-ip>
set -euo pipefail

HOST=${1:?usage: ./deploy/bootstrap_data.sh <host-ip>}
KEY_FILE=deploy/spotify-bot-paper-key.pem
DEST=ubuntu@$HOST
SSH="ssh -i $KEY_FILE -o StrictHostKeyChecking=accept-new"

echo "== .env (created as paper mode if absent on host) =="
$SSH "$DEST" "test -f /opt/spotify-bot/.env" 2>/dev/null \
  || printf 'TRADING_MODE=paper\n' | $SSH "$DEST" "cat > /opt/spotify-bot/.env"

echo "== backfill parquets (trader model + panel history) =="
$SSH "$DEST" "mkdir -p /opt/spotify-bot/data/backfill/kworb /opt/spotify-bot/data/backfill/wayback"
rsync -avz -e "$SSH" \
  data/backfill/trajectory_obs.parquet \
  "$DEST":/opt/spotify-bot/data/backfill/
rsync -avz -e "$SSH" \
  data/backfill/kworb/track_daily_history.parquet \
  data/backfill/kworb/track_weekly_history.parquet \
  "$DEST":/opt/spotify-bot/data/backfill/kworb/
rsync -avz -e "$SSH" \
  data/backfill/wayback/daily_charts.parquet \
  data/backfill/wayback/weekly_charts.parquet \
  "$DEST":/opt/spotify-bot/data/backfill/wayback/

echo "== done =="
$SSH "$DEST" "find /opt/spotify-bot -maxdepth 3 -type f | sort"
