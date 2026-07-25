#!/bin/sh
# Install the twice-daily snapshot collector into this user's crontab.
# Runs at 08:10 and 20:10 local time; logs to logs/collect_snapshots.log.
set -eu

REPO="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="$(command -v python3)"
LINE="10 8,20 * * * cd $REPO && $PYTHON scripts/collect_snapshots.py >> $REPO/logs/collect_snapshots.log 2>&1"

mkdir -p "$REPO/logs"
if crontab -l 2>/dev/null | grep -F "collect_snapshots.py" >/dev/null; then
    echo "cron entry already installed:"
    crontab -l | grep -F "collect_snapshots.py"
    exit 0
fi
( crontab -l 2>/dev/null || true; echo "$LINE" ) | crontab -
echo "installed: $LINE"
