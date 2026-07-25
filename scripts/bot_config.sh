#!/usr/bin/env bash
#
# bot_config.sh — inspect and change the deployed spotify-bot trader config.
#
# Runtime config lives in /opt/spotify-bot/.env on the EC2 host. The trader
# container only reads .env at start, so 'set' recreates it after editing.
#
# Commands:
#   status                 Show container state + settable trader params.
#   set                    Change params in the remote .env, then recreate.
#
# Settable params:
#   --size N               Flat USDC stake per signal   -> ORDER_SIZE_USDC
#   --mode live|paper      Trading mode                 -> TRADING_MODE
#   --max-exposure N       Cap on total open cost       -> MAX_EXPOSURE_USDC
#   --threshold N          Model edge required to enter -> EDGE_THRESHOLD
#   --exit-margin N        Dynamic-exit margin          -> EXIT_MARGIN
#
# Connection:
#   --host H               EC2 host        (env BOT_HOST, default 3.249.34.159)
#   --key PATH             SSH private key (env BOT_SSH_KEY,
#                          default deploy/spotify-bot-paper-key.pem)
#
# Examples:
#   scripts/bot_config.sh status
#   scripts/bot_config.sh set --size 10
#   scripts/bot_config.sh set --size 10 --max-exposure 100
#   scripts/bot_config.sh set --mode paper
#   scripts/bot_config.sh set --mode live --size 5 -y
#   scripts/bot_config.sh set --threshold 0.15 --dry-run
#   scripts/bot_config.sh set --size 10 --no-restart
#
# Requires: ssh access to the box (IP changes on instance stop/start).

set -euo pipefail

HOST="${BOT_HOST:-3.249.34.159}"
SSH_USER="${BOT_SSH_USER:-ubuntu}"
KEY="${BOT_SSH_KEY:-deploy/spotify-bot-paper-key.pem}"
REMOTE_DIR=/opt/spotify-bot
TRADER=spotify-bot-trader

# Params this script manages; never dump the whole .env (it holds PRIVATE_KEY).
PARAMS=(TRADING_MODE ORDER_SIZE_USDC MAX_EXPOSURE_USDC EDGE_THRESHOLD
        EXIT_MARGIN)

usage() {
  awk 'NR==1{next} /^#/{sub(/^# ?/,""); print; next} {exit}' "$0"
  exit "${1:-0}"
}

err() { echo "error: $*" >&2; exit 1; }

run_ssh() {
  [[ -f "$KEY" ]] || err "SSH key not found: $KEY (pass --key or set BOT_SSH_KEY)"
  ssh -i "$KEY" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 \
    "${SSH_USER}@${HOST}" "$@"
}

# Fetch the managed params from the remote .env as name=value lines.
fetch_params() {
  local pat
  pat="$(IFS='|'; echo "${PARAMS[*]}")"
  run_ssh "grep -E '^(${pat})=' ${REMOTE_DIR}/.env 2>/dev/null || true"
}

num_or_die() { [[ "$2" =~ ^[0-9]+([.][0-9]+)?$ ]] || err "$1 must be a number, got: $2"; }

cmd_status() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --host) HOST="${2:-}"; shift 2 ;;
      --key)  KEY="${2:-}"; shift 2 ;;
      -h|--help) usage 0 ;;
      *) err "unknown argument for status: $1" ;;
    esac
  done

  echo "Bot runtime config — ${SSH_USER}@${HOST}:${REMOTE_DIR}/.env"
  echo

  local out
  out="$(run_ssh "
    echo '== containers =='
    docker ps -a --filter name=spotify-bot \
      --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}'
    echo
    echo '== trader params (.env) =='
    grep -E '^($(IFS='|'; echo "${PARAMS[*]}"))=' ${REMOTE_DIR}/.env || \
      echo '(none set — trader uses built-in defaults)'
    echo
    echo '== effective runtime config (from trader startup log) =='
    docker logs ${TRADER} 2>&1 | grep 'trader starting' | tail -1 || \
      echo '(no startup line found)'
  ")"
  echo "$out"
}

cmd_set() {
  local SIZE="" MODE="" MAX_EXPOSURE="" THRESHOLD="" EXIT_MARGIN_V=""
  local DO_RESTART=1 DRY_RUN=0 ASSUME_YES=0

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --size)           SIZE="${2:-}"; shift 2 ;;
      --mode)           MODE="${2:-}"; shift 2 ;;
      --max-exposure)   MAX_EXPOSURE="${2:-}"; shift 2 ;;
      --threshold)      THRESHOLD="${2:-}"; shift 2 ;;
      --exit-margin)    EXIT_MARGIN_V="${2:-}"; shift 2 ;;
      --host)           HOST="${2:-}"; shift 2 ;;
      --key)            KEY="${2:-}"; shift 2 ;;
      --no-restart)     DO_RESTART=0; shift ;;
      --dry-run)        DRY_RUN=1; shift ;;
      -y|--yes)         ASSUME_YES=1; shift ;;
      -h|--help)        usage 0 ;;
      *)                err "unknown argument for set: $1 (see --help)" ;;
    esac
  done

  [[ -n "$SIZE$MODE$MAX_EXPOSURE$THRESHOLD$EXIT_MARGIN_V" ]] || \
    err "nothing to change: pass --size, --mode, --max-exposure, --threshold, and/or --exit-margin"

  [[ -z "$SIZE" ]]          || num_or_die --size "$SIZE"
  [[ -z "$MAX_EXPOSURE" ]]  || num_or_die --max-exposure "$MAX_EXPOSURE"
  [[ -z "$THRESHOLD" ]]     || num_or_die --threshold "$THRESHOLD"
  [[ -z "$EXIT_MARGIN_V" ]] || num_or_die --exit-margin "$EXIT_MARGIN_V"
  if [[ -n "$MODE" ]]; then
    MODE="$(printf '%s' "$MODE" | tr '[:upper:]' '[:lower:]')"
    case "$MODE" in live|paper) ;; *) err "--mode must be live or paper, got: $MODE" ;; esac
  fi

  declare -A NEW=()
  [[ -z "$MODE" ]]          || NEW[TRADING_MODE]="$MODE"
  [[ -z "$SIZE" ]]          || NEW[ORDER_SIZE_USDC]="$SIZE"
  [[ -z "$MAX_EXPOSURE" ]]  || NEW[MAX_EXPOSURE_USDC]="$MAX_EXPOSURE"
  [[ -z "$THRESHOLD" ]]     || NEW[EDGE_THRESHOLD]="$THRESHOLD"
  [[ -z "$EXIT_MARGIN_V" ]] || NEW[EXIT_MARGIN]="$EXIT_MARGIN_V"

  echo "Target: ${SSH_USER}@${HOST}:${REMOTE_DIR}/.env"

  declare -A CUR=()
  while IFS='=' read -r k v; do [[ -n "$k" ]] && CUR["$k"]="$v"; done < <(fetch_params)

  echo
  local p
  for p in "${PARAMS[@]}"; do
    if [[ -n "${NEW[$p]:-}" ]]; then
      printf '  %-20s : %s  ->  %s\n' "$p" "${CUR[$p]:-<unset>}" "${NEW[$p]}"
    else
      printf '  %-20s = %s (unchanged)\n' "$p" "${CUR[$p]:-<unset>}"
    fi
  done
  echo

  # Going live without wallet credentials fails at startup — catch it here.
  if [[ "${NEW[TRADING_MODE]:-}" == "live" ]]; then
    local haskey
    haskey="$(run_ssh "grep -Ec '^(PRIVATE_KEY|FUNDER_ADDRESS)=..+' ${REMOTE_DIR}/.env || true")"
    [[ "$haskey" == "2" ]] || \
      err "cannot go live: PRIVATE_KEY and/or FUNDER_ADDRESS empty in remote .env"
  fi

  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] no changes made."
    return 0
  fi

  if [[ "$ASSUME_YES" -ne 1 ]]; then
    local prompt="Apply and recreate trader? [y/N] "
    if [[ "${NEW[TRADING_MODE]:-}" == "live" || \
          ( -z "${NEW[TRADING_MODE]:-}" && "${CUR[TRADING_MODE]:-}" == "live" ) ]]; then
      prompt="This affects a LIVE bot. Apply and recreate trader? [y/N] "
    fi
    read -r -p "$prompt" reply
    [[ "$reply" =~ ^[Yy]$ ]] || { echo "aborted."; return 1; }
  fi

  # Build one remote script: upsert each key in .env, then optionally recreate.
  local remote="set -e; cd ${REMOTE_DIR}"
  for p in "${!NEW[@]}"; do
    remote+="
if grep -q '^${p}=' .env; then sed -i 's|^${p}=.*|${p}=${NEW[$p]}|' .env
else echo '${p}=${NEW[$p]}' >> .env; fi"
  done
  if [[ "$DO_RESTART" -eq 1 ]]; then
    # 'docker restart' would not re-read env_file; force-recreate does.
    remote+="
docker compose -f docker-compose.prod.yml up -d --force-recreate trader
sleep 5
echo
echo '== new trader startup =='
docker logs ${TRADER} --tail 5 2>&1"
  fi

  run_ssh "$remote"
  echo
  if [[ "$DO_RESTART" -eq 1 ]]; then
    echo "Done. Trader recreated with the new config."
  else
    echo ".env updated. Skipping restart (--no-restart) — changes apply on next"
    echo "trader recreate: docker compose -f docker-compose.prod.yml up -d --force-recreate trader"
  fi
}

[[ $# -gt 0 ]] || usage 1
CMD="$1"; shift
case "$CMD" in
  status) cmd_status "$@" ;;
  set)    cmd_set "$@" ;;
  -h|--help) usage 0 ;;
  *)      err "unknown command: $CMD (expected: status | set)" ;;
esac
