#!/bin/zsh
set -euo pipefail

PORT="${1:-9222}"
USER_DATA_DIR="${2:-/tmp/chrome_live_manual}"
START_URL="${3:-https://www.douyin.com/jingxuan}"
WAIT_SECONDS="${4:-40}"
LOG_PATH="${DOUYIN_DEBUG_CHROME_LOG:-/tmp/douyin_keyword_excel_chrome.log}"
VERSION_URL="http://127.0.0.1:${PORT}/json/version"
CHROME_APP="${DOUYIN_DEBUG_CHROME_APP:-/Applications/Google Chrome.app}"
FORCE_FRESH="${DOUYIN_FORCE_FRESH_CHROME:-1}"

if [ "$FORCE_FRESH" != "0" ]; then
  for pid in $(ps aux | rg -- "--remote-debugging-port=${PORT}" | rg -v rg | awk '{print $2}'); do
    kill "$pid" 2>/dev/null || true
  done
  sleep 1
fi

if curl -s "$VERSION_URL" | rg -q 'webSocketDebuggerUrl'; then
  echo "chrome_status=reused port=${PORT} user_data_dir=${USER_DATA_DIR}"
  exit 0
fi

open -j -g -n -a "${CHROME_APP}" \
  --stdout "${LOG_PATH}" \
  --stderr "${LOG_PATH}" \
  --args \
  --remote-debugging-port="${PORT}" \
  --user-data-dir="${USER_DATA_DIR}" \
  --no-first-run \
  --no-default-browser-check \
  "${START_URL}" >/dev/null 2>&1 &

for _ in $(seq 1 "$WAIT_SECONDS"); do
  if curl -s "$VERSION_URL" | rg -q 'webSocketDebuggerUrl'; then
    echo "chrome_status=started port=${PORT} user_data_dir=${USER_DATA_DIR}"
    exit 0
  fi
  sleep 1
done

echo "ERROR: Chrome DevTools endpoint not available on port ${PORT}." >&2
exit 2
