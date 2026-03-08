#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

KEYWORD="${1:?keyword required}"
ENGLISH_KEYWORDS="${2:?english_keywords required}"
TEMPLATE_PATH="${3:?excel path required}"
PORT="${DOUYIN_DEBUG_PORT:-9222}"
USER_DATA_DIR="${DOUYIN_USER_DATA_DIR:-/tmp/chrome_live_manual}"
START_URL="${DOUYIN_START_URL:-https://www.douyin.com/jingxuan}"
MANUAL_UNLOCK_WAIT_SECONDS="${DOUYIN_MANUAL_UNLOCK_WAIT_SECONDS:-180}"
MAX_SEARCH_ROUNDS="${DOUYIN_SEARCH_MAX_ROUNDS:-500}"
STALL_LIMIT="${DOUYIN_SEARCH_STALL_LIMIT:-40}"
SEARCH_ATTEMPTS="${DOUYIN_SEARCH_ATTEMPTS:-4}"
SEARCH_ATTEMPT_GATE_WAIT_SECONDS="${DOUYIN_SEARCH_ATTEMPT_GATE_WAIT_SECONDS:-}"
SEARCH_PAGE_TYPES="${DOUYIN_SEARCH_PAGE_TYPES:-general video}"
AWEME_CONCURRENCY="${DOUYIN_AWEME_CONCURRENCY:-4}"
AWEME_WAIT_MS="${DOUYIN_AWEME_WAIT_MS:-12000}"
COMMENT_MAX_VIDEOS="${DOUYIN_COMMENT_MAX_VIDEOS:-}"
COMMENT_CONCURRENCY="${DOUYIN_COMMENT_CONCURRENCY:-4}"
COMMENT_MAX_PER_VIDEO="${DOUYIN_COMMENT_MAX_PER_VIDEO:-40}"
COMMENT_PAGE_SIZE="${DOUYIN_COMMENT_PAGE_SIZE:-20}"
MAX_VIDEOS="${DOUYIN_MAX_VIDEOS:-0}"

if [ ! -f "$TEMPLATE_PATH" ]; then
  echo "ERROR: Excel path not found: $TEMPLATE_PATH" >&2
  exit 1
fi

ENGLISH_SLUG="$(printf '%s' "$ENGLISH_KEYWORDS" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//; s/_{2,}/_/g')"
if [ -z "$ENGLISH_SLUG" ]; then
  echo "ERROR: english_keywords normalizes to an empty slug." >&2
  exit 1
fi

if [ -z "$SEARCH_ATTEMPT_GATE_WAIT_SECONDS" ]; then
  SEARCH_ATTEMPT_GATE_WAIT_SECONDS="$(node -e 'const { chooseSearchAttemptWaitSeconds } = require(process.argv[1]); process.stdout.write(String(chooseSearchAttemptWaitSeconds({ manualUnlockWaitSeconds: Number(process.argv[2]), hiddenBackgroundMode: true })));' "$ROOT_DIR/douyin_search_api_helpers.js" "$MANUAL_UNLOCK_WAIT_SECONDS")"
fi

export DOUYIN_FORCE_FRESH_CHROME="${DOUYIN_FORCE_FRESH_CHROME:-0}"

OUT_DIR="$(cd "$(dirname "$TEMPLATE_PATH")" && pwd)"
DATE_STAMP="$(date +%Y%m%d)"
RUN_STAMP="$(date +%Y%m%d_%H%M%S)"
DEFAULT_FINAL_PATH="${OUT_DIR}/douyin_${ENGLISH_SLUG}_${DATE_STAMP}.xlsx"
FINAL_PATH="${DEFAULT_FINAL_PATH}"
TMP_WORKBOOK="/tmp/douyin_${ENGLISH_SLUG}_${RUN_STAMP}_precomments.xlsx"
LIVE_JSON=""
URLS_JSON=""
AWEME_JSON="/tmp/douyin_aweme_detail_${RUN_STAMP}.json"
COMMENTS_JSON="/tmp/douyin_comments_${RUN_STAMP}.json"
typeset -a SEARCH_ATTEMPT_ITEMS=()

for ((attempt=1; attempt<=SEARCH_ATTEMPTS; attempt++)); do
  ATTEMPT_TAG="$(printf 'a%02d' "$attempt")"
  ATTEMPT_LIVE_JSON="/tmp/douyin_live_keyword_${RUN_STAMP}_${ATTEMPT_TAG}.json"
  ATTEMPT_URLS_JSON="/tmp/douyin_live_keyword_urls_${RUN_STAMP}_${ATTEMPT_TAG}.json"
  ATTEMPT_MERGED_LINK_COUNT="0"
  ATTEMPT_ERROR_STAGE=""
  RECORDED_LIVE_JSON=""
  RECORDED_URLS_JSON=""
  typeset -a ATTEMPT_SURFACE_JSONS=()

  printf 'step=ensure_chrome attempt=%s/%s port=%s user_data_dir=%s\n' "$attempt" "$SEARCH_ATTEMPTS" "$PORT" "$USER_DATA_DIR"
  "${ROOT_DIR}/ensure_douyin_debug_chrome.sh" "$PORT" "$USER_DATA_DIR" "$START_URL" "40"

  for SEARCH_PAGE_TYPE in ${(z)SEARCH_PAGE_TYPES}; do
    SURFACE_LIVE_JSON="/tmp/douyin_live_keyword_${RUN_STAMP}_${ATTEMPT_TAG}_${SEARCH_PAGE_TYPE}.json"
    printf 'step=crawl_search attempt=%s/%s keyword=%s page_type=%s gate_wait_seconds=%s\n' \
      "$attempt" "$SEARCH_ATTEMPTS" "$KEYWORD" "$SEARCH_PAGE_TYPE" "$SEARCH_ATTEMPT_GATE_WAIT_SECONDS"
    if node "${ROOT_DIR}/crawl_douyin_live_keyword.js" \
      "$KEYWORD" "$SURFACE_LIVE_JSON" "$PORT" "$MAX_SEARCH_ROUNDS" "$STALL_LIMIT" 11000 1500 "$SEARCH_ATTEMPT_GATE_WAIT_SECONDS" "$SEARCH_PAGE_TYPE"; then
      ATTEMPT_SURFACE_JSONS+=("$SURFACE_LIVE_JSON")
      SURFACE_LINK_COUNT="$(node -e 'const fs=require("fs"); const data=JSON.parse(fs.readFileSync(process.argv[1], "utf8")); process.stdout.write(String(Number(data.merged_link_count ?? data.link_count ?? 0)));' "$SURFACE_LIVE_JSON")"
      printf 'search_surface_result attempt=%s/%s page_type=%s merged_link_count=%s live_json=%s\n' \
        "$attempt" "$SEARCH_ATTEMPTS" "$SEARCH_PAGE_TYPE" "$SURFACE_LINK_COUNT" "$SURFACE_LIVE_JSON"
    else
      printf 'search_surface_failed attempt=%s/%s page_type=%s live_json=%s\n' \
        "$attempt" "$SEARCH_ATTEMPTS" "$SEARCH_PAGE_TYPE" "$SURFACE_LIVE_JSON" >&2
    fi
  done

  if [ "${#ATTEMPT_SURFACE_JSONS[@]}" -gt 0 ]; then
    printf 'step=merge_search attempt=%s/%s surfaces=%s\n' \
      "$attempt" "$SEARCH_ATTEMPTS" "${(j:,:)ATTEMPT_SURFACE_JSONS}"
    if node -e 'const fs=require("fs"); const { mergeSearchCrawlPayloads } = require(process.argv[1]); const outPath = process.argv[2]; const inputs = process.argv.slice(3).map((path) => JSON.parse(fs.readFileSync(path, "utf8"))); const merged = mergeSearchCrawlPayloads(inputs); fs.writeFileSync(outPath, JSON.stringify(merged, null, 2));' \
      "$ROOT_DIR/douyin_search_api_helpers.js" "$ATTEMPT_LIVE_JSON" "${ATTEMPT_SURFACE_JSONS[@]}"; then
      RECORDED_LIVE_JSON="$ATTEMPT_LIVE_JSON"
      printf 'step=extract_urls attempt=%s/%s\n' "$attempt" "$SEARCH_ATTEMPTS"
      if python "${ROOT_DIR}/extract_video_urls_from_live_keyword_json.py" \
        --input-json "$ATTEMPT_LIVE_JSON" \
        --output-json "$ATTEMPT_URLS_JSON" \
        --max-videos "$MAX_VIDEOS"; then
        RECORDED_URLS_JSON="$ATTEMPT_URLS_JSON"
        ATTEMPT_MERGED_LINK_COUNT="$(node -e 'const fs=require("fs"); const data=JSON.parse(fs.readFileSync(process.argv[1], "utf8")); process.stdout.write(String(Number(data.merged_link_count ?? data.link_count ?? 0)));' "$ATTEMPT_LIVE_JSON")"
        printf 'search_attempt_result attempt=%s/%s merged_link_count=%s live_json=%s urls_json=%s\n' \
          "$attempt" "$SEARCH_ATTEMPTS" "$ATTEMPT_MERGED_LINK_COUNT" "$ATTEMPT_LIVE_JSON" "$ATTEMPT_URLS_JSON"
      else
        ATTEMPT_ERROR_STAGE="extract_urls"
        printf 'search_attempt_failed attempt=%s/%s stage=%s live_json=%s\n' \
          "$attempt" "$SEARCH_ATTEMPTS" "$ATTEMPT_ERROR_STAGE" "$ATTEMPT_LIVE_JSON" >&2
      fi
    else
      ATTEMPT_ERROR_STAGE="merge_search"
      printf 'search_attempt_failed attempt=%s/%s stage=%s live_json=%s\n' \
        "$attempt" "$SEARCH_ATTEMPTS" "$ATTEMPT_ERROR_STAGE" "$ATTEMPT_LIVE_JSON" >&2
    fi
  else
    ATTEMPT_ERROR_STAGE="crawl_search_all_surfaces"
    printf 'search_attempt_failed attempt=%s/%s stage=%s live_json=%s\n' \
      "$attempt" "$SEARCH_ATTEMPTS" "$ATTEMPT_ERROR_STAGE" "$ATTEMPT_LIVE_JSON" >&2
  fi

  SEARCH_ATTEMPT_ITEMS+=("$(node -e 'process.stdout.write(JSON.stringify({ liveJson: process.argv[1], urlsJson: process.argv[2], mergedLinkCount: Number(process.argv[3]) }));' "$RECORDED_LIVE_JSON" "$RECORDED_URLS_JSON" "$ATTEMPT_MERGED_LINK_COUNT")")
done

ATTEMPTS_JSON="[$(IFS=,; printf '%s' "${SEARCH_ATTEMPT_ITEMS[*]}")]"
BEST_ATTEMPT_JSON="$(node -e 'const { chooseBestSearchAttempt } = require(process.argv[1]); const attempts = JSON.parse(process.argv[2]); process.stdout.write(JSON.stringify(chooseBestSearchAttempt(attempts) || {}));' "$ROOT_DIR/douyin_search_api_helpers.js" "$ATTEMPTS_JSON")"
LIVE_JSON="$(node -e 'const data = JSON.parse(process.argv[1]); process.stdout.write(String(data.liveJson || ""));' "$BEST_ATTEMPT_JSON")"
URLS_JSON="$(node -e 'const data = JSON.parse(process.argv[1]); process.stdout.write(String(data.urlsJson || ""));' "$BEST_ATTEMPT_JSON")"
MERGED_LINK_COUNT="$(node -e 'const data = JSON.parse(process.argv[1]); process.stdout.write(String(Number(data.mergedLinkCount || 0)));' "$BEST_ATTEMPT_JSON")"

if [ -z "$LIVE_JSON" ] || [ -z "$URLS_JSON" ] || [ "$MERGED_LINK_COUNT" -le 0 ]; then
  echo "ERROR: search returned zero usable video URLs across ${SEARCH_ATTEMPTS} attempts." >&2
  exit 1
fi

printf 'search_attempt_best attempts=%s merged_link_count=%s live_json=%s urls_json=%s\n' \
  "$SEARCH_ATTEMPTS" "$MERGED_LINK_COUNT" "$LIVE_JSON" "$URLS_JSON"

EXISTING_VIDEO_COUNT="$(python - "$DEFAULT_FINAL_PATH" <<'PY'
import os
import sys
from openpyxl import load_workbook

path = sys.argv[1]
if not os.path.exists(path):
    print(-1)
    raise SystemExit

try:
    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb['Video_Data']
    count = 0
    first = True
    for row in ws.iter_rows(values_only=True):
        if first:
            first = False
            continue
        if any(v not in (None, '') for v in row):
            count += 1
    print(count)
except Exception:
    print(-1)
PY
)"
OUTPUT_META="$(node -e 'const { chooseWorkbookOutputPath } = require(process.argv[1]); const res = chooseWorkbookOutputPath({ defaultPath: process.argv[2], runStamp: process.argv[3], existingDiscoveryCount: Number(process.argv[4]), newDiscoveryCount: Number(process.argv[5]) }); process.stdout.write(JSON.stringify(res));' "$ROOT_DIR/douyin_search_api_helpers.js" "$DEFAULT_FINAL_PATH" "$RUN_STAMP" "$EXISTING_VIDEO_COUNT" "$MERGED_LINK_COUNT")"
FINAL_PATH="$(node -e 'const data = JSON.parse(process.argv[1]); process.stdout.write(String(data.outputPath || ""));' "$OUTPUT_META")"
OVERWRITE_ALLOWED="$(node -e 'const data = JSON.parse(process.argv[1]); process.stdout.write(data.overwriteAllowed ? "1" : "0");' "$OUTPUT_META")"
OUTPUT_REASON="$(node -e 'const data = JSON.parse(process.argv[1]); process.stdout.write(String(data.reason || ""));' "$OUTPUT_META")"
printf 'search_coverage merged_link_count=%s existing_video_count=%s chosen_output=%s overwrite_allowed=%s reason=%s\n' \
  "$MERGED_LINK_COUNT" "$EXISTING_VIDEO_COUNT" "$FINAL_PATH" "$OVERWRITE_ALLOWED" "$OUTPUT_REASON"

printf 'step=crawl_aweme_detail\n'
node "${ROOT_DIR}/crawl_douyin_aweme_detail_from_urls_cdp.js" \
  "$URLS_JSON" "$AWEME_JSON" "$PORT" "$MAX_VIDEOS" "$AWEME_CONCURRENCY" "$AWEME_WAIT_MS"

printf 'step=build_workbook\n'
python "${ROOT_DIR}/build_workbook_from_aweme_json.py" \
  --template-path "$TEMPLATE_PATH" \
  --input-json "$AWEME_JSON" \
  --output-path "$TMP_WORKBOOK" \
  --keyword "$KEYWORD"

printf 'step=crawl_comments\n'
node "${ROOT_DIR}/crawl_douyin_comments_via_api.js" \
  "$URLS_JSON" "$COMMENTS_JSON" "$PORT" "${COMMENT_MAX_VIDEOS:-$MAX_VIDEOS}" "$COMMENT_MAX_PER_VIDEO" "$COMMENT_PAGE_SIZE" "$COMMENT_CONCURRENCY" "https://www.douyin.com/"

printf 'step=fill_comments\n'
python "${ROOT_DIR}/fill_comment_sheet_from_json.py" \
  --workbook-path "$TMP_WORKBOOK" \
  --comments-json "$COMMENTS_JSON" \
  --output-path "$FINAL_PATH"

printf 'output=%s\n' "$FINAL_PATH"
printf 'template=%s\n' "$TEMPLATE_PATH"
printf 'live_json=%s\n' "$LIVE_JSON"
printf 'urls_json=%s\n' "$URLS_JSON"
printf 'aweme_json=%s\n' "$AWEME_JSON"
printf 'comments_json=%s\n' "$COMMENTS_JSON"
