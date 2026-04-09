---
name: dy-mcp-excel-pipeline
description: Use when processing Douyin/TikTok links from an Excel sheet into MCP folders and filled Climate SVP workbooks, especially for row-based DY runs.
---

# DY MCP Excel Pipeline

## Overview

Use this skill when the user gives Excel row numbers and wants URLs from that sheet processed into MCP output folders.
For the DY round workbook, the correct worksheet is usually `Video_Data` and URLs are in column `D`.

Bundled script:
- `scripts/dy_mcp_from_excel.py`

Companion pipeline skill:
- `tiktok-mcp-excel-pipeline`

Recommended setup:

```bash
DY_SKILL_DIR="${DY_SKILL_DIR:-$HOME/.config/opencode/skills/dy-mcp-excel-pipeline}"
TIKTOK_SKILL_DIR="${TIKTOK_SKILL_DIR:-$HOME/.config/opencode/skills/tiktok-mcp-excel-pipeline}"
EXCEL_PATH="/absolute/path/to/input.xlsx"
TEMPLATE_PATH="/absolute/path/to/Climate_SVP_MCP_Template_transcript_v3.xlsx"
OUTPUT_ROOT="/absolute/path/to/output-root"
```

If you installed either skill somewhere else, set `DY_SKILL_DIR` and `TIKTOK_SKILL_DIR` before running the commands below.

## Run Rows From Excel

```bash
python "$DY_SKILL_DIR/scripts/dy_mcp_from_excel.py" \
  --excel-path "$EXCEL_PATH" \
  --sheet-name "Video_Data" \
  --rows "101" \
  --url-col "D" \
  --template-path "$TEMPLATE_PATH" \
  --output-root "$OUTPUT_ROOT" \
  --pipeline-script "$TIKTOK_SKILL_DIR/scripts/tiktok_mcp_pipeline.py"
```

Default ASR behavior:
- `--asr-backend auto`
- tries `faster-whisper` first
- falls back to local `whisper` CLI if `faster-whisper` crashes or is unavailable
- if no `--whisper-cli-model` is passed, long audio on CPU may auto-select `tiny` instead of `turbo`

Multiple rows:

```bash
python "$DY_SKILL_DIR/scripts/dy_mcp_from_excel.py" \
  --excel-path "<EXCEL_PATH>" \
  --sheet-name "Video_Data" \
  --rows "101,102,110-115" \
  --url-col "D" \
  --template-path "$TEMPLATE_PATH" \
  --output-root "$OUTPUT_ROOT" \
  --pipeline-script "$TIKTOK_SKILL_DIR/scripts/tiktok_mcp_pipeline.py" \
  --continue-on-error
```

Optional ASR override:

```bash
python "$DY_SKILL_DIR/scripts/dy_mcp_from_excel.py" \
  --excel-path "<EXCEL_PATH>" \
  --sheet-name "Video_Data" \
  --rows "101" \
  --url-col "D" \
  --template-path "$TEMPLATE_PATH" \
  --output-root "$OUTPUT_ROOT" \
  --pipeline-script "$TIKTOK_SKILL_DIR/scripts/tiktok_mcp_pipeline.py" \
  --asr-backend whisper-cli \
  --whisper-cli-model turbo
```

Long-audio speed override:

Use this when the source audio is long (for example, a long-form video or a run that would otherwise spend a long time in CPU ASR) and faster turnaround matters more than maximum transcript quality.

```bash
python "$DY_SKILL_DIR/scripts/dy_mcp_from_excel.py" \
  --excel-path "<EXCEL_PATH>" \
  --sheet-name "Video_Data" \
  --rows "101" \
  --url-col "D" \
  --template-path "$TEMPLATE_PATH" \
  --output-root "$OUTPUT_ROOT" \
  --pipeline-script "$TIKTOK_SKILL_DIR/scripts/tiktok_mcp_pipeline.py" \
  --asr-backend whisper-cli \
  --whisper-cli-model tiny
```

## Run One Row To New Folder (Recommended)

Use a timestamped output root so each run is isolated in a new folder.

```bash
OUT_ROOT="$OUTPUT_ROOT/row238_matrix_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT_ROOT"

python "$DY_SKILL_DIR/scripts/dy_mcp_from_excel.py" \
  --excel-path "$EXCEL_PATH" \
  --sheet-name "Video_Data" \
  --rows "238" \
  --url-col "D" \
  --template-path "$TEMPLATE_PATH" \
  --output-root "$OUT_ROOT" \
  --pipeline-script "$TIKTOK_SKILL_DIR/scripts/tiktok_mcp_pipeline.py"
```

Expected output path format:
- `"$OUT_ROOT/Douyin_<videoid>_mcp/"`

Quick check for row URL:

```bash
python - "$EXCEL_PATH" <<'PY'
from openpyxl import load_workbook
import sys

wb = load_workbook(sys.argv[1], read_only=True, data_only=True)
ws = wb['Video_Data']
print(ws['D238'].value)
PY
```

## Folder Naming

- Douyin URLs -> `Douyin_<videoid>_mcp`
- TikTok URLs -> `tiktok_<videoid>_mcp`

## Output Contract

Each video folder contains:
- `meta.json`
- `frames/`
- `storyboard_hd.png`
- `storyboard_ts_hd.png`
- `audio.wav`
- `asr.json`
- `Climate_SVP_MCP_<videoid>_transcript_v3_filled.xlsx`

For Douyin cookie-gated fallback runs, the final folder also keeps:
- `douyin_<videoid>_raw.json`

Workbook check:
- `Summary_Card!B3 = <videoid>`

Failure behavior:
- If a video URL cannot be accessed/downloaded, the row is treated as failed and no new output folder/files are kept for that row.

## Notes

- Source videos are temporary (`/tmp`) and not kept in final output folders.
- For Douyin cookie-gated cases, the pipeline has local fallback metadata/play-url logic.
- When a Douyin local fallback raw JSON is used, it is also copied into the final `Douyin_<videoid>_mcp/` folder so each bundle stays self-contained.
- ASR now has automatic backend fallback, so a `faster-whisper` crash should not abort the whole MCP run if local `whisper` CLI is available.
- For long audio on CPU, prefer `--asr-backend whisper-cli --whisper-cli-model tiny` when turnaround matters. Treat this as a speed-first override, not a universal default, because smaller models can reduce transcript quality.
- If `--whisper-cli-model` is omitted, the underlying pipeline keeps `turbo` for shorter audio and auto-selects `tiny` for long audio on CPU (current threshold: 15 minutes). Pass `--whisper-cli-model` explicitly to override that policy.
- This skill expects `tiktok-mcp-excel-pipeline` to be available and passed in with `--pipeline-script`, or installed at the default sibling OpenCode skill path.
- If network access is restricted in sandbox, rerun the same command with escalated permissions.
