---
name: tiktok-mcp-excel-pipeline
description: Run the TikTok MCP extraction and Excel-filling workflow for one or more TikTok URLs. Use when the user asks to process TikTok video links into `tiktok_videoid_mcp/` folders with metadata, frames, HD storyboards, ASR transcript JSON, and a filled Climate SVP workbook in Data analysis outputs.
---

# TikTok MCP Excel Pipeline

Use the bundled script:
- `scripts/tiktok_mcp_pipeline.py`

Recommended setup:

```bash
SKILL_DIR="${SKILL_DIR:-$HOME/.config/opencode/skills/tiktok-mcp-excel-pipeline}"
TEMPLATE_PATH="/absolute/path/to/Climate_SVP_MCP_Template_transcript_v3.xlsx"
OUTPUT_ROOT="/absolute/path/to/output-root"
```

If you installed the skill somewhere else, set `SKILL_DIR` to that location before running the commands below.

## Run One URL

```bash
python "$SKILL_DIR/scripts/tiktok_mcp_pipeline.py" \
  --url "<TIKTOK_URL>" \
  --template-path "$TEMPLATE_PATH" \
  --output-root "$OUTPUT_ROOT"
```

Default ASR behavior:
- `--asr-backend auto`
- tries `faster-whisper` first
- falls back to local `whisper` CLI if `faster-whisper` fails on the machine
- if no `--whisper-cli-model` is passed, long audio on CPU may auto-select `tiny` instead of `turbo`

## Run Multiple URLs

Create a text file with one URL per line, then run:

```bash
while IFS= read -r url; do
  [ -z "$url" ] && continue
  python "$SKILL_DIR/scripts/tiktok_mcp_pipeline.py" \
    --url "$url" \
    --template-path "$TEMPLATE_PATH" \
    --output-root "$OUTPUT_ROOT"
done < urls.txt
```

Optional ASR override:

```bash
python "$SKILL_DIR/scripts/tiktok_mcp_pipeline.py" \
  --url "<TIKTOK_URL>" \
  --template-path "$TEMPLATE_PATH" \
  --output-root "$OUTPUT_ROOT" \
  --asr-backend whisper-cli \
  --whisper-cli-model turbo
```

Long-audio speed override:

Use this when the source audio is long and faster turnaround matters more than maximum transcript quality.

```bash
python "$SKILL_DIR/scripts/tiktok_mcp_pipeline.py" \
  --url "<TIKTOK_URL>" \
  --template-path "$TEMPLATE_PATH" \
  --output-root "$OUTPUT_ROOT" \
  --asr-backend whisper-cli \
  --whisper-cli-model tiny
```

## Output Contract

For each URL, script creates:
- `tiktok_<videoid>_mcp/meta.json`
- `tiktok_<videoid>_mcp/frames/`
- `tiktok_<videoid>_mcp/storyboard_hd.png`
- `tiktok_<videoid>_mcp/storyboard_ts_hd.png`
- `tiktok_<videoid>_mcp/audio.wav`
- `tiktok_<videoid>_mcp/asr.json`
- `tiktok_<videoid>_mcp/Climate_SVP_MCP_<videoid>_transcript_v3_filled.xlsx`

For Douyin cookie-gated fallback runs, the final folder also keeps:
- `Douyin_<videoid>_mcp/douyin_<videoid>_raw.json`

Workbook population includes:
- `Video_Summary`
- `Engagement_Snapshots`
- `Transcript_Segments`
- `OCR_Events`
- `Scenes`
- `Cuts`
- `Summary_Card` with `B3 = <videoid>`

## HD Storyboard Policy

Default behavior:
- Keep HD PNG storyboards only.
- Delete legacy JPG storyboard files after generation.

If JPG storyboards are needed:

```bash
python "$SKILL_DIR/scripts/tiktok_mcp_pipeline.py" \
  --url "<TIKTOK_URL>" \
  --template-path "$TEMPLATE_PATH" \
  --output-root "$OUTPUT_ROOT" \
  --keep-jpg-storyboards
```

## Temporary File Policy

The script uses `/tmp` and auto-cleans:
- `/tmp/tiktok_<videoid>.*`
- `/tmp/tiktok_<videoid>_*`

If the URL cannot be accessed/downloaded, the run fails without writing a partial output folder to `--output-root`.

## Validation Commands

List generated files for one video ID:

```bash
ls -1 "$OUTPUT_ROOT/tiktok_<videoid>_mcp"
```

Check `Summary_Card!B3`:

```bash
python - "$OUTPUT_ROOT" <<'PY'
from openpyxl import load_workbook
from pathlib import Path
import sys

video_id = "<videoid>"
output_root = Path(sys.argv[1])
wb = load_workbook(
    output_root / f"tiktok_{video_id}_mcp" / f"Climate_SVP_MCP_{video_id}_transcript_v3_filled.xlsx",
    data_only=True,
)
print(wb["Summary_Card"]["B3"].value)
PY
```

## Notes

- Works for normal video streams and audio-only extractor cases (audio+thumbnail fallback video is auto-generated).
- Do not keep downloaded source videos in final output folders; script handles this with `/tmp` cleanup.
- When a Douyin local fallback raw JSON is used, the pipeline copies that file into the final `Douyin_<videoid>_mcp/` folder so the bundle is self-contained.
- `asr.json` and workbook transcript fields now record the backend actually used. If `faster-whisper` crashes, the pipeline should still complete via `whisper` CLI unless both backends fail.
- If `--whisper-cli-model` is omitted, the pipeline keeps `turbo` for shorter audio and auto-selects `tiny` for long audio on CPU (current threshold: 15 minutes). Pass `--whisper-cli-model` explicitly to override that policy.
