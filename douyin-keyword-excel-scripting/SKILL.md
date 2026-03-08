---
name: douyin-keyword-excel-scripting
description: Use when a user wants a Douyin keyword run turned into a filled Excel workbook, especially when the workflow must reuse or launch a logged-in Chrome debug session and write a new `douyin_english_keywords_YYYYMMDD.xlsx` file.
---

# Douyin Keyword Excel Scripting

## Overview

Use this skill when the user provides a Douyin keyword and an Excel workbook path and wants the full Douyin extraction pipeline run end-to-end.
The bundled wrapper launches or reuses Chrome on port `9222`, crawls search results, captures authenticated aweme detail JSON, retrieves comments through Douyin's authenticated comment API, and writes a new workbook beside the input file.

## Required Inputs

Collect these before running the pipeline:
- Raw Douyin keyword, usually Chinese, for example `碳排放`
- Absolute Excel path to use as the template workbook

For the output filename, derive `english_keywords` yourself unless the user already gave one:
- Translate the keyword to concise English when possible
- Convert to lowercase snake_case
- Use meaning, not pinyin, when the English translation is clear
- Example: `碳排放` -> `carbon_emissions`

## Run The Pipeline

From the skill folder, run:

```bash
./scripts/run_douyin_keyword_excel_pipeline.sh "<keyword>" "<english_keywords>" "<excel_path>"
```

Example:

```bash
./scripts/run_douyin_keyword_excel_pipeline.sh \
  "碳排放" \
  "carbon_emissions" \
  "/Users/XM/Desktop/Data/DY data/01_collection/douyin_topic_batches/douyin_climate_change_20260215_R.xlsx"
```

The wrapper writes:
- `douyin_<english_keywords>_<YYYYMMDD>.xlsx`

in the same folder as the input workbook.

## Workflow Contract

The wrapper performs these steps in order:
1. Ensure Chrome remote debugging is reachable on `127.0.0.1:9222`, reusing an existing logged-in session when available.
2. Open Douyin search for the keyword and wait for login or verification to clear when necessary.
3. Extract video URLs from the search results.
4. Capture authenticated `/aweme/v1/web/aweme/detail/` responses for those URLs.
5. Build a workbook with `Video_Data`, `User_Data`, `Daily_Distribution`, and `Processing_Summary` filled.
6. Fetch comments for the discovered videos through the same logged-in Chrome session using Douyin's authenticated `comment/list` API.
7. Fill `Comment_Data` and save the final workbook.

## Failure Rules

Stop and report the actual blocker if any of these happen:
- Chrome debug endpoint does not start
- Douyin search remains on login shell or verification gate
- Search returns zero video URLs
- Aweme detail capture returns zero successful records

Do not claim success or leave the user with a misleading final workbook name if the pipeline did not finish.

## Verification

After the wrapper finishes, verify with direct evidence:
- the output workbook exists at the expected path
- `Processing_Summary` shows the keyword in `Collection Focus`
- `Video_Data`, `Comment_Data`, and `User_Data` have non-zero row counts when the crawl succeeded

## Bundled Scripts

- `scripts/run_douyin_keyword_excel_pipeline.sh`: end-to-end entrypoint
- `scripts/ensure_douyin_debug_chrome.sh`: launch or reuse Chrome debug session
- `scripts/crawl_douyin_live_keyword.js`: keyword search crawler with gate detection
- `scripts/extract_video_urls_from_live_keyword_json.py`: convert search results to URL rows
- `scripts/crawl_douyin_aweme_detail_from_urls_cdp.js`: authenticated aweme-detail capture without Playwright dependency
- `scripts/crawl_douyin_comments_via_api.js`: direct authenticated comment API crawler against discovered video IDs
- `scripts/douyin_workbook_lib.py`: workbook mapping and writing helpers
- `scripts/build_workbook_from_aweme_json.py`: build workbook from aweme-detail JSON
- `scripts/fill_comment_sheet_from_json.py`: write filtered comments into the workbook
