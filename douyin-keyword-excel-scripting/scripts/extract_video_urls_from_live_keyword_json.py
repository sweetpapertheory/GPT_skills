#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

VIDEO_RE = re.compile(r'/video/(\d{8,24})')


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='Extract video_id/video_url rows from live keyword crawl JSON')
    p.add_argument('--input-json', required=True)
    p.add_argument('--output-json', required=True)
    p.add_argument('--max-videos', type=int, default=0, help='0 means all videos')
    return p.parse_args()


def parse_video_id(url: str) -> str:
    m = VIDEO_RE.search(str(url or ''))
    return m.group(1) if m else ''


def normalize_url(url: str) -> str:
    return str(url or '').split('?', 1)[0].strip()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input_json).expanduser()
    output_path = Path(args.output_json).expanduser()

    if not input_path.exists():
        raise FileNotFoundError(f'input json not found: {input_path}')

    payload = json.loads(input_path.read_text(encoding='utf-8'))
    links = payload.get('links') or []

    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in links:
        if not isinstance(item, dict):
            continue
        video_url = normalize_url(item.get('url') or item.get('video_url') or '')
        if not video_url:
            continue
        video_id = parse_video_id(video_url)
        key = video_id or video_url
        if not key or key in seen:
            continue
        seen.add(key)
        rows.append({'video_id': video_id, 'video_url': video_url})
        if args.max_videos > 0 and len(rows) >= args.max_videos:
            break

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding='utf-8')

    print(f'output={output_path}')
    print(f'videos={len(rows)}')


if __name__ == '__main__':
    main()
