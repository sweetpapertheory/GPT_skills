#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import quote

from douyin_workbook_lib import (
    SourceSpec,
    map_user_record,
    map_video_record,
    normalize_space,
    write_workbook,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='Build workbook from authenticated Douyin aweme detail JSON')
    p.add_argument('--template-path', required=True)
    p.add_argument('--input-json', required=True)
    p.add_argument('--output-path', required=True)
    p.add_argument('--keyword', required=True)
    return p.parse_args()


def enrich_author_fields(author: dict) -> dict:
    if not isinstance(author, dict):
        return {}
    out = dict(author)
    if out.get('favoriting_count') and not out.get('total_favorited'):
        out['total_favorited'] = out['favoriting_count']
    return out


def main() -> None:
    args = parse_args()

    template_path = Path(args.template_path).expanduser()
    input_json = Path(args.input_json).expanduser()
    output_path = Path(args.output_path).expanduser()

    if not template_path.exists():
        raise FileNotFoundError(f'template not found: {template_path}')
    if not input_json.exists():
        raise FileNotFoundError(f'input json not found: {input_json}')

    payload = json.loads(input_json.read_text(encoding='utf-8'))
    records = payload.get('records') or []
    if not records:
        raise RuntimeError('aweme detail JSON contains zero successful records')

    processing_date = date.today().isoformat()
    started_at = datetime.now(tz=timezone.utc)

    video_rows: list[dict] = []
    user_rows: list[dict] = []
    seen_video_keys: set[str] = set()
    seen_users: set[str] = set()

    for rec in records:
        aweme = rec.get('aweme_detail')
        if not isinstance(aweme, dict):
            continue

        aweme_copy = dict(aweme)
        if normalize_space(rec.get('video_id')) and not normalize_space(aweme_copy.get('aweme_id')):
            aweme_copy['aweme_id'] = normalize_space(rec.get('video_id'))
        if normalize_space(rec.get('video_url')) and not normalize_space(aweme_copy.get('share_url')):
            aweme_copy['share_url'] = normalize_space(rec.get('video_url'))

        author = enrich_author_fields(aweme_copy.get('author') or {})
        user_row = map_user_record(author=author, profile={})
        username = normalize_space(user_row.get('username'))
        if username and username not in seen_users:
            seen_users.add(username)
            user_rows.append(user_row)

        aweme_copy['author'] = author
        video_row = map_video_record(
            aweme=aweme_copy,
            user=user_row,
            processing_date=processing_date,
            source_label=f'keyword:{args.keyword}',
        )
        if not normalize_space(video_row.get('video_url')):
            video_row['video_url'] = normalize_space(rec.get('video_url'))

        key = normalize_space(video_row.get('video_id')) or normalize_space(video_row.get('video_url'))
        if not key or key in seen_video_keys:
            continue
        seen_video_keys.add(key)
        video_rows.append(video_row)

    ended_at = datetime.now(tz=timezone.utc)

    if not video_rows:
        raise RuntimeError('no video rows were produced from aweme detail JSON')

    sources = [
        SourceSpec(
            label=f'keyword:{args.keyword}',
            url=f'https://www.douyin.com/search/{quote(args.keyword, safe="")}?type=video',
            source_type='keyword',
        )
    ]

    write_workbook(
        template_path=template_path,
        output_path=output_path,
        sources=sources,
        video_rows=video_rows,
        comment_rows=[],
        user_rows=user_rows,
        started_at=started_at,
        ended_at=ended_at,
    )

    print(f'output={output_path}')
    print(f'videos={len(video_rows)} users={len(user_rows)} comments=0')


if __name__ == '__main__':
    main()
