import shutil
from pathlib import Path


def publish_douyin_raw_json(raw_json_path, out_dir):
    if not raw_json_path:
        return None

    src = Path(raw_json_path)
    if not src.is_file():
        return None

    dest_dir = Path(out_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    shutil.copy2(src, dest)
    return dest
