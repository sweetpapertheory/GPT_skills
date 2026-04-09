import sys
import tempfile
import unittest
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from douyin_raw_publish import publish_douyin_raw_json  # type: ignore


class PublishDouyinRawJsonTests(unittest.TestCase):
    def test_copies_raw_json_into_output_folder_preserving_filename(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            src = tmp_path / "douyin_123_raw.json"
            out_dir = tmp_path / "Douyin_123_mcp"
            src.write_text('{"aweme_id":"123"}', encoding="utf-8")

            copied = publish_douyin_raw_json(src, out_dir)

            self.assertEqual(copied, out_dir / src.name)
            self.assertTrue(copied.exists())
            self.assertEqual(copied.read_text(encoding="utf-8"), '{"aweme_id":"123"}')

    def test_missing_source_returns_none_without_creating_output(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            src = tmp_path / "douyin_999_raw.json"
            out_dir = tmp_path / "Douyin_999_mcp"

            copied = publish_douyin_raw_json(src, out_dir)

            self.assertIsNone(copied)
            self.assertFalse(out_dir.exists())


if __name__ == "__main__":
    unittest.main()
