import sys
import unittest
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from asr_policy import (  # type: ignore
    DEFAULT_WHISPER_CLI_MODEL,
    LONG_AUDIO_THRESHOLD_SEC,
    LONG_AUDIO_WHISPER_CLI_MODEL,
    resolve_whisper_cli_model,
)


class ResolveWhisperCliModelTests(unittest.TestCase):
    def test_short_audio_keeps_default_model(self):
        model, reason = resolve_whisper_cli_model(
            requested_model=None,
            asr_backend="auto",
            duration_sec=LONG_AUDIO_THRESHOLD_SEC - 1,
        )

        self.assertEqual(model, DEFAULT_WHISPER_CLI_MODEL)
        self.assertIsNone(reason)

    def test_long_audio_uses_smaller_model_when_not_explicit(self):
        model, reason = resolve_whisper_cli_model(
            requested_model=None,
            asr_backend="auto",
            duration_sec=LONG_AUDIO_THRESHOLD_SEC,
        )

        self.assertEqual(model, LONG_AUDIO_WHISPER_CLI_MODEL)
        self.assertIn("long audio", reason.lower())

    def test_explicit_model_is_preserved_for_long_audio(self):
        model, reason = resolve_whisper_cli_model(
            requested_model="turbo",
            asr_backend="whisper-cli",
            duration_sec=LONG_AUDIO_THRESHOLD_SEC * 2,
        )

        self.assertEqual(model, "turbo")
        self.assertIsNone(reason)

    def test_faster_whisper_backend_does_not_trigger_whisper_cli_policy(self):
        model, reason = resolve_whisper_cli_model(
            requested_model=None,
            asr_backend="faster-whisper",
            duration_sec=LONG_AUDIO_THRESHOLD_SEC * 2,
        )

        self.assertEqual(model, DEFAULT_WHISPER_CLI_MODEL)
        self.assertIsNone(reason)


if __name__ == "__main__":
    unittest.main()
