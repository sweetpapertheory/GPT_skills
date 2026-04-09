DEFAULT_WHISPER_CLI_MODEL = "turbo"
LONG_AUDIO_WHISPER_CLI_MODEL = "tiny"
LONG_AUDIO_THRESHOLD_SEC = 15 * 60


def resolve_whisper_cli_model(requested_model, asr_backend, duration_sec):
    if requested_model:
        return requested_model, None

    if asr_backend not in {"auto", "whisper-cli"}:
        return DEFAULT_WHISPER_CLI_MODEL, None

    if duration_sec is None:
        return DEFAULT_WHISPER_CLI_MODEL, None

    try:
        duration_value = float(duration_sec)
    except (TypeError, ValueError):
        return DEFAULT_WHISPER_CLI_MODEL, None

    if duration_value >= LONG_AUDIO_THRESHOLD_SEC:
        return (
            LONG_AUDIO_WHISPER_CLI_MODEL,
            (
                f"Auto-selected whisper CLI model '{LONG_AUDIO_WHISPER_CLI_MODEL}' "
                f"for long audio ({duration_value:.1f}s >= {LONG_AUDIO_THRESHOLD_SEC}s threshold)."
            ),
        )

    return DEFAULT_WHISPER_CLI_MODEL, None
