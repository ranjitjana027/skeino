"""Tests for SSE encoding and retry classification."""

import json

from skeino.streaming import (
    STREAM_MAX_RETRIES,
    STREAM_RETRY_BACKOFF_SECS,
    is_retriable_stream_error,
    sse_event,
)


def test_sse_event_shape() -> None:
    chunk = sse_event("values", {"a": 1}, 7)
    assert chunk.startswith("id: 7\n")
    assert "event: values\n" in chunk
    assert chunk.endswith("\n\n")
    data_line = next(line for line in chunk.splitlines() if line.startswith("data: "))
    payload = json.loads(data_line[6:])
    assert payload == {"a": 1}


def test_retry_constants_have_sensible_defaults() -> None:
    assert STREAM_MAX_RETRIES >= 1
    assert STREAM_RETRY_BACKOFF_SECS > 0


def test_is_retriable_stream_error_detects_transient_classes() -> None:
    assert is_retriable_stream_error(TimeoutError("read timed out"))
    assert is_retriable_stream_error(OSError("connection reset"))
    assert is_retriable_stream_error(Exception("ssl handshake failed"))
    assert is_retriable_stream_error(Exception("could not receive data from server"))


def test_is_retriable_stream_error_skips_permanent_errors() -> None:
    assert not is_retriable_stream_error(ValueError("bad input"))
    assert not is_retriable_stream_error(KeyError("missing"))
