"""Server-sent-event encoding and stream-retry classification."""

import json
from typing import Final

from skeino.schemas import JsonValue

STREAM_MAX_RETRIES: Final[int] = 3
STREAM_RETRY_BACKOFF_SECS: Final[float] = 2.0


def _json_dumps(payload: dict[str, JsonValue]) -> str:
    """Serialize a JSON-safe payload for SSE output."""
    return json.dumps(payload, separators=(",", ":"))


def sse_event(event: str, data: dict[str, JsonValue], event_id: int | None) -> str:
    """Format a server-sent event chunk.

    ``event_id`` of ``None`` omits the ``id:`` line — for an event synthesised
    after the fact (e.g. a finished run's final state on join), which has no
    place in the run's event sequence and so cannot be resumed from.
    """
    body = f"event: {event}\ndata: {_json_dumps(data)}\n\n"
    return body if event_id is None else f"id: {event_id}\n{body}"


def is_retriable_stream_error(exc: BaseException) -> bool:
    """Return True for transient errors worth retrying during graph streaming."""
    msg = str(exc).lower()
    if "timeout" in msg or "timed out" in msg:
        return True
    if "ssl" in msg or "syscall" in msg or "connection" in msg:
        return True
    if "could not receive data from server" in msg:
        return True
    return False
