"""Unit tests for the per-run event fan-out and resumable replay buffer."""

import pytest

from skeino.streaming import (
    RunEventStream,
    RunStreamRegistry,
    sse_event,
    stream_mode_matches,
)
from skeino.streaming.run_streams import SubscriberOverflowError


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


async def _collect(stream: RunEventStream, after: int | None) -> list[int]:
    return [e.event_id async for e in stream.subscribe(after=after)]


def test_sse_event_without_id_omits_the_id_line() -> None:
    assert sse_event("end", {"a": 1}, None) == 'event: end\ndata: {"a":1}\n\n'
    assert sse_event("end", {}, 7).startswith("id: 7\nevent: end\n")


@pytest.mark.parametrize(
    ("event", "modes", "expected"),
    [
        ("values", [], True),
        ("updates", ["values"], False),
        ("values", ["values"], True),
        ("metadata", ["values"], True),
        ("error", ["values"], True),
        ("end", ["updates"], True),
        ("messages", ["messages-tuple"], True),
        ("messages/partial", ["messages"], True),
        ("messages", ["values"], False),
        ("values|sub:1", ["values"], True),
        ("updates|sub:1", ["values"], False),
        ("custom", ["custom", "values"], True),
    ],
)
def test_stream_mode_matches(event: str, modes: list[str], expected: bool) -> None:
    assert stream_mode_matches(event, modes) is expected


async def test_resumable_stream_replays_after_id_then_ends_on_close() -> None:
    stream = RunEventStream("t", "r", resumable=True)
    for _ in range(3):
        stream.publish("values", {})
    live = stream.subscribe(after=1)  # replay 2, 3; then live
    stream.publish("end", {})
    stream.close(now=0.0)
    assert [e.event_id async for e in live] == [2, 3, 4]
    # After close: replay still works, nothing live, no hang.
    assert await _collect(stream, -1) == [1, 2, 3, 4]
    assert await _collect(stream, None) == []


async def test_resumable_history_evicts_oldest_events_and_expires_old_cursors() -> None:
    stream = RunEventStream(
        "t", "r", resumable=True, max_history_events=2, max_history_bytes=10_000
    )
    for _ in range(4):
        stream.publish("values", {})
    stream.close(now=0.0)

    assert stream.cursor_expired(-1)
    assert not stream.cursor_expired(2)
    assert await _collect(stream, 2) == [3, 4]


async def test_resumable_history_evicts_frames_over_the_byte_budget() -> None:
    stream = RunEventStream(
        "t", "r", resumable=True, max_history_events=10, max_history_bytes=1
    )
    stream.publish("values", {"payload": "x"})
    stream.close(now=0.0)

    assert stream.cursor_expired(-1)
    assert not stream.cursor_expired(1)
    assert await _collect(stream, 1) == []


async def test_non_resumable_stream_keeps_no_history() -> None:
    stream = RunEventStream("t", "r", resumable=False)
    stream.publish("values", {})
    live = stream.subscribe(after=-1)
    stream.publish("end", {})
    stream.close(now=0.0)
    assert [e.event_id async for e in live] == [2]


async def test_subscriber_is_detached_when_drained_or_closed_early() -> None:
    stream = RunEventStream("t", "r", resumable=True)
    first = stream.subscribe(after=None)
    second = stream.subscribe(after=None)
    assert stream.subscriber_count == 2
    stream.publish("values", {})
    assert (await first.__anext__()).event_id == 1
    await first.aclose()
    assert stream.subscriber_count == 1
    stream.close(now=0.0)
    assert [e.event_id async for e in second] == [1]
    assert stream.subscriber_count == 0


async def test_slow_subscriber_is_detached_when_its_bounded_queue_fills() -> None:
    stream = RunEventStream("t", "r", resumable=False)
    slow = stream.subscribe(after=None)
    for _ in range(256):
        stream.publish("values", {})
    assert stream.subscriber_count == 1

    stream.publish("values", {})
    assert stream.subscriber_count == 0
    with pytest.raises(SubscriberOverflowError):
        await slow.__anext__()


async def test_close_detaches_subscriber_when_its_queue_is_full() -> None:
    stream = RunEventStream("t", "r", resumable=False)
    slow = stream.subscribe(after=None)
    for _ in range(256):
        stream.publish("values", {})

    stream.close(now=0.0)
    assert stream.subscriber_count == 0
    with pytest.raises(SubscriberOverflowError):
        await slow.__anext__()


def test_publish_after_close_fails_loudly() -> None:
    stream = RunEventStream("t", "r", resumable=True)
    stream.close(now=0.0)
    stream.close(now=5.0)  # idempotent
    assert stream.closed_at == 0.0
    with pytest.raises(RuntimeError):
        stream.publish("values", {})


def test_registry_scopes_runs_to_their_thread() -> None:
    registry = RunStreamRegistry(retention_seconds=60)
    stream = registry.open("thread-a", "run-1", resumable=True)
    assert registry.get("thread-a", "run-1") is stream
    assert registry.get("thread-b", "run-1") is None
    assert registry.get("thread-a", "run-2") is None


def test_registry_retains_finished_resumable_streams_for_the_window() -> None:
    clock = _Clock()
    registry = RunStreamRegistry(retention_seconds=60, clock=clock)
    resumable = registry.open("t", "kept", resumable=True)
    plain = registry.open("t", "dropped", resumable=False)
    registry.close(resumable)
    registry.close(plain)
    assert registry.get("t", "kept") is resumable
    assert registry.get("t", "dropped") is None  # no history worth keeping
    clock.now += 59
    assert registry.get("t", "kept") is resumable
    clock.now += 1
    assert registry.get("t", "kept") is None


def test_registry_never_evicts_a_running_stream() -> None:
    clock = _Clock()
    registry = RunStreamRegistry(retention_seconds=1, clock=clock)
    stream = registry.open("t", "r", resumable=True)
    clock.now += 10_000
    assert registry.get("t", "r") is stream


def test_zero_retention_drops_streams_on_close() -> None:
    registry = RunStreamRegistry(retention_seconds=0)
    stream = registry.open("t", "r", resumable=True)
    registry.close(stream)
    assert registry.get("t", "r") is None
