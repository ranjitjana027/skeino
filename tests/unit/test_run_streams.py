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


@pytest.mark.parametrize("after", [None, -1], ids=["live-tail", "replay-all"])
async def test_resumable_subscriber_catches_up_from_history_after_overflow(
    after: int | None,
) -> None:
    # A long replay (or a slow client) lets the live queue fill; with the
    # events still retained, the subscriber resumes from history, losing none.
    stream = RunEventStream("t", "r", resumable=True)
    for _ in range(300):
        stream.publish("values", {})
    joined = stream.subscribe(after=after)
    first = await joined.__anext__() if after is not None else None
    for _ in range(300):
        stream.publish("values", {})
    stream.publish("end", {"status": "success"})
    stream.close(now=0.0)
    rest = [event.event_id async for event in joined]
    got = ([first.event_id] if first is not None else []) + rest
    start = 1 if after is not None else 301
    assert got == list(range(start, 602))
    assert stream.subscriber_count == 0


async def test_resumable_subscriber_overflows_once_missed_events_are_evicted() -> None:
    stream = RunEventStream("t", "r", resumable=True, max_history_events=300)
    joined = stream.subscribe(after=None)
    for _ in range(600):  # overflows the queue and evicts what it missed
        stream.publish("values", {})
    with pytest.raises(SubscriberOverflowError):
        async for _ in joined:
            pass
    assert stream.subscriber_count == 0


async def test_subscriber_never_receives_events_at_or_below_its_cursor() -> None:
    stream = RunEventStream("t", "r", resumable=True)
    stream.publish("values", {})
    ahead = stream.subscribe(after=3)
    for _ in range(4):
        stream.publish("values", {})
    stream.close(now=0.0)
    assert stream.last_event_id == 5
    assert [e.event_id async for e in ahead] == [4, 5]


async def test_close_preserves_all_events_when_queue_is_exactly_full() -> None:
    stream = RunEventStream("t", "r", resumable=False)
    slow = stream.subscribe(after=None)
    for _ in range(255):
        stream.publish("values", {})
    stream.publish("end", {"status": "success"})

    stream.close(now=0.0)
    events = [event async for event in slow]
    assert [event.event_id for event in events] == list(range(1, 257))
    assert events[-1].event == "end"
    assert stream.subscriber_count == 0


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


def test_finished_stream_budget_evicts_oldest_without_affecting_active_runs() -> None:
    registry = RunStreamRegistry(retention_seconds=600, max_retained_streams=2)
    active = registry.open("t", "active", resumable=True)
    for index in range(10):
        stream = registry.open("t", str(index), resumable=True)
        stream.publish("values", {"index": index})
        registry.close(stream)
        registry.close(stream)
        assert len(registry._expiry) <= 2
    assert registry.get("t", "0") is None
    assert registry.get("t", "7") is None
    assert registry.get("t", "8") is not None
    assert registry.get("t", "9") is not None
    assert registry.get("t", "active") is active


def test_negative_finished_stream_budget_is_rejected() -> None:
    with pytest.raises(ValueError):
        RunStreamRegistry(retention_seconds=600, max_retained_streams=-1)


def test_expiry_tracks_close_order_and_ignores_repeated_close() -> None:
    clock = _Clock()
    registry = RunStreamRegistry(retention_seconds=10, clock=clock)
    first = registry.open("t", "first", resumable=True)
    second = registry.open("t", "second", resumable=True)
    registry.close(second)
    registry.close(second)
    clock.now += 5
    registry.close(first)
    assert len(registry._expiry) == 2
    clock.now += 5
    assert registry.get("t", "second") is None
    assert registry.get("t", "first") is first
    clock.now += 5
    assert registry.get("t", "first") is None


async def test_subscriber_queue_holds_only_events_its_modes_accept() -> None:
    # Events a subscriber would filter out must not fill its bounded queue:
    # a values-only join stays attached through any volume of other modes.
    stream = RunEventStream("t", "r", resumable=False)
    values_only = stream.subscribe(after=None, stream_modes=["values"])
    for _ in range(300):
        stream.publish("custom", {})
    stream.publish("values", {})
    stream.publish("end", {"status": "success"})
    stream.close(now=0.0)
    got = [event.event async for event in values_only]
    assert got == ["values", "end"]


async def test_replay_skips_events_the_subscriber_modes_reject() -> None:
    stream = RunEventStream("t", "r", resumable=True)
    stream.publish("metadata", {})
    stream.publish("updates", {})
    stream.publish("values", {})
    stream.close(now=0.0)
    replayed = stream.subscribe(after=-1, stream_modes=["values"])
    assert [event.event_id async for event in replayed] == [1, 3]
