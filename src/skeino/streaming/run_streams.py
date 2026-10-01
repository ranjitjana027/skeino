"""Per-run SSE fan-out and the resumable replay buffer.

A streaming run executes in a background task that **publishes** its SSE events
to a :class:`RunEventStream`. Every HTTP response that carries those events —
the ``POST /runs/stream`` that created the run, and any later
``GET /runs/{run_id}/stream`` join — is a **subscriber**. Decoupling the two is
what lets a run outlive the connection that started it (``on_disconnect:
"continue"``) and lets a client re-attach to it.

Event ids are a per-run counter starting at 1, assigned once at publish time,
so a subscriber sees exactly the ids the original stream carried. A run created
with ``stream_resumable: true`` also keeps its events in a history list, which a
join replays after the client's ``Last-Event-ID`` before tailing live events.
Non-resumable runs keep no history: a join only sees events from the moment it
subscribes (LangGraph Platform behaves the same).

Scope is the current process — the same single-process assumption as
:class:`skeino.concurrency.BackgroundRunRegistry` and
:class:`skeino.concurrency.ThreadLockManager`, which already confine a run's
task, lock, and multitask admission to the worker that started it. A clustered
deployment would need a shared broker (e.g. Redis streams) for all three.
"""

import asyncio
import time
from collections.abc import AsyncGenerator, Callable, Sequence
from dataclasses import dataclass
from typing import Final

from skeino.schemas import JsonValue
from skeino.streaming.sse import sse_event

# Lifecycle events every subscriber receives regardless of its stream_mode
# filter: without them a joined client cannot tell which run it is attached to
# (``metadata``), that the run failed (``error``), or that it is over (``end``).
_LIFECYCLE_EVENTS: Final[frozenset[str]] = frozenset({"metadata", "error", "end"})
_SUBSCRIBER_QUEUE_SIZE: Final[int] = 256


def stream_mode_matches(event: str, stream_modes: Sequence[str]) -> bool:
    """Return whether an event should reach a subscriber filtering on modes.

    Mirrors LangGraph Platform's join filter: no modes means everything; a
    ``messages`` or ``messages-tuple`` request matches every ``messages*``
    event; a subgraph event ``mode|namespace`` matches its base ``mode``.
    Lifecycle events always pass (see ``_LIFECYCLE_EVENTS``).
    """
    if not stream_modes or event in _LIFECYCLE_EVENTS or event in stream_modes:
        return True
    if event.startswith("messages") and (
        "messages" in stream_modes or "messages-tuple" in stream_modes
    ):
        return True
    base, sep, _ = event.partition("|")
    return bool(sep) and base in stream_modes


@dataclass(frozen=True, slots=True)
class StreamEvent:
    """One published event: its id, its name, and its encoded SSE frame."""

    event_id: int
    event: str
    frame: str


class RunEventStream:
    """The event fan-out (and, if resumable, the history) of one run."""

    def __init__(self, thread_id: str, run_id: str, *, resumable: bool) -> None:
        """Start an open stream with no events and no subscribers."""
        self.thread_id = thread_id
        self.run_id = run_id
        self.resumable = resumable
        self.closed_at: float | None = None
        self._next_id = 1
        self._history: list[StreamEvent] = []
        self._subscribers: set[asyncio.Queue[StreamEvent | None]] = set()

    @property
    def closed(self) -> bool:
        """Whether the run has finished publishing."""
        return self.closed_at is not None

    @property
    def subscriber_count(self) -> int:
        """Number of subscribers currently attached (live, not yet drained)."""
        return len(self._subscribers)

    def publish(self, event: str, data: dict[str, JsonValue]) -> StreamEvent:
        """Assign the next id to an event and deliver it to every subscriber."""
        if self.closed:
            raise RuntimeError(f"Run {self.run_id} event stream is already closed.")
        published = StreamEvent(
            self._next_id, event, sse_event(event, data, self._next_id)
        )
        self._next_id += 1
        if self.resumable:
            self._history.append(published)
        for queue in tuple(self._subscribers):
            if queue.full():
                self._detach(queue)
            else:
                queue.put_nowait(published)
        return published

    def close(self, now: float) -> None:
        """Mark the stream finished and release every subscriber. Idempotent."""
        if self.closed:
            return
        self.closed_at = now
        for queue in tuple(self._subscribers):
            if queue.full():
                self._detach(queue)
            else:
                queue.put_nowait(None)

    def _detach(self, queue: asyncio.Queue[StreamEvent | None]) -> None:
        """Stop a lagging subscriber and discard its queued events."""
        self._subscribers.discard(queue)
        while not queue.empty():
            queue.get_nowait()
        queue.put_nowait(None)

    def subscribe(self, *, after: int | None) -> AsyncGenerator[StreamEvent, None]:
        """Attach a subscriber; return its event iterator.

        Registration is eager (it happens here, not on first iteration), so a
        subscriber taken before the producer starts misses nothing. Retained
        events with an id greater than ``after`` are replayed first; ``after``
        of ``None`` replays nothing (live tail only). The replay snapshot and
        the live registration happen in one synchronous step, so no event is
        lost or duplicated between them.
        """
        replay = (
            [e for e in self._history if e.event_id > after]
            if after is not None
            else []
        )
        queue: asyncio.Queue[StreamEvent | None] = asyncio.Queue(
            maxsize=_SUBSCRIBER_QUEUE_SIZE
        )
        if self.closed:
            queue.put_nowait(None)
        else:
            self._subscribers.add(queue)
        return self._drain(replay, queue)

    async def _drain(
        self, replay: list[StreamEvent], queue: asyncio.Queue[StreamEvent | None]
    ) -> AsyncGenerator[StreamEvent, None]:
        try:
            for event in replay:
                yield event
            while (event_or_end := await queue.get()) is not None:
                yield event_or_end
        finally:
            self._subscribers.discard(queue)


class RunStreamRegistry:
    """Process-local map of run id → :class:`RunEventStream`, with retention.

    A closed resumable stream is kept for ``retention_seconds`` so a client
    that comes back after the run ended can still replay it; a closed
    non-resumable stream (no history to replay) is dropped at once. Expired
    streams are swept on every ``open``/``get``/``close``.
    """

    def __init__(
        self,
        *,
        retention_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Create an empty registry keeping finished resumable streams a while."""
        self._retention = retention_seconds
        self._clock = clock
        self._streams: dict[str, RunEventStream] = {}

    def open(self, thread_id: str, run_id: str, *, resumable: bool) -> RunEventStream:
        """Register and return a fresh stream for a run that is about to start."""
        self._sweep()
        stream = RunEventStream(thread_id, run_id, resumable=resumable)
        self._streams[run_id] = stream
        return stream

    def get(self, thread_id: str, run_id: str) -> RunEventStream | None:
        """Return the run's stream, or ``None`` if unknown or expired.

        A run id looked up under the wrong thread is also ``None``: it must not
        leak another thread's events.
        """
        self._sweep()
        stream = self._streams.get(run_id)
        if stream is None or stream.thread_id != thread_id:
            return None
        return stream

    def close(self, stream: RunEventStream) -> None:
        """Finish a stream, keeping it for replay only if it has history."""
        stream.close(self._clock())
        if not stream.resumable or self._retention <= 0:
            self._streams.pop(stream.run_id, None)
        self._sweep()

    def _sweep(self) -> None:
        now = self._clock()
        expired = [
            run_id
            for run_id, stream in self._streams.items()
            if stream.closed_at is not None
            and now - stream.closed_at >= self._retention
        ]
        for run_id in expired:
            del self._streams[run_id]
