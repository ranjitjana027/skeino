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
import weakref
from collections import deque
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
_DEFAULT_HISTORY_MAX_EVENTS: Final[int] = 10_000
_DEFAULT_HISTORY_MAX_BYTES: Final[int] = 16 * 1024 * 1024


class SubscriberOverflowError(Exception):
    """A subscriber lost events because its bounded delivery queue filled."""


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

    def __init__(
        self,
        thread_id: str,
        run_id: str,
        *,
        resumable: bool,
        max_history_events: int = _DEFAULT_HISTORY_MAX_EVENTS,
        max_history_bytes: int = _DEFAULT_HISTORY_MAX_BYTES,
    ) -> None:
        """Start an open stream with no events and no subscribers."""
        self.thread_id = thread_id
        self.run_id = run_id
        self.resumable = resumable
        self.closed_at: float | None = None
        self._next_id = 1
        self._history: deque[StreamEvent] = deque()
        self._history_bytes = 0
        self._evicted_through_id = 0
        self._max_history_events = max_history_events
        self._max_history_bytes = max_history_bytes
        # Each live subscriber's queue, mapped to the stream modes it wants.
        self._subscribers: dict[
            asyncio.Queue[StreamEvent | SubscriberOverflowError | None],
            tuple[str, ...],
        ] = {}
        # Each subscription's iterator, mapped to the queue it attached with,
        # so one that will never be iterated can still be detached
        # (:meth:`detach`). A started one detaches itself when it ends.
        self._queue_of: weakref.WeakKeyDictionary[
            AsyncGenerator[StreamEvent, None],
            asyncio.Queue[StreamEvent | SubscriberOverflowError | None],
        ] = weakref.WeakKeyDictionary()

    @property
    def closed(self) -> bool:
        """Whether the run has finished publishing."""
        return self.closed_at is not None

    @property
    def last_event_id(self) -> int:
        """Id of the most recently published event (``0`` before the first)."""
        return self._next_id - 1

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
            self._history_bytes += len(published.frame.encode("utf-8"))
            while self._history and (
                len(self._history) > self._max_history_events
                or self._history_bytes > self._max_history_bytes
            ):
                evicted = self._history.popleft()
                self._history_bytes -= len(evicted.frame.encode("utf-8"))
                self._evicted_through_id = evicted.event_id
        for queue, stream_modes in tuple(self._subscribers.items()):
            if not stream_mode_matches(event, stream_modes):
                # Filtered at fan-out, so an event a subscriber never receives
                # cannot fill its bounded queue and overflow it.
                continue
            if queue.full():
                self._detach(queue)
            else:
                queue.put_nowait(published)
        return published

    def cursor_expired(self, after: int) -> bool:
        """Whether replay after ``after`` would omit previously evicted events."""
        return self._evicted_through_id > 0 and after < self._evicted_through_id

    def close(self, now: float) -> None:
        """Mark the stream finished and release every subscriber. Idempotent."""
        if self.closed:
            return
        self.closed_at = now
        for queue in tuple(self._subscribers):
            # A full queue contains valid events, not an overflow. Its drain
            # observes closed state once those events have all been consumed.
            if not queue.full():
                queue.put_nowait(None)

    def _detach(
        self, queue: asyncio.Queue[StreamEvent | SubscriberOverflowError | None]
    ) -> None:
        """Stop a lagging subscriber and discard its queued events."""
        self._subscribers.pop(queue, None)
        while not queue.empty():
            queue.get_nowait()
        queue.put_nowait(
            SubscriberOverflowError(
                "Subscriber queue overflow; streamed output was lost."
            )
        )

    def subscribe(
        self, *, after: int | None, stream_modes: Sequence[str] = ()
    ) -> AsyncGenerator[StreamEvent, None]:
        """Attach a subscriber; return its event iterator.

        Only events matching ``stream_modes`` (see :func:`stream_mode_matches`;
        empty means all) are replayed or queued for it, so its bounded queue
        only ever holds events it will deliver.

        Registration is eager (it happens here, not on first iteration), so a
        subscriber taken before the producer starts misses nothing. Retained
        events with an id greater than ``after`` are replayed first; ``after``
        of ``None`` replays nothing (live tail only). The replay snapshot and
        the live registration happen in one synchronous step, so no event is
        lost or duplicated between them.

        A resumable subscriber whose live queue overflows (for example while a
        long replay is still being sent) catches up from the retained history
        instead of failing; it only gets :class:`SubscriberOverflowError` once
        the events it missed have been evicted. A non-resumable one has no
        history to fall back on and fails at the first overflow.
        """
        delivered = after if after is not None else self.last_event_id
        modes = tuple(stream_modes)
        replay, queue = self._attach(after, modes)
        events = self._drain(replay, queue, delivered, modes)
        self._queue_of[events] = queue
        return events

    def detach(self, events: AsyncGenerator[StreamEvent, None]) -> None:
        """Drop a subscription nobody will iterate, releasing its queue.

        Closing a never-started iterator does not run its cleanup, so a
        subscription abandoned before its first read must be detached here.
        """
        queue = self._queue_of.pop(events, None)
        if queue is not None:
            self._subscribers.pop(queue, None)

    def _attach(
        self, after: int | None, stream_modes: tuple[str, ...]
    ) -> tuple[
        list[StreamEvent],
        asyncio.Queue[StreamEvent | SubscriberOverflowError | None],
    ]:
        replay = (
            [
                e
                for e in self._history
                if e.event_id > after and stream_mode_matches(e.event, stream_modes)
            ]
            if after is not None
            else []
        )
        queue: asyncio.Queue[StreamEvent | SubscriberOverflowError | None] = (
            asyncio.Queue(maxsize=_SUBSCRIBER_QUEUE_SIZE)
        )
        if self.closed:
            queue.put_nowait(None)
        else:
            self._subscribers[queue] = stream_modes
        return replay, queue

    async def _drain(
        self,
        replay: list[StreamEvent],
        queue: asyncio.Queue[StreamEvent | SubscriberOverflowError | None],
        delivered: int,
        stream_modes: tuple[str, ...],
    ) -> AsyncGenerator[StreamEvent, None]:
        try:
            while True:
                for event in replay:
                    yield event
                    delivered = event.event_id
                overflow: SubscriberOverflowError | None = None
                while not (self.closed and queue.empty()):
                    event_or_end = await queue.get()
                    if event_or_end is None:
                        break
                    if isinstance(event_or_end, SubscriberOverflowError):
                        overflow = event_or_end
                        break
                    if event_or_end.event_id <= delivered:
                        continue  # already delivered (or before the cursor)
                    yield event_or_end
                    delivered = event_or_end.event_id
                if overflow is None:
                    return
                if not self.resumable or self.cursor_expired(delivered):
                    raise overflow
                # Everything after ``delivered`` is still retained: resume from
                # history with a fresh live queue.
                self._subscribers.pop(queue, None)
                replay, queue = self._attach(delivered, stream_modes)
        finally:
            self._subscribers.pop(queue, None)


class RunStreamRegistry:
    """Process-local map of run id → :class:`RunEventStream`, with retention.

    A closed resumable stream is kept for ``retention_seconds`` so a client
    that comes back after the run ended can still replay it, unless more than
    ``max_retained_streams`` finished streams are held, in which case the
    oldest are evicted early. A closed non-resumable stream (no history to
    replay) is dropped at once. Expired streams are swept on every
    ``open``/``get``/``close``.
    """

    def __init__(
        self,
        *,
        retention_seconds: float,
        max_retained_streams: int = 16,
        max_history_events: int = _DEFAULT_HISTORY_MAX_EVENTS,
        max_history_bytes: int = _DEFAULT_HISTORY_MAX_BYTES,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Create an empty registry keeping finished resumable streams a while."""
        self._retention = retention_seconds
        if max_retained_streams < 0:
            raise ValueError("max_retained_streams must be non-negative")
        self._max_retained_streams = max_retained_streams
        self._max_history_events = max_history_events
        self._max_history_bytes = max_history_bytes
        self._clock = clock
        self._streams: dict[str, RunEventStream] = {}
        self._expiry: deque[RunEventStream] = deque()

    def open(self, thread_id: str, run_id: str, *, resumable: bool) -> RunEventStream:
        """Register and return a fresh stream for a run that is about to start."""
        self._sweep()
        stream = RunEventStream(
            thread_id,
            run_id,
            resumable=resumable,
            max_history_events=self._max_history_events,
            max_history_bytes=self._max_history_bytes,
        )
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
        was_closed = stream.closed
        stream.close(self._clock())
        if not stream.resumable or self._retention <= 0:
            self._streams.pop(stream.run_id, None)
        elif not was_closed:
            self._expiry.append(stream)
        self._sweep()

        while len(self._expiry) > self._max_retained_streams:
            oldest = self._expiry.popleft()
            if self._streams.get(oldest.run_id) is oldest:
                del self._streams[oldest.run_id]

    def _sweep(self) -> None:
        now = self._clock()
        while self._expiry:
            stream = self._expiry[0]
            if stream.closed_at is None or now - stream.closed_at < self._retention:
                break
            self._expiry.popleft()
            if self._streams.get(stream.run_id) is stream:
                del self._streams[stream.run_id]
