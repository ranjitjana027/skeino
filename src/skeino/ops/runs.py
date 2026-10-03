"""Run lifecycle: background create, wait, stream, join, cancel, delete, list, get.

Every run executes inside a background :class:`asyncio.Task` tracked by the
:class:`BackgroundRunRegistry`. ``create_run`` returns immediately with a
``pending`` run; ``wait_run`` / ``join_run`` await the task; ``cancel_run`` and
the interrupt/rollback multitask strategies cancel it. Streaming runs are tasks
too: they publish SSE events to a :class:`RunEventStream` that the creating
response — and any later ``join_run_stream`` — subscribes to, so a run can
outlive its client (``on_disconnect="continue"``) and be re-attached to.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, AsyncGenerator, AsyncIterator, Final
from uuid import UUID, uuid4

from fastapi import HTTPException, status
from langchain_core.callbacks import UsageMetadataCallbackHandler

from skeino.concurrency import BackgroundRunRegistry, ThreadLockManager
from skeino.ops.assistants import AssistantOps
from skeino.ops.threads import ThreadOps
from skeino.persistence import MetadataStoreProtocol, RunRow
from skeino.schemas import (
    CancelAction,
    JsonValue,
    MultitaskStrategy,
    RunCreateRequest,
    RunModel,
    RunStatus,
    ThreadSearchRequest,
    ThreadStatus,
)
from skeino.serialization import (
    build_thread_config,
    coerce_stream_modes,
    normalize_command_payload,
    normalize_input_payload,
    serialize_mapping,
    serialize_value,
)
from skeino.streaming import (
    STREAM_MAX_RETRIES,
    STREAM_RETRY_BACKOFF_SECS,
    RunEventStream,
    RunStreamRegistry,
    Streamer,
    StreamEvent,
    is_retriable_stream_error,
    sse_event,
    stream_mode_matches,
)
from skeino.streaming.run_streams import SubscriberOverflowError
from skeino.tracing import resolve_session_name, run_tracing_context
from skeino.usage import (
    attach_usage_handler,
    total_tokens_from_messages,
    total_tokens_from_usage,
)

_THREAD_BUSY: Final[ThreadStatus] = "busy"
_THREAD_IDLE: Final[ThreadStatus] = "idle"
_THREAD_ERROR: Final[ThreadStatus] = "error"
_THREAD_INTERRUPTED: Final[ThreadStatus] = "interrupted"
_RUN_RUNNING: Final[RunStatus] = "running"
_RUN_SUCCESS: Final[RunStatus] = "success"
_RUN_ERROR: Final[RunStatus] = "error"
_RUN_INTERRUPTED: Final[RunStatus] = "interrupted"
_TERMINAL_STATUSES: Final[frozenset[str]] = frozenset(
    {"success", "error", "interrupted", "timeout"}
)
_INTERRUPT_CHANNEL: Final[str] = "__interrupt__"
_ORPHANED_RUN_ERROR: Final[str] = (
    "Run orphaned: the server process executing it stopped before it finished "
    "(e.g. a crash, OOM kill, or restart), so it will never complete. "
    "Start a new run to retry."
)

_DEFAULT_STREAM_RETENTION_SECS: Final[float] = 600.0

logger = logging.getLogger(__name__)


def _parse_last_event_id(value: str | None) -> int | None:
    """Parse an SSE ``Last-Event-ID`` into the id to replay after.

    Ids are the per-run integer counter skeino assigns (``1``, ``2``, ...);
    ``-1`` — what the SDK's ``useStream`` sends to mean "from the beginning" —
    replays everything. Blank means no header. Anything else (including a
    negative id other than ``-1``) is a client bug, and replaying from a
    guessed position would silently drop or repeat events, so it is a 422.
    """
    if value is None or not value.strip():
        return None
    try:
        last_event_id = int(value.strip())
    except ValueError:
        last_event_id = None
    if last_event_id is None or last_event_id < -1:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Invalid Last-Event-ID {value!r}: expected an event id "
                "(a non-negative integer) or -1."
            ),
        )
    return last_event_id


def _pending_interrupts(snapshot: Any) -> tuple[Any, ...]:
    """Return the interrupts a state snapshot is parked on.

    ``StateSnapshot.interrupts`` is where LangGraph keeps them; older
    snapshots expose pending interrupts only per task, so fall back to
    walking ``tasks``. Empty when the graph is not waiting on anything.
    """
    interrupts = getattr(snapshot, "interrupts", ()) or ()
    if interrupts:
        return tuple(interrupts)
    from_tasks: list[Any] = []
    for task in getattr(snapshot, "tasks", ()) or ():
        from_tasks.extend(getattr(task, "interrupts", ()) or ())
    return tuple(from_tasks)


# The persisted terminal state a run's own final write lost to: the row that
# another writer finalized first, or ``_deleted_outcome`` for a row now gone.
_Outcome = Mapping[str, Any]


def _deleted_outcome(run_id: str) -> _Outcome:
    """Return the outcome of a run whose row was deleted before it finished."""
    return {
        "status": _RUN_ERROR,
        "error": f"Run {run_id} was deleted before it finished.",
    }


def _terminal_event_for_row(run_id: str, row: _Outcome) -> tuple[str, dict[str, Any]]:
    """Return the terminal stream event matching a finalized run row."""
    if str(row["status"]) == _RUN_ERROR:
        return "error", {
            "detail": str(row.get("error") or "Run failed."),
            "run_id": run_id,
        }
    return "end", {"run_id": run_id, "status": str(row["status"])}


def _as_utc(value: datetime) -> datetime:
    """Read a store timestamp as aware UTC (some drivers return naive UTC)."""
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _session_name_of(kwargs: Any) -> str | None:
    """Read the LangSmith session a stored run was traced into, if any."""
    if isinstance(kwargs, dict):
        value = kwargs.get("langsmith_session_name")
        if isinstance(value, str) and value:
            return value
    return None


class RunOps:
    """Create, stream, await, cancel, and inspect runs against a single graph."""

    def __init__(
        self,
        *,
        graph: Any,
        metadata_store: MetadataStoreProtocol,
        streamer: Streamer,
        thread_ops: ThreadOps,
        assistant_ops: AssistantOps,
        lock_manager: ThreadLockManager,
        registry: BackgroundRunRegistry,
        streams: RunStreamRegistry | None = None,
    ) -> None:
        """Capture every collaborator a run needs."""
        self._graph = graph
        self._metadata_store = metadata_store
        self._streamer = streamer
        self._thread_ops = thread_ops
        self._assistant_ops = assistant_ops
        self._lock_manager = lock_manager
        self._registry = registry
        self._streams = streams or RunStreamRegistry(
            retention_seconds=_DEFAULT_STREAM_RETENTION_SECS
        )
        # Threads whose orphaned runs were claimed but whose release failed;
        # retried every liveness pass, since a later sweep never re-claims them.
        self._unreleased_threads: set[str] = set()
        # Threads whose run succeeded but whose settle write failed (or was
        # cancelled); retried every liveness pass so they don't stay ``busy``.
        self._unsettled_threads: set[str] = set()
        # Streaming runs still being admitted (lock wait, row insert): a join
        # waits for admission so it attaches to the producer, not to this task.
        self._admitting: dict[str, asyncio.Task[Any]] = {}

    async def create_run(self, thread_id: str, request: RunCreateRequest) -> RunModel:
        """Start a background run and return its (pending) metadata immediately."""
        run_row, _task = await self._admit_and_spawn(thread_id, request)
        return self._run_row_to_model(run_row)

    async def wait_run(
        self, thread_id: str, request: RunCreateRequest
    ) -> tuple[JsonValue, int]:
        """Start a run, wait for it to finish, and return its output + tokens.

        Returns the final graph state values (the run output, matching the
        LangGraph SDK ``runs.wait`` contract) and the run's total token count
        for the ``X-Tokens-Used`` header.
        """
        run_row, task = await self._admit_and_spawn(thread_id, request)
        run_id = str(run_row["run_id"])
        await asyncio.wait({task})
        return await self._collect_terminal_output(thread_id, run_id, task)

    async def join_run(self, thread_id: str, run_id: str) -> JsonValue:
        """Wait for a run to reach a terminal state and return its output.

        Mirrors the LangGraph SDK ``runs.join`` contract (returns the final
        graph state values). If the run is already terminal this returns at once.
        """
        run = await self.get_run(thread_id, run_id)  # 404 if unknown
        await self._await_admission(run_id)
        task = self._registry.get(run_id)
        if task is None:
            # The reads above yield: the run may have finished, and its task
            # been forgotten, in between. Use a fresh persisted status.
            run = await self.get_run(thread_id, run_id)
        if task is not None:
            task = await self._registry.wait(run_id)
        elif run.status not in _TERMINAL_STATUSES:
            # No task to await and the run is still in flight — it is not
            # running in this process (e.g. another worker, or a row stranded
            # by a crash). Fail fast rather than return a non-terminal snapshot
            # and break the contract.
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"Run {run_id} is {run.status} with no joinable task "
                    "on this server."
                ),
            )
        output, _tokens = await self._collect_terminal_output(thread_id, run_id, task)
        return output

    async def cancel_run(
        self,
        thread_id: str,
        run_id: str,
        *,
        action: CancelAction,
        wait: bool,
    ) -> None:
        """Cancel an in-flight run.

        ``action="interrupt"`` cancels the run and leaves it ``interrupted``;
        ``action="rollback"`` cancels it and deletes the run row. Returns 409 if
        the run is already terminal or has no task in this process to cancel.

        ``rollback`` always waits for the task to fully unwind before deleting,
        regardless of ``wait`` — otherwise a still-running task could keep
        mutating thread state (and race the deletion with its own persistence)
        after the row is already gone.
        """
        run = await self.get_run(thread_id, run_id)  # 404 if unknown
        if run.status in _TERMINAL_STATUSES:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Run {run_id} is already {run.status}; nothing to cancel.",
            )
        cancel_wait = wait or action == "rollback"
        cancelled = await self._registry.cancel(run_id, wait=cancel_wait)
        if not cancelled:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(f"Run {run_id} has no cancellable task on this server."),
            )
        if action == "rollback":
            # rollback always waited above, so the task has fully unwound.
            await self._metadata_store.delete_run(thread_id, run_id)
        elif cancel_wait:
            # We waited for the task to settle; persist ``interrupted`` as an
            # idempotent backstop in case the task's own handler didn't (e.g. it
            # was cancelled before its body ever ran).
            await self._metadata_store.update_run_status(
                run_id, _RUN_INTERRUPTED, error="Run cancelled."
            )
        # else (interrupt, wait=False): don't mark the run terminal eagerly —
        # the task is still unwinding (and may still hold the execution lock).
        # Its ``CancelledError`` handler persists ``interrupted`` as it exits.

    async def delete_run(self, thread_id: str, run_id: str) -> None:
        """Delete a terminal run row. Returns 409 if the run is still active."""
        run = await self.get_run(thread_id, run_id)  # 404 if unknown
        if run.status not in _TERMINAL_STATUSES:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Run {run_id} is {run.status}; cancel it before deleting.",
            )
        await self._metadata_store.delete_run(thread_id, run_id)

    async def create_streaming_run(
        self,
        thread_id: str,
        request: RunCreateRequest,
        *,
        after_run: Callable[[], Awaitable[None]] | None = None,
    ) -> tuple[RunModel, AsyncIterator[str]]:
        """Create a run and stream its output as SSE.

        The run executes in a background task that publishes to a
        :class:`RunEventStream`; the returned iterator is one subscriber of it,
        attached before the task starts so it sees every event from ``id: 1``.
        The task is tracked by the run registry like a background run, so the
        run can be joined (``/join`` or ``/stream``), cancelled, and superseded
        by an ``interrupt``/``rollback`` multitask strategy.

        ``request.on_disconnect`` decides what a client disconnect does:
        ``"cancel"`` cancels the run, ``"continue"`` (the default, as on
        LangGraph Platform) leaves it running for a later join — even while it
        is still queued. ``after_run`` is awaited exactly once, after the run
        has finished or once it can no longer start (stateless cleanup).
        """
        handed_off = asyncio.Event()
        try:
            return await self._create_streaming_run(
                thread_id, request, after_run=after_run, handed_off=handed_off
            )
        except BaseException:
            if not handed_off.is_set() and after_run is not None:
                # Failed before admission took ownership of the cleanup.
                await after_run()
            raise

    async def _create_streaming_run(
        self,
        thread_id: str,
        request: RunCreateRequest,
        *,
        after_run: Callable[[], Awaitable[None]] | None,
        handed_off: asyncio.Event,
    ) -> tuple[RunModel, AsyncIterator[str]]:
        await self._thread_ops.ensure_thread_for_run(thread_id, request.if_not_exists)
        self._assistant_ops.ensure_supported(request.assistant_id)
        self._validate_run_request(request)
        stream_modes = coerce_stream_modes(request.stream_mode)
        if "events" in stream_modes and len(stream_modes) > 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="'events' stream_mode cannot be combined with other modes.",
            )
        run_id = str(uuid4())
        lock = self._lock_manager.get(thread_id)
        # Reserve the thread slot under the admission lock so concurrent creates
        # (streaming or background) see this run as active and cannot silently
        # degrade reject/rollback/interrupt to enqueue.
        async with self._registry.admission(thread_id):
            await self._resolve_multitask(thread_id, request.multitask_strategy)
            self._registry.register_external(thread_id, run_id)

        def start_producer(
            run_row: RunRow,
        ) -> tuple[RunModel, AsyncGenerator[StreamEvent, None], asyncio.Task[Any]]:
            """Hand the admitted run to its producer task; never awaits.

            Runs inside the admission task, so the registry tracks the producer
            the moment admission ends: there is no window in which the run has
            neither task and a ``reject``/``interrupt``/``rollback`` misses it.
            """
            run = self._run_row_to_model(run_row)
            stream = self._streams.open(
                thread_id, run_id, resumable=request.stream_resumable
            )
            # Subscribed before the producer exists, so it sees ``id: 1``.
            events = stream.subscribe(after=None)
            released = False

            def finish() -> None:
                # Release resources in the tracked finalizer, including when
                # cancellation prevented the producer from ever starting.
                nonlocal released
                if not released:
                    released = True
                    lock.release()
                self._streams.close(stream)

            async def finalize_run() -> None:
                """Complete cleanup within the registry's awaited lifecycle."""
                try:
                    if task.cancelled():
                        superseded = await self._mark_run_interrupted(run_id, thread_id)
                        if superseded is None:
                            stream.publish(
                                "end", {"run_id": run_id, "status": _RUN_INTERRUPTED}
                            )
                        else:
                            if str(superseded["status"]) == _RUN_SUCCESS:
                                # Cancelled after ``success`` was saved
                                # (possibly before the thread was settled);
                                # settling again is idempotent.
                                await self._settle_thread_after_success(thread_id)
                            stream.publish(*_terminal_event_for_row(run_id, superseded))
                    if after_run is not None:
                        await after_run()
                finally:
                    finish()

            task = self._registry.spawn(
                thread_id,
                run_id,
                self._publish_run(run, request, stream_modes, stream),
                finalize=lambda: finalize_run(),
            )
            return run, events, task

        admission_started = False

        async def admit() -> tuple[
            RunModel, AsyncGenerator[StreamEvent, None], asyncio.Task[Any]
        ]:
            nonlocal admission_started
            admission_started = True
            try:
                await lock.acquire()
            except BaseException:
                # Cancelled while queued: nothing inserted, nothing held.
                if after_run is not None:
                    await after_run()  # no producer will run it
                raise
            try:
                run_row = await self._metadata_store.create_run(
                    run_id=run_id,
                    thread_id=thread_id,
                    assistant_id=request.assistant_id,
                    metadata=request.metadata,
                    kwargs=self._build_run_kwargs(request),
                    multitask_strategy=request.multitask_strategy,
                )
            except BaseException:
                lock.release()
                # The insert may have committed before it failed or was
                # cancelled. The run never started, so the thread is not its
                # to touch.
                await self._interrupt_row_only(run_id)
                if after_run is not None:
                    await after_run()  # no producer will run it
                raise
            try:
                return start_producer(run_row)
            except BaseException:
                lock.release()
                await self._interrupt_row_only(run_id)
                if after_run is not None:
                    await after_run()
                raise

        # Lock wait, row insert and producer start run as one tracked task, so
        # an ``interrupt``/``rollback`` can supersede this run at any point and
        # the run stays tracked through the hand-off to its producer. With
        # ``on_disconnect="continue"`` it is shielded from this request: a
        # client leaving while the run is queued doesn't stop the run.
        admit_task = self._registry.spawn(thread_id, run_id, admit())
        handed_off.set()  # from here admission, then the producer, owns after_run
        self._admitting[run_id] = admit_task
        admit_task.add_done_callback(lambda _t: self._admitting.pop(run_id, None))
        try:
            run, events, task = await (
                admit_task
                if request.on_disconnect == "cancel"
                else asyncio.shield(admit_task)
            )
        except asyncio.CancelledError:
            if admit_task.cancelled() and not admission_started:
                # Cancelled before its first step, so ``admit`` never ran and
                # cannot run the cleanup it owns.
                if after_run is not None:
                    await after_run()
            current = asyncio.current_task()
            if current is not None and current.cancelling() > 0:
                # The client left. ``cancel`` propagated into admission (which
                # terminalizes its own row); ``continue`` lets it carry on.
                if admit_task.done():
                    self._abandon_admission(
                        admit_task, cancel_run=request.on_disconnect == "cancel"
                    )
                else:
                    # ``continue``: admission carries on with nobody to hand
                    # its result (or its error) to.
                    admit_task.add_done_callback(
                        lambda done: self._abandon_admission(done, cancel_run=False)
                    )
                raise
            # Superseded by interrupt/rollback while queued: the request itself
            # is fine, so answer it instead of leaking the cancel.
            logger.warning(
                "Queued streaming run %s on thread %s was superseded before it started",
                run_id,
                thread_id,
            )
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Run {run_id} was superseded by a newer run "
                "(multitask_strategy) before it started.",
            ) from None

        return run, self._relay(
            events,
            task,
            run_id=run_id,
            cancel_on_disconnect=request.on_disconnect == "cancel",
        )

    async def join_run_stream(
        self,
        thread_id: str,
        run_id: str,
        *,
        stream_modes: list[str],
        last_event_id: str | None,
        cancel_on_disconnect: bool,
    ) -> AsyncIterator[str]:
        """Re-attach to a run's event stream (LangGraph ``runs.joinStream``).

        * A run with a live or retained event stream: replay the events after
          ``last_event_id`` (resumable runs only; ``-1`` replays everything,
          absent replays nothing), then tail live events until the run ends.
        * A finished run whose events are not retained (non-resumable, expired,
          or joined without a ``Last-Event-ID``): its final state as one
          ``values`` event (no id), then ``end`` — or ``error`` if it failed.
        * A background run (``POST /runs``, which streams nothing): wait for
          it, then the same final-state events.
        * A run that is in flight with no stream or task in this process: 409.

        Unknown thread, unknown run, or a run of another thread: 404. A
        malformed ``Last-Event-ID``: 422. All are raised before the response
        starts, so they arrive as real HTTP statuses.
        """
        run = await self.get_run(thread_id, run_id)  # 404 if unknown
        after = _parse_last_event_id(last_event_id)
        # A run still being admitted has no event stream yet; once admitted it
        # has one to attach to.
        await self._await_admission(run_id)
        task = self._registry.get(run_id)
        stream = self._streams.get(thread_id, run_id)
        # A done producer may not have been finalized (stream closed) yet; its
        # ``end`` is already published, so a live tail would see nothing.
        producer_finished = stream is not None and (
            stream.closed or (task is not None and task.done())
        )
        if stream is not None and not (
            producer_finished and (after is None or not stream.resumable)
        ):
            if after is not None and after > stream.last_event_id:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=(
                        f"Last-Event-ID {after} is ahead of run {run_id}'s event "
                        f"stream (last id {stream.last_event_id})."
                    ),
                )
            if after is not None and stream.cursor_expired(after):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=(
                        f"Last-Event-ID {after} predates the retained event window "
                        f"for run {run_id}."
                    ),
                )
            return self._relay(
                stream.subscribe(after=after, stream_modes=stream_modes),
                task,
                run_id=run_id,
                cancel_on_disconnect=cancel_on_disconnect,
            )
        if task is None:
            # An async status read may have raced with the run finishing and
            # its local handles being removed. Use a fresh persisted status.
            run = await self.get_run(thread_id, run_id)
        if task is None and run.status not in _TERMINAL_STATUSES:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"Run {run_id} is {run.status} but has no event stream on "
                    "this server to join."
                ),
            )
        return self._final_state_events(
            thread_id,
            run_id,
            task,
            stream_modes=stream_modes,
            cancel_on_disconnect=cancel_on_disconnect,
        )

    async def _publish_run(
        self,
        run: RunModel,
        request: RunCreateRequest,
        stream_modes: list[str],
        stream: RunEventStream,
    ) -> int:
        """Execute a streaming run in its own task, publishing its events.

        Returns total tokens. A disconnecting client does not unwind it.
        """
        run_id = str(run.run_id)
        thread_id = str(run.thread_id)
        emitted_data = False
        try:
            finalized = await self._claim_run(thread_id, run_id)
            if finalized is not None:
                # Finalized elsewhere before it started (e.g. swept as
                # orphaned): report that outcome and run nothing.
                stream.publish(*_terminal_event_for_row(run_id, finalized))
                return 0
            await self._metadata_store.update_thread(
                thread_id, status_value=_THREAD_BUSY
            )
            stream.publish(
                "metadata",
                {
                    "run_id": run_id,
                    "thread_id": thread_id,
                    "run": serialize_value(run.model_dump(mode="json")),
                },
            )

            runnable_input = self._resolve_run_input(request)
            config = build_thread_config(
                thread_id, request.config, request.checkpoint, run_id=run_id
            )
            # One handler for all retry attempts: tokens consumed by a failed
            # attempt were still consumed, so they count.
            usage_handler = attach_usage_handler(config)
            # Route the run's trace to its requested LangSmith project too
            # (skeino.tracing). Around the whole attempt loop, so a retried
            # attempt traces to the same projects as the first.
            with run_tracing_context(request.langsmith_tracer):
                for attempt in range(STREAM_MAX_RETRIES):
                    try:
                        async for event_name, payload in self._streamer.stream(
                            runnable_input, config, request, stream_modes
                        ):
                            stream.publish(event_name, payload)
                            emitted_data = True
                            # Async generators may emit already-buffered chunks
                            # without suspending. Give response relays a chance
                            # to drain bounded queues before the next publish.
                            await asyncio.sleep(0)
                        break
                    except Exception as exc:
                        # Only retry while nothing has been published. Graph
                        # execution is not idempotent, so replaying a partially
                        # streamed run would duplicate output and re-invoke the
                        # model. ``CancelledError`` is a BaseException and is
                        # deliberately not caught here so a cancel propagates
                        # to the cancellation handler below.
                        if (
                            attempt < STREAM_MAX_RETRIES - 1
                            and not emitted_data
                            and is_retriable_stream_error(exc)
                        ):
                            backoff = STREAM_RETRY_BACKOFF_SECS * (2**attempt)
                            logger.warning(
                                "Stream attempt %s failed (retrying in %.1fs): %s",
                                attempt + 1,
                                backoff,
                                exc,
                                exc_info=exc,
                            )
                            await asyncio.sleep(backoff)
                        else:
                            raise

            superseded = await self._finish_run_status(thread_id, run_id, _RUN_SUCCESS)
            if superseded is not None:
                # Another writer finalized the row first (e.g. swept it as
                # orphaned), or it was deleted: report that outcome, not a
                # contradicting success, and leave the thread to whoever
                # finalized it.
                stream.publish(*_terminal_event_for_row(run_id, superseded))
                return 0
            await self._settle_thread_after_success(thread_id)
            total_tokens = total_tokens_from_usage(usage_handler.usage_metadata)
            if total_tokens == 0:
                # Fallback for providers the callback handler can't see.
                total_tokens = await self._total_run_tokens(thread_id)
            stream.publish(
                "end",
                {
                    "run_id": run_id,
                    "status": _RUN_SUCCESS,
                    "usage": {"total_tokens": total_tokens},
                },
            )
            return total_tokens
        except asyncio.CancelledError:
            logger.warning(
                "Streaming run %s cancelled for thread %s", run_id, thread_id
            )
            raise
        except Exception as exc:
            logger.error(
                "Streaming run %s failed for thread %s: %s",
                run_id,
                thread_id,
                exc,
                exc_info=exc,
            )
            # Persist the failure best-effort; a store outage must not stop
            # subscribers from receiving the 'error' event.
            superseded = await self._mark_run_failed(run_id, thread_id, str(exc))
            if superseded is not None:
                # e.g. swept as orphaned, or deleted, while the graph ran:
                # report that outcome rather than this failure.
                stream.publish(*_terminal_event_for_row(run_id, superseded))
            else:
                stream.publish("error", {"detail": str(exc), "run_id": run_id})
            return 0

    async def _relay(
        self,
        events: AsyncGenerator[StreamEvent, None],
        task: asyncio.Task[Any] | None,
        *,
        run_id: str,
        cancel_on_disconnect: bool,
    ) -> AsyncIterator[str]:
        """Forward one subscriber's events as SSE frames.

        Mode filtering is the subscription's (``RunEventStream.subscribe``).

        If the client goes away before the stream ends (or its queue
        overflows, which is treated the same way) and ``cancel_on_disconnect``
        is set, the run is cancelled; otherwise it keeps running for whoever
        joins next.
        """
        completed = False
        try:
            async for event in events:
                yield event.frame
            completed = True
        except SubscriberOverflowError as exc:
            cancelled = (
                cancel_on_disconnect and task is not None and not task.cancelling()
            )
            if cancelled and task is not None:
                task.cancel()
            completed = True
            logger.warning(
                "Subscriber to run %s overflowed and was detached; %s",
                run_id,
                "cancelling the run" if cancelled else "the run continues",
            )
            yield sse_event(
                "error",
                {
                    "code": "subscriber_overflow",
                    "detail": str(exc),
                    "run_id": run_id,
                },
                None,
            )
        finally:
            # Decide before awaiting: on a disconnect this runs inside an
            # already-cancelled scope, where an await may raise again.
            if (
                not completed
                and cancel_on_disconnect
                and task is not None
                and not task.cancelling()
            ):
                task.cancel()
            await events.aclose()

    async def _final_state_events(
        self,
        thread_id: str,
        run_id: str,
        task: asyncio.Task[Any] | None,
        *,
        stream_modes: Sequence[str],
        cancel_on_disconnect: bool,
    ) -> AsyncIterator[str]:
        """Stream a finished run's outcome: final ``values`` + ``end``, or ``error``.

        For a joined run with no event history to replay. The events carry no
        ``id`` — they are not part of the run's event sequence, so there is
        nothing to resume from.
        """
        try:
            if task is not None:
                try:
                    await self._registry.wait(run_id)
                except asyncio.CancelledError:
                    current = self._registry.get(run_id) or task
                    if (
                        cancel_on_disconnect
                        and not current.done()
                        and not current.cancelling()
                    ):
                        current.cancel()
                    raise
                except Exception as exc:
                    # The run's finalizer failed (the registry logged it); the
                    # state read below was never attempted.
                    logger.error(
                        "Joined run %s did not finish cleanly: %s",
                        run_id,
                        exc,
                        exc_info=exc,
                    )
                    yield sse_event(
                        "error",
                        {
                            "detail": f"Run {run_id} did not finish cleanly.",
                            "run_id": run_id,
                        },
                        None,
                    )
                    return
            row = await self._metadata_store.fetch_run_row(thread_id, run_id)
            output: JsonValue = None
            if (
                row is not None
                and str(row["status"]) != _RUN_ERROR
                and stream_mode_matches("values", stream_modes)
            ):
                output = await self._final_state_values(
                    thread_id, run_id, raise_on_error=True
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Headers are already sent: report on the stream, never end clean.
            logger.error(
                "Failed to read final state for joined run %s: %s",
                run_id,
                exc,
                exc_info=exc,
            )
            yield sse_event(
                "error",
                {
                    "detail": f"Could not read the final state of run {run_id}.",
                    "run_id": run_id,
                },
                None,
            )
            return
        if row is None:
            # Deleted while we waited (rollback / DELETE).
            yield sse_event(
                "error",
                {"detail": f"Run {run_id} not found.", "run_id": run_id},
                None,
            )
            return
        run_status = str(row["status"])
        if run_status == _RUN_ERROR:
            yield sse_event(
                "error",
                {"detail": row.get("error") or "Run failed.", "run_id": run_id},
                None,
            )
            return
        if isinstance(output, dict):
            yield sse_event("values", self._streamer.filter_values(output), None)
        yield sse_event("end", {"run_id": run_id, "status": run_status}, None)

    # ------------------------------------------------------------------
    # Stateless runs (top-level /runs, /runs/wait, /runs/stream, /runs/batch)
    #
    # Each runs on an ephemeral thread the checkpointer can write to, deleted
    # on the way out whatever the outcome. The caller never sees it.
    # ------------------------------------------------------------------

    async def create_stateless_run(self, request: RunCreateRequest) -> RunModel:
        """Run to completion on an ephemeral thread; return the run metadata.

        Unlike the thread-scoped ``POST /threads/{id}/runs`` (background), this
        blocks: the thread is deleted before returning, so there would be
        nothing left to poll or join against.
        """
        payload = self._as_stateless(request)
        thread_id = str(uuid4())
        try:
            run_row, task = await self._admit_and_spawn(thread_id, payload)
            run_id = str(run_row["run_id"])
            await asyncio.wait({task})
            return await self.get_run(thread_id, run_id)
        finally:
            await self._discard_thread(thread_id)

    async def wait_stateless_run(self, request: RunCreateRequest) -> Any:
        """Run to completion on an ephemeral thread; return the final state.

        The output is captured from the graph invocation rather than read back
        afterwards: by the time this returns, the thread and its checkpoints
        are gone, so there is nothing left to read.
        """
        payload = self._as_stateless(request)
        thread_id = str(uuid4())
        run_id = str(uuid4())
        try:
            await self._thread_ops.ensure_thread_for_run(
                thread_id, payload.if_not_exists
            )
            self._assistant_ops.ensure_supported(payload.assistant_id)
            self._validate_run_request(payload)
            _, result = await self._execute_graph_run(thread_id, payload, run_id=run_id)
            return serialize_value(result)
        except HTTPException:
            raise
        except Exception as exc:
            logger.error("Stateless run failed: %s", exc, exc_info=exc)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=str(exc),
            ) from exc
        finally:
            await self._discard_thread(thread_id)

    async def create_stateless_streaming_run(
        self, request: RunCreateRequest
    ) -> tuple[RunModel, AsyncIterator[str]]:
        """Stream a run on an ephemeral thread, deleting it once the run ends.

        Cleanup is tied to the run, not to the response: with
        ``on_disconnect="continue"`` the run outlives a departed client, and
        deleting the thread when the response closed would pull the
        checkpointer out from under it.
        """
        payload = self._as_stateless(request)
        # This endpoint exposes neither the ephemeral thread id nor a usable
        # run URL, so retained history could never be joined or replayed.
        payload = payload.model_copy(update={"stream_resumable": False})
        thread_id = str(uuid4())

        async def discard() -> None:
            await self._discard_thread(thread_id)

        # ``create_streaming_run`` runs ``discard`` exactly once, including
        # when the run never starts.
        return await self.create_streaming_run(thread_id, payload, after_run=discard)

    async def run_stateless_batch(self, requests: list[RunCreateRequest]) -> list[Any]:
        """Run each payload on its own ephemeral thread; return outputs in order.

        Sequential on purpose: a parallel fan-out is a hidden load multiplier.
        Callers wanting concurrency can issue concurrent requests.
        """
        return [await self.wait_stateless_run(request) for request in requests]

    def _as_stateless(self, request: RunCreateRequest) -> RunCreateRequest:
        """Adapt a run payload to a thread that does not exist yet."""
        if request.checkpoint is not None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    "A stateless run has no checkpoint history to resume from. "
                    "Use POST /threads/{thread_id}/runs/... instead."
                ),
            )
        # if_not_exists is forced rather than honoured: the caller never sees
        # the thread id, so it cannot have created it, and the default
        # ("reject") would 404 every stateless run.
        return request.model_copy(update={"if_not_exists": "create"})

    async def _discard_thread(self, thread_id: str) -> None:
        """Delete an ephemeral thread and its checkpoints, best-effort.

        A leftover thread is litter, not a fault: failing the request over it
        would turn a finished run into an error the caller cannot act on.
        """
        try:
            await self._thread_ops.delete(thread_id)
        except Exception as exc:  # noqa: BLE001 - cleanup must not mask the run
            logger.warning(
                "Could not delete ephemeral thread %s: %s", thread_id, exc, exc_info=exc
            )

    async def list_runs(
        self,
        thread_id: str,
        *,
        limit: int,
        offset: int,
        status_value: RunStatus | None,
    ) -> list[RunModel]:
        """List run metadata rows for a thread."""
        await self._thread_ops.ensure_exists(thread_id)
        rows = await self._metadata_store.list_run_rows(
            thread_id,
            limit=limit,
            offset=offset,
            status_value=status_value,
        )
        return [self._run_row_to_model(row) for row in rows]

    async def get_run(self, thread_id: str, run_id: str) -> RunModel:
        """Return a single run metadata record."""
        await self._thread_ops.ensure_exists(thread_id)
        row = await self._metadata_store.fetch_run_row(thread_id, run_id)
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Run {run_id} not found for thread {thread_id}.",
            )
        return self._run_row_to_model(row)

    # ------------------------------------------------------------------
    # Orphaned runs
    #
    # A run's task lives in the process that started it. If that process dies
    # without a graceful shutdown (crash, OOM kill, ``docker kill``), its
    # ``pending``/``running`` rows stay that way forever and its threads stay
    # ``busy``. Every process heartbeats the runs it owns (bumping
    # ``updated_at``) and fails in-flight rows whose heartbeat has stopped —
    # LangGraph Platform's end state too, minus its retries.
    # ------------------------------------------------------------------

    async def heartbeat_runs(self) -> None:
        """Refresh the liveness of every run this process is executing."""
        run_ids = [run_id for _, run_id in self._registry.all_active()]
        if run_ids:
            await self._metadata_store.touch_runs(run_ids)

    async def fail_orphaned_runs(self, *, stale_after_seconds: float) -> list[str]:
        """Fail in-flight runs whose owner stopped heartbeating.

        Returns the ids of the runs this pass failed. This process's own active
        runs are never touched, whatever their row says. A thread still
        ``busy`` with nothing left in flight is moved to ``error``, as when one
        of its runs fails normally; a thread a later run already settled keeps
        its status. Each thread is released on its own, so one failing store
        call cannot strand the rest ``busy``, and a failed release is retried
        on the next pass. The pass ends with :meth:`_release_stuck_threads`,
        which settles threads left ``busy`` with nothing in flight.
        """
        local = {run_id for _, run_id in self._registry.all_active()}
        rows = await self._metadata_store.fail_stale_runs(
            stale_after_seconds=stale_after_seconds,
            exclude_run_ids=local,
            error=_ORPHANED_RUN_ERROR,
        )
        for row in rows:
            logger.warning(
                "Failed orphaned run %s on thread %s (status was in flight with "
                "no heartbeat for over %.0fs)",
                row["run_id"],
                row["thread_id"],
                stale_after_seconds,
            )
        self._unreleased_threads.update(str(row["thread_id"]) for row in rows)
        for thread_id in sorted(self._unreleased_threads):
            try:
                await self._release_orphaned_thread(thread_id)
            except Exception as exc:
                # Kept in ``_unreleased_threads``: the next pass retries it.
                logger.error(
                    "Failed to release thread %s after failing its orphaned runs: %s",
                    thread_id,
                    exc,
                    exc_info=exc,
                )
            else:
                self._unreleased_threads.discard(thread_id)
        await self._release_stuck_threads(stale_after_seconds=stale_after_seconds)
        return [str(row["run_id"]) for row in rows]

    async def _release_stuck_threads(self, *, stale_after_seconds: float) -> None:
        """Settle ``busy`` threads that nothing is running on any more.

        The durable backstop for the in-process retry sets: a process that dies
        after a run's terminal status is saved but before its thread is moved
        off ``busy`` (a sweeper releasing an orphan's thread, or a run settling
        after ``success``) leaves no record anywhere else; neither does a
        failed or interrupted run whose thread update failed, which has no
        in-process retry. A thread qualifies
        once it has been ``busy`` for longer than the orphan timeout with no
        run in flight. It settles as its latest run would have left it: as
        after a clean finish on ``success``, ``idle`` on ``interrupted``, and
        ``error`` otherwise.
        """
        cutoff = datetime.now(UTC) - timedelta(seconds=stale_after_seconds)
        page_size, offset = 100, 0
        busy: list[str] = []
        # Oldest first (indexed on every store), so the scan stops at the first
        # thread busy for less than the timeout. Collected before releasing:
        # a released thread leaves the filtered result and would shift pages.
        while True:
            page = await self._metadata_store.search_thread_rows(
                ThreadSearchRequest(
                    status=_THREAD_BUSY,
                    limit=page_size,
                    offset=offset,
                    sort_by="updated_at",
                    sort_order="asc",
                )
            )
            stale = [t for t in page if _as_utc(t["updated_at"]) < cutoff]
            busy.extend(str(thread["thread_id"]) for thread in stale)
            if len(stale) < page_size:
                break
            offset += page_size
        for thread_id in busy:
            try:
                if self._registry.active_runs(thread_id):
                    continue
                latest = await self._metadata_store.list_run_rows(
                    thread_id, limit=1, offset=0, status_value=None
                )
                outcome = str(latest[0]["status"]) if latest else None
                settled: ThreadStatus
                if outcome == _RUN_SUCCESS:
                    settled = await self._settled_thread_status(thread_id)
                elif outcome == _RUN_INTERRUPTED:
                    settled = _THREAD_IDLE
                else:
                    settled = _THREAD_ERROR
                # Conditional, so a run another worker started on the thread
                # since the scan keeps it ``busy``.
                if not await self._metadata_store.release_busy_thread(
                    thread_id,
                    settled,
                    # As the run's own settle would have: it changed the state.
                    mark_state_updated=outcome == _RUN_SUCCESS,
                ):
                    continue
            except Exception as exc:
                # Still ``busy``, so the next pass finds it again.
                logger.error(
                    "Failed to release stuck busy thread %s: %s",
                    thread_id,
                    exc,
                    exc_info=exc,
                )
                continue
            logger.warning(
                "Released thread %s: busy for over %.0fs with no run in flight",
                thread_id,
                stale_after_seconds,
            )

    async def _release_orphaned_thread(self, thread_id: str) -> None:
        """Move a thread whose runs were swept from ``busy`` to ``error``.

        Only while nothing is in flight on it, checked atomically with the
        write (``release_busy_thread``): a run another worker starts on the
        thread meanwhile keeps it ``busy``.
        """
        if not self._registry.active_runs(thread_id):
            await self._metadata_store.release_busy_thread(thread_id, _THREAD_ERROR)

    async def liveness_pass(self, *, stale_after_seconds: float | None) -> None:
        """One heartbeat, then one sweep; failures are logged, never raised.

        A metadata-store blip must neither crash startup nor stop liveness
        tracking for good: the next pass simply tries again.
        """
        await self._heartbeat_pass()
        await self._sweep_pass(stale_after_seconds=stale_after_seconds)

    async def maintain_runs(
        self, *, heartbeat_seconds: float, stale_after_seconds: float | None
    ) -> None:
        """Heartbeat and sweep every ``heartbeat_seconds``, forever.

        The two run on independent schedules: a slow sweep (many stuck
        threads, a slow store) must not delay this process's heartbeats past
        the orphan timeout, or another worker would fail its live runs.
        """

        async def every(interval: float, step: Callable[[], Awaitable[None]]) -> None:
            while True:
                await asyncio.sleep(interval)
                await step()

        async with asyncio.TaskGroup() as group:
            group.create_task(every(heartbeat_seconds, self._heartbeat_pass))
            group.create_task(
                every(
                    heartbeat_seconds,
                    lambda: self._sweep_pass(stale_after_seconds=stale_after_seconds),
                )
            )

    async def _heartbeat_pass(self) -> None:
        try:
            await self.heartbeat_runs()
        except Exception as exc:
            logger.error("Run heartbeat failed: %s", exc, exc_info=exc)

    async def _sweep_pass(self, *, stale_after_seconds: float | None) -> None:
        try:
            await self._retry_unsettled_threads()
            if stale_after_seconds is not None:
                await self.fail_orphaned_runs(stale_after_seconds=stale_after_seconds)
        except Exception as exc:
            logger.error("Run orphan sweep failed: %s", exc, exc_info=exc)

    async def shutdown(self) -> None:
        """Cancel every run task this process tracks (runtime shutdown).

        That covers background and streaming runs alike.

        A task cancelled before it ever started executing never runs its own
        ``interrupted`` cleanup, so after cancelling we sweep the runs that were
        active and persist ``interrupted`` for any still left non-terminal — no
        run row is stranded at ``pending``/``running`` across a restart.
        """
        active = self._registry.all_active()
        await self._registry.shutdown()
        for thread_id, run_id in active:
            row = await self._metadata_store.fetch_run_row(thread_id, run_id)
            if row is not None and str(row["status"]) not in _TERMINAL_STATUSES:
                await self._mark_run_interrupted(run_id, thread_id)

    # ------------------------------------------------------------------ helpers

    async def _admit_and_spawn(
        self, thread_id: str, request: RunCreateRequest
    ) -> tuple[RunRow, asyncio.Task[Any]]:
        """Admit a run (multitask policy), persist it, and spawn its task.

        Holds the per-thread admission lock across the strategy check, the row
        insert, and the spawn so the new run is registered as active before the
        lock is released — no admission can race it.
        """
        await self._thread_ops.ensure_thread_for_run(thread_id, request.if_not_exists)
        self._assistant_ops.ensure_supported(request.assistant_id)
        self._validate_run_request(request)
        run_id = str(uuid4())
        async with self._registry.admission(thread_id):
            await self._resolve_multitask(thread_id, request.multitask_strategy)
            run_row = await self._metadata_store.create_run(
                run_id=run_id,
                thread_id=thread_id,
                assistant_id=request.assistant_id,
                metadata=request.metadata,
                kwargs=self._build_run_kwargs(request),
                multitask_strategy=request.multitask_strategy,
            )
            task = self._registry.spawn(
                thread_id,
                run_id,
                self._run_to_completion(run_id, thread_id, request),
            )
        return run_row, task

    async def _resolve_multitask(
        self, thread_id: str, strategy: MultitaskStrategy
    ) -> None:
        """Apply the multitask strategy against the thread's active runs.

        ``reject`` 409s when busy; ``interrupt`` cancels every active run
        (background or streaming; running, queued, or still being admitted);
        ``rollback`` cancels and deletes them; ``enqueue`` is a no-op (the new
        run's task simply waits on the execution lock).
        """
        active = self._registry.active_runs(thread_id)
        if not active:
            return
        if strategy == "reject":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"Thread {thread_id} already has an active run; "
                    f"multitask strategy {strategy!r} rejected."
                ),
            )
        if strategy in ("interrupt", "rollback"):
            for run_id in active:
                cancelled = await self._registry.cancel(run_id, wait=True)
                if cancelled and strategy == "rollback":
                    await self._metadata_store.delete_run(thread_id, run_id)

    async def _run_to_completion(
        self, run_id: str, thread_id: str, request: RunCreateRequest
    ) -> int:
        """Execute a run to completion in the background; return total tokens.

        Acquires the execution lock for the duration (so ``enqueue`` runs
        serialise — one at a time). Graph failures are persisted and swallowed —
        the task has no awaiter in the background path, so a raised exception
        would be logged by asyncio as "never retrieved". ``CancelledError`` is
        persisted as ``interrupted`` and re-raised so the task is genuinely
        cancelled.
        """
        lock = self._lock_manager.get(thread_id)
        try:
            await lock.acquire()
        except asyncio.CancelledError:
            # Cancelled while still queued for the lock (e.g. shutdown, or a
            # superseding interrupt/rollback) before execution began. No lock is
            # held to release; persist ``interrupted`` so the run row does not
            # stay stuck at ``pending``. The run never started, so the thread
            # (another run's, if any) is not its to touch.
            logger.warning("Queued run %s cancelled for thread %s", run_id, thread_id)
            await self._interrupt_row_only(run_id)
            raise
        try:
            if await self._claim_run(thread_id, run_id) is not None:
                # Finalized elsewhere while queued; keep its outcome, run nothing.
                return 0
            await self._metadata_store.update_thread(
                thread_id, status_value=_THREAD_BUSY
            )
            usage_handler, _ = await self._execute_graph_run(
                thread_id, request, run_id=run_id
            )
            if (
                await self._finish_run_status(thread_id, run_id, _RUN_SUCCESS)
                is not None
            ):
                # Another writer finalized the row first (e.g. swept it as
                # orphaned), or it was deleted; keep that outcome.
                return 0
            await self._settle_thread_after_success(thread_id)
            total_tokens = total_tokens_from_usage(usage_handler.usage_metadata)
            if total_tokens == 0:
                # Fallback for providers the callback handler can't see. Read
                # while we still hold the lock; otherwise an enqueued run for
                # this thread could advance the graph state between release and
                # the read, yielding another run's totals.
                total_tokens = await self._total_run_tokens(thread_id)
            return total_tokens
        except asyncio.CancelledError:
            logger.warning(
                "Background run %s cancelled for thread %s", run_id, thread_id
            )
            superseded = await self._mark_run_interrupted(run_id, thread_id)
            if superseded is not None and str(superseded["status"]) == _RUN_SUCCESS:
                # Cancelled after ``success`` was saved (possibly before the
                # thread was settled); settling again is idempotent.
                await self._settle_thread_after_success(thread_id)
            raise
        except Exception as exc:
            logger.error(
                "Run %s failed for thread %s: %s", run_id, thread_id, exc, exc_info=exc
            )
            await self._mark_run_failed(run_id, thread_id, str(exc))
            return 0
        finally:
            lock.release()

    async def _collect_terminal_output(
        self, thread_id: str, run_id: str, task: asyncio.Task[Any] | None
    ) -> tuple[JsonValue, int]:
        """Return ``(output, tokens)`` for a finished run.

        Raises 404 if the run row is gone (it can be deleted by a concurrent
        ``cancel(action=rollback)`` or ``DELETE`` while a waiter/joiner is in
        flight) and 500 if the run itself errored.
        """
        final = await self._metadata_store.fetch_run_row(thread_id, run_id)
        if final is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Run {run_id} not found for thread {thread_id}.",
            )
        if str(final["status"]) == _RUN_ERROR:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=final.get("error") or "Run failed.",
            )
        tokens = 0
        if task is not None and not task.cancelled():
            result = task.result()
            tokens = result if isinstance(result, int) else 0
        try:
            output = await self._final_state_values(
                thread_id, run_id, raise_on_error=True
            )
        except Exception as exc:
            # Fail closed: a fallback read could return a later run's state,
            # and a null output would look like a run that produced nothing.
            logger.error(
                "Failed to read the final state of run %s: %s",
                run_id,
                exc,
                exc_info=exc,
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Could not read the final state of run {run_id}.",
            ) from exc
        return output, tokens

    async def _final_state_values(
        self, thread_id: str, run_id: str, *, raise_on_error: bool = False
    ) -> JsonValue:
        """Return the requested run's final graph state values (its output).

        Reads the most recent checkpoint tagged with this ``run_id`` so a
        follow-on enqueued run — which can start the instant this run releases
        the execution lock — cannot leak its own state into this waiter's output
        (the LangGraph ``runs.wait``/``runs.join`` contract returns output for
        the requested run). Falls back to the latest thread state when no
        run-scoped checkpoint is available (e.g. no checkpointer).

        Whatever checkpoint answers, a run parked on ``interrupt()`` reports it
        — see :meth:`_output_with_interrupts`. A failed state read returns
        ``None`` (logged) unless ``raise_on_error``, which re-raises it.
        """
        config = {"configurable": {"thread_id": thread_id}}
        get_history = getattr(self._graph, "aget_state_history", None)
        if get_history is not None:
            try:
                async for snapshot in get_history(
                    config, filter={"run_id": run_id}, limit=1
                ):
                    return self._output_with_interrupts(snapshot)
            except Exception as exc:
                if raise_on_error:
                    # The latest thread state may belong to a later run.
                    raise
                # Run-scoped read is best-effort; fall back to the latest state.
                logger.warning(
                    "Run-scoped state read failed for run %s; using latest "
                    "thread state: %s",
                    run_id,
                    exc,
                    exc_info=exc,
                )
        try:
            snapshot = await self._graph.aget_state(config)
        except Exception as exc:
            if raise_on_error:
                raise
            logger.error(
                "Failed to read final state for thread %s: %s",
                thread_id,
                exc,
                exc_info=exc,
            )
            return None
        return self._output_with_interrupts(snapshot)

    def _output_with_interrupts(self, snapshot: Any) -> JsonValue:
        """Serialize a snapshot's values, carrying its pending interrupts along.

        Interrupts live on the snapshot, not in ``values``, so a parked run would
        look finished. They are merged onto the reserved ``__interrupt__``
        channel, where streaming, Platform's ``runs.wait`` and ``useStream``
        expect them.
        """
        output = serialize_value(getattr(snapshot, "values", None))
        interrupts = _pending_interrupts(snapshot)
        if not interrupts:
            return output
        if not isinstance(output, dict):
            # Non-mapping graph state has no channel to carry them on. Log
            # loudly rather than drop the pause silently: the client is about
            # to read a parked run as a completed one.
            logger.warning(
                "Paused run output is %s, not a mapping; cannot report %s",
                type(output).__name__,
                _INTERRUPT_CHANNEL,
            )
            return output
        if output.get(_INTERRUPT_CHANNEL):
            # Already on the channel (a graph that writes it as state, or a
            # LangGraph version that surfaces it in ``values``) — don't
            # second-guess it.
            return output
        output[_INTERRUPT_CHANNEL] = serialize_value(interrupts)
        return output

    async def _execute_graph_run(
        self, thread_id: str, request: RunCreateRequest, run_id: str | None = None
    ) -> tuple[UsageMetadataCallbackHandler, Any]:
        """Execute a graph run without streaming.

        Returns the usage handler and the graph's final state. The state is
        what ``POST /runs/wait`` answers with: a stateless run deletes its
        thread on the way out, so the output has to be captured here rather
        than read back from a checkpoint that will no longer exist.
        """
        runnable_input = self._resolve_run_input(request)
        config = build_thread_config(
            thread_id, request.config, request.checkpoint, run_id=run_id
        )
        usage_handler = attach_usage_handler(config)
        with run_tracing_context(request.langsmith_tracer):
            result = await self._graph.ainvoke(
                runnable_input,
                config,
                context=normalize_input_payload(request.context),
                stream_mode="values",
                interrupt_before=request.interrupt_before,
                interrupt_after=request.interrupt_after,
                durability=request.durability,
            )
        return usage_handler, result

    async def _settled_thread_status(self, thread_id: str) -> ThreadStatus:
        """Return the status a thread settles into once its run finishes cleanly.

        ``interrupted`` when the graph is parked on ``interrupt()`` (as on
        LangGraph Platform; clients use it to offer a resume), else ``idle``.

        A read failure raises rather than guessing ``idle``, which would lose a
        pending interrupt for good: every caller leaves the thread ``busy`` and
        retries (the settle retry, or the stuck-thread release).
        """
        snapshot = await self._graph.aget_state(
            {"configurable": {"thread_id": thread_id}}
        )
        if snapshot is not None and _pending_interrupts(snapshot):
            return _THREAD_INTERRUPTED
        return _THREAD_IDLE

    async def _total_run_tokens(self, thread_id: str) -> int:
        """Fallback token count: sum usage over the final checkpoint's messages.

        Used only when the per-run usage callback recorded nothing (providers
        that don't populate ``usage_metadata`` + ``model_name``). Reads the
        latest checkpoint's raw messages and sums their token counts. Caveat:
        this covers the thread's whole message history, so on multi-turn
        threads it reports cumulative totals, not this run's. Degrades to 0
        when no checkpointer or state is available.
        """
        try:
            config = {"configurable": {"thread_id": thread_id}}
            snapshot = await self._graph.aget_state(config)
        except Exception as exc:
            # A read failure (as opposed to "no checkpointer / no state") means
            # usage is unknown, not zero — log at error level so the silent 0
            # reported to the quota gateway is at least observable.
            logger.error(
                "Failed to read state for token usage on thread %s; "
                "reporting 0 tokens: %s",
                thread_id,
                exc,
                exc_info=exc,
            )
            return 0
        values = getattr(snapshot, "values", None)
        if not isinstance(values, dict):
            return 0
        messages = values.get("messages") or []
        if not isinstance(messages, list):
            return 0
        return total_tokens_from_messages(messages)

    def _run_row_to_model(self, row: RunRow) -> RunModel:
        """Convert a run metadata row into the API response model."""
        return RunModel(
            run_id=UUID(str(row["run_id"])),
            thread_id=UUID(str(row["thread_id"])),
            assistant_id=str(row["assistant_id"]),
            created_at=row["created_at"].isoformat(),
            updated_at=row["updated_at"].isoformat(),
            status=row["status"],
            metadata=serialize_mapping(row["metadata"]),
            kwargs=serialize_mapping(row["kwargs"]),
            multitask_strategy=row["multitask_strategy"],
            langsmith_session_name=_session_name_of(row["kwargs"]),
        )

    def _resolve_run_input(self, request: RunCreateRequest) -> Any:
        """Resolve the input or command object passed to the graph."""
        self._validate_run_request(request)
        command = normalize_command_payload(request.command)
        if command is not None:
            return command
        return normalize_input_payload(request.input)

    def _validate_run_request(self, request: RunCreateRequest) -> None:
        """Reject platform-only request options that this server does not support."""
        if request.after_seconds is not None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Scheduled runs are not supported by the OSS server.",
            )
        if request.webhook is not None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Webhook callbacks are not supported by the OSS server.",
            )

    def _build_run_kwargs(self, request: RunCreateRequest) -> dict[str, JsonValue]:
        """Persist the key run settings used to invoke the graph."""
        checkpoint: dict[str, JsonValue] | None = None
        if request.checkpoint is not None:
            checkpoint = serialize_mapping(request.checkpoint.model_dump(mode="python"))
        return {
            "assistant_id": request.assistant_id,
            "config": serialize_value(request.config),
            "context": serialize_value(request.context),
            "checkpoint": checkpoint,
            "stream_mode": serialize_value(request.stream_mode),
            "stream_subgraphs": request.stream_subgraphs,
            "stream_resumable": request.stream_resumable,
            "interrupt_before": serialize_value(request.interrupt_before),
            "interrupt_after": serialize_value(request.interrupt_after),
            "on_disconnect": request.on_disconnect,
            "durability": request.durability,
            # Stored with the run rather than in a new column, so the session
            # name survives without a metadata-store migration. Resolved at
            # creation: it records where this run's trace was sent.
            "langsmith_tracer": (
                serialize_mapping(request.langsmith_tracer.model_dump(mode="python"))
                if request.langsmith_tracer is not None
                else None
            ),
            "langsmith_session_name": resolve_session_name(request.langsmith_tracer),
        }

    def _abandon_admission(self, task: asyncio.Task[Any], *, cancel_run: bool) -> None:
        """Settle an admission whose client already left.

        A failure is logged, as nobody else will see it. A success detaches the
        client's eager subscription, which no relay will ever drain, and with
        ``cancel_run`` also stops the run.
        """
        if task.cancelled():
            return
        if (exc := task.exception()) is not None:
            logger.error(
                "Streaming run admission failed after its client left: %s",
                exc,
                exc_info=exc,
            )
            return
        run, events, producer = task.result()
        stream = self._streams.get(str(run.thread_id), str(run.run_id))
        if stream is not None:
            stream.detach(events)
        if cancel_run:
            producer.cancel()

    async def _await_admission(self, run_id: str) -> None:
        """Wait, without cancelling it, for a streaming run's admission."""
        admitting = self._admitting.get(run_id)
        if admitting is not None:
            await asyncio.wait({admitting})

    async def _settle_thread_after_success(self, thread_id: str) -> None:
        """Settle a succeeded run's thread; never raises an ordinary exception.

        A cancel still propagates.

        The run's ``success`` is already saved, so a failure here must not turn
        it into an error. A failed (or cancelled) settle is queued for the
        liveness pass to retry instead of leaving the thread ``busy``.
        """
        self._unsettled_threads.add(thread_id)
        try:
            await self._metadata_store.update_thread(
                thread_id,
                status_value=await self._settled_thread_status(thread_id),
                mark_state_updated=True,
            )
        except Exception as exc:
            logger.error(
                "Failed to settle thread %s after its run succeeded; will retry: %s",
                thread_id,
                exc,
                exc_info=exc,
            )
        else:
            self._unsettled_threads.discard(thread_id)

    async def _retry_unsettled_threads(self) -> None:
        """Retry thread settles that failed after a run's ``success`` was saved.

        A thread that is gone or no longer ``busy`` is dropped: whoever changed
        it settled it. One with a run in flight is skipped for now, not dropped:
        that run may be the settle still in progress, or one whose own settle
        could fail too. The settle is conditional (``release_busy_thread``), so
        a run another worker starts on the thread meanwhile keeps it ``busy``.
        """
        for thread_id in sorted(self._unsettled_threads):
            try:
                thread = await self._metadata_store.fetch_thread_row(thread_id)
                if thread is None or str(thread["status"]) != _THREAD_BUSY:
                    self._unsettled_threads.discard(thread_id)
                    continue
                if self._registry.active_runs(thread_id):
                    continue
                settled = await self._settled_thread_status(thread_id)
                if not await self._metadata_store.release_busy_thread(
                    thread_id, settled, mark_state_updated=True
                ):
                    # A run is in flight on it (kept for the next pass), or it
                    # changed since the check (dropped on the next pass, as above).
                    continue
            except Exception as exc:
                logger.error(
                    "Failed to retry the settle of thread %s; will retry: %s",
                    thread_id,
                    exc,
                    exc_info=exc,
                )
                continue
            self._unsettled_threads.discard(thread_id)

    async def _claim_run(self, thread_id: str, run_id: str) -> _Outcome | None:
        """Move an admitted run to ``running``.

        Returns ``None`` once claimed, or the winning outcome (its final row,
        or a deleted outcome) if it can no longer run. Claimed before the
        thread is marked ``busy`` or the graph runs, so a run finalized
        elsewhere meanwhile (e.g. swept as orphaned) or deleted neither
        executes nor takes its thread back.
        """
        if await self._metadata_store.update_run_status(run_id, _RUN_RUNNING):
            return None
        row = await self._metadata_store.fetch_run_row(thread_id, run_id)
        return _deleted_outcome(run_id) if row is None else row

    async def _finish_run_status(
        self,
        thread_id: str,
        run_id: str,
        status_value: RunStatus,
        *,
        error: str | None = None,
    ) -> _Outcome | None:
        """Persist a terminal status; return the winning outcome if it lost.

        ``update_run_status`` only moves in-flight rows and a terminal status is
        final, so a write that updated nothing lost to another writer that
        finalized the row first: an orphan sweep on another worker, or this
        run's own earlier ``success`` write. A row that is
        gone (its thread deleted, or the swept row deleted) lost too: its late
        owner must neither report an outcome of its own nor touch the thread.
        ``None`` means this write took effect.
        """
        if await self._metadata_store.update_run_status(
            run_id, status_value, error=error
        ):
            return None
        row = await self._metadata_store.fetch_run_row(thread_id, run_id)
        if row is None:
            logger.warning(
                "Run %s finished as %s, but its row was deleted; "
                "leaving its thread alone",
                run_id,
                status_value,
            )
            return _deleted_outcome(run_id)
        logger.warning(
            "Run %s finished as %s, but its row was already finalized as %s; "
            "keeping the persisted status",
            run_id,
            status_value,
            row["status"],
        )
        return row

    async def _mark_run_failed(
        self, run_id: str, thread_id: str, error: str
    ) -> _Outcome | None:
        """Persist error state for a failed run; best-effort.

        Never raises an ordinary exception (a cancel still propagates). The
        store outage that fails these writes is often the same one that failed
        the run, so they must not mask the original exception or block the
        client's 'error' event. Returns the winning outcome (the persisted row,
        or a synthesized deleted outcome) when the run was already finalized
        otherwise (e.g. an orphan sweep on another worker failed it first, or
        its row was deleted), leaving the thread to that outcome.
        """
        try:
            superseded = await self._finish_run_status(
                thread_id, run_id, _RUN_ERROR, error=error
            )
            if superseded is None:
                await self._metadata_store.update_thread(
                    thread_id, status_value=_THREAD_ERROR
                )
            return superseded
        except Exception as exc:
            logger.error(
                "Failed to persist error state for run %s: %s",
                run_id,
                exc,
                exc_info=exc,
            )
            return None

    async def _mark_run_interrupted(
        self, run_id: str, thread_id: str
    ) -> _Outcome | None:
        """Persist cancellation/disconnect state; best-effort.

        Never raises an ordinary exception (a cancel still propagates). Returns
        the winning outcome (the persisted row, or a synthesized deleted
        outcome) when the run was already finalized otherwise (a cancel landing
        after ``success`` committed, an orphan sweep, or a deleted row),
        leaving the thread to that outcome.
        """
        try:
            superseded = await self._finish_run_status(
                thread_id, run_id, _RUN_INTERRUPTED, error="Run interrupted."
            )
            if superseded is None:
                await self._metadata_store.update_thread(
                    thread_id, status_value=_THREAD_IDLE
                )
            return superseded
        except Exception as exc:
            logger.error(
                "Failed to persist interrupted state for run %s: %s",
                run_id,
                exc,
                exc_info=exc,
            )
            return None

    async def _interrupt_row_only(self, run_id: str) -> None:
        """Mark a run that never started ``interrupted``, leaving its thread.

        For a run stopped before it ran: superseded, its client left with
        ``on_disconnect="cancel"``, its insert failed, or shutdown.
        Best-effort, never raises an ordinary exception. No-op when the row was
        never inserted, or is already terminal.
        """
        try:
            await self._metadata_store.update_run_status(
                run_id, _RUN_INTERRUPTED, error="Run cancelled before it started."
            )
        except Exception as exc:
            logger.error(
                "Failed to persist interrupted state for unstarted run %s: %s",
                run_id,
                exc,
                exc_info=exc,
            )
