"""Run lifecycle: background create, wait, stream, join, cancel, delete, list, get.

Every run executes inside a background :class:`asyncio.Task` tracked by the
:class:`BackgroundRunRegistry`. ``create_run`` returns immediately with a
``pending`` run; ``wait_run`` / ``join_run`` await the task; ``cancel_run`` and
the interrupt/rollback multitask strategies cancel it. Streaming runs execute in
the request (live SSE) but register with the same registry + execution lock so
multitask admission stays uniform across paths.
"""

import asyncio
from typing import Any, AsyncIterator, Final
from uuid import UUID, uuid4

from fastapi import HTTPException, status

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
    Streamer,
    is_retriable_stream_error,
    sse_event,
)
from skeino.usage import (
    attach_usage_handler,
    total_tokens_from_messages,
    total_tokens_from_usage,
)

_THREAD_BUSY: Final[ThreadStatus] = "busy"
_THREAD_IDLE: Final[ThreadStatus] = "idle"
_THREAD_ERROR: Final[ThreadStatus] = "error"
_RUN_RUNNING: Final[RunStatus] = "running"
_RUN_SUCCESS: Final[RunStatus] = "success"
_RUN_ERROR: Final[RunStatus] = "error"
_RUN_INTERRUPTED: Final[RunStatus] = "interrupted"
_TERMINAL_STATUSES: Final[frozenset[str]] = frozenset(
    {"success", "error", "interrupted", "timeout"}
)


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
        logger: Any | None = None,
    ) -> None:
        """Capture every collaborator a run needs."""
        self._graph = graph
        self._metadata_store = metadata_store
        self._streamer = streamer
        self._thread_ops = thread_ops
        self._assistant_ops = assistant_ops
        self._lock_manager = lock_manager
        self._registry = registry
        self._logger = logger

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
        task = self._registry.get(run_id)
        if task is not None:
            await asyncio.wait({task})
        elif run.status not in _TERMINAL_STATUSES:
            # No background task to await and the run is still in flight — a live
            # streaming run has no joinable server-side handle in v1. Fail fast
            # rather than return a non-terminal snapshot and break the contract.
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"Run {run_id} is {run.status} with no joinable background "
                    "task; live streaming runs cannot be joined in this version."
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
        the run is already terminal or cannot be cancelled (e.g. a live
        streaming run, which is cancelled by client disconnect in v1).

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
                detail=(
                    f"Run {run_id} has no cancellable background task; "
                    "live streaming runs are cancelled by client disconnect."
                ),
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
        self, thread_id: str, request: RunCreateRequest
    ) -> tuple[RunModel, AsyncIterator[str]]:
        """Create a run and stream its output as SSE (live, in-request)."""
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
        # Acquire the execution lock (``enqueue`` waits here). Unregister on any
        # failure so a cancelled wait does not leak an active slot.
        try:
            await lock.acquire()
        except BaseException:
            self._registry.unregister_external(thread_id, run_id)
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
            run = self._run_row_to_model(run_row)
        except BaseException:
            lock.release()
            self._registry.unregister_external(thread_id, run_id)
            raise

        async def event_stream() -> AsyncIterator[str]:
            event_id = 1
            emitted_data = False
            try:
                await self._metadata_store.update_thread(
                    thread_id, status_value=_THREAD_BUSY
                )
                await self._metadata_store.update_run_status(
                    str(run.run_id), _RUN_RUNNING
                )
                yield sse_event(
                    "metadata",
                    {
                        "run_id": str(run.run_id),
                        "thread_id": str(thread_id),
                        "run": serialize_value(run.model_dump(mode="json")),
                    },
                    event_id,
                )
                event_id += 1

                runnable_input = self._resolve_run_input(request)
                config = build_thread_config(
                    thread_id,
                    request.config,
                    request.checkpoint,
                    run_id=str(run.run_id),
                )
                # One handler for all retry attempts: tokens consumed by a
                # failed attempt were still consumed, so they count.
                usage_handler = attach_usage_handler(config)
                for attempt in range(STREAM_MAX_RETRIES):
                    try:
                        async for event_name, payload in self._streamer.stream(
                            runnable_input, config, request, stream_modes
                        ):
                            yield sse_event(event_name, payload, event_id)
                            event_id += 1
                            emitted_data = True
                        break
                    except Exception as exc:
                        # Only retry while nothing has reached the client. Graph
                        # execution is not idempotent, so replaying a partially
                        # streamed run would duplicate output and re-invoke the
                        # model. ``CancelledError`` is a BaseException and is
                        # deliberately not caught here so client disconnects
                        # propagate to the cancellation handler below.
                        if (
                            attempt < STREAM_MAX_RETRIES - 1
                            and not emitted_data
                            and is_retriable_stream_error(exc)
                        ):
                            backoff = STREAM_RETRY_BACKOFF_SECS * (2**attempt)
                            self._log_warning(
                                "Stream attempt %s failed (retrying in %.1fs): %s",
                                attempt + 1,
                                backoff,
                                exc,
                            )
                            await asyncio.sleep(backoff)
                        else:
                            raise

                await self._metadata_store.update_run_status(
                    str(run.run_id), _RUN_SUCCESS
                )
                await self._metadata_store.update_thread(
                    thread_id,
                    status_value=_THREAD_IDLE,
                    mark_state_updated=True,
                )
                total_tokens = total_tokens_from_usage(usage_handler.usage_metadata)
                if total_tokens == 0:
                    # Fallback for providers the callback handler can't see.
                    total_tokens = await self._total_run_tokens(thread_id)
                yield sse_event(
                    "end",
                    {
                        "run_id": str(run.run_id),
                        "status": _RUN_SUCCESS,
                        "usage": {"total_tokens": total_tokens},
                    },
                    event_id,
                )
            except asyncio.CancelledError:
                self._log_warning(
                    "Streaming run %s cancelled for thread %s", run.run_id, thread_id
                )
                await self._mark_run_interrupted(str(run.run_id), thread_id)
                raise
            except Exception as exc:
                self._log_error(
                    "Streaming run %s failed for thread %s: %s",
                    run.run_id,
                    thread_id,
                    exc,
                    exc=exc,
                )
                # Persist the failure best-effort; a store outage must not stop
                # the client from receiving the 'error' event.
                await self._mark_run_failed(str(run.run_id), thread_id, str(exc))
                yield sse_event(
                    "error",
                    {"detail": str(exc), "run_id": str(run.run_id)},
                    event_id,
                )
            finally:
                if lock.locked():
                    lock.release()
                self._registry.unregister_external(thread_id, run_id)

        return run, event_stream()

    async def list_runs(
        self,
        thread_id: str,
        *,
        limit: int,
        offset: int,
        status_value: RunStatus | None,
    ) -> list[RunModel]:
        """List run metadata rows for a thread.

        ``status_value`` is validated against :data:`RunStatus` at the API edge
        (FastAPI parses the query param into the Literal), so no membership
        check is needed here.
        """
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

    async def shutdown(self) -> None:
        """Cancel all in-flight background runs (runtime shutdown).

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

        ``reject`` 409s when busy; ``interrupt`` cancels active background runs;
        ``rollback`` cancels and deletes them; ``enqueue`` is a no-op (the new
        run's task simply waits on the execution lock). Live streaming runs have
        no cancellable task, so interrupt/rollback leave them running and the new
        run queues behind them (resumable-stream cancellation is a follow-up).
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
            # stay stuck at ``pending``.
            self._log_warning(
                "Queued run %s cancelled for thread %s", run_id, thread_id
            )
            await self._mark_run_interrupted(run_id, thread_id)
            raise
        try:
            await self._metadata_store.update_thread(
                thread_id, status_value=_THREAD_BUSY
            )
            await self._metadata_store.update_run_status(run_id, _RUN_RUNNING)
            usage_handler = await self._execute_graph_run(
                thread_id, request, run_id=run_id
            )
            await self._metadata_store.update_run_status(run_id, _RUN_SUCCESS)
            await self._metadata_store.update_thread(
                thread_id,
                status_value=_THREAD_IDLE,
                mark_state_updated=True,
            )
            total_tokens = total_tokens_from_usage(usage_handler.usage_metadata)
            if total_tokens == 0:
                # Fallback for providers the callback handler can't see. Read
                # while we still hold the lock; otherwise an enqueued run for
                # this thread could advance the graph state between release and
                # the read, yielding another run's totals.
                total_tokens = await self._total_run_tokens(thread_id)
            return total_tokens
        except asyncio.CancelledError:
            self._log_warning(
                "Background run %s cancelled for thread %s", run_id, thread_id
            )
            await self._mark_run_interrupted(run_id, thread_id)
            raise
        except Exception as exc:
            self._log_error(
                "Run %s failed for thread %s: %s", run_id, thread_id, exc, exc=exc
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
        output = await self._final_state_values(thread_id, run_id)
        return output, tokens

    async def _final_state_values(self, thread_id: str, run_id: str) -> JsonValue:
        """Return the requested run's final graph state values (its output).

        Reads the most recent checkpoint tagged with this ``run_id`` so a
        follow-on enqueued run — which can start the instant this run releases
        the execution lock — cannot leak its own state into this waiter's output
        (the LangGraph ``runs.wait``/``runs.join`` contract returns output for
        the requested run). Falls back to the latest thread state when no
        run-scoped checkpoint is available (e.g. no checkpointer).
        """
        config = {"configurable": {"thread_id": thread_id}}
        get_history = getattr(self._graph, "aget_state_history", None)
        if get_history is not None:
            try:
                async for snapshot in get_history(
                    config, filter={"run_id": run_id}, limit=1
                ):
                    return serialize_value(getattr(snapshot, "values", None))
            except Exception as exc:
                # Run-scoped read is best-effort; fall back to the latest state.
                self._log_warning(
                    "Run-scoped state read failed for run %s; using latest "
                    "thread state: %s",
                    run_id,
                    exc,
                )
        try:
            snapshot = await self._graph.aget_state(config)
        except Exception as exc:
            self._log_error(
                "Failed to read final state for thread %s: %s",
                thread_id,
                exc,
                exc=exc,
            )
            return None
        return serialize_value(getattr(snapshot, "values", None))

    async def _execute_graph_run(
        self, thread_id: str, request: RunCreateRequest, run_id: str | None = None
    ) -> Any:
        """Execute a graph run without streaming; return its usage handler."""
        runnable_input = self._resolve_run_input(request)
        config = build_thread_config(
            thread_id, request.config, request.checkpoint, run_id=run_id
        )
        usage_handler = attach_usage_handler(config)
        await self._graph.ainvoke(
            runnable_input,
            config,
            context=normalize_input_payload(request.context),
            stream_mode="values",
            interrupt_before=request.interrupt_before,
            interrupt_after=request.interrupt_after,
            durability=request.durability,
        )
        return usage_handler

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
            self._log_error(
                "Failed to read state for token usage on thread %s; "
                "reporting 0 tokens: %s",
                thread_id,
                exc,
                exc=exc,
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
        }

    async def _mark_run_failed(self, run_id: str, thread_id: str, error: str) -> None:
        """Persist error state for a failed run; best-effort, never raises.

        The store outage that fails these writes is often the same one that
        failed the run, so they must not mask the original exception or block
        the client's 'error' event.
        """
        try:
            await self._metadata_store.update_run_status(
                run_id, _RUN_ERROR, error=error
            )
            await self._metadata_store.update_thread(
                thread_id, status_value=_THREAD_ERROR
            )
        except Exception as exc:
            self._log_error(
                "Failed to persist error state for run %s: %s", run_id, exc, exc=exc
            )

    async def _mark_run_interrupted(self, run_id: str, thread_id: str) -> None:
        """Persist cancellation/disconnect state; best-effort, never raises."""
        try:
            await self._metadata_store.update_run_status(
                run_id, _RUN_INTERRUPTED, error="Run interrupted."
            )
            await self._metadata_store.update_thread(
                thread_id, status_value=_THREAD_IDLE
            )
        except Exception as exc:
            self._log_error(
                "Failed to persist interrupted state for run %s: %s",
                run_id,
                exc,
                exc=exc,
            )

    def _log_warning(self, msg: str, *args: Any) -> None:
        if self._logger is not None:
            self._logger.warning(msg, *args)

    def _log_error(
        self, msg: str, *args: Any, exc: BaseException | None = None
    ) -> None:
        if self._logger is not None:
            self._logger.error(msg, *args, exc_info=exc)
