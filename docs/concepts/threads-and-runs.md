# Threads & runs

skeino models conversations the same way the LangGraph Platform does: a
**thread** holds the evolving state, a **run** is one execution against that
thread, and a **checkpoint** is an immutable snapshot of the graph state at a
point in time.

## The data model

```
Thread  (thread_id)
├── metadata, config, status, ttl        ← stored in the metadata store
└── checkpoint stream                     ← stored in the checkpointer
        ├── checkpoint (stamped run_id)
        ├── checkpoint (stamped run_id)
        └── ...
Run     (run_id, belongs to a thread)
└── assistant_id, status, kwargs, error  ← stored in the metadata store
```

- A **thread** owns a single checkpoint namespace. Every run against the thread
  reads and appends to the same checkpoint stream, so the thread's "current
  state" is simply its most recent checkpoint — regardless of which run wrote it.
- A **run** is one invocation of the graph against a thread. Multiple runs
  execute sequentially against the same thread (see
  [Concurrency](#concurrency-one-run-at-a-time) below). Each run records the
  parameters it was invoked with and its terminal status.
- A **checkpoint** is a LangGraph checkpoint tuple (config, values, metadata).
  skeino stamps each checkpoint's metadata with the `run_id` that produced it, so
  clients (including LangGraph Studio) can group checkpoints by run.

### Two stores, two responsibilities

State is deliberately split across two backends:

| Concern | Stored in | Backends |
| --- | --- | --- |
| Thread & run rows (metadata, status, config, kwargs) | **metadata store** | Postgres (`app_threads`, `app_runs` tables) or in-memory |
| Graph state / checkpoints | **checkpointer** | Postgres saver or in-memory `MemorySaver` |

A single `checkpointer_scheme` selects both halves (e.g. `postgres` uses
Postgres for both); the default `memory` keeps both in-memory (ephemeral). See
[Persistence & checkpointers](persistence.md) for the details.

## Thread lifecycle

A thread is created with `POST /threads`. The request can:

- supply an explicit `thread_id` (otherwise one is generated),
- attach `metadata`,
- choose `if_exists` behaviour (`"raise"` — the default — or `"do_nothing"`),
- configure a `ttl`, and
- seed initial state via `supersteps` (a list of node updates applied before any
  run executes).

Thread status is one of `idle`, `busy`, `interrupted`, or `error`. You can read
a thread's metadata-plus-latest-values with `GET /threads/{id}`, its full latest
checkpoint with `GET /threads/{id}/state`, and walk its checkpoint history with
`GET`/`POST /threads/{id}/history`. To read state at a specific point in time,
use `GET /threads/{id}/state/{checkpoint_id}` (or the `POST .../state/checkpoint`
variant with a full config body).

You can update a thread's metadata with `PATCH /threads/{id}`, delete it (along
with its runs and checkpoints) with `DELETE /threads/{id}`, and **edit its state**
directly with `POST /threads/{id}/state` — a human-in-the-loop write that applies
`values` (optionally `as_node`, and from a specific `checkpoint`) and returns the
new checkpoint config.

Threads are searchable with `POST /threads/search`, which filters by ids,
metadata, state values, and status, with pagination and field selection.

You can **fork** a thread with `POST /threads/{id}/copy`. This creates a new,
independent thread seeded with the source's *latest* state (its metadata is
copied and stamped with `forked_from`), so you can branch and explore — a
what-if continuation, an isolated debug replay — without mutating the original.
The copy is shallow: the current state carries over, not the full checkpoint
history.

## Run lifecycle

A run is created against an existing thread. Runs execute **in the background**:
the graph runs in an `asyncio` task while the request returns. Pick how you want
to consume the result:

- `POST /threads/{id}/runs` — **background** create. Returns immediately with a
  `pending`/`running` [`RunModel`][skeino.schemas.runs.RunModel]; poll
  `GET .../runs/{run_id}` or join.
- `POST /threads/{id}/runs/wait` — run to completion and return the final graph
  state values (the run output, matching the LangGraph SDK `runs.wait`).
- `POST /threads/{id}/runs/stream` — execute and stream events over SSE (see
  [Streaming](streaming.md)).
- `GET /threads/{id}/runs/{run_id}/join` — wait for an in-flight run to reach a
  terminal state and return its final graph state values.

A run progresses through these statuses:

`pending` → `running` → `success` · `error` · `interrupted` (and `timeout`)

The run row records the `assistant_id`, the serialized invocation `kwargs`
(input, config, checkpoint selection, stream options), the `multitask_strategy`,
and — on failure — an `error` message. List a thread's runs with
`GET /threads/{id}/runs` and fetch one with `GET /threads/{id}/runs/{run_id}`.

### Cancel and delete

- `POST /threads/{id}/runs/{run_id}/cancel?action=interrupt` cancels an in-flight
  run and leaves it `interrupted`; `action=rollback` cancels it **and** deletes
  the run row. Pass `wait=true` to block until the cancellation has settled.
- `DELETE /threads/{id}/runs/{run_id}` removes a **terminal** run row (it returns
  **409** while the run is still active — cancel it first).

A live SSE stream (`/runs/stream`) is cancelled by the client disconnecting;
cross-request cancellation of a streaming run (reconnect/resume) is a planned
follow-up.

### Input vs. command

A run is driven by **either** an `input` payload (new state to merge in) **or** a
`command` (`update` / `resume` / `goto`) used to resume an interrupted graph.
`input.messages` is converted to LangChain message objects automatically; see
[Streaming → serialization](streaming.md#serialization-on-the-wire).

### `if_not_exists`

By default a run against a missing thread is rejected. Set
`if_not_exists: "create"` on the run request to have skeino create the thread on
demand.

## Concurrency: one run at a time

skeino runs **at most one *executing* run per thread**. With the `enqueue`
strategy, additional background runs can sit `pending`, queued behind the
execution lock — only one runs at any moment. Each thread has its own execution
lock held by the running task; admission decisions are serialized by a per-thread
admission lock, so a queued caller can't observe a stale "free" slot.

The `multitask_strategy` on the run request decides what happens when a thread is
already busy:

| Strategy | Behaviour when the thread is busy |
| --- | --- |
| `enqueue` (default) | Wait for the active run to finish, then proceed. |
| `reject` | Fail immediately with **409 Conflict**. |
| `rollback` | Cancel **and delete** the active run, then start the new one. |
| `interrupt` | Cancel the active run (left `interrupted`), then start the new one. |

`interrupt`/`rollback` cancel active **background** runs. A live SSE streaming run
has no cancellable server-side handle, so a new run queues behind it instead
(consistent with `enqueue`) until resumable streaming lands.

!!! info "Single-process scope"
    Locks are in-process `asyncio` locks, which is correct for a single-process
    deployment. A clustered, multi-process deployment would need a shared lock
    service; that is out of scope for v1.

## Token usage

skeino measures each run's token usage with a LangChain
`UsageMetadataCallbackHandler` attached to the run's config. The handler
records every LLM call made during the run — including calls whose responses
never reach checkpoint state — and is scoped to that run, so multi-turn
threads report per-run totals. The total is surfaced:

- on `POST /threads/{id}/runs` via the `X-Tokens-Used` response header, and
- on the streaming endpoint inside the terminal `end` event's `usage` field.

When the handler records nothing (providers that don't populate
`usage_metadata` plus `model_name` on their messages), skeino falls back to
summing the total tokens across all AI messages in the final state,
normalising the different provider formats (LangChain `usage_metadata`,
Gemini, OpenAI/Groq/Bedrock). The fallback covers the thread's whole message
history, so on multi-turn threads it reports cumulative totals rather than
the run's own.

## Related reference

- [HTTP endpoints (v1)](../api-reference/http.md) — every route, request, and
  response shape.
- [Python API](../api-reference/python.md) — the schema models referenced above.
