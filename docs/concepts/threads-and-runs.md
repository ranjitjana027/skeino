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

When you set a `postgres_uri`, both halves use Postgres; otherwise both fall
back to in-memory (ephemeral) implementations. See
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
`GET`/`POST /threads/{id}/history`.

Threads are searchable with `POST /threads/search`, which filters by ids,
metadata, state values, and status, with pagination and field selection.

## Run lifecycle

A run is created against an existing thread:

- `POST /threads/{id}/runs` — execute to completion and return the final
  [`RunModel`][skeino.schemas.runs.RunModel].
- `POST /threads/{id}/runs/stream` — execute and stream events over SSE (see
  [Streaming](streaming.md)).

A run progresses through these statuses:

`pending` → `running` → `success` · `error` · `interrupted` (and `timeout`)

The run row records the `assistant_id`, the serialized invocation `kwargs`
(input, config, checkpoint selection, stream options), the `multitask_strategy`,
and — on failure — an `error` message. List a thread's runs with
`GET /threads/{id}/runs` and fetch one with `GET /threads/{id}/runs/{run_id}`.

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

skeino enforces **at most one in-flight run per thread**. Each thread has its own
lock, acquired before the run row is created (and, for streaming runs, before the
SSE generator is returned — so a queued caller can't observe a stale "free"
lock).

The `multitask_strategy` on the run request decides what happens when a thread is
already busy:

| Strategy | Behaviour when the thread is busy |
| --- | --- |
| `enqueue` (default) | Wait for the active run to finish, then proceed. |
| `reject` | Fail immediately with **409 Conflict**. |
| `rollback` | Fail immediately with **409 Conflict**. |
| `interrupt` | Fail immediately with **409 Conflict**. |

!!! info "Single-process scope"
    Locks are in-process `asyncio` locks, which is correct for a single-process
    deployment. A clustered, multi-process deployment would need a shared lock
    service; that is out of scope for v1.

## Token usage

After a run completes, skeino sums the total tokens across all AI messages in the
final state, normalising the different provider formats (LangChain
`usage_metadata`, Gemini, OpenAI/Groq/Bedrock). The total is surfaced:

- on `POST /threads/{id}/runs` via the `X-Tokens-Used` response header, and
- on the streaming endpoint inside the terminal `end` event's `usage` field.

## Related reference

- [HTTP endpoints (v1)](../api-reference/http.md) — every route, request, and
  response shape.
- [Python API](../api-reference/python.md) — the schema models referenced above.
