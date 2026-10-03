# Streaming (SSE)

`POST /threads/{thread_id}/runs/stream` executes a run and streams its progress
back as **Server-Sent Events** (`text/event-stream`). This is how UIs render
token-by-token output and live state updates.

## The response

The endpoint returns a streaming response with:

- `Content-Type: text/event-stream`
- `Cache-Control: no-cache`
- `Connection: keep-alive`
- `Content-Location: /threads/{thread_id}/runs/{run_id}` — so the client knows
  the run id immediately.

Each event is encoded in the standard SSE framing, with a monotonically
increasing `id`:

```
id: 1
event: metadata
data: {"run_id":"...","thread_id":"...","run":{...}}

id: 2
event: values
data: {"messages":[...]}

id: 3
event: end
data: {"run_id":"...","status":"success","usage":{"total_tokens":1234}}

```

JSON payloads are serialized compactly (no extra whitespace).

## Event sequence

A successful stream always looks like:

1. **`metadata`** (first event) — the run is starting. Payload:
   `{"run_id", "thread_id", "run": <RunModel>}`.
2. **Zero or more data events** — the graph's output, in a shape determined by
   the requested `stream_mode` (see below).
3. **`end`** (terminal, on success) — `{"run_id", "status": "success",
   "usage": {"total_tokens": <int>}}`. The total is measured per run by a
   usage callback attached to the run's config (see
   [Token usage](threads-and-runs.md#token-usage)).

If the graph raises, the terminal event is instead:

- **`error`** — `{"detail": "<message>", "run_id": "..."}`.

If the run is cancelled (`POST .../cancel`, a superseding `interrupt`/
`rollback` run, or a disconnect with `on_disconnect: "cancel"`), the terminal
event is `end` with `"status": "interrupted"`.

The terminal event always matches the persisted run status. A run whose
`success` was already saved when it was cancelled still ends with `end`
`"status": "success"`; one already failed by another worker's orphan sweep ends
with that `error`. A run whose row was deleted before it finished (e.g. its
thread was deleted) ends with an `error` saying so.

## Disconnects

The run executes in a server-side task, not in the request: the SSE response is
one subscriber to the run's events. A subscriber's 256-event delivery queue
holds only the events its `stream_mode` filter accepts, so a join that asks for
`values` is not overflowed by `updates` or `custom` traffic it would never
receive. When a subscriber's queue overflows on a resumable stream
(`stream_resumable: true`) and every event it missed is still in the retained
history, skeino catches it up from that history and keeps the response open;
the client sees no gap and no error.

Otherwise (a non-resumable stream, or missed events already evicted from
history) the subscriber receives an `error` event with code
`subscriber_overflow` before its response closes. This event has no id: a
client of a resumable stream can reconnect with its last received event id,
subject to the retained history window. A non-resumable client cannot recover
the discarded output. This overflow is treated like a disconnect: it cancels
the run if that subscriber would cancel on disconnect (`on_disconnect:
"cancel"` for the creating stream, `cancel_on_disconnect=true` for a join).

What a client disconnect does is the run
request's `on_disconnect`:

- **`"continue"`** (default, as on LangGraph Platform) — the run keeps going;
  the client can come back and [join](#joining-a-run-stream) it. This holds
  even if the client leaves while the run is still queued behind another run
  on the thread: it starts when the thread frees up.
- **`"cancel"`** — the run is cancelled and marked `interrupted` (or, if the
  client leaves before its row was created, never starts).

## Joining a run stream

`GET /threads/{thread_id}/runs/{run_id}/stream` re-attaches to a run — what the
SDK's `client.runs.joinStream(threadId, runId, {streamMode, lastEventId})` and
`useStream`'s `joinStream` / `reconnectOnMount` call.

For a thread-scoped run created with **`stream_resumable: true`**, skeino keeps
the most recent events (with the same `id`s the original stream carried), up to
`SkeinoSettings.resumable_stream_max_events` (default 10,000) and
`SkeinoSettings.resumable_stream_max_bytes` (default 16 MiB) per run. The
retained history stays available for the run's lifetime plus
`SkeinoSettings.resumable_stream_ttl_seconds` (default 600). A join then:

1. replays the retained events with an id greater than the `Last-Event-ID`
   header — `-1` (what `useStream` sends) replays from the first event; no
   header replays nothing, matching LangGraph Platform;
2. tails live events until the run ends, then closes after `end` / `error`.

A run created without `stream_resumable` keeps no history, so a join only sees
events from the moment it attaches.

When a history limit is reached, skeino evicts the oldest events. If
`Last-Event-ID` is older than the retained window, the join returns `409` rather
than silently replaying an incomplete stream. Stateless `POST /runs/stream`
never retains history, even if `stream_resumable: true` is sent, because its
ephemeral run identifiers cannot be used to join later.

Each worker retains at most `SkeinoSettings.resumable_stream_max_retained_runs`
finished streams (default 16; 0 disables finished history retention). With the
default per-run byte cap this bounds registry-owned finished history to 256 MiB
of encoded frames, plus object overhead. Oldest finished buffers are evicted
first; joins to those runs return synthetic final-state events with `200`,
even when a replay cursor is supplied. Active streams do not count toward this
finished-history limit and remain subject to the per-run event and byte caps.

| Situation | Response |
| --- | --- |
| Run in flight, resumable | `200`: replay after `Last-Event-ID`, then live events |
| Run in flight, not resumable | `200`: live events only |
| Background run (`POST /runs`, which streams nothing) | `200`: waits, then the final state |
| Run finished, events retained, `Last-Event-ID` sent | `200`: replay of the retained events |
| Run finished otherwise (not resumable, retention expired, or no `Last-Event-ID`) | `200`: final `values` (no `id`) then `end` `{run_id, status}` — or `error` if the run failed |
| Unknown thread, unknown run, or a run of another thread | `404` |
| Run `pending`/`running` with no task or stream on this server; or `Last-Event-ID` predates retained history, or is ahead of the run's last event id | `409` |
| `Last-Event-ID` not an integer (or negative other than `-1`), malformed `stream_mode` | `422` |

LangGraph Platform reports an unknown run as a `200` stream carrying an
`error` event; skeino checks before the response starts so clients get a real
status code.

`stream_mode` (a single mode, a JSON array, or the parameter repeated) filters
what the join delivers, with LangGraph's matching rules (`messages` covers
`messages-tuple`; `mode|namespace` matches `mode`). `metadata`, `end`, and
`error` are always delivered. `cancel_on_disconnect=true` cancels the run when
the joining client goes away; the default leaves it running.

!!! info "Process-local buffer"
    Events are buffered in the memory of the worker running the run — the same
    single-process scope as the run's task and thread lock (see
    [Deployment](../guides/deployment.md)). With several workers, replay and
    live tailing require a join to reach the worker that owns the run. If a
    join lands elsewhere while the run is still active, it gets `409`; if the
    run is already terminal, skeino returns synthetic final-state events with
    `200` from persisted state instead. The buffer is independent of the
    persistence backend, and does not survive a restart (nor does the run:
    shutdown marks it `interrupted`).

## Stream modes

The `stream_mode` field on the run request (a single mode or a list) selects how
graph output is emitted. skeino behaves like a real LangGraph server — it
forwards each requested mode faithfully rather than transforming it. Dispatch is
two ways:

=== "`events`"

    Passes LangGraph's `astream_events` (v2) output straight through as `events`
    events — the full event firehose, serialized as-is. Exclusive: cannot be
    combined with other modes.

=== "all other modes"

    `values`, `updates`, `messages`, `messages-tuple`, `tasks`, `checkpoints`,
    `debug`, `custom` — skeino calls `graph.astream(stream_mode=...)` and
    forwards each chunk under an event name matching the mode:

    - `values` — the full state snapshot after every super-step.
    - `updates` — per-node deltas (`{node: {state_key: value}}`).
    - `custom` — arbitrary data your graph emits via `get_stream_writer()`; the
      canonical way to surface pipeline progress (e.g. status lines).

    For live, low-bandwidth UIs, request incremental modes such as
    `["updates", "custom"]` — you get only each node's new output plus your
    progress events, with no full-history re-send.

The recognised modes are `values`, `messages`, `messages-tuple`, `tasks`,
`checkpoints`, `updates`, `events`, `debug`, and `custom`.

### Output filtering

If your graph declares an `output_schema`, skeino only returns the fields that
schema declares from stream events, run output (`runs/wait`, `runs/{id}/join`),
and thread reads (`GET /threads/{id}`, thread search, history).

The state endpoints (`GET /threads/{id}/state` and its checkpoint variants)
are the exception: as on LangGraph server, they return the raw checkpoint state,
internal keys included, because tools such as LangGraph Studio inspect it. Don't
expose them to clients that must not see internal state.

## Resilience

Streaming runs are hardened against transient backend failures:

- **Retry with backoff.** If the graph stream fails **before any event has been
  delivered** with a *retriable* error (timeouts, SSL/connection/syscall
  errors, "could not receive data from server"), skeino retries with exponential
  backoff — up to a small number of attempts.
- **No replay after delivery.** Once any event has reached the client, skeino
  does **not** retry — retrying would duplicate already-streamed output. The
  error surfaces instead.
- **Permanent errors fail fast.** Programming errors (`ValueError`, `KeyError`,
  …) are never retried.
- **Disconnect handling.** After the run's task ends (success, error, or
  cancelled, even before it started), a tracked finalizer releases the thread
  lock and closes the event stream, so neither a dropped connection nor a
  cancel wedges the thread.

## Serialization on the wire

skeino normalises data in both directions:

**Inbound** — request `input` is converted to LangGraph-native objects. In
particular, an `input.messages` list is converted to LangChain message objects,
and run config is merged with the thread/checkpoint/run identifiers LangGraph
needs.

**Outbound** — graph state is serialized to JSON-safe values. LangChain messages
get a stable shape:

- AI/human/system: `{"id", "type", "content", "tool_calls"?, "additional_kwargs"?}`
- tool: `{"id", "type": "tool", "tool_call_id", "name", "content"}`

Multi-block message content is flattened to a single string, UUIDs and datetimes
are stringified, and arbitrary objects fall back to their public attributes.

## Summary of event types

| Event | When | Payload |
| --- | --- | --- |
| `metadata` | first, always | `{run_id, thread_id, run}` |
| `values` | `values` mode | full state snapshot `{messages: [...], ...}` per super-step |
| `events` | `events` mode | raw LangGraph v2 event |
| `updates` / `messages` / `messages-tuple` / `tasks` / `checkpoints` / `debug` / `custom` | matching mode | LangGraph chunk for that mode (`updates` deltas are output-key filtered) |
| `end` | terminal, success / cancelled | `{run_id, status: "success", usage: {total_tokens}}`, or `{run_id, status: "interrupted"}`; `usage` is omitted when the event reports an outcome saved before the run's own finish |
| `error` | terminal, failure | `{detail, run_id}`; a subscriber that fell behind and cannot be caught up from retained history gets `{code: "subscriber_overflow", detail, run_id}` with no `id` |

See [Threads & runs](threads-and-runs.md) for run lifecycle and the
[HTTP reference](../api-reference/http.md) for the request schema.
