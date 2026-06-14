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

If the client disconnects mid-stream, the run is marked `interrupted`.

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

If your graph declares an `output_schema`, skeino only emits the fields that
schema declares, so internal state never leaks onto the wire.

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
- **Disconnect handling.** A client disconnect (`CancelledError`) marks the run
  `interrupted` and releases the thread lock in a `finally` block, so a dropped
  connection never wedges the thread.

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
| `end` | terminal, success | `{run_id, status: "success", usage: {total_tokens}}` |
| `error` | terminal, failure | `{detail, run_id}` |

See [Threads & runs](threads-and-runs.md) for run lifecycle and the
[HTTP reference](../api-reference/http.md) for the request schema.
