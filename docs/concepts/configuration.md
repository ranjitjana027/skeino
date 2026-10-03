# Configuration

skeino is configured two ways, which can be combined:

- **[`SkeinoSettings`][skeino.SkeinoSettings]** — a typed Pydantic record you
  pass to `create_app`.
- **`langgraph.json`** — a manifest consumed by `from_langgraph_json`, which
  derives settings (and graph targets) from the file.

## `SkeinoSettings`

`SkeinoSettings` is an ordinary, **frozen** Pydantic `BaseModel` — it lives in
your code, typed and version-controlled. It has **no environment-variable
binding of its own**. If you want to read configuration from the environment,
use `pydantic-settings` in your own project and pass the result in:

```python
from pydantic_settings import BaseSettings
from skeino import SkeinoSettings, create_app


class Env(BaseSettings):
    checkpointer_scheme: str = "memory"
    checkpointer_uri: str | None = None


env = Env()  # reads CHECKPOINTER_SCHEME / CHECKPOINTER_URI from the environment
app = create_app(
    graphs={"my_agent": graph},
    settings=SkeinoSettings(
        checkpointer_scheme=env.checkpointer_scheme,
        checkpointer_uri=env.checkpointer_uri,
    ),
)
```

### Fields

#### Persistence

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `checkpointer_scheme` | `str` | `"memory"` | **Selects the persistence backend** (`memory`/`postgres`/`sqlite`/`mongodb`/`redis`/custom). The scheme alone decides it — both the checkpointer and (where native) the metadata store follow it. DB backends are optional extras. |
| `checkpointer_uri` | `str \| None` | `None` | Connection string/path for the selected scheme (`postgresql://…`, a SQLite path or `:memory:`, `mongodb://…`). For Mongo, the URI path selects the database used by both the checkpointer and the metadata store (`mongodb://host/mydb`). Ignored for `memory`. A URI without a matching scheme is **not** a selector. |
| `checkpointer_options` | `dict[str, object]` | `{}` | Extra options passed to the checkpointer builder (e.g. `{"setup_schema": False}`). |
| `allow_ephemeral_metadata` | `bool` | `False` | Permit a durable scheme with no native metadata store (e.g. `redis`/custom) to run with the in-memory metadata store. Off by default so the split-brain fails loudly at startup. |

#### Run liveness

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `run_heartbeat_seconds` | `float` | `30.0` | How often each process refreshes `updated_at` on the runs it is executing, marking them alive. Must be `> 0`. |
| `orphaned_run_timeout_seconds` | `float \| None` | `120.0` | A `pending`/`running` run whose `updated_at` is older than this has lost its process (crash, OOM kill, restart) and is marked `error`, freeing its thread. Swept at startup and every `run_heartbeat_seconds`. Must be `> 0` and **at least 3× `run_heartbeat_seconds`** (validated at construction), so one failed heartbeat pass is tolerated. `None` disables the sweep. Workers sharing a SQLite/MongoDB store need synchronised clocks. See [Threads & runs](threads-and-runs.md). |

#### Resumable streams

Retention for runs created with `stream_resumable: true`; see
[Streaming](streaming.md#joining-a-run-stream).

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `resumable_stream_ttl_seconds` | `float` | `600.0` | How long a finished run keeps its buffered SSE events for replay via `GET /threads/{thread_id}/runs/{run_id}/stream`. `0` drops them as soon as the run ends (a join then gets the final state only). |
| `resumable_stream_max_retained_runs` | `int` | `16` | Maximum finished resumable streams retained per worker; oldest are evicted first and a join then returns final state instead of replay. `0` disables finished-history retention. |
| `resumable_stream_max_events` | `int` | `10000` | Maximum recent SSE events retained per resumable thread-scoped run. A join whose `Last-Event-ID` predates the retained window returns `409`. Must be `>= 1`. |
| `resumable_stream_max_bytes` | `int` | `16777216` (16 MiB) | Maximum encoded SSE frame bytes retained per resumable thread-scoped run; older events are evicted past this budget (same `409` on a stale cursor). Must be `>= 1`. |

#### Assistant identity

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `default_assistant_id` | `str \| None` | `None` | Assistant id used in single-graph mode. Must be a key in `graphs`; falls back to the first key. |
| `supported_assistant_ids` | `frozenset[str] \| None` | `None` | Reserved for future multi-assistant routing. |
| `assistant_name` | `str \| None` | `None` | Human-readable name, surfaced in `/assistants/{id}`. |
| `assistant_description` | `str \| None` | `None` | Human-readable description, surfaced in `/assistants/{id}`. |
| `assistant_namespace` | `str` | `"https://skeino.local/assistants"` | URI namespace for assistant identifiers. |

#### Server presentation

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `server_title` | `str` | `"skeino"` | FastAPI/OpenAPI title. |
| `server_description` | `str` | `"LangGraph-compatible HTTP API powered by skeino."` | OpenAPI description. |
| `server_version` | `str` | `"1.0.0"` | Version reported by `/info` and `/api/health`. |
| `welcome_message` | `str \| None` | `None` | Message returned by `/api/initial-message`. |

#### CORS

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `cors_origins` | `list[str]` | `["*"]` | Allowed origins. |
| `cors_methods` | `list[str]` | `["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]` | Allowed methods. |
| `cors_headers` | `list[str]` | `["*"]` | Allowed headers. |

!!! tip "Lock down CORS in production"
    The `["*"]` defaults are convenient for local development. For a deployed
    server, set `cors_origins` to your actual front-end origins.

## `langgraph.json`

[`from_langgraph_json`][skeino.from_langgraph_json] reads a manifest, loads each
graph, and builds `SkeinoSettings` from the `http.cors` and `store` sections.
Any settings you pass explicitly to `from_langgraph_json(..., settings=...)`
override the manifest-derived values — useful for server options that the JSON
doesn't express.

```json title="langgraph.json"
{
  "env": ".env",
  "graphs": {
    "my_agent": "./src/graph.py:graph",
    "other": "./src/other.py:build_graph"
  },
  "http": {
    "cors": {
      "allow_origins": ["https://app.example.com", "http://localhost:3000"],
      "allow_methods": ["GET", "POST"],
      "allow_headers": ["Authorization", "Content-Type"]
    }
  },
  "store": {
    "uri": "${POSTGRES_URI}"
  }
}
```

| Key | Meaning |
| --- | --- |
| `env` | Path to a `.env` file, loaded before graph resolution and variable expansion. |
| `graphs` | Map of assistant id → `path:attribute` target. The attribute must be a `CompiledStateGraph` **or** a `(checkpointer) -> CompiledStateGraph` builder. |
| `http.cors` | Maps to `cors_origins` / `cors_methods` / `cors_headers`. |
| `store.uri` | Maps to `checkpointer_uri`, with `checkpointer_scheme` derived from the URI prefix (a loader convenience; the programmatic API stays scheme-driven). Supports `${VAR}` expansion. |

### Resolution rules

- **Graph targets** are resolved relative to the manifest's directory, via
  `importlib`, so they work without a package layout.
- **`${VAR}` placeholders** in string values (such as `store.uri`) are expanded
  from the environment after the `env` file is loaded; unset variables expand to
  an empty string.

### Not implemented in v1

If the manifest contains an `http.app` (a user-supplied FastAPI app),
`auth`, or `ui` section, skeino logs a debug warning and ignores it — skeino
builds its own app and does not merge these in v1. To mount skeino alongside your
own routes, see [Embed in an existing FastAPI app](../guides/embedding-fastapi.md).
