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

#### Assistant identity

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `default_assistant_id` | `str \| None` | `None` | Assistant id used in single-graph mode. Must be a key in `graphs`; falls back to the first key. |
| `supported_assistant_ids` | `frozenset[str] \| None` | `None` | Reserved for future multi-assistant routing. |
| `assistant_name` | `str \| None` | `None` | Human-readable name, surfaced in `/assistants/{id}`. |
| `assistant_description` | `str \| None` | `None` | Human-readable description, surfaced in `/assistants/{id}`. |
| `assistant_namespace` | `str` | `"https://skeino.local/assistants"` | URI namespace for assistant identifiers. |

#### Streaming behaviour

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `agent_nodes` | `frozenset[str]` | `frozenset()` | Node names treated as "agent" nodes — only these contribute token-by-token message chunks in `values` streaming. |
| `status_field` | `str \| None` | `None` | A state-list field whose new entries are emitted as `status` events during streaming. |

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
override the manifest-derived values — useful for graph-specific options like
`agent_nodes` and `status_field` that the JSON doesn't express.

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
