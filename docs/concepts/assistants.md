# Assistants

In the LangGraph Platform model, an **assistant** is a named, configured entry
point to a graph. skeino exposes the assistant surface that SDK clients and
LangGraph Studio expect, mapping it onto the graph(s) you register.

## How skeino models assistants

When you call [`create_app`][skeino.create_app], the `graphs` mapping's keys are
your assistant ids:

```python
create_app(
    graphs={"my_agent": graph},  # "my_agent" is the assistant id
    settings=SkeinoSettings(
        assistant_name="My Agent",
        assistant_description="A helpful assistant.",
    ),
)
```

In v1, skeino routes a **single assistant** (the default). The default id is:

1. `SkeinoSettings.default_assistant_id` if set (it must be a key in `graphs`), or
2. the first key of the `graphs` mapping otherwise.

An incoming `assistant_id` is accepted if it matches a supported id, the default
id, or the assistant's **deterministic UUID** (derived from `assistant_namespace`
and the default id; compared by value, so any valid textual form — lowercase,
uppercase, URN, or braces — matches). This is the id skeino returns from
`/assistants/search` and `/info`, so Studio and SDK clients round-trip it back
unchanged. Any other id — including an unrelated but syntactically valid UUID —
returns **404**.

## The endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/assistants/search` | List/search assistants (returns the singleton). |
| `GET` | `/assistants/{id}` | Fetch assistant metadata. |
| `GET` | `/assistants/{id}/schemas` | Input / output / state / config / context JSON schemas. |
| `GET` | `/assistants/{id}/graph` | Graph structure (nodes & edges) for visualization. |
| `GET` | `/assistants/{id}/subgraphs` | Subgraph schemas. |

### Schemas

`GET /assistants/{id}/schemas` returns a
[`GraphSchemaModel`][skeino.schemas.assistants.GraphSchemaModel] derived from the
compiled graph: `input_schema`, `output_schema`, `state_schema`,
`config_schema`, and `context_schema`. These are the JSON schemas clients use to
build forms and validate input.

### Graph & subgraphs

`GET /assistants/{id}/graph` returns the node/edge structure (with an optional
`xray` query parameter for nested detail), and
`GET /assistants/{id}/subgraphs` returns subgraph schemas (with an optional
`recurse` parameter). Both are primarily consumed by visualization tooling.

## Identity in responses

The assistant's presentation fields come from settings:

- `assistant_name` / `assistant_description` — shown in
  `/assistants/{id}` responses.
- `assistant_namespace` — the URI namespace used to derive assistant
  identifiers (defaults to `https://skeino.local/assistants`).

See [Configuration](configuration.md) for these settings and the
[HTTP reference](../api-reference/http.md) for the full request/response shapes.
