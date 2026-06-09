---
template: home.html
title: skeino — an embeddable LangGraph HTTP server
hide:
  - navigation
  - toc
---

# skeino

## Why skeino

<div class="grid cards" markdown>

-   :material-puzzle: **Studio-compatible**

    Implements the v1 HTTP surface — threads, runs, streaming/SSE, assistants,
    health/info — that LangGraph SDK clients and Studio already speak.

-   :material-layers-triple: **Modular by design**

    `api`, `ops`, `persistence`, `streaming`, and `serialization` are separate
    concerns with explicit dependencies — easy to read, easy to extend.

-   :material-database-cog: **Pluggable persistence**

    Checkpointers register through a small decorator-based registry. Postgres
    and in-memory ship in the box; add Redis or Mongo without touching core.

-   :material-language-python: **Typed & documented**

    Strict mypy and enforced docstrings across the package — so the
    [Python API reference](api-reference/python.md) comes straight from source.

</div>

## Two ways in

=== "Programmatic"

    ```python
    from skeino import create_app, SkeinoSettings
    from my_project.graph import graph

    app = create_app(
        graphs={"my_agent": graph},
        settings=SkeinoSettings(
            checkpointer_scheme="postgres",
            checkpointer_uri="postgresql://localhost/skeino",
        ),
    )
    ```

=== "langgraph.json"

    ```python
    from skeino import from_langgraph_json

    app = from_langgraph_json("langgraph.json")
    ```

Then serve it like any ASGI app:

```bash
uvicorn app:app --port 8000
```

Your graph is now reachable over the LangGraph HTTP API, with interactive
OpenAPI docs at `/docs` — and an always-current [API explorer](api-reference/explorer.md)
right here in these docs.

## Explore the docs

<div class="grid cards" markdown>

-   :material-rocket-launch-outline: **[Getting started](getting-started.md)**

    Install skeino and stand up a server — programmatically or from
    `langgraph.json`.

-   :material-sitemap-outline: **[Concepts](concepts/threads-and-runs.md)**

    The thread/run/checkpoint model, streaming semantics, and persistence.

-   :material-api: **[API reference](api-reference/http.md)**

    Every v1 HTTP endpoint, the interactive explorer, and the generated Python
    API.

-   :material-wrench-outline: **[How-to guides](guides/embedding-fastapi.md)**

    Embed in an existing app, configure Postgres, write a checkpointer, deploy.

</div>

## Status

skeino is **stable** (1.x, semver). The supported public surface is
[`create_app`][skeino.create_app], [`from_langgraph_json`][skeino.from_langgraph_json],
[`SkeinoSettings`][skeino.SkeinoSettings], and [`GraphRegistry`][skeino.GraphRegistry].
Sub-modules are importable for advanced use but aren't part of the stability
contract. See the [changelog](changelog.md) for release history.

Some `langgraph dev` features are intentionally **out of scope for v1**: no
bundled web UI, no cron/webhook scheduling, no built-in auth, and no `/store/*`
endpoints. skeino focuses on the core threads/runs/streaming/assistants surface
that SDK clients depend on.
