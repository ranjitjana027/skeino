# Python API

This reference is generated from the source docstrings with
[mkdocstrings](https://mkdocstrings.github.io/). It starts with the supported
public surface, then documents the schema models and the advanced sub-modules.

## Public surface

The supported, stable API exported from the top-level `skeino` package:

::: skeino.create_app

::: skeino.from_langgraph_json

::: skeino.SkeinoSettings

::: skeino.GraphRegistry

## Schemas

The Pydantic request/response models behind the [HTTP API](http.md).

### Common types

::: skeino.schemas.common

### Threads

::: skeino.schemas.threads

### Runs

::: skeino.schemas.runs

### Assistants

::: skeino.schemas.assistants

### Server

::: skeino.schemas.server

## Persistence

!!! info "Advanced"
    These are importable for advanced use (e.g. registering a custom
    checkpointer) but are not part of the stability contract. See
    [Persistence & checkpointers](../concepts/persistence.md) and
    [Write a custom checkpointer](../guides/custom-checkpointer.md).

::: skeino.persistence

## Streaming

::: skeino.streaming
