"""Output-schema filtering must fail closed, never open.

When a graph declares an output schema, internal state keys are stripped before
reaching clients. If the schema can't be introspected the filter must drop
everything (fail closed) rather than pass everything through (a data leak).
"""

from types import SimpleNamespace
from typing import TypedDict

from skeino.streaming.runner import Streamer


class _OutputSchema(TypedDict):
    messages: list
    pipeline_status: list


def test_no_output_schema_passes_all_values() -> None:
    # No declared output schema → no filtering is the intended behaviour.
    streamer = Streamer(SimpleNamespace())
    assert streamer._filter_values({"a": 1, "b": 2}) == {"a": 1, "b": 2}


def test_unintrospectable_output_schema_fails_closed() -> None:
    # output_schema instance has neither ``model_fields`` nor ``__annotations__``
    # → drop every value.
    streamer = Streamer(SimpleNamespace(output_schema=object()))
    assert streamer._filter_values({"secret_internal": "leak"}) == {}


def test_typeddict_output_schema_keeps_declared_keys() -> None:
    # The common ``StateGraph(State, output=OutputState)`` pattern yields a
    # TypedDict output schema: no ``model_fields``, but introspectable via
    # ``__annotations__``. Declared keys (incl. ``messages``) must pass; internal
    # pipeline keys must still be dropped. Regression: failing closed here
    # stripped ``messages`` from every streamed value, killing token-by-token
    # streaming so the UI only saw the message after the post-run state fetch.
    streamer = Streamer(SimpleNamespace(output_schema=_OutputSchema))
    filtered = streamer._filter_values(
        {"messages": [{"id": "a"}], "evidence": "internal-secret"}
    )
    assert filtered == {"messages": [{"id": "a"}]}
