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


def test_updates_filter_strips_internal_keys_per_node() -> None:
    # An ``updates`` payload is ``{node: {state_key: value}}``. Each node's delta
    # carries internal routing/raw fields that must be stripped, while declared
    # output keys (e.g. ``messages``) pass. Regression: without per-node
    # filtering, ``updates`` mode leaked ``agent_type``/``final_report_raw`` etc.
    streamer = Streamer(SimpleNamespace(output_schema=_OutputSchema))
    filtered = streamer._filter_updates(
        {
            "router": {"agent_type": "simple", "pipeline_status": ["go"]},
            "simple": {"messages": [{"id": "a"}], "final_report_raw": "leak"},
        }
    )
    assert filtered == {
        "router": {"pipeline_status": ["go"]},
        "simple": {"messages": [{"id": "a"}]},
    }


def test_updates_filter_no_schema_passes_through() -> None:
    # No declared output schema → no filtering, same posture as _filter_values.
    streamer = Streamer(SimpleNamespace())
    payload = {"node": {"messages": [{"id": "a"}], "anything": 1}}
    assert streamer._filter_updates(payload) == payload
