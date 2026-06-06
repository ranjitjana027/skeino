"""Output-schema filtering must fail closed, never open.

When a graph declares an output schema, internal state keys are stripped before
reaching clients. If the schema can't be introspected the filter must drop
everything (fail closed) rather than pass everything through (a data leak).
"""

from types import SimpleNamespace

from skeino.streaming.runner import Streamer


def test_no_output_schema_passes_all_values() -> None:
    # No declared output schema → no filtering is the intended behaviour.
    streamer = Streamer(SimpleNamespace())
    assert streamer._filter_values({"a": 1, "b": 2}) == {"a": 1, "b": 2}


def test_unintrospectable_output_schema_fails_closed() -> None:
    # output_schema is present but has no ``model_fields`` → drop every value.
    streamer = Streamer(SimpleNamespace(output_schema=object()))
    assert streamer._filter_values({"secret_internal": "leak"}) == {}
