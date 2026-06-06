"""Unit tests for RunEnrichingCheckpointer._enrich metadata rules.

``_enrich`` is a pure staticmethod, so it is tested directly without a live
Postgres connection. It drives how LangGraph Studio groups checkpoints into a
run, so the exclusion and precedence rules below are behaviourally important.
"""

from skeino.persistence.enriching import RunEnrichingCheckpointer

_enrich = RunEnrichingCheckpointer._enrich


def test_excludes_checkpoint_internal_and_dunder_keys() -> None:
    config = {
        "configurable": {
            "checkpoint_ns": "ns",
            "checkpoint_id": "cid",
            "__pregel_private": 1,
            "thread_id": "t-1",
        }
    }
    out = _enrich({}, config)
    assert "checkpoint_ns" not in out
    assert "checkpoint_id" not in out
    assert "__pregel_private" not in out
    assert out["thread_id"] == "t-1"


def test_metadata_takes_precedence_over_config() -> None:
    config = {
        "configurable": {"x": "from_configurable"},
        "metadata": {"x": "from_config_meta"},
    }
    assert _enrich({"x": "from_metadata"}, config)["x"] == "from_metadata"


def test_config_metadata_takes_precedence_over_configurable() -> None:
    config = {
        "configurable": {"x": "from_configurable"},
        "metadata": {"x": "from_config_meta"},
    }
    assert _enrich({}, config)["x"] == "from_config_meta"


def test_backfills_run_id_when_absent() -> None:
    assert _enrich({}, {"configurable": {"run_id": "r-123"}})["run_id"] == "r-123"
    assert _enrich({}, {"run_id": "r-top"})["run_id"] == "r-top"
    assert _enrich({}, {"metadata": {"run_id": "r-meta"}})["run_id"] == "r-meta"


def test_preserves_existing_run_id() -> None:
    config = {"configurable": {"run_id": "r-config"}}
    assert _enrich({"run_id": "r-meta"}, config)["run_id"] == "r-meta"


def test_handles_missing_config() -> None:
    assert _enrich({"a": 1}, None) == {"a": 1}
