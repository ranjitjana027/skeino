"""Unit tests for the checkpoint-metadata enrichment rules.

``enrich_metadata`` is a plain function, tested directly without any database
driver. It drives how LangGraph Studio groups checkpoints into a run, so the
exclusion and precedence rules below are behaviourally important. The
backend-agnostic wrapper that applies it (``build_run_enriching_checkpointer``)
is exercised here over an in-memory saver.
"""

from typing import Any

from langgraph.checkpoint.base import BaseCheckpointSaver, empty_checkpoint
from langgraph.checkpoint.memory import MemorySaver

from skeino.persistence.enriching import build_run_enriching_checkpointer
from skeino.persistence.enriching import enrich_metadata as _enrich


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


async def test_wrapper_stamps_run_id_on_write_over_any_saver() -> None:
    """The wrapper enriches writes for a non-postgres saver (the #49 fix).

    Drives the write path directly over a MemorySaver: without the wrapper the
    stored checkpoint metadata would carry no ``run_id``.
    """
    wrapped = build_run_enriching_checkpointer(MemorySaver())
    assert isinstance(wrapped, BaseCheckpointSaver)  # LangGraph requires this

    config: Any = {
        "configurable": {"thread_id": "t-1", "checkpoint_ns": ""},
        "run_id": "r-9",
    }
    saved = await wrapped.aput(config, empty_checkpoint(), {"source": "loop"}, {})

    tup = await wrapped.aget_tuple(saved)
    assert tup is not None
    assert tup.metadata["run_id"] == "r-9"
    assert tup.metadata["source"] == "loop"


async def test_wrapper_delegates_reads_to_inner() -> None:
    """Reads are served by the wrapped saver, not the empty base stubs."""
    inner = MemorySaver()
    wrapped = build_run_enriching_checkpointer(inner)
    config: Any = {
        "configurable": {"thread_id": "t-2", "checkpoint_ns": ""},
        "run_id": "r-1",
    }
    await wrapped.aput(config, empty_checkpoint(), {}, {})

    # alist must yield the checkpoint just written (delegated, not a stub).
    listed = [
        item async for item in wrapped.alist({"configurable": {"thread_id": "t-2"}})
    ]
    assert len(listed) == 1
    # serde is delegated to the inner saver, not BaseCheckpointSaver's default.
    assert wrapped.serde is inner.serde
