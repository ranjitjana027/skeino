"""Checkpointer wrapper that stamps ``run_id`` into checkpoint metadata.

LangGraph Studio groups checkpoints into a single run when each checkpoint
carries the same ``run_id`` in its metadata. Without this, Studio renders
every node execution as its own fork ("Fork N of N"). This wrapper mirrors
``langgraph_api._checkpointer._adapter._enrich_metadata`` without importing
``langgraph_api`` (which requires platform-only environment variables).

The Postgres saver is an optional dependency (``skeino[postgres]``), so the
``AsyncPostgresSaver`` subclass is built **lazily** by
:func:`build_run_enriching_checkpointer` rather than at import time — importing
this module never requires psycopg. ``enrich_metadata`` is a plain function so
it can be used and tested without Postgres installed.
"""

from collections.abc import Mapping
from typing import Any

_EXCLUDED_CP_KEYS: frozenset[str] = frozenset({"checkpoint_ns", "checkpoint_id"})

_enriching_cls: type[Any] | None = None


def enrich_metadata(
    metadata: Mapping[str, Any], config: Mapping[str, Any] | None
) -> dict[str, Any]:
    """Merge config-derived metadata into checkpoint metadata, stamping run_id.

    Precedence (low → high): non-internal ``configurable`` keys, ``config``
    metadata, then the checkpoint ``metadata``. ``run_id`` is backfilled from the
    config when the checkpoint metadata doesn't already carry one.
    """
    configurable = config.get("configurable", {}) if config else {}
    config_meta = config.get("metadata", {}) if config else {}
    enriched = {
        **{
            k: v
            for k, v in configurable.items()
            if not k.startswith("__") and k not in _EXCLUDED_CP_KEYS
        },
        **config_meta,
        **metadata,
    }
    if not enriched.get("run_id"):
        run_id = (
            (config.get("run_id") if config else None)
            or config_meta.get("run_id")
            or configurable.get("run_id")
        )
        if run_id:
            enriched["run_id"] = str(run_id)
    return enriched


def build_run_enriching_checkpointer(inner: Any) -> Any:
    """Wrap an ``AsyncPostgresSaver`` so writes stamp ``run_id`` into metadata.

    The ``AsyncPostgresSaver`` subclass is defined on first use (and cached), so
    the postgres dependency is only required when the postgres checkpointer is
    actually selected.
    """
    global _enriching_cls
    if _enriching_cls is None:
        from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

        class RunEnrichingCheckpointer(AsyncPostgresSaver):
            """Saver over ``inner``'s connection that enriches writes with run_id.

            We construct a fully-functional ``AsyncPostgresSaver`` over the
            shared connection rather than copying ``inner.__dict__``; only the
            write path is overridden, with reads using the inherited
            implementation against the same connection.
            """

            def __init__(self, inner: AsyncPostgresSaver) -> None:
                super().__init__(inner.conn, serde=inner.serde)

            async def aput(
                self, config: Any, checkpoint: Any, metadata: Any, new_versions: Any
            ) -> Any:
                enriched: Any = enrich_metadata(metadata, config)
                return await super().aput(config, checkpoint, enriched, new_versions)

            def put(
                self, config: Any, checkpoint: Any, metadata: Any, new_versions: Any
            ) -> Any:
                enriched: Any = enrich_metadata(metadata, config)
                return super().put(config, checkpoint, enriched, new_versions)

        _enriching_cls = RunEnrichingCheckpointer
    return _enriching_cls(inner)
