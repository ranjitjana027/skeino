"""Checkpointer wrapper that stamps ``run_id`` into checkpoint metadata.

LangGraph Studio groups checkpoints into a single run when each checkpoint
carries the same ``run_id`` in its metadata. Without this, Studio renders
every node execution as its own fork ("Fork N of N"). This wrapper mirrors
``langgraph_api._checkpointer._adapter._enrich_metadata`` without importing
``langgraph_api`` (which requires platform-only environment variables).
"""

from typing import Any

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

_EXCLUDED_CP_KEYS: frozenset[str] = frozenset({"checkpoint_ns", "checkpoint_id"})


class RunEnrichingCheckpointer(AsyncPostgresSaver):
    """Thin wrapper that enriches checkpoint metadata with run_id from config."""

    def __init__(self, inner: AsyncPostgresSaver) -> None:
        super().__init__(inner.conn, serde=inner.serde)
        self.__dict__.update(inner.__dict__)
        self._inner = inner

    @staticmethod
    def _enrich(metadata: Any, config: Any) -> Any:
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

    async def aput(
        self, config: Any, checkpoint: Any, metadata: Any, new_versions: Any
    ) -> Any:
        return await self._inner.aput(
            config, checkpoint, self._enrich(metadata, config), new_versions
        )

    def put(
        self, config: Any, checkpoint: Any, metadata: Any, new_versions: Any
    ) -> Any:
        return self._inner.put(
            config, checkpoint, self._enrich(metadata, config), new_versions
        )
