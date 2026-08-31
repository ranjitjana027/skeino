"""Checkpointer wrapper that stamps ``run_id`` into checkpoint metadata.

LangGraph Studio groups checkpoints into a single run when each checkpoint
carries the same ``run_id`` in its metadata. Without this, Studio renders
every node execution as its own fork ("Fork N of N"). This wrapper mirrors
``langgraph_api._checkpointer._adapter._enrich_metadata`` without importing
``langgraph_api`` (which requires platform-only environment variables).

The wrapper is **backend-agnostic**: it subclasses
:class:`~langgraph.checkpoint.base.BaseCheckpointSaver` (LangGraph rejects a
checkpointer that isn't an instance of it) and delegates the whole saver
interface to the wrapped ``inner`` saver, intercepting only the write path
(``aput``/``put``) to enrich metadata. skeino applies it to the postgres,
sqlite, and redis builders — savers that do not themselves merge config
metadata into checkpoint metadata. MongoDB's saver merges it natively, so it is
left unwrapped. ``enrich_metadata`` is a plain function so it can be used and
tested without any database driver installed.
"""

from collections.abc import Mapping
from typing import Any

from langgraph.checkpoint.base import BaseCheckpointSaver

_EXCLUDED_CP_KEYS: frozenset[str] = frozenset({"checkpoint_ns", "checkpoint_id"})


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


class _RunEnrichingCheckpointer(BaseCheckpointSaver):
    """Delegating saver that enriches writes with ``run_id``.

    Only ``aput``/``put`` are intercepted; every other call is forwarded to
    ``inner`` so reads, writes-of-pending-writes, deletes, and version bumps use
    the real backend implementation. ``__getattr__`` forwards any attribute not
    defined here (backend-specific helpers); ``config_specs`` is delegated
    explicitly because :class:`BaseCheckpointSaver` defines it at the class
    level, which would otherwise shadow ``__getattr__``.
    """

    def __init__(self, inner: BaseCheckpointSaver) -> None:
        # Adopt the wrapped saver's serializer so ``self.serde`` matches it; the
        # wrapper holds no other state of its own.
        super().__init__(serde=inner.serde)
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        # Fires only for names absent on this instance/class; forward them to the
        # wrapped saver so backend-specific attributes keep working.
        return getattr(object.__getattribute__(self, "_inner"), name)

    @property
    def config_specs(self) -> Any:
        return self._inner.config_specs

    # --- write path: enrich --------------------------------------------------
    async def aput(
        self, config: Any, checkpoint: Any, metadata: Any, new_versions: Any
    ) -> Any:
        enriched: Any = enrich_metadata(metadata, config)
        return await self._inner.aput(config, checkpoint, enriched, new_versions)

    def put(
        self, config: Any, checkpoint: Any, metadata: Any, new_versions: Any
    ) -> Any:
        enriched: Any = enrich_metadata(metadata, config)
        return self._inner.put(config, checkpoint, enriched, new_versions)

    # --- delegated interface -------------------------------------------------
    # BaseCheckpointSaver defines these as stubs, so __getattr__ won't forward
    # them; delegate explicitly to the wrapped saver.
    async def aget_tuple(self, config: Any) -> Any:
        return await self._inner.aget_tuple(config)

    def get_tuple(self, config: Any) -> Any:
        return self._inner.get_tuple(config)

    def alist(self, config: Any, **kwargs: Any) -> Any:
        return self._inner.alist(config, **kwargs)

    def list(self, config: Any, **kwargs: Any) -> Any:
        return self._inner.list(config, **kwargs)

    async def aput_writes(self, *args: Any, **kwargs: Any) -> Any:
        return await self._inner.aput_writes(*args, **kwargs)

    def put_writes(self, *args: Any, **kwargs: Any) -> Any:
        return self._inner.put_writes(*args, **kwargs)

    async def adelete_thread(self, thread_id: Any) -> Any:
        return await self._inner.adelete_thread(thread_id)

    def delete_thread(self, thread_id: Any) -> Any:
        return self._inner.delete_thread(thread_id)

    def get_next_version(self, current: Any, channel: Any = None) -> Any:
        return self._inner.get_next_version(current, channel)


def build_run_enriching_checkpointer(inner: BaseCheckpointSaver) -> BaseCheckpointSaver:
    """Wrap any saver so writes stamp ``run_id`` into checkpoint metadata."""
    return _RunEnrichingCheckpointer(inner)
