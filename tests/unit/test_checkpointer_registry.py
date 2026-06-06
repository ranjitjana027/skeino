"""Tests for the pluggable checkpointer registry."""

from contextlib import asynccontextmanager

import pytest

from skeino.persistence import open_checkpointer, register_checkpointer
from skeino.persistence.checkpointer import CheckpointerSpec


@pytest.mark.asyncio
async def test_memory_resolution_no_uri() -> None:
    """No URI → memory checkpointer."""
    async with open_checkpointer() as ckpt:
        assert ckpt.__class__.__name__ in {"MemorySaver", "InMemorySaver"}


@pytest.mark.asyncio
async def test_explicit_memory_scheme() -> None:
    async with open_checkpointer(scheme="memory") as ckpt:
        assert ckpt.__class__.__name__ in {"MemorySaver", "InMemorySaver"}


@pytest.mark.asyncio
async def test_unknown_scheme_raises() -> None:
    with pytest.raises(ValueError, match="No checkpointer registered"):
        async with open_checkpointer(uri="redis://localhost"):
            pass


@pytest.mark.asyncio
async def test_register_checkpointer_adds_scheme() -> None:
    sentinel = object()

    @register_checkpointer("skeino-test-scheme")
    @asynccontextmanager
    async def _builder(_spec: CheckpointerSpec):
        yield sentinel  # type: ignore[misc]

    async with open_checkpointer(scheme="skeino-test-scheme") as ckpt:
        assert ckpt is sentinel
