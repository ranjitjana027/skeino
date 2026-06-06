"""Tests for the GraphRegistry."""

from types import SimpleNamespace

import pytest

from skeino import GraphRegistry


def test_registry_requires_at_least_one_graph() -> None:
    with pytest.raises(ValueError, match="at least one graph"):
        GraphRegistry({})


def test_default_id_is_first_when_unspecified() -> None:
    reg = GraphRegistry({"a": SimpleNamespace(), "b": SimpleNamespace()})
    assert reg.default_id == "a"


def test_explicit_default_must_exist() -> None:
    with pytest.raises(ValueError, match="not present"):
        GraphRegistry({"a": SimpleNamespace()}, default="missing")


def test_get_membership_and_iteration() -> None:
    a, b = SimpleNamespace(), SimpleNamespace()
    reg = GraphRegistry({"a": a, "b": b}, default="b")
    assert reg.default_graph is b
    assert reg.get("a") is a
    assert reg.get("missing") is None
    assert "a" in reg
    assert "missing" not in reg
    assert list(reg) == ["a", "b"]
    assert len(reg) == 2
