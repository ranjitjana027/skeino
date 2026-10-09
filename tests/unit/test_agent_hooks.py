"""Tests for the shared Codex / Claude Code hook scripts in ``.claude/hooks/``."""

import importlib.util
import io
import json
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

HOOKS_DIR = Path(__file__).resolve().parents[2] / ".claude" / "hooks"


def _load(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        name.replace("-", "_"), HOOKS_DIR / f"{name}.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


protect_files = _load("protect-files")
stop_hook = _load("run-tests-on-stop")


def _patch(*lines: str, eol: str = "\n") -> str:
    return eol.join(["*** Begin Patch", *lines, "*** End Patch"]) + eol


def _run_guard(monkeypatch: pytest.MonkeyPatch, payload: object) -> int:
    """Run protect-files.main() on ``payload``; return its exit status."""
    raw = payload if isinstance(payload, str) else json.dumps(payload)
    monkeypatch.setattr("sys.stdin", io.StringIO(raw))
    try:
        protect_files.main()
    except SystemExit as exc:
        return int(exc.code or 0)
    return 0


@pytest.mark.parametrize(
    "tool_input",
    [
        {"file_path": "poetry.lock"},
        {"file_path": "/repo/.env.production"},
        {"command": _patch("*** Update File: poetry.lock", "@@")},
        {"command": _patch("*** Update File: src/a.py", "*** Move to: poetry.lock")},
        {"command": _patch("*** Add File: .env")},
        # CRLF line endings must not hide the target behind a trailing "\r".
        {"command": _patch("*** Update File: poetry.lock", eol="\r\n")},
        {"command": _patch("*** Delete File: sub/.env", eol="\r\n")},
    ],
)
def test_guard_blocks_protected_targets(
    monkeypatch: pytest.MonkeyPatch, tool_input: dict[str, str]
) -> None:
    assert _run_guard(monkeypatch, {"tool_input": tool_input}) == 2


@pytest.mark.parametrize(
    "tool_input",
    [
        {"file_path": "src/skeino/app.py"},
        {"command": _patch("*** Update File: src/skeino/app.py", "@@")},
        {"command": _patch("*** Update File: docs/poetry.lock.md", eol="\r\n")},
        {"command": "ls -la"},
    ],
)
def test_guard_allows_ordinary_edits(
    monkeypatch: pytest.MonkeyPatch, tool_input: dict[str, str]
) -> None:
    assert _run_guard(monkeypatch, {"tool_input": tool_input}) == 0


def test_guard_ignores_unparseable_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    assert _run_guard(monkeypatch, "not json") == 0


def _fake_git(monkeypatch: pytest.MonkeyPatch, status: str) -> list[list[str]]:
    """Make the stop hook's ``git status`` return ``status``; record argv."""
    calls: list[list[str]] = []

    def run(cmd: list[str], **_: object) -> SimpleNamespace:
        calls.append(cmd)
        return SimpleNamespace(returncode=0, stdout=status, stderr="")

    monkeypatch.setattr(stop_hook, "subprocess", SimpleNamespace(run=run))
    return calls


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("?? src/skeino/new_pkg/mod.py\n", True),
        (" M tests/unit/test_x.py\n", True),
        # Moving a module out of src/ is still a code change.
        ("R  src/skeino/a.py -> docs/a.py\n", True),
        (" M README.md\n?? docs/new.py\n", False),
        ("", False),
    ],
)
def test_touched_code_paths(
    monkeypatch: pytest.MonkeyPatch, status: str, expected: bool
) -> None:
    calls = _fake_git(monkeypatch, status)
    assert stop_hook._touched_code_paths() is expected
    # Without this flag git collapses a new directory into "?? dir/".
    assert "--untracked-files=all" in calls[0]


def _failing_suite(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    marker = tmp_path / ".marker"
    monkeypatch.setattr(stop_hook, "RETRY_MARKER", marker)
    monkeypatch.setattr(stop_hook, "_touched_code_paths", lambda: True)
    monkeypatch.setattr("sys.stdin", io.StringIO("{}"))

    def run(cmd: list[str], **_: object) -> SimpleNamespace:
        return SimpleNamespace(returncode=1, stdout="FAILED test_x\n", stderr="")

    monkeypatch.setattr(stop_hook, "subprocess", SimpleNamespace(run=run))
    return marker


def test_stop_hook_blocks_early_failures(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    marker = _failing_suite(monkeypatch, tmp_path)
    with pytest.raises(SystemExit) as exc:
        stop_hook.main()
    assert exc.value.code == 2
    assert marker.read_text() == "1"
    assert "FAILED test_x" in capsys.readouterr().err


def test_stop_hook_reports_final_failure_when_it_gives_up(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    marker = _failing_suite(monkeypatch, tmp_path)
    marker.write_text(str(stop_hook.MAX_BLOCKS - 1))
    stop_hook.main()  # does not block, so no SystemExit
    out = json.loads(capsys.readouterr().out)
    assert "FAILED test_x" in out["systemMessage"]
    assert not marker.exists()
