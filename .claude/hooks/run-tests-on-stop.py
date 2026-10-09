#!/usr/bin/env python3
"""Stop hook: don't let a turn end with a broken test suite.

When the working tree has uncommitted changes under ``src/`` or ``tests/``,
run the full pytest suite (in-memory, sub-second) before Claude is allowed to
stop. On failure, exit 2 so the failure output is fed back and the turn
continues until the suite is green.

Loop guard: each consecutive block bumps a marker-file counter; after
``MAX_BLOCKS`` consecutive red runs the hook lets the turn end (so an
unfixable failure can be reported to the user instead of looping forever).
A green run resets the counter. When the guard gives up, it still reports the
final failure (with the test output tail) as a non-blocking system message.
"""

import json
import subprocess
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[2]
# Codex passes ``--agent codex`` so each agent keeps its own retry counter.
MARKER_NAME = (
    ".codex-stop-test-blocks"
    if sys.argv[1:3] == ["--agent", "codex"]
    else ".stop-test-blocks"
)
RETRY_MARKER = Path(__file__).resolve().parent / MARKER_NAME
MAX_BLOCKS = 3


def _touched_code_paths() -> bool:
    """True if the working tree has staged/unstaged/untracked src or test .py files."""
    result = subprocess.run(
        # --untracked-files=all lists files inside brand-new directories too.
        ["git", "status", "--porcelain", "--untracked-files=all"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True,
        check=False,
    )
    for line in result.stdout.splitlines():
        # Porcelain format: ``XY path`` (or ``XY old -> new`` for renames).
        # Check both sides so moving a file out of src/ or tests/ still counts.
        for part in line[3:].split(" -> "):
            path = part.strip().strip('"')
            if path.endswith(".py") and path.startswith(("src/", "tests/")):
                return True
    return False


def _consecutive_blocks() -> int:
    try:
        return int(RETRY_MARKER.read_text())
    except (OSError, ValueError):
        return 0


def main() -> None:
    try:
        json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        pass  # Payload is informational here; never break stopping over it.

    if not _touched_code_paths():
        RETRY_MARKER.unlink(missing_ok=True)
        return

    result = subprocess.run(
        ["poetry", "run", "pytest", "-q", "-x"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0:
        RETRY_MARKER.unlink(missing_ok=True)
        return

    blocks = _consecutive_blocks() + 1
    tail = "\n".join((result.stdout + result.stderr).splitlines()[-40:])
    if blocks >= MAX_BLOCKS:
        # Give up blocking: let the turn end so the failure gets reported to
        # the user rather than burning attempts forever, but say so.
        RETRY_MARKER.unlink(missing_ok=True)
        print(
            json.dumps(
                {
                    "systemMessage": (
                        "Test suite is still failing after "
                        f"{MAX_BLOCKS - 1} blocked attempts; the stop guard "
                        "gave up. Report this failure.\n\n" + tail
                    )
                }
            )
        )
        return
    RETRY_MARKER.write_text(str(blocks))

    print(
        "Stop blocked: the test suite is failing after your changes "
        f"(attempt {blocks}/{MAX_BLOCKS}). Fix the regression before "
        "finishing the turn, or revert the offending change.\n\n" + tail,
        file=sys.stderr,
    )
    sys.exit(2)


if __name__ == "__main__":
    main()
