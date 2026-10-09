#!/usr/bin/env python3
"""PreToolUse guardrail: block edits to protected files.

Reads the Claude Code or Codex hook payload from stdin and exits with status 2 (which
blocks the tool call and feeds the message back to Claude) when an Edit/Write
targets a dependency lock file or a secrets file.
"""

import json
import re
import sys

# poetry.lock must only change via `poetry` commands; never hand-edit secrets.
PROTECTED = re.compile(r"(^|/)(poetry\.lock|\.env(\.[^/]+)?)$")


def main() -> None:
    """Block the tool call if it targets a protected path."""
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return  # No parseable payload: don't get in the way.

    tool_input = payload.get("tool_input", {})
    paths = [tool_input.get("file_path", "")]
    # Include every patch target and rename destination in Codex patches.
    patch = tool_input.get("command", "")
    paths.extend(
        re.findall(
            r"^\*\*\* (?:Add File|Update File|Delete File|Move to): (.+)$", patch, re.M
        )
    )
    path = next((path for path in paths if PROTECTED.search(path)), "")
    if path:
        print(
            f"Blocked edit to protected file: {path}\n"
            "- Update poetry.lock via `poetry add/update/lock`, not direct edits.\n"
            "- Never write secrets into a committed .env file.",
            file=sys.stderr,
        )
        sys.exit(2)


if __name__ == "__main__":
    main()
