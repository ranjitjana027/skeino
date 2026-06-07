---
name: respond-to-pr-review
description: >-
  Triage and address review comments (Copilot or human) on a skeino pull
  request: fetch the comments, separate real issues from known false positives,
  fix the valid ones with tests, reply, and resolve the review threads. Use when
  asked to "address PR comments", "check Copilot comments", or handle review
  feedback on an open PR.
---

# Respond to PR review comments

## 1. Fetch the comments

For each open PR (focus the newest first), pull inline review comments and
top-level comments with timestamps so you can tell new from already-handled:

```bash
gh api repos/<owner>/<repo>/pulls/<N>/comments \
  --jq '.[] | "\(.created_at) \(.user.login) \(.path):\(.line // .original_line)\n  \(.body)\n"'
gh api repos/<owner>/<repo>/issues/<N>/comments \
  --jq '.[] | "\(.created_at) \(.user.login): \(.body[0:200])"'
```

A comment that you already replied to / resolved may still appear — check the
thread's `isResolved` (see step 4) before treating it as new.

## 2. Triage: real vs false positive

Verify each claim against the actual code before acting — reviewers (especially
Copilot) are often wrong. When unsure, reproduce it (write a throwaway check /
test). Known false positives in skeino — do **not** "fix" these:

- `logger.error(..., exc_info=<exception instance>)` is valid since Python 3.5;
  it uses the instance's `__traceback__`, even outside an `except` block.
- `asyncio_mode = "auto"` means bare `async def` tests need no
  `@pytest.mark.asyncio`.

Common **valid** classes worth fixing: truthiness coercion that drops empty
lists (`x or {}`), request models whose empty body mutates state, and tests that
pass vacuously (e.g. the fake graph ignores the thing under test).

## 3. Fix the valid ones

Apply the fix with a test that would fail without it, then run the full check
suite (`ruff format` / `ruff check` / `mypy src` / `bandit -r src` / `pytest`).
Commit on the PR branch with a message that names the feedback addressed.

## 4. Reply and resolve

Post one comment summarising what changed (cite the commit), then resolve each
addressed thread via GraphQL:

```bash
# list unresolved thread ids
gh api graphql -f query='{repository(owner:"<owner>",name:"<repo>"){pullRequest(number:<N>){reviewThreads(first:50){nodes{id isResolved}}}}}' \
  --jq '.data.repository.pullRequest.reviewThreads.nodes[] | select(.isResolved==false) | .id'
# resolve one
gh api graphql -f query='mutation{resolveReviewThread(input:{threadId:"<THREAD_ID>"}){thread{isResolved}}}'
```

For a confirmed false positive, reply explaining why (with evidence) instead of
changing code — then resolve it.

## 5. Confirm green

Poll CI until checks finish and the PR is `MERGEABLE / CLEAN`:

```bash
until gh pr checks <N> 2>/dev/null | grep -qE 'pass|fail' \
  && ! gh pr checks <N> 2>/dev/null | grep -q 'pending'; do sleep 5; done
gh pr view <N> --json mergeable,mergeStateStatus
```
