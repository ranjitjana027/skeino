---
name: review-and-merge-prs
description: >-
  Triage, review, and land open skeino pull requests: read each diff for
  correctness, confirm CI is green, verify mergeability (against main and against
  each other), resolve the recurring additive conflicts, squash-merge, and clean
  up. Use when asked to "check open PRs", "review and merge PRs", or land
  contributor work.
---

# Review and merge open PRs

## 1. List and read

```bash
gh pr list --state open --json number,title,headRefName,isDraft,author \
  --jq '.[] | "#\(.number) \(.headRefName) draft=\(.isDraft) \(.title) [@\(.author.login)]"'
gh pr view <N> --json title,body,additions,deletions,changedFiles,mergeable,mergeStateStatus
gh pr diff <N>
```

Read the **whole** diff, not just the description. Check it against the issue it
`Closes`.

## 2. Review for correctness

Hold each PR to skeino's definition of done (see `CLAUDE.md`):

- Layers respected (`api` thin → `ops` logic → `persistence`/`streaming`), shared
  `Literal` types not widened to `str`, **both** store backends updated when
  persistence changes.
- Tests are **non-vacuous** — they'd fail if the feature broke (e.g. `FakeGraph`
  was extended to actually exercise the new behaviour), and cover 404/409/422.
- Errors fail closed/loud; `exc_info` on failure paths.
- Verify factual claims in the PR against the code/live sources (e.g. a config
  name, a manifest) rather than trusting the description.
- Known Copilot false positives (don't ask for "fixes"): `exc_info=<instance>` is
  valid; `asyncio_mode=auto` needs no `@pytest.mark.asyncio`.

## 3. Confirm CI

```bash
gh pr checks <N> --watch --interval 20
```

All of CI (3.11–3.13), CodeQL, and build must pass.

## 4. Verify mergeability (the part that bites)

`mergeable: UNKNOWN` just means GitHub hasn't recomputed; check with merge-tree:

```bash
git fetch origin
git merge-tree --write-tree origin/main origin/<branch> >/dev/null 2>&1 \
  && echo CLEAN || echo CONFLICTS
# do two open PRs conflict with EACH OTHER?
git merge-tree --write-tree origin/<branchA> origin/<branchB> 2>&1 | grep -i CONFLICT
```

PRs that each merge cleanly into `main` can still conflict with **each other** —
historically on `CHANGELOG.md`, `docs/api-reference/http.md`, and adjacent inserts
in `api/threads.py` / `ops/threads.py`. (Changelog conflicts are now avoided by
`changelog.d/` fragments — see `changelog.d/README.md`.)

## 5. Resolve conflicts when landing the second PR

Merge the clean one first. Then update the other branch and resolve the
**additive** conflicts (keep both sides — both add adjacent code):

```bash
git checkout <branchB> && git merge origin/main   # surfaces conflicts
# edit each file: keep both blocks, delete the <<<<<<< / ======= / >>>>>>> markers
poetry run ruff format . && poetry run ruff check . \
  && poetry run mypy src && poetry run bandit -r src && poetry run pytest
git commit --no-edit && git push
```

Wait for CI to go green again on the updated branch before merging.

## 6. Merge

Squash only (merge commits are rejected). The `main` ruleset requires a review
that the PR author can't self-supply (solo maintainer), and a
`RepositoryRole:admin` bypass actor is configured — so merge with `--admin`:

```bash
gh pr merge <N> --squash --delete-branch --admin
git checkout main && git pull
```

Verify the linked issue auto-closed (`gh issue view <issue> --json state`), then
delete stale local branches.

## 7. Don't release implicitly

Merging only stages changes under unreleased fragments. Publishing to PyPI is a
separate, explicit step — use the `cut-release` skill when the user asks.
