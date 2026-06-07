# Changelog fragments

This project uses [towncrier](https://towncrier.readthedocs.io/) for its
changelog. **Do not edit `CHANGELOG.md` directly** for unreleased changes — add a
*fragment* here instead. Because every PR adds its own file, parallel PRs never
conflict on the changelog.

## Adding a fragment

Create one file per user-facing change, named `<id>.<type>.md`, where `<id>` is
the issue or PR number (or a short slug if there's no number) and `<type>` is one
of:

| type         | section in CHANGELOG |
| ------------ | -------------------- |
| `added`      | Added                |
| `changed`    | Changed              |
| `deprecated` | Deprecated           |
| `removed`    | Removed              |
| `fixed`      | Fixed                |
| `security`   | Security             |

The file contents are the changelog entry itself (plain Markdown, no leading
`-`). For example, `changelog.d/42.added.md`:

```markdown
`POST /threads/{id}/copy` — fork a thread into an independent copy.
```

Purely internal changes (refactors, CI, tests) don't need a fragment.

## Previewing and building

```bash
poetry run towncrier build --draft --version X.Y.Z   # preview, no changes
poetry run towncrier build --version X.Y.Z           # collate + delete fragments
```

`towncrier build` is run at release time (see the `cut-release` skill); it writes
the collated section into `CHANGELOG.md` and removes the consumed fragments.
