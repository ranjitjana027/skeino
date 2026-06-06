# Security Policy

## Supported Versions

skeino is currently in the `0.1.x` (beta) series. Security fixes are applied to
the latest released version. Until a `1.0.0` release, only the most recent
`0.1.x` release is supported.

| Version | Supported |
| ------- | --------- |
| 0.1.x   | ✅        |
| < 0.1   | ❌        |

## Reporting a Vulnerability

**Please do not open a public issue for security vulnerabilities.**

Report privately using either of the following:

1. **GitHub Security Advisories** (preferred) — go to the
   [Security tab](https://github.com/ranjitjana027/skeino/security/advisories/new)
   and click "Report a vulnerability". This keeps the report private until a fix
   is published.
2. **Email** — `admin@equityresearchlab.in` with the subject line
   `[skeino security]`.

Please include:

- A description of the issue and its impact.
- Steps to reproduce (a minimal proof of concept is ideal).
- Affected version(s) and environment details.

## What to Expect

- **Acknowledgement** within 3 business days.
- An initial assessment and severity classification within 7 business days.
- Coordinated disclosure: we will work with you on a fix and a disclosure
  timeline, and credit you in the advisory unless you prefer to remain anonymous.

Because skeino executes user-supplied LangGraph graphs and exposes an HTTP
surface (threads, runs, streaming, persistence), reports about request handling,
serialization/deserialization, run isolation, and checkpoint storage are
especially welcome.
