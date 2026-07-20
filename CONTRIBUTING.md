# Contributing to LangAlpha

Thanks for your interest in contributing to LangAlpha! This guide covers how to
set up your environment and submit changes.

> **Working language:** English is our preferred language for communication.
> Chinese is also welcome in issues and pull requests — but all code changes and
> docstrings must be written in English.

## Prerequisites

- Docker and Docker Compose

For running on your host instead of containers (optional): Python 3.12+ with
[uv](https://docs.astral.sh/uv/), and Node.js 22+ with pnpm.

## Quick Start

The whole stack — backend, frontend, PostgreSQL, and Redis — runs with Docker
Compose:

```bash
git clone https://github.com/ginlix-ai/langalpha.git
cd langalpha
cp .env.example .env
make config   # interactive wizard: LLM, data, sandbox, web search, and web fetch
make up       # build + start the full stack
```

- **Backend API** → http://localhost:8000 (hot-reload; `./src` is mounted)
- **Frontend** → http://localhost:5173 (hot-reload)
- **PostgreSQL + Redis** — managed by Compose

Verify the backend is healthy:

```bash
curl http://localhost:8000/health   # → {"status": "healthy"}
```

Stop the stack with `make down` (or `make clean` to also reclaim Docker disk).

No keys are strictly required — see [Data Provider Fallback Chain](README.md#data-provider-fallback-chain).
For the full experience, set `DAYTONA_API_KEY` and `FMP_API_KEY` in `.env`; for
LLM access, set an API key or connect via OAuth in the UI.

<details>
<summary>Running on your host instead of Docker</summary>

```bash
make install    # backend (uv) + frontend (pnpm) dependencies
make setup-db   # PostgreSQL + Redis in Docker + initialize tables
make dev        # backend on :8000 (hot-reload)
make dev-web    # frontend on :5173 (run in a separate terminal)
```

For web crawling on the host, install the browser dependencies (already bundled
in the Docker image):

```bash
source .venv/bin/activate && scrapling install
```

</details>

## Contributing Changes

1. **Please start a feature with an issue.** Before building a feature, we'd
   kindly ask you to open a [GitHub Issue](https://github.com/ginlix-ai/langalpha/issues)
   with a short proposal. This lets a maintainer weigh in early and ensures a
   swift follow-up once your pull request lands. Bug fixes are welcome to go
   straight to a PR.
2. **Please check with us before adding a dependency.** We'd kindly ask that you
   not add a new third-party dependency or external service without checking with
   a maintainer first. If you think a library or service would be a good fit, we'd
   love to hear about it — please propose it in an issue or email
   [contact@ginlix.ai](mailto:contact@ginlix.ai) before wiring it in.
3. **Please show that your change works.** We'd kindly ask that every change
   demonstrate the intended behavior — the bug fix or new feature — works as
   expected. Please verify it end to end, and add tests that guard against
   regressions in the areas your change touches.
   ```bash
   make test       # backend unit tests
   make test-web   # frontend unit tests
   make lint       # linters
   ```
4. **Open a pull request** against `main` with a clear description of what changed
   and how you verified it. Thank you for contributing!

## Code Style

**Python:**
- Linted with [Ruff](https://docs.astral.sh/ruff/) — `uv run ruff check src/`
- Async-first: use `async def` for handlers and services
- No ORM — raw SQL via psycopg3

**Frontend (TypeScript/React):**
- Linted with ESLint 9 (flat config) — `cd web && pnpm lint`
- Components use shadcn/ui + Tailwind CSS

## Reporting Issues

Open a [GitHub Issue](https://github.com/ginlix-ai/langalpha/issues) with what you
expected vs what happened, steps to reproduce, and relevant logs or screenshots.

## Questions?

Open a [GitHub Discussion](https://github.com/ginlix-ai/langalpha/discussions) or
email [contact@ginlix.ai](mailto:contact@ginlix.ai).

## License

LangAlpha is licensed under the [Apache License 2.0](LICENSE). By submitting a
contribution, you agree that it is licensed under those same Apache-2.0 terms
(the standard "inbound = outbound" model — see Apache-2.0 §5). Please only submit
work you have the right to license under those terms.
