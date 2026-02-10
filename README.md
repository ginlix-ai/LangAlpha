<p align="center">
  <img src="web/public/logo_words.svg" alt="LangAlpha" height="120" />
  <br>
  <strong>A vibe investing agent harness</strong>
  <br>
  LangAlpha is built to help interpret financial markets and support investment decisions.
  <br><br>
  <a href="https://ai.google.dev/gemini-api"><img src="https://img.shields.io/badge/Gemini_3-8E75B2?logo=googlegemini&logoColor=white" alt="Gemini 3" /></a>
  <img src="https://img.shields.io/badge/python-3.12+-blue.svg" alt="Python 3.12+" />
  <a href="https://github.com/langchain-ai/langchain"><img src="https://img.shields.io/badge/LangChain-1c3c3c?logo=langchain&logoColor=white" alt="LangChain" /></a>
  <img src="https://img.shields.io/badge/license-Apache%202.0-green.svg" alt="License" />
</p>

---

<p align="center">
  <a href="#getting-started">Getting Started</a> &bull;
  <a href="docs/api/README.md">API Docs</a> &bull;
  <a href="src/ptc_agent/">Agent Core</a> &bull;
  <a href="src/server/">Backend</a> &bull;
  <a href="web/">Web</a> &bull;
  <a href="libs/ptc-cli/">TUI</a> &bull;
  <a href="skills/">Skills</a> &bull;
  <a href="mcp_servers/">MCP</a>
</p>

## Why LangAlpha
Every AI finance tool today treats investing as one-shot: ask a question, get an answer, move on. But real investing is Bayesian — you start with a thesis, new data arrives daily, and you update your conviction accordingly. It's an iterative process that unfolds over weeks and months: refining theses, revisiting positions, layering new analysis on top of old. No single prompt captures that.

### *From vibe coding to vibe investing*
Inspired by software engineering: a codebase persists, and every commit builds on what came before. Code agent harnesses like Claude Code and OpenCode succeeded by building agents that embrace this pattern, exploring existing context and building on prior work. LangAlpha brings that same insight: give the agent a persistent workspace, and research naturally compounds.

In practice, you create a workspace per research goal ("Q2 rebalance", "data center demand deep dive", "energy sector rotation"). The agent interviews you about your goals and style, produces its first deliverable, and saves everything to the workspace filesystem. Come back tomorrow and your files, threads, and accumulated research are still there.

## What Powers It

### Built on Gemini 3

LangAlpha is built natively on **Gemini 3** and ships with two modes:

- **PTC mode** uses **Gemini 3 Pro** for deep, multi-step investment research. Strong reasoning drives multi-step analysis where the agent plans its approach, thinks through financial data, and writes code for complex analysis. Long context lets it cross-reference SEC filings and research reports in a single pass, and native multimodal intelligence means it can interpret charts, financial PDFs, and visual data directly.
- **Flash mode** uses **Gemini 3 Flash** for fast conversational responses: quick market lookups, chart-and-chat in TradingCenter, and lightweight Q&A without spinning up a full workspace.

### Programmatic Tool Calling (PTC) and Workspace Architecture

Most AI agents interact with data through one-off JSON tool calls which dump the result into the context window directly. Programmatic Tool Calling flips this: instead of passing raw data through the LLM, the agent writes and executes code inside a [Daytona](https://www.daytona.io/) cloud sandbox that processes data locally and returns only the final result. This dramatically reduces token waste while enabling analysis that would otherwise exceed context limits.

In addition, the workspace environment enables persistence beyond a single session. The agent reads and writes deliverables (reports, data, charts, code) to structured directories in the sandbox filesystem, so intermediate results survive across sessions. Each workspace supports multiple conversation threads tied to a single research goal.

### Financial Data Ecosystem

While PTC excels at complex work like multi-step data processing, financial modeling, and chart creation, spinning up code execution for every data lookup is overkill. So we also built a native financial data toolset that transforms frequently used data into an LLM-digestible format. These tools also come with artifacts that render directly in the frontend, giving the human layer immediate visual context alongside the agent's analysis.

**Native tools** for quick reference via direct tool calls:
- **Company overview** with real-time quotes, price performance, key financial metrics, analyst consensus, and revenue breakdown
- **SEC filings** (10-K, 10-Q, 8-K) with earnings call transcripts and formatted markdown for citation
- **Market indices** and **sector performance** for broad market context
- **Web search** (Tavily, Serper, Bocha) and **web crawling** with circuit breaker fault tolerance

**MCP servers** for raw data consumed through PTC code execution:
- **Price data** for OHLCV time series across stocks, commodities, crypto, and forex
- **Fundamentals** for multi-year financial statements, ratios, growth metrics, and valuation
- **News** via TickerTick for ticker-specific and curated market news
- **Yahoo Finance** for options chains, institutional holdings, insider transactions, ESG data, and cross-company comparisons

The agent picks the right layer automatically: native tools for fast lookups that fit in context, MCP servers when the task requires bulk data processing, charting, or multi-year trend analysis in the sandbox.

> [!NOTE]
>Most native tools and MCP servers use [Financial Modeling Prep](https://site.financialmodelingprep.com/) as the data provider (`FMP_API_KEY` required).

### Agent Swarm

The core agent runs on [LangGraph](https://github.com/langchain-ai/langgraph) and spawns parallel async subagents via a `Task()` tool. Subagents execute concurrently with isolated context windows, preventing drift in long reasoning chains. Each subagent returns synthesized results back to the main agent, keeping the orchestrator lean. The main agent can choose to wait for a subagent's result or continue other pending work. Interrupting the main agent does not stop running subagents, so you can halt the orchestrator, update your requirements, or dispatch additional subagents while existing ones finish in the background. You can also switch to the **Subagents** view in the UI to see their progress in real time (web frontend only).

### Middleware Stack

The agent ships with a middleware stack, including:
- **Dynamic skill loading** via a `LoadSkill` tool that lets the agent discover and activate skill toolsets on demand, keeping the default tool surface lean while making specialized capabilities available when needed
- **Multimodal** intercepts file reads for images and PDFs, downloads content from the sandbox or URLs, and injects it as base64 into the conversation so multimodal models can interpret them natively
- **Plan mode** with human-in-the-loop interrupts lets you review and approve the agent's strategy before execution
- **Auto-summarization** compresses conversation history when approaching token limits, preserving key context while freeing space

See [`src/ptc_agent/agent/middleware/`](src/ptc_agent/agent/middleware/) for the full set.

Acknowledgement: some of them (like summarization) are adapted from the implementation in [LangChain DeepAgents](https://github.com/langchain-ai/deepagents).

### Streaming and Infrastructure

The server streams all agent activity over SSE: text chunks, tool calls with arguments and results, subagent status updates, file operation artifacts, and human-in-the-loop interrupts. Every agent decision is fully traceable in the UI.

PostgreSQL backs LangGraph checkpointing, conversation history, and user data (watchlists, portfolios, preferences), so agent state and user context persist across sessions. Redis buffers SSE events so that browser refreshes and network drops do not lose in-flight messages: the client reconnects and replays automatically. The server also handles synchronization between local data and sandbox data, keeping MCP, skills, and user context in sync. See the full [API reference](docs/api/README.md) for details.

## Getting Started

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager
- Docker (for PostgreSQL and Redis)
- Node.js 18+ (optional, for the web UI)

### 1. Clone and install

```bash
git clone https://github.com/ginlix-ai/langalpha.git
cd langalpha

# Install Python dependencies
uv sync

# Optional: install browser dependencies for web crawling
source .venv/bin/activate
crawl4ai-setup
```

### 2. Configure environment

Copy `.env.example` to `.env` and fill in your keys:

```bash
cp .env.example .env
```

**Required:**

| Variable | Purpose |
|----------|---------|
| `GEMINI_API_KEY` | Gemini API key |
| `DAYTONA_API_KEY` | Cloud sandbox access ([daytona.io](https://www.daytona.io/)) |
| `FMP_API_KEY` | Financial data ([financialmodelingprep.com](https://site.financialmodelingprep.com/)) |

**Database** (defaults work with `make setup-db`):

| Variable | Default |
|----------|---------|
| `DB_HOST` | `localhost` |
| `DB_PORT` | `5432` |
| `DB_USER` | `ptc_admin` |
| `REDIS_URL` | `redis://localhost:6379/0` |

**Optional:**  `SERPER_API_KEY`, `TAVILY_API_KEY`, `LANGSMITH_API_KEY`

### 3. Start infrastructure

```bash
make setup-db
```

This starts PostgreSQL and Redis in Docker and initializes the database tables.

### 4. Run the backend

```bash
uv run server.py
```

API available at **http://localhost:8000** (interactive docs at `/docs`).

### 5. Run the frontend (optional)

```bash
cd web && npm install && npm run dev
```

Open **http://localhost:5173** for the full workspace UI: Chat Agent, Dashboard, and Trading Center.

### 6. Or use the CLI

```bash
ptc-agent              # interactive session
ptc-agent --plan-mode  # with plan approval
```

## Documentation

- **[API Reference](docs/api/README.md)** with endpoints for chat streaming, workspaces, workflow state, and more
- **Interactive API docs** at `http://localhost:8000/docs` when the server is running

## License

Apache License 2.0
