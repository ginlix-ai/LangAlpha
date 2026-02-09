# LangAlpha

**LangAlpha** is a Plan-Think-Code (PTC) AI agent dedicated to finance analysis: workspace-based sandbox execution, optional web UI (chat, dashboard, trading), and MCP integrations. Run locally or deploy with Docker.

## Quick Start

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager
- Docker (for PostgreSQL and Redis)
- Node.js 16+ (optional, for web UI and MCP servers)

### 1. Setup Environment

```bash
# Clone and enter the project
git clone <your-repo-url>
cd LangAlpha

# Create virtual environment and install dependencies
uv sync

# Optional: browser dependencies for web crawling
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
crawl4ai-setup
```

### 2. Configure Environment Variables

Copy `.env.example` to `.env` and set at least:

- **LLM:** `ANTHROPIC_API_KEY` and/or `OPENAI_API_KEY`
- **Sandbox:** `DAYTONA_API_KEY`
- **Database:** `DB_TYPE=postgres`, `MEMORY_DB_TYPE=postgres`, and `DB_*` / `REDIS_URL` (defaults work with `make setup-db`)

### 3. Start Database Services

```bash
make setup-db
```

Starts PostgreSQL and Redis in Docker and sets up DB tables.

### 4. Run the Backend

```bash
uv run server.py
```

API: **http://localhost:8000** (docs: http://localhost:8000/docs)

### 5. Use the Agent

**Option A: Interactive CLI**

```bash
ptc-agent
```

**Option B: Web UI**

```bash
cd web && npm install && npm run dev
```

Open **http://localhost:5173** (Chat Agent, Dashboard, Trading Center).

**Option C: API Requests**

```bash
# Create a workspace
curl -X POST "http://localhost:8000/api/v1/workspaces" \
  -H "Content-Type: application/json" \
  -H "X-User-Id: user-123" \
  -d '{"name": "My Project"}'

# Start a chat session
curl -N -X POST "http://localhost:8000/api/v1/chat/stream" \
  -H "Content-Type: application/json" \
  -d '{
    "workspace_id": "<workspace_id_from_above>",
    "user_id": "user-123",
    "messages": [{"role": "user", "content": "Hello, create a Python script"}]
  }'
```

## Documentation

- **[Local Deployment Guide](docs/LOCAL_DEPLOYMENT.md)** – Full local setup, Docker Compose, troubleshooting
- **[API Reference](docs/api/README.md)** – API documentation:
  - [Chat API](docs/api/chat.md) – Streaming chat with SSE
  - [Workspaces API](docs/api/workspaces.md) – Workspace CRUD and thread management
  - [Workflow API](docs/api/workflow.md) – Workflow state and checkpoints
  - [Data Models](docs/api/models.md) – Request/response schemas
  - [Cache API](docs/api/cache.md) – Cache management

## Project Structure

```
├── src/
│   ├── config/           # Configuration management
│   ├── llms/             # LLM providers and utilities
│   ├── ptc_agent/        # Core PTC agent implementation
│   │   ├── agent/        # Agent graph, tools, middleware
│   │   └── core/         # Sandbox, MCP, session management
│   ├── server/           # FastAPI server
│   │   ├── app/          # API routes (chat, workspaces, workflow)
│   │   ├── handlers/     # Streaming and event handlers
│   │   ├── models/       # Pydantic request/response models
│   │   ├── services/     # Business logic services
│   │   └── database/     # Database connections
│   ├── tools/            # Tool implementations
│   └── utils/            # Shared utilities
│
├── libs/
│   └── ptc-cli/          # Interactive CLI application
│
├── mcp_servers/          # MCP server implementations
│   ├── price_data_mcp_server.py
│   └── tickertick_mcp_server.py
│
├── web/                  # React frontend (Vite, Ant Design)
├── scripts/              # Setup and utility scripts
├── docs/                 # Documentation
│   ├── LOCAL_DEPLOYMENT.md
│   └── api/              # API reference documentation
│
├── server.py             # Server entrypoint
├── config.yaml           # Infrastructure configuration
├── agent_config.yaml     # Agent and tool configuration
└── Makefile              # Build commands
```

## Configuration

- **config.yaml** - Infrastructure configuration (Redis, background tasks, logging, CORS)
- **agent_config.yaml** - Agent configuration (LLM selection, MCP servers, tools, security)

## License

Apache License 2.0
