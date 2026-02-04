# PTC Agent

A Plan-Think-Code AI agent system with workspace-based sandbox execution.

## Quick Start

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager
- Docker (for PostgreSQL and Redis)
- Node.js (for MCP servers)

### 1. Setup Environment

```bash
# Clone and enter the project
git clone https://github.com/ginlix-ai/stealth-agent.git
cd stealth-agent

# Create virtual environment and install dependencies
uv sync

# Install browser dependencies for web crawling
crawl4ai-setup

# Activate the virtual environment
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

### 2. Configure Environment Variables

Create a `.env` file with the required keys:

```bash
# LLM Provider (choose one or more)
ANTHROPIC_API_KEY=your-key
OPENAI_API_KEY=your-key

# Daytona Sandbox (required)
DAYTONA_API_KEY=your-key

# Database type (required for persistence)
DB_TYPE=postgres
MEMORY_DB_TYPE=postgres

# PostgreSQL connection (optional - defaults work with make setup-db)
# DB_HOST=localhost
# DB_PORT=5432
# DB_NAME=postgres
# DB_USER=postgres
# DB_PASSWORD=postgres

# Redis (optional - defaults work with make setup-db)
# REDIS_URL=redis://localhost:6379/0
```

### 3. Start Database Services

Start PostgreSQL and Redis containers:

```bash
make setup-db
```

### 4. Run the Server

```bash
uv run server.py
```

The API server will be available at `http://localhost:8000`.

### 5. Use the Agent

**Option A: Interactive CLI**

```bash
ptc-agent
```

**Option B: API Requests**

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

See the [API Reference](docs/api/README.md) for complete API documentation:

- [Chat API](docs/api/chat.md) - Streaming chat with SSE
- [Workspaces API](docs/api/workspaces.md) - Workspace CRUD and thread management
- [Workflow API](docs/api/workflow.md) - Workflow state and checkpoints
- [Data Models](docs/api/models.md) - Request/response schemas
- [Cache API](docs/api/cache.md) - Cache management

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
│   ├── yfinance_mcp_server.py
│   └── tickertick_mcp_server.py
│
├── scripts/              # Setup and utility scripts
├── docs/                 # Documentation
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
