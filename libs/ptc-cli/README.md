# PTC CLI

Interactive command-line interface for PTC Agent.

![PTC CLI](../../docs/assets/ptc_agent.png)

## Features

- Interactive AI agent sessions with sandboxed code execution
- Multi-agent support with isolated memory
- Session persistence (reuse sandboxes across runs)
- File operations: view, copy, download from sandbox
- Auto-completion for commands and file paths
- Token usage tracking

## Prerequisites

- Python 3.13+
- Node.js (for MCP servers)
- Daytona account with API key

## Installation

From the repository root:

```bash
uv sync
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

## Quick Start

1. Set your Daytona API key in `.env`:
   ```
   DAYTONA_API_KEY=your_api_key_here
   ```

2. Run the CLI:
   ```bash
   ptc-agent
   ```

3. Enter your task and start interacting with the agent.

## Configuration
![Model Selection](../../docs/assets/model_selection.png)

### Environment Variables

| Variable | Description |
|----------|-------------|
| `DAYTONA_API_KEY` | Required. Daytona API key for sandbox access |
| `ANTHROPIC_API_KEY` | API key for Anthropic models |
| `OPENAI_API_KEY` | API key for OpenAI models |
| `TAVILY_API_KEY` | API key for Tavily web search |

### Configuration Files

| File | Purpose |
|------|---------|
| `agent_config.yaml` | Your own MCP servers, Daytona settings, filesystem config |
| `plugins/` | The MCP servers and skills that ship with the app, one Agent Plugins package each |
| `llms.json` | LLM provider definitions |
| `.env` | API keys and credentials |

### Theme Configuration

The CLI supports customizable color themes via environment variables or config file.

**Environment Variables:**

| Variable | Description |
|----------|-------------|
| `PTC_THEME` | Theme mode: `auto` (default), `dark`, or `light` |
| `PTC_PALETTE` | Color palette (see available palettes below) |
| `NO_COLOR` | Set to any value to disable colors |

**Available Palettes:**
- Basic: `emerald`, `cyan`, `amber`, `teal`
- Terminal themes: `nord` (dark default), `gruvbox`, `catppuccin`, `tokyo_night` (light default)

**Config File (agent_config.yaml):**

```yaml
cli:
  theme: "auto"      # auto, dark, or light
  palette: "nord"    # color palette name
```

**Examples:**

```bash
# Use catppuccin palette with dark theme
PTC_PALETTE=catppuccin PTC_THEME=dark ptc-agent

# Disable colors for accessibility
NO_COLOR=1 ptc-agent
```

## CLI Commands

### Main Command

```bash
ptc-agent [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--agent NAME` | Use named agent with separate memory (default: "agent") |
| `--plan-mode` | Enable plan mode: agent submits plan for approval before execution |
| `--sandbox-id ID` | Reuse existing Daytona sandbox (skips creation) |
| `--no-splash` | Disable the startup ASCII art banner |
| `--new-sandbox` | Create new sandbox (don't reuse existing session) |

### Subcommands

```bash
# List all available agents
ptc-agent list

# Show help information
ptc-agent help

# Reset agent to default prompt
ptc-agent reset --agent NAME

# Copy memory from another agent
ptc-agent reset --agent NAME --target SOURCE_AGENT
```

## Interactive Commands

During a session, use these slash commands:

| Command | Description |
|---------|-------------|
| `/help` | Show help panel |
| `/clear` | Clear screen, start new conversation, and clear sandbox files (data/, results/, code/) |
| `/tokens` | Display token usage statistics |
| `/files [all]` | List sandbox files (use `all` to include internal dirs) |
| `/view <path>` | View file with syntax highlighting |
| `/copy <path>` | Copy file content to clipboard |
| `/download <path> [local]` | Download file from sandbox to local machine |
| `/model` | Switch LLM model (only available at session start) |
| `/exit`, `/q` | Exit the CLI |

## Special Input Syntax

| Syntax | Description |
|--------|-------------|
| `!command` | Run bash command in sandbox |
| `@path/to/file` | Include sandbox file content in your prompt |
| `quit`, `exit`, `q` | Exit the CLI |

**Note**: The `@` syntax reads files from the Daytona sandbox, not your local filesystem. Auto-completion for sandbox files is available after your first task completes.

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `Enter` | Submit input |
| `Esc+Enter` / `Alt+Enter` | Insert newline (multiline input) |
| `Ctrl+E` | Open input in external editor |
| `Shift+Tab` | Toggle plan mode |
| `Ctrl+C` | Clear input / Interrupt streaming / Exit (triple press) |

## Session Persistence

Sessions are cached in `~/.ptc-agent/{agent}/session.json` for faster startup:

- Automatically reconnects to existing Daytona sandboxes
- Auto-invalidates when configuration changes
- Sessions expire after 24 hours of inactivity
- Disable with `--new-sandbox` flag

## Plan Mode

When plan mode is enabled, the agent submits a plan for approval before executing:

- Enable via `--plan-mode` flag or toggle with `Shift+Tab` during a session
- Plans are displayed in a panel for review
- Accept or reject with arrow key navigation
- Rejecting allows optional feedback
- Status shown in bottom toolbar
