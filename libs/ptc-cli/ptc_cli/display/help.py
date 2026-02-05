"""Help display utilities for the CLI."""

from rich.panel import Panel

from ptc_cli.core import COLORS, console


def show_help() -> None:
    """Show help information."""
    help_text = """
[bold]PTC Agent CLI[/bold] - Programmatic Tool Calling AI Assistant

[bold]Usage:[/bold]
  ptc-agent                     Start interactive session
  ptc-agent --flash             Flash mode (no sandbox, external tools only)
  ptc-agent --agent NAME        Use named agent with separate memory
  ptc-agent --model NAME        Use specific LLM model
  ptc-agent --plan-mode         Enable plan mode (agent submits plan first)
  ptc-agent --reconnect         Reconnect to latest workflow
  ptc-agent --reconnect ID      Reconnect to specific workflow
  ptc-agent --list-sessions     List available reconnection sessions

[bold]Interactive Commands:[/bold]
  /help                         Show this help
  /new                          Start a new conversation thread
  /tokens                       Show token usage
  /model                        Interactive model selection (arrow keys)
  /model <name>                 Switch to model by name (e.g., /model gpt-5.2)
  /status                       Show workflow and background task status
  /cancel                       Cancel running workflow
  /summarize [keep=N]           Summarize conversation (keep N recent messages)
  /workspace                    List/switch/start/stop workspaces
  /conversation                 List/open past conversations
  /reconnect                    Reconnect to running workflow after ESC
  /onboarding                   Start user profile onboarding flow
  /files [all]                  List files (all=include system dirs)
  /refresh                      Refresh tools + skills in sandbox
  /view <path>                  View file content (supports images)
  /copy <path>                  Copy file content to clipboard
  /download <path> [local]      Download file from sandbox
  /exit, /q                     Exit the CLI

[bold]Special Input:[/bold]
  !command                      Run local bash command
  @path/to/file                 Include file content in prompt

[bold]Keyboard Shortcuts:[/bold]
  Enter                         Submit input
  Esc+Enter / Alt+Enter         Insert newline
  Ctrl+E                        Open in external editor
  Shift+Tab                     Toggle plan mode
  Esc (during streaming)        Soft-interrupt (background tasks continue)
  Ctrl+C                        Clear input / Interrupt / Exit (x3)

[bold]Configuration:[/bold]
  agent_config.yaml               Main configuration file
  src/llms/manifest/models.json LLM model definitions
  .env                          API keys and credentials

[bold]Memory Files:[/bold] [dim](not yet active - agent runs in sandbox)[/dim]
  ~/.ptc-agent/{agent}/agent.md User-level agent memory
  .ptc-agent/agent.md           Project-level agent memory
"""

    console.print(Panel(help_text.strip(), title="Help", border_style=COLORS["primary"]))
    console.print()
