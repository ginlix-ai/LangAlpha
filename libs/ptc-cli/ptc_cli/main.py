"""Main entry point and CLI loop for ptc-cli (API mode).

This module provides the command-line interface for the PTC Agent via server API, including:
- Command-line argument parsing
- Server connection and workspace management
- Interactive CLI loop with prompt handling
- Dependency checking and logging setup
"""

import argparse
import asyncio
import importlib.util
import os
import sys
import time
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog

logger = structlog.get_logger(__name__)

if TYPE_CHECKING:
    from ptc_cli.api.client import SSEStreamClient
    from ptc_cli.core.state import SessionState


def _cleanup_old_logs(log_dir: Path, *, keep_days: int = 7) -> None:
    """Remove log files older than keep_days."""
    cutoff = time.time() - (keep_days * 24 * 60 * 60)
    try:
        for path in log_dir.glob("*.log"):
            try:
                if path.stat().st_mtime < cutoff:
                    path.unlink(missing_ok=True)
            except OSError as e:
                logger.debug("cli_log_cleanup_file_failed", path=str(path), error=str(e))
                continue
    except OSError as e:
        logger.debug("cli_log_cleanup_failed", log_dir=str(log_dir), error=str(e))


def setup_logging(*, agent_name: str) -> Path:
    """Redirect logging to a per-session log file.

    Returns:
        Path to the session log file.
    """
    import logging.handlers

    # Create log directory
    log_dir = Path.home() / ".ptc-agent" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    _cleanup_old_logs(log_dir, keep_days=7)

    # One log per session
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    safe_agent = "".join(c for c in agent_name if c.isalnum() or c in ("-", "_")) or "agent"
    log_file = log_dir / f"ptc-cli-{safe_agent}-{timestamp}-{os.getpid()}.log"
    os.environ["PTC_CLI_LOG_FILE"] = str(log_file)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))

    # Remove all existing handlers from root logger
    root = logging.getLogger()
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    # Add file handler only
    root.addHandler(file_handler)
    root.setLevel(logging.INFO)

    # Suppress specific noisy loggers
    for logger_name in ["httpx", "httpcore", "urllib3", "asyncio", "anyio"]:
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    # Configure structlog
    try:
        import structlog

        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
            ],
            wrapper_class=structlog.stdlib.BoundLogger,
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )
    except ImportError:
        pass

    return log_file


def check_cli_dependencies() -> None:
    """Check if CLI dependencies are installed."""
    missing = []

    if importlib.util.find_spec("rich") is None:
        missing.append("rich")
    if importlib.util.find_spec("prompt_toolkit") is None:
        missing.append("prompt-toolkit")
    if importlib.util.find_spec("dotenv") is None:
        missing.append("python-dotenv")
    if importlib.util.find_spec("httpx") is None:
        missing.append("httpx")

    if missing:
        print("\nMissing required CLI dependencies!")
        print("\nThe following packages are required:")
        for pkg in missing:
            print(f"  - {pkg}")
        print("\nPlease install them with:")
        print("  pip install ptc-cli")
        sys.exit(1)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="PTC Agent CLI - Connects to PTC Agent server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Help command
    subparsers.add_parser("help", help="Show help information")

    # Default interactive mode
    parser.add_argument(
        "--agent",
        default=os.getenv("AUTH_USER_ID", "agent"),
        help="Agent identifier for session storage (default: $AUTH_USER_ID or 'agent').",
    )
    parser.add_argument(
        "--server",
        default=os.environ.get("PTC_SERVER_URL", "http://localhost:8000"),
        help="PTC Agent server URL (default: http://localhost:8000)",
    )
    parser.add_argument(
        "--workspace",
        help="Workspace ID to use (creates new if not provided)",
    )
    parser.add_argument(
        "--auto-approve",
        action="store_true",
        help="Auto-approve tool usage without prompting (disables human-in-the-loop)",
    )
    parser.add_argument(
        "--no-splash",
        action="store_true",
        help="Disable the startup splash screen",
    )
    parser.add_argument(
        "--new-workspace",
        action="store_true",
        help="Create new workspace (don't reuse existing session)",
    )
    parser.add_argument(
        "--plan-mode",
        action="store_true",
        help="Enable plan mode: agent must submit a plan for approval before execution",
    )
    parser.add_argument(
        "--reconnect",
        nargs="?",
        const=True,
        metavar="THREAD_ID",
        help="Reconnect to a previous workflow (auto-selects latest if no THREAD_ID)",
    )
    parser.add_argument(
        "--list-sessions",
        action="store_true",
        help="List available reconnection sessions and exit",
    )
    parser.add_argument(
        "--model",
        help="LLM model name from models.json (e.g., 'minimax-m2.1', 'claude-sonnet-4-5')",
    )
    parser.add_argument(
        "--flash",
        action="store_true",
        help="Use Flash Agent: fast responses without sandbox (no code execution)",
    )

    return parser.parse_args()


async def chat_loop(
    client: "SSEStreamClient",
    workspace_id: str | None,
    assistant_id: str | None,
    session_state: "SessionState",
    *,
    no_splash: bool = False,
) -> None:
    """Main CLI loop.

    Args:
        client: SSE stream client for API communication
        workspace_id: Active workspace ID (None in flash mode)
        assistant_id: Agent identifier for session storage
        session_state: Session state with settings
        no_splash: If True, skip displaying the startup splash screen
    """
    from ptc_cli.commands import handle_command
    from ptc_cli.core import COLORS, PTC_AGENT_ASCII, console
    from ptc_cli.display import TokenTracker
    from ptc_cli.input import create_prompt_session
    from ptc_cli.streaming import execute_task

    if not no_splash:
        console.print(PTC_AGENT_ASCII, style=f"bold {COLORS['primary']}")
        console.print()

    # Display mode-specific info
    if session_state.flash_mode:
        console.print("[bold cyan]Flash Mode[/bold cyan] [dim](no sandbox, external tools only)[/dim]")
    else:
        console.print(f"[yellow]Workspace: {workspace_id}[/yellow]")
    console.print(f"[dim]Server: {client.base_url}[/dim]")
    console.print()

    if session_state.llm_model:
        console.print(f"  [cyan]Model: {session_state.llm_model}[/cyan]")
        console.print()

    if session_state.plan_mode:
        console.print("  [cyan]Plan Mode: ON[/cyan] [dim](agent will submit plan for approval)[/dim]")
        console.print()

    if session_state.auto_approve:
        console.print("  [yellow]Auto-approve: ON[/yellow] [dim](tools run without confirmation)[/dim]")
        console.print()

    # Tips
    if sys.platform == "darwin":
        tips = "  Tips: Enter to submit, Option+Enter for newline, Ctrl+C to interrupt"
    else:
        tips = "  Tips: Enter to submit, Alt+Enter for newline, Ctrl+C to interrupt"
    console.print(tips, style=f"dim {COLORS['dim']}")
    console.print()

    # Create prompt session and token tracker
    from ptc_cli.input.completers import SandboxFileCompleter

    sandbox_completer = None
    files: list[str] = []

    # Skip file completer setup in flash mode (no sandbox)
    if not session_state.flash_mode:
        sandbox_completer = SandboxFileCompleter()
        try:
            files = await client.list_workspace_files(include_system=False)
            sandbox_completer.set_files(files)
        except Exception:
            # Non-fatal: autocomplete will populate after first /files or file ops.
            pass

    # Store for streaming updates (artifact events) and /files refresh.
    session_state.sandbox_completer = sandbox_completer
    session_state.sandbox_files = files

    prompt_session = create_prompt_session(assistant_id, session_state, sandbox_completer, {})
    token_tracker = TokenTracker()

    logger.info(
        "cli_loop_start",
        assistant_id=assistant_id,
        workspace_id=workspace_id,
        server_url=client.base_url,
    )

    while True:
        try:
            user_input = await prompt_session.prompt_async()
            if session_state.exit_hint_handle:
                session_state.exit_hint_handle.cancel()
                session_state.exit_hint_handle = None
            session_state.exit_hint_until = None
            session_state.exit_requested = False
            user_input = user_input.strip()

        except EOFError:
            session_state.last_exit_reason = "prompt_eoferror"
            if not sys.stdin.isatty():
                break
            prompt_session = create_prompt_session(assistant_id, session_state, sandbox_completer, {})
            continue
        except KeyboardInterrupt:
            if getattr(session_state, "exit_requested", False):
                session_state.last_exit_reason = "ctrlc_triple_exit"
                console.print("\nGoodbye!", style=COLORS["primary"])
                break
            session_state.ctrl_c_count = 0
            session_state.exit_hint_until = None
            session_state.exit_requested = False
            if session_state.exit_hint_handle:
                session_state.exit_hint_handle.cancel()
                session_state.exit_hint_handle = None
            continue

        if not user_input:
            continue

        # Check for slash commands
        if user_input.startswith("/"):
            result = await handle_command(
                user_input,
                client,
                token_tracker,
                session_state,
            )
            if result == "exit":
                console.print("\nGoodbye!", style=COLORS["primary"])
                break
            if result:
                continue

        # Handle quit keywords
        if user_input.lower() in ["quit", "exit", "q"]:
            console.print("\nGoodbye!", style=COLORS["primary"])
            break

        try:
            await execute_task(
                user_input,
                client,
                assistant_id,
                session_state,
                token_tracker,
            )
        except Exception as e:
            console.print()
            console.print(f"[red]Error: {e}[/red]")
            console.print()
            continue

    logger.info("cli_loop_end", assistant_id=assistant_id)


async def main(
    assistant_id: str,
    session_state: "SessionState",
    server_url: str,
    workspace_id: str | None = None,
) -> None:
    """Main entry point with session initialization.

    Args:
        assistant_id: Agent identifier for session storage
        session_state: Session state with settings
        server_url: PTC Agent server URL
        workspace_id: Optional workspace ID to use
    """
    from rich.progress import Progress, SpinnerColumn, TextColumn

    from ptc_cli.api.client import SSEStreamClient
    from ptc_cli.agent import create_api_session
    from ptc_cli.core import console

    client = None

    try:
        console.print()

        # Flash mode: skip workspace creation entirely
        if session_state.flash_mode:
            console.print("[bold cyan]Starting Flash Mode...[/bold cyan]")
            client = SSEStreamClient(base_url=server_url, user_id=assistant_id)
            await chat_loop(
                client,
                None,  # No workspace in flash mode
                assistant_id,
                session_state,
                no_splash=session_state.no_splash,
            )
        else:
            # Full mode with workspace creation
            with Progress(
                SpinnerColumn(spinner_name="dots"),
                TextColumn("[progress.description]{task.description}"),
                console=console,
                transient=True,
            ) as progress:
                task = progress.add_task("Connecting to server...", total=None)

                def update_step(step: str) -> None:
                    progress.update(task, description=step)

                client, workspace_id, reusing = await create_api_session(
                    agent_name=assistant_id,
                    server_url=server_url,
                    workspace_id=workspace_id,
                    persist_session=session_state.persist_session,
                    on_progress=update_step,
                )
                session_state.reusing_workspace = reusing

            if reusing:
                console.print("[green]Reconnected to workspace[/green]")
            else:
                console.print("[green]Workspace created[/green]")
            console.print()

            await chat_loop(
                client,
                workspace_id,
                assistant_id,
                session_state,
                no_splash=session_state.no_splash,
            )

    except asyncio.CancelledError as e:
        raise KeyboardInterrupt from e
    except KeyboardInterrupt:
        console.print("\n\n[yellow]Interrupted[/yellow]")
        sys.exit(0)
    except Exception as e:  # noqa: BLE001
        console.print(f"\n[bold red]Error:[/bold red] {e}\n")
        console.print_exception()
        sys.exit(1)
    finally:
        if client:
            with suppress(Exception):
                await client.close()
            console.print("[dim]Disconnected from server[/dim]")


async def run_reconnect_mode(
    thread_id: str,
    last_event_id: int,
    server_url: str,
    workspace_id: str | None = None,
) -> None:
    """Run in reconnect mode - reconnect to a running/completed workflow.

    Args:
        thread_id: Thread ID to reconnect to
        last_event_id: Last received event ID for deduplication
        server_url: PTC Agent server URL
        workspace_id: Optional workspace ID
    """
    from ptc_cli.api.client import SSEStreamClient
    from ptc_cli.core import SessionState, console
    from ptc_cli.streaming.executor import reconnect_to_workflow

    session_state = SessionState()
    session_state.thread_id = thread_id

    async with SSEStreamClient(base_url=server_url) as client:
        client.thread_id = thread_id
        client.last_event_id = last_event_id
        if workspace_id:
            client.workspace_id = workspace_id

        try:
            await reconnect_to_workflow(client, session_state)
        except Exception as e:
            console.print(f"\n[red]Reconnection failed: {e}[/red]")


def run_cli() -> None:
    """Entry point for console script."""
    # Check dependencies first
    check_cli_dependencies()

    # Parse args
    args = parse_args()

    # Setup logging
    log_path = setup_logging(agent_name=args.agent)

    # Import after dependency check
    from ptc_cli.core import ReconnectStateManager, SessionState, console
    from ptc_cli.display import show_help

    # Handle --list-sessions
    if args.list_sessions:
        state_manager = ReconnectStateManager()
        sessions = state_manager.list_sessions()
        if not sessions:
            console.print("[yellow]No reconnection sessions available[/yellow]")
            console.print("[dim]Sessions are saved during workflow streaming[/dim]")
        else:
            console.print(f"[bold cyan]Available Sessions ({len(sessions)})[/bold cyan]\n")
            for idx, session in enumerate(sessions, 1):
                thread_id = session["thread_id"]
                last_event_id = session["last_event_id"]
                timestamp = session["timestamp"]
                metadata = session.get("metadata", {})
                query = metadata.get("query", "")

                console.print(f"[bold green]{idx}. Thread:[/bold green] {thread_id[:16]}...")
                console.print(f"   [dim]Last Event ID:[/dim] {last_event_id}")
                console.print(f"   [dim]Time:[/dim] {timestamp[:19]}")
                if query:
                    console.print(f"   [dim]Query:[/dim] {query}")
                console.print()

            console.print("[dim cyan]To reconnect: ptc-agent --reconnect [THREAD_ID][/dim cyan]")
        sys.exit(0)

    # Handle --reconnect
    if args.reconnect:
        state_manager = ReconnectStateManager()

        # Determine thread_id
        if isinstance(args.reconnect, str):
            thread_id = args.reconnect
        else:
            thread_id = state_manager.get_latest_thread_id()
            if not thread_id:
                console.print("[red]No saved sessions found to reconnect to.[/red]")
                console.print("[dim]Use --list-sessions to see available sessions[/dim]")
                sys.exit(1)
            console.print(f"[dim cyan]Auto-selected latest session: {thread_id[:16]}...[/dim cyan]")

        # Load saved state
        saved_state = state_manager.load_state(thread_id)
        last_event_id = saved_state["last_event_id"] if saved_state else 0
        metadata = saved_state.get("metadata", {}) if saved_state else {}

        console.print(f"\n[bold cyan]Reconnecting to Workflow[/bold cyan]")
        console.print(f"[dim]Thread ID: {thread_id}[/dim]")
        console.print(f"[dim]Last Event ID: {last_event_id}[/dim]")
        if query := metadata.get("query"):
            console.print(f"[dim]Query: {query}[/dim]")
        console.print()

        asyncio.run(
            run_reconnect_mode(
                thread_id=thread_id,
                last_event_id=last_event_id,
                server_url=args.server,
                workspace_id=metadata.get("workspace_id"),
            )
        )
        sys.exit(0)

    try:
        if args.command == "help":
            show_help()
        else:
            # Create session state from args
            session_state = SessionState(
                auto_approve=args.auto_approve,
                no_splash=args.no_splash,
                persist_session=not args.new_workspace and not args.flash,
                plan_mode=args.plan_mode,
                llm_model=args.model,
                flash_mode=args.flash,
            )
            session_state.log_file_path = str(log_path)

            asyncio.run(
                main(
                    args.agent,
                    session_state,
                    args.server,
                    args.workspace,
                )
            )
    except KeyboardInterrupt:
        console.print("\n\n[yellow]Interrupted[/yellow]")
        sys.exit(0)


if __name__ == "__main__":
    run_cli()
