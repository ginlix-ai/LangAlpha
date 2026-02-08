"""Slash command handlers for the CLI (API mode)."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import httpx

from ptc_cli.core import console
from ptc_cli.display import show_help
from ptc_cli.streaming.executor import execute_task, reconnect_to_workflow, replay_conversation
from ptc_cli.utils.http_helpers import handle_http_error
from ptc_cli.utils.menu import create_interactive_menu


async def _select_or_create_workspace_interactive(
    client: "SSEStreamClient",
) -> str | None:
    """Select an existing workspace or create a new one."""
    workspaces = await client.list_workspaces()

    options: list[tuple[str, dict[str, Any]]] = [("Create a new workspace", {"action": "create"})]
    for ws in workspaces:
        workspace_id = str(ws.get("workspace_id", ""))
        name = str(ws.get("name", "(unnamed)"))
        status = str(ws.get("status", ""))
        options.append(
            (
                f"Use existing: {name} ({workspace_id[:12]}) [{status}]",
                {"action": "use", "workspace_id": workspace_id},
            )
        )

    result = await create_interactive_menu(
        options,
        title="Select a workspace (Up/Down, Enter):",
    )
    if result is None:
        return None

    _index, picked = result
    if picked.get("action") == "use":
        return str(picked.get("workspace_id"))

    name_default = f"cli-{time.strftime('%Y%m%d-%H%M%S')}"
    name = console.input(f"Workspace name [dim]({name_default})[/dim]: ").strip() or name_default
    console.print("[dim]Creating workspace (can take ~60s)...[/dim]")
    ws = await client.create_workspace(name=name)
    return ws.get("workspace_id")


async def _ensure_workspace_running(client: "SSEStreamClient", workspace_id: str) -> bool:
    try:
        workspace = await client.get_workspace(workspace_id)
        if not workspace:
            return False
        status = workspace.get("status")
        # "flash" workspaces have no sandbox - skip start
        if status not in ("running", "flash"):
            await client.start_workspace(workspace_id)
        return True
    except Exception:
        return False


async def _ensure_workspace_available(client: "SSEStreamClient") -> bool:
    """Check that client has a workspace and it's running.

    Returns:
        True if workspace is available, False otherwise (with error message printed)
    """
    if not client.workspace_id:
        console.print("[yellow]No active workspace[/yellow]")
        return False

    if not await _ensure_workspace_running(client, client.workspace_id):
        console.print(f"[red]Workspace not available: {client.workspace_id}[/red]")
        return False

    return True


def _normalize_path(path: str) -> str:
    """Normalize server/sandbox paths for CLI display.

    The backend returns virtual paths (e.g. "results/foo.txt") but we also
    accept absolute sandbox paths (e.g. "/home/daytona/results/foo.txt").
    """

    if not path:
        return path

    if path.startswith("/home/daytona/"):
        return path[len("/home/daytona/"):]

    if path.startswith("/") and not path.startswith("/tmp/"):
        return path.lstrip("/")

    return path


def _render_tree(files: list[str]) -> list[str]:
    """Render a simple directory tree from file paths."""

    # Normalize and sort for stable output.
    normalized = sorted({_normalize_path(p) for p in files if p})
    if not normalized:
        return []

    tree: dict[str, Any] = {}
    for file_path in normalized:
        parts = [p for p in file_path.split("/") if p]
        node = tree
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node.setdefault("__files__", []).append(parts[-1])

    lines: list[str] = []

    def walk(node: dict[str, Any], prefix: str = "") -> None:
        dirs = sorted([k for k in node.keys() if k != "__files__"])
        files_here = sorted(node.get("__files__", []))

        entries: list[tuple[str, str, Any]] = []
        for d in dirs:
            entries.append(("dir", d, node[d]))
        for f in files_here:
            entries.append(("file", f, None))

        for idx, (kind, name, child) in enumerate(entries):
            last = idx == len(entries) - 1
            connector = "└── " if last else "├── "
            lines.append(f"{prefix}{connector}{name}")
            if kind == "dir":
                extension = "    " if last else "│   "
                walk(child, prefix + extension)

    # Root prints entries without a top-level dot.
    walk(tree, prefix="")
    return lines


async def _handle_files_command(client: "SSEStreamClient", *, show_all: bool) -> list[str]:
    if not await _ensure_workspace_available(client):
        return []

    files = await client.list_workspace_files(include_system=show_all)
    if not files:
        console.print("[dim]No files found[/dim]")
        return []

    console.print("[cyan]Sandbox files:[/cyan]")
    for line in _render_tree(files):
        console.print(line)
    return files


async def _handle_view_command(client: "SSEStreamClient", path: str) -> None:
    if not path:
        console.print("[yellow]Usage:[/yellow] /view <path>")
        return

    if not await _ensure_workspace_available(client):
        return

    normalized = _normalize_path(path)

    async def _handle_directory_view(dir_path: str) -> None:
        try:
            files = await client.list_workspace_files(
                path=dir_path,
                include_system=True,
                pattern="*",
            )
        except httpx.HTTPStatusError as e:
            handle_http_error(e, console)
            return

        console.print(f"[cyan]Directory:[/cyan] {normalized}")
        if not files:
            console.print("[dim]No files found[/dim]")
            return

        for line in _render_tree(files):
            console.print(line)

    # If the user points at a directory, list its immediate children.
    is_directory_hint = path.endswith("/") or normalized.endswith("/") or path in (".", "./")
    if is_directory_hint:
        await _handle_directory_view(path)
        return

    # If it's a common binary type, download instead of printing.
    lower = normalized.lower()
    is_binary = any(lower.endswith(ext) for ext in (".png", ".jpg", ".jpeg", ".gif", ".webp", ".pdf"))

    if is_binary:
        try:
            content = await client.download_workspace_file(path=path)
        except httpx.HTTPStatusError as e:
            handle_http_error(e, console)
            return

        from pathlib import Path

        out_path = Path.cwd() / (normalized.split("/")[-1] or "download")
        out_path.write_bytes(content)
        console.print(f"[green]Downloaded:[/green] {out_path}")
        return

    try:
        data = await client.read_workspace_file(path=path)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            # Might be a directory; fall back to listing.
            await _handle_directory_view(f"{path}/")
            return
        handle_http_error(e, console)
        return

    content = str(data.get("content") or "")
    if not content:
        console.print("[yellow]File is empty[/yellow]")
        return

    # Rich syntax highlighting.
    from rich.syntax import Syntax
    from ptc_cli.core import get_syntax_theme

    file_name = normalized.split("/")[-1]
    syntax = Syntax(content, "text", theme=get_syntax_theme(), line_numbers=True)
    console.print(f"[cyan]{file_name}[/cyan]")
    console.print(syntax)


async def _handle_copy_command(client: "SSEStreamClient", path: str) -> None:
    if not path:
        console.print("[yellow]Usage:[/yellow] /copy <path>")
        return

    if not await _ensure_workspace_available(client):
        return

    try:
        data = await client.read_workspace_file(path=path)
    except httpx.HTTPStatusError as e:
        handle_http_error(e, console)
        return

    content = str(data.get("content") or "")
    if not content:
        console.print("[yellow]File not found or empty[/yellow]")
        return

    try:
        import pyperclip  # type: ignore
    except Exception:
        console.print("[yellow]Clipboard support requires 'pyperclip'[/yellow]")
        return

    pyperclip.copy(content)
    console.print("[green]Copied to clipboard[/green]")


async def _handle_download_command(client: "SSEStreamClient", remote_path: str, local_path: str | None) -> None:
    if not remote_path:
        console.print("[yellow]Usage:[/yellow] /download <path> [local]")
        return

    if not await _ensure_workspace_available(client):
        return

    try:
        content = await client.download_workspace_file(path=remote_path)
    except httpx.HTTPStatusError as e:
        handle_http_error(e, console)
        return

    from pathlib import Path

    default_name = _normalize_path(remote_path).split("/")[-1] or "download"
    out_path = Path(local_path) if local_path else (Path.cwd() / default_name)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(content)
    console.print(f"[green]Downloaded:[/green] {out_path}")


if TYPE_CHECKING:
    from ptc_cli.api.client import SSEStreamClient
    from ptc_cli.core.state import SessionState
    from ptc_cli.display.tokens import TokenTracker


async def handle_command(
    command: str,
    client: SSEStreamClient,
    token_tracker: TokenTracker,
    session_state: SessionState,
) -> str | None:
    """Handle slash commands.

    Args:
        command: The command string (e.g., "/help")
        client: SSE stream client for API communication
        token_tracker: Token tracker for usage display
        session_state: Session state for conversation management

    Returns:
        "exit" if should exit, "handled" if command was processed, None otherwise
    """
    cmd = command.strip()
    cmd_lower = cmd.lower()

    # Exit command
    if cmd_lower in ("/exit", "/q"):
        return "exit"

    if cmd_lower == "/help":
        show_help()

    elif cmd_lower in ("/new", "/clear"):
        # Guard sandbox command in flash mode
        if getattr(session_state, "flash_mode", False):
            # In flash mode, just reset the thread
            session_state.reset_thread()
            client.thread_id = session_state.thread_id
            console.print("[green]Started new conversation.[/green]")
            console.print(f"[dim]Thread: {client.thread_id}[/dim]")
            console.print()
            return "handled"

        if cmd_lower == "/clear":
            console.print("[dim]/clear is deprecated; use /new[/dim]")

        # Ensure we have a workspace first (workspace_id is required for chat)
        if not client.workspace_id:
            console.print()
            console.print("[yellow]No workspace selected.[/yellow]")

            workspace_id = await _select_or_create_workspace_interactive(client)
            if not workspace_id:
                console.print("[dim]Cancelled[/dim]")
                console.print()
                return "handled"

            client.workspace_id = workspace_id

            if not await _ensure_workspace_available(client):
                console.print()
                return "handled"

        # Start a fresh conversation thread
        session_state.reset_thread()
        client.thread_id = session_state.thread_id
        console.print("[green]Started new conversation.[/green]")
        console.print(f"[dim]Thread: {client.thread_id}[/dim]")
        console.print(f"[dim]Workspace: {client.workspace_id}[/dim]")
        console.print()

    elif cmd_lower == "/tokens":
        token_tracker.display()

    elif cmd_lower == "/model" or cmd_lower.startswith("/model "):
        from ptc_cli.input import select_model_interactive

        console.print()
        if cmd_lower == "/model":
            # Show interactive model selection
            current_model = getattr(session_state, "llm_model", None)
            selected = await select_model_interactive(current_model)
            if selected:
                session_state.llm_model = selected
                console.print(f"[green]Model set to:[/green] {selected}")
            else:
                console.print("[dim]Model selection cancelled[/dim]")
        else:
            # Direct model name input
            model_name = cmd[7:].strip()
            if model_name:
                session_state.llm_model = model_name
                console.print(f"[green]Model set to:[/green] {model_name}")
            else:
                console.print("[yellow]Please specify a model name[/yellow]")
        console.print()

    elif cmd_lower == "/refresh":
        # Guard sandbox command in flash mode
        if getattr(session_state, "flash_mode", False):
            console.print("[yellow]This command is not available in Flash mode (no sandbox)[/yellow]")
            console.print()
            return "handled"

        console.print()
        try:
            data = await client.refresh_workspace()
            console.print("[green]Refreshed sandbox[/green]")
            msg = data.get("message") if isinstance(data, dict) else None
            if msg:
                console.print(f"[dim]{msg}[/dim]")
        except httpx.HTTPStatusError as e:
            handle_http_error(e, console)
            console.print()
            return "handled"

        files = await _handle_files_command(client, show_all=False)
        session_state.sandbox_files = files
        completer = getattr(session_state, "sandbox_completer", None)
        if completer is not None and hasattr(completer, "set_files"):
            try:
                completer.set_files(files)
            except Exception:
                pass
        console.print()

    elif cmd_lower == "/files" or cmd_lower.startswith("/files "):
        # Guard sandbox command in flash mode
        if getattr(session_state, "flash_mode", False):
            console.print("[yellow]This command is not available in Flash mode (no sandbox)[/yellow]")
            console.print()
            return "handled"

        console.print()
        parts = cmd.split()
        show_all = len(parts) >= 2 and parts[1].lower() == "all"
        files = await _handle_files_command(client, show_all=show_all)
        session_state.sandbox_files = files
        completer = getattr(session_state, "sandbox_completer", None)
        if completer is not None and hasattr(completer, "set_files"):
            try:
                completer.set_files(files)
            except Exception:
                pass
        console.print()

    elif cmd_lower == "/view" or cmd_lower.startswith("/view "):
        # Guard sandbox command in flash mode
        if getattr(session_state, "flash_mode", False):
            console.print("[yellow]This command is not available in Flash mode (no sandbox)[/yellow]")
            console.print()
            return "handled"

        console.print()
        path = cmd[5:].strip() if cmd_lower.startswith("/view ") else ""
        await _handle_view_command(client, path)
        console.print()

    elif cmd_lower == "/copy" or cmd_lower.startswith("/copy "):
        # Guard sandbox command in flash mode
        if getattr(session_state, "flash_mode", False):
            console.print("[yellow]This command is not available in Flash mode (no sandbox)[/yellow]")
            console.print()
            return "handled"

        console.print()
        path = cmd[5:].strip() if cmd_lower.startswith("/copy ") else ""
        await _handle_copy_command(client, path)
        console.print()

    elif cmd_lower == "/download" or cmd_lower.startswith("/download "):
        # Guard sandbox command in flash mode
        if getattr(session_state, "flash_mode", False):
            console.print("[yellow]This command is not available in Flash mode (no sandbox)[/yellow]")
            console.print()
            return "handled"

        console.print()
        rest = cmd[9:].strip() if cmd_lower.startswith("/download") else ""
        parts = rest.split(maxsplit=1) if rest else []
        remote = parts[0] if parts else ""
        local = parts[1] if len(parts) > 1 else None
        await _handle_download_command(client, remote, local)
        console.print()

    elif cmd_lower == "/status":
        console.print()

        # Show workflow status if thread_id exists
        if client.thread_id:
            try:
                status = await client.get_workflow_status(client.thread_id)
                console.print(f"[cyan]Thread:[/cyan] {client.thread_id}")
                console.print(f"[cyan]Workflow Status:[/cyan] {status.get('status', 'unknown')}")

                # Show background task info
                active_subagents = status.get("active_subagents", [])
                completed_subagents = status.get("completed_subagents", [])

                if active_subagents:
                    console.print(f"[cyan]Running:[/cyan]")
                    for task in active_subagents:
                        console.print(f"  - {task}")

                if completed_subagents:
                    console.print(f"[cyan]Completed:[/cyan]")
                    for task in completed_subagents:
                        console.print(f"  - {task}")

                if status.get("soft_interrupted"):
                    console.print("[yellow]Workflow was soft-interrupted (subagents may still be running)[/yellow]")

            except Exception as e:
                console.print(f"[dim]Thread: {client.thread_id}[/dim]")
                console.print(f"[dim]Could not get workflow status: {e}[/dim]")
        else:
            console.print("[dim]No active workflow thread[/dim]")

        console.print()

        # Show workspace status if workspace_id exists
        if client.workspace_id:
            try:
                workspace = await client.get_workspace(client.workspace_id)
                if workspace:
                    console.print(f"[cyan]Workspace:[/cyan] {workspace.get('name', 'N/A')}")
                    console.print(f"[cyan]Workspace ID:[/cyan] {workspace.get('workspace_id', 'N/A')}")
                    console.print(f"[cyan]Workspace Status:[/cyan] {workspace.get('status', 'N/A')}")
                else:
                    console.print("[dim]Workspace not found[/dim]")
            except Exception as e:
                console.print(f"[dim]Could not get workspace status: {e}[/dim]")

        console.print(f"[cyan]Server:[/cyan] {client.base_url}")
        console.print()

    elif cmd_lower.startswith("/workspace") or cmd_lower == "/workspaces":
        # Guard sandbox command in flash mode
        if getattr(session_state, "flash_mode", False):
            console.print("[yellow]This command is not available in Flash mode (no sandbox)[/yellow]")
            console.print()
            return "handled"

        parts = cmd_lower.split()

        # Shortcut: /workspace stop (stop current workspace)
        if len(parts) >= 2 and parts[1] == "stop":
            if not client.workspace_id:
                console.print("[yellow]No active workspace[/yellow]")
                console.print()
                return "handled"
            try:
                await client.stop_workspace(client.workspace_id)
                console.print(f"[green]Workspace stopped:[/green] {client.workspace_id}")
                console.print()
            except Exception as e:
                console.print(f"[yellow]Could not stop workspace: {e}[/yellow]")
                console.print()
            return "handled"

        # Interactive workspace picker
        from prompt_toolkit.application import Application
        from prompt_toolkit.key_binding import KeyBindings
        from prompt_toolkit.layout import Layout
        from prompt_toolkit.layout.containers import Window
        from prompt_toolkit.layout.controls import FormattedTextControl

        console.print()

        workspaces: list[dict[str, Any]] = await client.list_workspaces()
        if not workspaces:
            console.print("[yellow]No workspaces found[/yellow]")
            console.print("[dim]Use /new to create one.[/dim]")
            console.print()
            return "handled"

        selected = [0]
        status_line = ["Enter = switch | s = start | x = stop | n = new | Ctrl+C = cancel"]

        def menu_text() -> str:
            lines = ["Workspaces:", status_line[0], ""]
            for idx, ws in enumerate(workspaces):
                workspace_id = str(ws.get("workspace_id", ""))
                name = str(ws.get("name", "(unnamed)"))
                status = str(ws.get("status", ""))
                prefix = ">" if idx == selected[0] else " "
                active = " *" if client.workspace_id == workspace_id else ""
                lines.append(
                    f" {prefix} {idx+1}. {name} ({workspace_id[:12]}) [{status}]{active}"
                )
            return "\n".join(lines)

        kb = KeyBindings()

        @kb.add("up")
        def _(_event: Any) -> None:
            selected[0] = max(0, selected[0] - 1)

        @kb.add("down")
        def _(_event: Any) -> None:
            selected[0] = min(len(workspaces) - 1, selected[0] + 1)

        @kb.add("enter")
        def _(event: Any) -> None:
            event.app.exit(result=("switch", selected[0]))

        @kb.add("s")
        def _(event: Any) -> None:
            event.app.exit(result=("start", selected[0]))

        @kb.add("x")
        def _(event: Any) -> None:
            event.app.exit(result=("stop", selected[0]))

        @kb.add("n")
        def _(event: Any) -> None:
            event.app.exit(result=("new", -1))

        @kb.add("c-c")
        def _(event: Any) -> None:
            event.app.exit(result=("cancel", -1))

        workspace_app: Application[tuple[str, int]] = Application(
            layout=Layout(Window(FormattedTextControl(menu_text))),
            key_bindings=kb,
            full_screen=False,
        )

        action, index = await workspace_app.run_async()
        if action == "cancel":
            console.print("[dim]Cancelled[/dim]")
            console.print()
            return "handled"

        if action == "new":
            workspace_id = await _select_or_create_workspace_interactive(client)
            if not workspace_id:
                console.print("[dim]Cancelled[/dim]")
                console.print()
                return "handled"
            client.workspace_id = workspace_id
            await _ensure_workspace_running(client, client.workspace_id)
            session_state.reset_thread()
            client.thread_id = session_state.thread_id
            console.print("[green]Switched to new workspace[/green]")
            console.print(f"[dim]Workspace: {client.workspace_id}[/dim]")
            console.print(f"[dim]Thread: {client.thread_id}[/dim]")
            console.print()
            return "handled"

        if index < 0 or index >= len(workspaces):
            console.print("[red]Invalid selection[/red]")
            console.print()
            return "handled"

        chosen = workspaces[index]
        workspace_id = str(chosen.get("workspace_id", ""))
        if not workspace_id:
            console.print("[red]Invalid workspace selection[/red]")
            console.print()
            return "handled"

        if action == "start":
            try:
                await client.start_workspace(workspace_id)
                console.print(f"[green]Workspace started:[/green] {workspace_id}")
            except Exception as e:
                console.print(f"[yellow]Could not start workspace: {e}[/yellow]")
            console.print()
            return "handled"

        if action == "stop":
            try:
                await client.stop_workspace(workspace_id)
                console.print(f"[green]Workspace stopped:[/green] {workspace_id}")
            except Exception as e:
                console.print(f"[yellow]Could not stop workspace: {e}[/yellow]")
            console.print()
            return "handled"

        # switch
        client.workspace_id = workspace_id
        await _ensure_workspace_running(client, client.workspace_id)
        session_state.reset_thread()
        client.thread_id = session_state.thread_id

        console.print("[green]Switched workspace.[/green]")
        console.print(f"[dim]Workspace: {client.workspace_id}[/dim]")
        console.print(f"[dim]Thread reset: {client.thread_id}[/dim]")
        console.print()
        return "handled"

    elif cmd_lower == "/cancel":
        # Cancel running workflow
        if client.thread_id:
            try:
                await client.cancel_workflow(client.thread_id)
                console.print("[green]Workflow cancelled[/green]")
            except Exception as e:
                console.print(f"[yellow]Could not cancel workflow: {e}[/yellow]")
        else:
            console.print("[yellow]No active workflow to cancel[/yellow]")

        # Refresh cached file list for autocomplete after stopping.
        try:
            files = await _handle_files_command(client, show_all=False)
            session_state.sandbox_files = files
            completer = getattr(session_state, "sandbox_completer", None)
            if completer is not None and hasattr(completer, "set_files"):
                try:
                    completer.set_files(files)
                except Exception:
                    pass
        except Exception:
            pass

    elif cmd_lower == "/summarize" or cmd_lower.startswith("/summarize "):
        # Manually trigger conversation summarization
        if not client.thread_id:
            console.print("[yellow]No active conversation to summarize[/yellow]")
            console.print()
            return "handled"

        # Parse optional keep_messages argument: /summarize keep=10
        keep_messages = 5  # default
        if cmd_lower.startswith("/summarize "):
            args = cmd[11:].strip()  # after "/summarize "
            if args.startswith("keep="):
                try:
                    keep_messages = int(args[5:])
                    if keep_messages < 1 or keep_messages > 20:
                        console.print("[yellow]keep must be between 1 and 20[/yellow]")
                        console.print()
                        return "handled"
                except ValueError:
                    console.print("[yellow]Invalid keep value. Usage: /summarize keep=5[/yellow]")
                    console.print()
                    return "handled"
            elif args:
                console.print("[yellow]Usage: /summarize [keep=N] (N between 1-20)[/yellow]")
                console.print()
                return "handled"

        console.print(f"[dim]Summarizing conversation (keeping {keep_messages} recent messages)...[/dim]")
        try:
            result = await client.summarize_thread(client.thread_id, keep_messages=keep_messages)
            if result.get("success"):
                orig = result.get("original_message_count", 0)
                new = result.get("new_message_count", 0)
                summary_len = result.get("summary_length", 0)
                console.print("[green]Summarization complete[/green]")
                console.print(f"[dim]Messages: {orig} → {new} (summary: {summary_len} chars)[/dim]")
            else:
                console.print("[yellow]Summarization did not complete successfully[/yellow]")
        except httpx.HTTPStatusError as e:
            from ptc_cli.utils.http_helpers import parse_error_detail

            if e.response.status_code == 400:
                detail = parse_error_detail(e.response) or str(e)
                console.print(f"[yellow]{detail}[/yellow]")
            elif e.response.status_code == 404:
                console.print("[yellow]Thread not found[/yellow]")
            else:
                console.print(f"[yellow]Could not summarize: {e}[/yellow]")
        except Exception as e:
            console.print(f"[yellow]Could not summarize: {e}[/yellow]")
        console.print()

    elif cmd_lower == "/conversation":
        # List and open past conversations for this user
        console.print()
        data = await client.list_conversations(limit=50)
        threads = data.get("threads", []) or []
        if not threads:
            console.print("[yellow]No conversations found[/yellow]")
            console.print()
            return "handled"

        # Build options for menu
        options: list[tuple[str, dict[str, Any]]] = []
        for item in threads:
            thread_id = str(item.get("thread_id", ""))
            workspace_id = str(item.get("workspace_id", ""))
            status = str(item.get("current_status", ""))
            first_query = str(item.get("first_query_content") or "")
            if len(first_query) > 60:
                first_query = first_query[:57] + "..."
            preview = f" - {first_query}" if first_query else ""
            label = f"{thread_id[:12]}  ws={workspace_id[:12]}  {status}{preview}"
            options.append((label, item))

        result = await create_interactive_menu(
            options,
            title="Select a conversation (Up/Down, Enter):",
        )
        if result is None:
            console.print("[dim]Cancelled[/dim]")
            console.print()
            return "handled"

        _index, chosen = result
        thread_id = str(chosen.get("thread_id"))
        workspace_id = str(chosen.get("workspace_id"))

        if not thread_id or not workspace_id:
            console.print("[red]Invalid conversation selection[/red]")
            console.print()
            return "handled"

        # Switch active thread/workspace
        session_state.thread_id = thread_id
        client.thread_id = thread_id
        client.workspace_id = workspace_id

        try:
            ws = await client.get_workspace(workspace_id)
            if not ws:
                console.print(f"[red]Workspace not found: {workspace_id}[/red]")
                console.print()
                return "handled"
            status = ws.get("status")
            # "flash" workspaces have no sandbox - skip start
            if status not in ("running", "flash"):
                await client.start_workspace(workspace_id)
        except Exception as e:
            console.print(f"[yellow]Could not start workspace: {e}[/yellow]")
            console.print()

        await replay_conversation(client, session_state)
        return "handled"

    elif cmd_lower == "/reconnect":
        # Reconnect to running workflow (useful after ESC interrupt)
        from ptc_cli.core.state import ReconnectStateManager

        state_manager = ReconnectStateManager()

        if session_state.thread_id:
            # Load last_event_id from saved state
            saved_state = state_manager.load_state(session_state.thread_id)
            if saved_state:
                client.last_event_id = saved_state.get("last_event_id", 0)
            await reconnect_to_workflow(client, session_state, token_tracker)
        else:
            # Try to load latest session
            latest_thread = state_manager.get_latest_thread_id()
            if latest_thread:
                session_state.thread_id = latest_thread
                client.thread_id = latest_thread
                saved_state = state_manager.load_state(latest_thread)
                if saved_state:
                    client.last_event_id = saved_state.get("last_event_id", 0)
                console.print(f"[dim]Loaded session: {latest_thread[:16]}...[/dim]")
                await reconnect_to_workflow(client, session_state, token_tracker)
            else:
                console.print("[yellow]No workflow sessions to reconnect to[/yellow]")
                console.print("[dim]Start a task first, then use ESC to soft-interrupt.[/dim]")

    elif cmd_lower == "/onboarding":
        # Guard sandbox command in flash mode
        if getattr(session_state, "flash_mode", False):
            console.print("[yellow]This command is not available in Flash mode (no sandbox)[/yellow]")
            console.print()
            return "handled"

        # Start user onboarding flow with user-profile skill
        console.print()

        # Ensure we have a workspace first
        if not client.workspace_id:
            console.print("[yellow]No workspace selected.[/yellow]")
            workspace_id = await _select_or_create_workspace_interactive(client)
            if not workspace_id:
                console.print("[dim]Cancelled[/dim]")
                console.print()
                return "handled"
            client.workspace_id = workspace_id

        if not await _ensure_workspace_available(client):
            console.print()
            return "handled"

        # Start a fresh conversation thread for onboarding
        session_state.reset_thread()
        client.thread_id = session_state.thread_id

        console.print("[cyan]Starting onboarding...[/cyan]")
        console.print(f"[dim]Thread: {client.thread_id}[/dim]")
        console.print()

        # Build skill context for user-profile onboarding
        additional_context = [
            {
                "type": "skills",
                "name": "user-profile",
                "instruction": """Help the user with first time onboarding. referecen the skills/user-profile/onboarding.md for more details.
                You should use LoadSkill tool to load the user-profile skill before calling any of the tools.
                """
            }
        ]

        # Execute with onboarding prompt
        await execute_task(
            user_input="Hi! I'm new here and would like to set up my profile.",
            client=client,
            assistant_id=None,
            session_state=session_state,
            token_tracker=token_tracker,
            additional_context=additional_context,
        )
        return "handled"

    else:
        # Unknown command
        console.print(f"[yellow]Unknown command: {command}[/yellow]")
        console.print(
            "[dim]Available: /help, /new, /workspace, /conversation, /tokens, /model, /status, /cancel, /summarize, /reconnect, /onboarding, /exit[/dim]"
        )

    return "handled"
