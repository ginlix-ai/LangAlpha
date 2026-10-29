"""Prompt session creation and configuration."""

import asyncio
import os
import time
from collections.abc import Callable
from typing import Any

from prompt_toolkit import PromptSession
from prompt_toolkit.completion import Completer, merge_completers
from prompt_toolkit.enums import EditingMode
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.key_binding.key_processor import KeyPressEvent
from prompt_toolkit.styles import Style

from ptc_cli.core import COLORS, SessionState, get_toolbar_styles
from ptc_cli.input.completers import (
    AT_MENTION_RE,
    SLASH_COMMAND_RE,
    CommandCompleter,
    SandboxFileCompleter,
)

EXIT_CONFIRM_WINDOW = 3.0
CTRL_C_EXIT_COUNT = 3  # Number of Ctrl+C presses required to exit


def get_bottom_toolbar(
    session_state: SessionState,
    session_ref: dict,
    agent_ref: dict | None = None,
) -> Callable[[], list[tuple[str, str]]]:
    """Return toolbar function that shows auto-approve status, BASH MODE, and subagent count."""

    def toolbar() -> list[tuple[str, str]]:
        parts = []

        # Check if we're in BASH mode (input starts with !)
        try:
            session = session_ref.get("session")
            if session:
                current_text = session.default_buffer.text
                if current_text.startswith("!"):
                    parts.append(("bg:#ff1493 fg:#ffffff bold", " BASH MODE "))
        except (AttributeError, TypeError):
            # Silently ignore - toolbar is non-critical and called frequently
            pass

        # Show background subagent status from session state (API mode)
        # This is populated from SSE subagent_status events
        bg_status = getattr(session_state, "background_status", None)
        if bg_status:
            running = len(bg_status.get("active_tasks") or bg_status.get("active_subagents") or [])
            completed = len(bg_status.get("completed_tasks") or bg_status.get("completed_subagents") or [])
            if running > 0 or completed > 0:
                parts.append(("class:toolbar-cyan", f" {running} running | {completed} completed "))

        # Show if workflow was soft-interrupted
        if getattr(session_state, "soft_interrupted", False):
            thread_id = getattr(session_state, "thread_id", "unknown")
            parts.append(("class:toolbar-yellow", f" [Paused: {thread_id[:8]}...] "))

        # Fallback: Show background subagent status from in-process agent (non-API mode)
        if not bg_status:
            try:
                if agent_ref:
                    agent = agent_ref.get("agent")
                    if agent and hasattr(agent, "middleware") and hasattr(agent.middleware, "registry"):
                        registry = agent.middleware.registry

                        running = getattr(registry, "pending_count", 0)
                        completed_unseen = 0

                        tasks = getattr(registry, "_tasks", {})
                        for task in tasks.values():
                            asyncio_task = getattr(task, "asyncio_task", None)
                            # Avoid counting a task as "completed" until it is truly finished and we have
                            # either an explicit completed flag or some terminal state captured.
                            is_done = bool(getattr(task, "completed", False)) or bool(asyncio_task and asyncio_task.done())
                            has_terminal_info = (
                                bool(getattr(task, "completed", False))
                                or getattr(task, "result", None) is not None
                                or getattr(task, "error", None) is not None
                            )
                            if is_done and has_terminal_info and not getattr(task, "result_seen", False):
                                completed_unseen += 1

                        if running > 0 or completed_unseen > 0:
                            parts.append(("class:toolbar-cyan", f" {running} running | {completed_unseen} completed "))
            except (AttributeError, TypeError):
                # Silently ignore - toolbar is non-critical
                pass

        # Show revision hint if active (after first Esc)
        hint_until = session_state.esc_hint_until
        if hint_until is not None:
            now = time.monotonic()
            if now < hint_until:
                parts.append(("class:toolbar-exit", " Esc again to revise "))
            else:
                session_state.esc_hint_until = None

        # Show Ctrl+C exit hint if active
        exit_hint_until = session_state.exit_hint_until
        if exit_hint_until is not None:
            now = time.monotonic()
            if now < exit_hint_until and session_state.ctrl_c_count > 0:
                remaining = CTRL_C_EXIT_COUNT - session_state.ctrl_c_count
                if remaining == 1:
                    parts.append(("class:toolbar-exit", " Ctrl+C 1 more time to exit "))
                else:
                    parts.append(("class:toolbar-exit", f" Ctrl+C {remaining} more times to exit "))
            else:
                session_state.exit_hint_until = None
                session_state.ctrl_c_count = 0

        joined: list[tuple[str, str]] = []
        for part in parts:
            if joined:
                joined.append(("", " | "))
            joined.append(part)
        return joined

    return toolbar


def create_prompt_session(
    _assistant_id: str | None,
    session_state: SessionState,
    sandbox_completer: SandboxFileCompleter | None = None,
    agent_ref: dict | None = None,
) -> PromptSession[str]:
    """Create a configured PromptSession with all features.

    Args:
        _assistant_id: Agent identifier (unused, for future use)
        session_state: Session state with auto-approve settings
        sandbox_completer: Optional completer for sandbox file paths
        agent_ref: Optional reference to agent for toolbar status display

    Returns:
        Configured PromptSession
    """
    # Set default editor if not already set
    if "EDITOR" not in os.environ:
        os.environ["EDITOR"] = "nano"

    # Create key bindings
    kb = KeyBindings()

    @kb.add("c-c")
    def _(event: KeyPressEvent) -> None:
        """Clear input if present, or exit on triple press."""
        app = event.app
        buffer = event.current_buffer

        # If there's content in the input, just clear it
        if buffer.text:
            buffer.reset()
            session_state.ctrl_c_count = 0
            session_state.exit_hint_until = None
            session_state.exit_requested = False
            if session_state.exit_hint_handle:
                session_state.exit_hint_handle.cancel()
                session_state.exit_hint_handle = None
            return

        # No content - track press count for triple-exit
        now = time.monotonic()

        # Reset count if window expired
        if session_state.exit_hint_until is None or now >= session_state.exit_hint_until:
            session_state.ctrl_c_count = 0

        session_state.ctrl_c_count += 1
        session_state.exit_hint_until = now + EXIT_CONFIRM_WINDOW

        # Cancel existing timer
        if session_state.exit_hint_handle:
            session_state.exit_hint_handle.cancel()

        if session_state.ctrl_c_count >= CTRL_C_EXIT_COUNT:
            # Third press - exit
            session_state.exit_hint_handle = None
            session_state.exit_requested = True
            app.exit(exception=KeyboardInterrupt())
            return

        # Set up timer to clear hint after window expires
        loop = asyncio.get_running_loop()
        app_ref = app

        def clear_hint() -> None:
            if session_state.exit_hint_until is not None and time.monotonic() >= session_state.exit_hint_until:
                session_state.exit_hint_until = None
                session_state.exit_hint_handle = None
                session_state.ctrl_c_count = 0
                app_ref.invalidate()

        session_state.exit_hint_handle = loop.call_later(EXIT_CONFIRM_WINDOW, clear_hint)

        app.invalidate()

    # Bind regular Enter to submit (intuitive behavior)
    @kb.add("enter")
    def _(event: KeyPressEvent) -> None:
        """Enter submits the input, unless completion menu is active."""
        buffer = event.current_buffer

        # If completion menu is showing, apply the current completion
        if buffer.complete_state:
            # Get the current completion (the highlighted one)
            current_completion = buffer.complete_state.current_completion

            # If no completion is selected (user hasn't navigated), select and apply the first one
            if not current_completion and buffer.complete_state.completions:
                # Move to the first completion
                buffer.complete_next()
                # Now apply it (complete_next ensures current_completion is set)
                if buffer.complete_state and buffer.complete_state.current_completion:
                    buffer.apply_completion(buffer.complete_state.current_completion)
            elif current_completion:
                # Apply the already-selected completion
                buffer.apply_completion(current_completion)
            else:
                # No completions available, close menu
                buffer.complete_state = None
        # Don't submit if buffer is empty or only whitespace
        elif buffer.text.strip():
            # Normal submit
            buffer.validate_and_handle()
            # If empty, do nothing (don't submit)

    # Alt+Enter for newlines (press ESC then Enter, or Option+Enter on Mac)
    @kb.add("escape", "enter")
    def _(event: KeyPressEvent) -> None:
        """Alt+Enter inserts a newline for multi-line input."""
        event.current_buffer.insert_text("\n")

    # Ctrl+E to open in external editor
    @kb.add("c-e")
    def _(event: KeyPressEvent) -> None:
        """Open the current input in an external editor (nano by default)."""
        event.current_buffer.open_in_editor()

    # Backspace handler to retrigger completions after deletion
    @kb.add("backspace")
    def _(event: KeyPressEvent) -> None:
        """Handle backspace and retrigger completion if in @ or / context."""
        buffer = event.current_buffer

        # Perform the normal backspace action
        buffer.delete_before_cursor(count=1)

        # Check if we're in a completion context (@ or /)
        text = buffer.document.text_before_cursor
        if AT_MENTION_RE.search(text) or SLASH_COMMAND_RE.match(text):
            # Retrigger completion
            buffer.start_completion(select_first=False)

    # Define styles for the toolbar with full-width background colors
    # Uses theme-aware styles from the theme module
    toolbar_style = Style.from_dict(get_toolbar_styles())

    # Create session reference dict for toolbar to access session
    session_ref: dict[str, Any] = {}

    # Build completers list
    completers: list[Completer] = [CommandCompleter()]
    if sandbox_completer:
        completers.append(sandbox_completer)

    # Create the session
    session: PromptSession[str] = PromptSession(
        message=HTML(f'<style fg="{COLORS["user"]}">></style> '),
        multiline=True,  # Keep multiline support but Enter submits
        key_bindings=kb,
        completer=merge_completers(completers),
        editing_mode=EditingMode.EMACS,
        complete_while_typing=True,  # Show completions as you type
        complete_in_thread=True,  # Async completion prevents menu freezing
        mouse_support=False,
        enable_open_in_editor=True,  # Allow Ctrl+X Ctrl+E to open external editor
        bottom_toolbar=get_bottom_toolbar(session_state, session_ref, agent_ref),  # Persistent status bar at bottom
        style=toolbar_style,  # Apply toolbar styling
        refresh_interval=0.5,
        reserve_space_for_menu=7,  # Reserve space for completion menu to show 5-6 results
    )

    # Store session reference for toolbar to access
    session_ref["session"] = session

    return session


async def select_model_interactive(current_model: str | None = None) -> str | None:
    """Show inline model selection with arrow key navigation.

    Args:
        current_model: Currently selected model (will be pre-selected)

    Returns:
        Selected model name, or None if cancelled
    """
    from ptc_cli.core.config import MODEL_OPTIONS
    from ptc_cli.core import console

    options = [m["name"] for m in MODEL_OPTIONS]
    descriptions = {m["name"]: m["description"] for m in MODEL_OPTIONS}

    # Find starting index
    try:
        idx = options.index(current_model) if current_model in options else 0
    except ValueError:
        idx = 0

    kb = KeyBindings()
    selected: list[str | None] = [None]  # Use list to allow mutation in closure
    done = asyncio.Event()

    @kb.add("up")
    @kb.add("k")
    def _up(event: KeyPressEvent) -> None:
        nonlocal idx
        idx = (idx - 1) % len(options)
        event.app.invalidate()

    @kb.add("down")
    @kb.add("j")
    def _down(event: KeyPressEvent) -> None:
        nonlocal idx
        idx = (idx + 1) % len(options)
        event.app.invalidate()

    @kb.add("enter")
    def _select(event: KeyPressEvent) -> None:
        selected[0] = options[idx]
        event.app.exit()

    @kb.add("escape")
    @kb.add("q")
    @kb.add("c-c")
    def _cancel(event: KeyPressEvent) -> None:
        selected[0] = None
        event.app.exit()

    def get_prompt() -> HTML:
        lines = ["<b>Select model</b> (↑/↓ navigate, Enter select, Esc cancel)\n"]
        for i, opt in enumerate(options):
            desc = descriptions.get(opt, "")
            if i == idx:
                lines.append(f"<style fg='{COLORS['primary']}'> > {opt}</style> <style fg='{COLORS['dim']}'>{desc}</style>\n")
            else:
                lines.append(f"   <style fg='{COLORS['dim']}'>{opt} - {desc}</style>\n")
        return HTML("".join(lines))

    session: PromptSession[str] = PromptSession(
        message=get_prompt,
        key_bindings=kb,
        refresh_interval=0.1,
    )

    try:
        await session.prompt_async()
    except (EOFError, KeyboardInterrupt):
        return None

    return selected[0]
