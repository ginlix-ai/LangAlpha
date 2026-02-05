"""Session state management for the PTC Agent CLI."""

from __future__ import annotations

import json
import uuid
from asyncio import Task, TimerHandle
from datetime import datetime
from pathlib import Path
from typing import Any


def _find_project_root(start_path: Path | None = None) -> Path:
    """Find git repository root by walking up from start_path.

    Returns:
        Project root if found, otherwise current working directory.
    """
    current = (start_path or Path.cwd()).resolve()
    while current != current.parent:
        if (current / ".git").exists():
            return current
        current = current.parent
    return Path.cwd()


class ReconnectStateManager:
    """Manages reconnection state persistence for cross-session reconnection.

    Stores thread_id and last_event_id to enable reconnection after:
    - CLI exit
    - Network interruptions
    - Session restarts

    State is stored in project root's `.ptc-agent/reconnect_state.json`.
    Auto-cleans entries older than 24 hours (matching workflow TTL).
    """

    def __init__(self, state_dir: Path | None = None) -> None:
        """Initialize state manager.

        Args:
            state_dir: Override state directory (default: project_root/.ptc-agent)
        """
        if state_dir:
            self.state_dir = state_dir
        else:
            project_root = _find_project_root()
            self.state_dir = project_root / ".ptc-agent"
        self.state_file = self.state_dir / "reconnect_state.json"
        self._ensure_directory()

    def _ensure_directory(self) -> None:
        """Create state directory if it doesn't exist."""
        try:
            self.state_dir.mkdir(parents=True, exist_ok=True)
        except Exception:  # noqa: BLE001
            pass  # Silently ignore - will fail on save if needed

    def save_state(
        self,
        thread_id: str,
        last_event_id: int,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        """Save reconnection state for a thread.

        Args:
            thread_id: Thread identifier
            last_event_id: Last received event sequence number
            metadata: Optional metadata (query, workspace_id, etc.)

        Returns:
            True if saved successfully, False otherwise
        """
        try:
            state = self._load_state_file()

            state[thread_id] = {
                "last_event_id": last_event_id,
                "timestamp": datetime.now().isoformat(),
                "metadata": metadata or {},
            }

            # Cleanup old entries before saving
            state = self._cleanup_old_entries(state)

            with open(self.state_file, "w") as f:
                json.dump(state, f, indent=2)

            return True
        except Exception:  # noqa: BLE001
            return False

    def load_state(self, thread_id: str) -> dict[str, Any] | None:
        """Load reconnection state for a thread.

        Args:
            thread_id: Thread identifier

        Returns:
            Dict with last_event_id and metadata, or None if not found
        """
        try:
            state = self._load_state_file()
            return state.get(thread_id)
        except Exception:  # noqa: BLE001
            return None

    def list_sessions(self) -> list[dict[str, Any]]:
        """List all available reconnection sessions.

        Returns:
            List of session info dicts sorted by timestamp (newest first)
        """
        try:
            state = self._load_state_file()
            state = self._cleanup_old_entries(state)

            sessions = []
            for thread_id, info in state.items():
                sessions.append({
                    "thread_id": thread_id,
                    "last_event_id": info["last_event_id"],
                    "timestamp": info["timestamp"],
                    "metadata": info.get("metadata", {}),
                })

            sessions.sort(key=lambda x: x["timestamp"], reverse=True)
            return sessions
        except Exception:  # noqa: BLE001
            return []

    def get_latest_thread_id(self) -> str | None:
        """Get the most recent thread_id from saved sessions.

        Returns:
            The latest thread_id, or None if no sessions available
        """
        sessions = self.list_sessions()
        if sessions:
            return sessions[0]["thread_id"]
        return None

    def delete_state(self, thread_id: str) -> bool:
        """Delete reconnection state for a thread."""
        try:
            state = self._load_state_file()
            if thread_id in state:
                del state[thread_id]
                with open(self.state_file, "w") as f:
                    json.dump(state, f, indent=2)
                return True
            return False
        except Exception:  # noqa: BLE001
            return False

    def _load_state_file(self) -> dict[str, Any]:
        """Load state file, return empty dict if doesn't exist."""
        if not self.state_file.exists():
            return {}

        try:
            with open(self.state_file) as f:
                return json.load(f)
        except (json.JSONDecodeError, Exception):  # noqa: BLE001
            return {}

    def _cleanup_old_entries(self, state: dict[str, Any]) -> dict[str, Any]:
        """Remove entries older than 24 hours (workflow TTL)."""
        cutoff_time = datetime.now().timestamp() - (24 * 3600)
        cleaned_state = {}

        for thread_id, info in state.items():
            try:
                timestamp = datetime.fromisoformat(info["timestamp"]).timestamp()
                if timestamp >= cutoff_time:
                    cleaned_state[thread_id] = info
            except (ValueError, KeyError):
                continue

        return cleaned_state


class SessionState:
    """Holds mutable session state."""

    def __init__(
        self,
        *,
        auto_approve: bool = False,
        no_splash: bool = False,
        persist_session: bool = True,
        plan_mode: bool = False,
        llm_model: str | None = None,
        flash_mode: bool = False,
    ) -> None:
        """Initialize session state.

        Args:
            auto_approve: Whether to auto-approve tool executions
            no_splash: Whether to skip the splash screen
            persist_session: Whether to persist sandbox sessions
            plan_mode: Whether to inject plan mode reminder
            llm_model: LLM model name from models.json (e.g., 'minimax-m2.1')
            flash_mode: Whether to use Flash Agent (no sandbox)
        """
        self.auto_approve = auto_approve
        self.no_splash = no_splash
        self.persist_session = persist_session
        self.plan_mode = plan_mode  # If True, inject plan mode reminder
        self.llm_model = llm_model  # Per-session LLM model override
        self.flash_mode = flash_mode  # If True, use Flash Agent without sandbox
        self.reusing_sandbox = False  # Set to True when reconnecting to existing sandbox
        self.thread_id = str(uuid.uuid4())

        # Live sandbox file state for autocomplete and /files.
        self.sandbox_files: list[str] = []
        self.sandbox_completer: Any | None = None

        # Esc key handling for interrupt and revision
        self.esc_hint_until: float | None = None
        self.esc_hint_handle: TimerHandle | None = None
        self.last_user_message: str | None = None
        self.revision_requested: bool = False
        self.esc_interrupt_requested: bool = False

        # Ctrl+C exit handling (triple press to exit)
        # NOTE: PromptSession may also raise a raw KeyboardInterrupt (terminal signal).
        # Track explicit exit requests to avoid quitting the CLI unintentionally.
        self.exit_hint_until: float | None = None
        self.exit_hint_handle: TimerHandle | None = None
        self.ctrl_c_count: int = 0
        self.exit_requested: bool = False
        self.last_interrupt_time: float | None = None
        self.last_exit_reason: str | None = None
        self.log_file_path: str | None = None

        # Background task tracking (for ESC soft interrupt)
        self.background_status: dict | None = None  # {active_subagents: [], completed_subagents: []}
        self.soft_interrupted: bool = False  # True if workflow was soft-interrupted
        self.soft_interrupt_result: dict | None = None  # Result from soft interrupt API

        # Subagent status stream tracking (status-only SSE)
        self.status_stream_task: Task | None = None
        self.status_stream_thread_id: str | None = None

        # HITL pending response (for resuming after interrupt)
        self.pending_hitl_response: dict | None = None

    def toggle_auto_approve(self) -> bool:
        """Toggle auto-approve and return new state."""
        self.auto_approve = not self.auto_approve
        return self.auto_approve

    def toggle_plan_mode(self) -> bool:
        """Toggle plan mode.

        Returns:
            New plan_mode state
        """
        self.plan_mode = not self.plan_mode
        return self.plan_mode

    def reset_thread(self) -> str:
        """Reset conversation by generating new thread_id.

        Returns:
            New thread_id
        """
        self.thread_id = str(uuid.uuid4())
        return self.thread_id
