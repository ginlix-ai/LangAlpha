"""Pytest configuration and shared fixtures for ptc-cli tests."""

import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


# ============================================================================
# Console & Display Fixtures
# ============================================================================


@pytest.fixture
def mock_console():
    """Mock Rich console for testing display functions."""
    console = Mock()
    console.print = Mock()
    console.clear = Mock()
    console.rule = Mock()
    return console


# ============================================================================
# SessionState Fixtures
# ============================================================================


@pytest.fixture
def session_state():
    """Create a default SessionState instance."""
    from ptc_cli.core.state import SessionState

    return SessionState()


@pytest.fixture
def session_state_with_auto_approve():
    """Create a SessionState instance with auto-approve enabled."""
    from ptc_cli.core.state import SessionState

    return SessionState(auto_approve=True)


@pytest.fixture
def session_state_no_persist():
    """Create a SessionState instance with persistence disabled."""
    from ptc_cli.core.state import SessionState

    return SessionState(persist_session=False)


@pytest.fixture
def mock_session_state():
    """Create a mock SessionState instance."""
    state = Mock()
    state.thread_id = "test-thread-id"
    state.reset_thread = Mock(return_value="new-thread-id")
    state.auto_approve = False
    return state


# ============================================================================
# Temporary Directory Fixtures
# ============================================================================


@pytest.fixture
def temp_home(tmp_path, monkeypatch):
    """Create a temporary home directory for testing."""
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setattr(Path, "home", lambda: home_dir)
    return home_dir


@pytest.fixture
def temp_project(tmp_path):
    """Create a temporary project directory with .git folder."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    git_dir = project_dir / ".git"
    git_dir.mkdir()
    return project_dir


# ============================================================================
# Settings Fixtures
# ============================================================================


@pytest.fixture
def settings_with_project(temp_project, monkeypatch):
    """Create Settings instance with a mock project."""
    from ptc_cli.core.config import Settings

    monkeypatch.setenv("DAYTONA_API_KEY", "test-key-123")
    return Settings.from_environment(start_path=temp_project)


@pytest.fixture
def settings_no_project(tmp_path, monkeypatch):
    """Create Settings instance without a project."""
    from ptc_cli.core.config import Settings

    # Create a directory without .git
    no_project_dir = tmp_path / "no_project"
    no_project_dir.mkdir()
    return Settings.from_environment(start_path=no_project_dir)


# ============================================================================
# Sandbox & Session Fixtures
# ============================================================================


@pytest.fixture
def mock_sandbox():
    """Create a mock sandbox instance."""
    sandbox = AsyncMock()
    sandbox.sandbox_id = "test-sandbox-123"
    sandbox.is_healthy = AsyncMock(return_value=True)
    sandbox.execute = AsyncMock(return_value={"stdout": "", "stderr": "", "exit_code": 0})
    sandbox.aglob_files = AsyncMock(return_value=[])
    sandbox.normalize_path = Mock(side_effect=lambda x: f"/home/workspace/{x}")
    sandbox.read_file = Mock(return_value=None)
    sandbox.download_file_bytes = Mock(return_value=None)

    # Async helpers used by slash commands and agent tools.
    # Wire them to existing sync mocks so tests can keep setting return_value.
    sandbox.aread_file_text = AsyncMock(side_effect=lambda p: sandbox.read_file(p))
    sandbox.adownload_file_bytes = AsyncMock(side_effect=lambda p: sandbox.download_file_bytes(p))

    return sandbox


@pytest.fixture
def mock_session(mock_sandbox):
    """Create a mock session manager with sandbox."""
    session = AsyncMock()
    session.sandbox = mock_sandbox
    session.get_sandbox = AsyncMock(return_value=mock_sandbox)
    session.close = AsyncMock()
    return session


# ============================================================================
# Agent Fixtures
# ============================================================================


@pytest.fixture
def mock_agent():
    """Create a mock PTCAgent instance."""
    agent = AsyncMock()
    agent.invoke = AsyncMock()
    agent.stream = AsyncMock()
    return agent


@pytest.fixture
def mock_agent_config():
    """Mock AgentConfig instance."""
    config = Mock()
    config.to_core_config = Mock()

    # Mock core config
    core_config = Mock()
    core_config.daytona = Mock()
    core_config.daytona.base_url = "https://api.daytona.io"
    core_config.daytona.python_version = "3.11"
    core_config.daytona.snapshot_enabled = False
    core_config.daytona.snapshot_name = None

    # Mock MCP config
    core_config.mcp = Mock()
    core_config.mcp.servers = []

    config.to_core_config.return_value = core_config
    return config


# ============================================================================
# Token Tracker Fixtures
# ============================================================================


@pytest.fixture
def token_tracker():
    """Create a TokenTracker instance."""
    from ptc_cli.display.tokens import TokenTracker

    return TokenTracker()


@pytest.fixture
def mock_token_tracker():
    """Create a mock TokenTracker instance."""
    tracker = Mock()
    tracker.display = Mock()
    tracker.input_tokens = 100
    tracker.output_tokens = 50
    tracker.total = 150
    return tracker


# ============================================================================
# Completer Fixtures
# ============================================================================


@pytest.fixture
def sandbox_file_completer():
    """Create a SandboxFileCompleter instance."""
    from ptc_cli.input.completers import SandboxFileCompleter

    return SandboxFileCompleter()


@pytest.fixture
def command_completer():
    """Create a CommandCompleter instance."""
    from ptc_cli.input.completers import CommandCompleter

    return CommandCompleter()


# ============================================================================
# Persistence Fixtures
# ============================================================================


@pytest.fixture
def mock_persisted_session_data():
    """Sample persisted session data."""
    return {
        "sandbox_id": "test-sandbox-456",
        "config_hash": "abc12345",
        "created_at": datetime.now(tz=UTC).isoformat(),
        "last_used": datetime.now(tz=UTC).isoformat(),
    }


@pytest.fixture
def mock_session_file(tmp_path, mock_persisted_session_data):
    """Create a mock session file with valid data."""
    session_file = tmp_path / "session.json"
    session_file.write_text(json.dumps(mock_persisted_session_data, indent=2))
    return session_file
