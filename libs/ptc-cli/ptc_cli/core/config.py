"""Configuration, constants, and settings for the PTC Agent CLI."""

import os
import re
from collections.abc import ItemsView, Iterator, KeysView, ValuesView
from dataclasses import dataclass
from pathlib import Path

import dotenv
from rich.console import Console

dotenv.load_dotenv()

# Color scheme - use theme-aware colors
# Import here to avoid circular imports (theme imports happen after dotenv.load_dotenv)
from ptc_cli.core.theme import get_colors, get_theme  # noqa: E402


class _ColorsProxy(dict[str, str]):
    """Proxy object that returns current theme colors.

    This provides backward compatibility with code that accesses
    COLORS as a dictionary (e.g., COLORS["primary"]).

    Inherits from dict for type compatibility with dict[str, str].
    """

    def __getitem__(self, key: str) -> str:
        """Get color value by key."""
        return get_colors()[key]

    def get(self, key: str, default: str = "") -> str:  # type: ignore[override]
        """Get color value with default fallback."""
        return get_colors().get(key, default)

    def __contains__(self, key: object) -> bool:
        """Check if color key exists."""
        return key in get_colors()

    def __iter__(self) -> Iterator[str]:
        """Iterate over color keys."""
        return iter(get_colors())

    def __len__(self) -> int:
        """Get number of colors."""
        return len(get_colors())

    def keys(self) -> KeysView[str]:  # type: ignore[override]
        """Get all color keys."""
        return get_colors().keys()

    def values(self) -> ValuesView[str]:  # type: ignore[override]
        """Get all color values."""
        return get_colors().values()

    def items(self) -> ItemsView[str, str]:  # type: ignore[override]
        """Get all color items."""
        return get_colors().items()


COLORS = _ColorsProxy()

# ASCII art banner
PTC_AGENT_ASCII = """
 ██████╗ ████████╗ ██████╗
 ██╔══██╗╚══██╔══╝██╔════╝
 ██████╔╝   ██║   ██║
 ██╔═══╝    ██║   ██║
 ██║        ██║   ╚██████╗
 ╚═╝        ╚═╝    ╚═════╝

  █████╗   ██████╗  ███████╗ ███╗   ██╗ ████████╗
 ██╔══██╗ ██╔════╝  ██╔════╝ ████╗  ██║ ╚══██╔══╝
 ███████║ ██║  ███╗ █████╗   ██╔██╗ ██║    ██║
 ██╔══██║ ██║   ██║ ██╔══╝   ██║╚██╗██║    ██║
 ██║  ██║ ╚██████╔╝ ███████╗ ██║ ╚████║    ██║
 ╚═╝  ╚═╝  ╚═════╝  ╚══════╝ ╚═╝  ╚═══╝    ╚═╝
"""

# Available model options for user selection
MODEL_OPTIONS = [
    {"name": "gpt-5.2", "description": "OpenAI GPT-5.2 with reasoning"},
    {"name": "glm-4.7", "description": "Z.AI GLM-4.7 with thinking"},
    {"name": "minimax-m2.1", "description": "MiniMax M2.1 for coding"},
    {"name": "claude-sonnet-4-5-proxy", "description": "Claude 4.5 Sonnet via proxy"},
    {"name": "claude-opus-4-5-proxy", "description": "Claude 4.5 Opus via proxy"},
    {"name": "claude-haiku-4-5-proxy", "description": "Claude 4.5 Haiku via proxy"},
]

# Interactive commands (for auto-completion)
COMMANDS = {
    "new": "Start a new conversation thread (no clear)",
    "help": "Show help information",
    "tokens": "Show token usage for current session",
    "files": "List files in sandbox (use 'files all' for system dirs)",
    "refresh": "Refresh sandbox tools + skills",
    "view": "View file content: /view <path>",
    "copy": "Copy file to clipboard: /copy <path>",
    "download": "Download file: /download <path> [local]",
    "model": "Switch LLM model (only at session start)",
    "conversation": "List and open past conversations",
    "status": "Show workflow and workspace status",
    "cancel": "Cancel running workflow",
    "reconnect": "Reconnect to a paused workflow",
    "workspace": "List/switch/start/stop workspaces",
    "compact": "Compact conversation context: /compact [keep=5]",
    "exit": "Exit the CLI",
}

# Maximum argument length for display
MAX_ARG_LENGTH = 150

# Maximum error message length for display
MAX_ERROR_LENGTH = 500

# Rich console instance (respects NO_COLOR environment variable)
console = Console(highlight=False, no_color=get_theme().colors_disabled)


def _find_project_root(start_path: Path | None = None) -> Path | None:
    """Find the project root by looking for .git directory.

    Walks up the directory tree from start_path (or cwd) looking for a .git
    directory, which indicates the project root.

    Args:
        start_path: Directory to start searching from. Defaults to current working directory.

    Returns:
        Path to the project root if found, None otherwise.
    """
    current = Path(start_path or Path.cwd()).resolve()

    # Walk up the directory tree
    for parent in [current, *list(current.parents)]:
        git_dir = parent / ".git"
        if git_dir.exists():
            return parent

    return None


def _find_project_agent_md(project_root: Path) -> list[Path]:
    """Find project-specific agent.md file(s).

    Checks two locations and returns ALL that exist:
    1. project_root/.ptc-agent/agent.md
    2. project_root/agent.md

    Both files will be loaded and combined if both exist.

    Args:
        project_root: Path to the project root directory.

    Returns:
        List of paths to project agent.md files (may contain 0, 1, or 2 paths).
    """
    paths = []

    # Check .ptc-agent/agent.md (preferred)  # noqa: ERA001
    ptc_agent_md = project_root / ".ptc-agent" / "agent.md"
    if ptc_agent_md.exists():
        paths.append(ptc_agent_md)

    # Check root agent.md (fallback, but also include if both exist)
    root_md = project_root / "agent.md"
    if root_md.exists():
        paths.append(root_md)

    return paths


@dataclass
class Settings:
    """Global settings and environment detection for ptc-cli.

    This class is initialized once at startup and provides access to:
    - Available API keys
    - Current project information
    - File system paths

    Attributes:
        project_root: Current project root directory (if in a git project)
        daytona_api_key: Daytona API key if available
    """

    # API keys
    daytona_api_key: str | None

    # Project information
    project_root: Path | None

    @classmethod
    def from_environment(cls, *, start_path: Path | None = None) -> "Settings":
        """Create settings by detecting the current environment.

        Args:
            start_path: Directory to start project detection from (defaults to cwd)

        Returns:
            Settings instance with detected configuration
        """
        # Detect API keys
        daytona_key = os.environ.get("DAYTONA_API_KEY")

        # Detect project
        project_root = _find_project_root(start_path)

        return cls(
            daytona_api_key=daytona_key,
            project_root=project_root,
        )

    @property
    def has_daytona(self) -> bool:
        """Check if Daytona API key is configured."""
        return self.daytona_api_key is not None

    @property
    def has_project(self) -> bool:
        """Check if currently in a git project."""
        return self.project_root is not None

    @property
    def user_ptc_agent_dir(self) -> Path:
        """Get the base user-level .ptc-agent directory.

        Returns:
            Path to ~/.ptc-agent
        """
        return Path.home() / ".ptc-agent"

    def get_user_agent_md_path(self, agent_name: str) -> Path:
        """Get user-level agent.md path for a specific agent.

        Returns path regardless of whether the file exists.

        Args:
            agent_name: Name of the agent

        Returns:
            Path to ~/.ptc-agent/{agent_name}/agent.md
        """
        return Path.home() / ".ptc-agent" / agent_name / "agent.md"

    def get_project_agent_md_path(self) -> Path | None:
        """Get project-level agent.md path.

        Returns path regardless of whether the file exists.

        Returns:
            Path to {project_root}/.ptc-agent/agent.md, or None if not in a project
        """
        if not self.project_root:
            return None
        return self.project_root / ".ptc-agent" / "agent.md"

    @staticmethod
    def _is_valid_agent_name(agent_name: str) -> bool:
        """Validate prevent invalid filesystem paths and security issues."""
        if not agent_name or not agent_name.strip():
            return False
        # Allow only alphanumeric, hyphens, underscores, and whitespace
        return bool(re.match(r"^[a-zA-Z0-9_\-\s]+$", agent_name))

    def get_agent_dir(self, agent_name: str) -> Path:
        """Get the global agent directory path.

        Args:
            agent_name: Name of the agent

        Returns:
            Path to ~/.ptc-agent/{agent_name}
        """
        if not self._is_valid_agent_name(agent_name):
            msg = f"Invalid agent name: {agent_name!r}. Agent names can only contain letters, numbers, hyphens, underscores, and spaces."
            raise ValueError(msg)
        return Path.home() / ".ptc-agent" / agent_name

    def get_session_file_path(self, agent_name: str) -> Path:
        """Get the session persistence file path for an agent.

        Args:
            agent_name: Name of the agent

        Returns:
            Path to ~/.ptc-agent/{agent_name}/session.json
        """
        return self.get_agent_dir(agent_name) / "session.json"

    def ensure_agent_dir(self, agent_name: str) -> Path:
        """Ensure the global agent directory exists and return its path.

        Args:
            agent_name: Name of the agent

        Returns:
            Path to ~/.ptc-agent/{agent_name}
        """
        if not self._is_valid_agent_name(agent_name):
            msg = f"Invalid agent name: {agent_name!r}. Agent names can only contain letters, numbers, hyphens, underscores, and spaces."
            raise ValueError(msg)
        agent_dir = self.get_agent_dir(agent_name)
        agent_dir.mkdir(parents=True, exist_ok=True)
        return agent_dir

    def ensure_project_ptc_agent_dir(self) -> Path | None:
        """Ensure the project .ptc-agent directory exists and return its path.

        Returns:
            Path to project .ptc-agent directory, or None if not in a project
        """
        if not self.project_root:
            return None

        project_ptc_agent_dir = self.project_root / ".ptc-agent"
        project_ptc_agent_dir.mkdir(parents=True, exist_ok=True)
        return project_ptc_agent_dir


# Global settings instance (initialized once)
settings = Settings.from_environment()
