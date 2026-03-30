"""Core configuration and state management for the PTC Agent CLI."""

from ptc_cli.core.config import (
    COLORS,
    COMMANDS,
    MAX_ARG_LENGTH,
    MAX_ERROR_LENGTH,
    PTC_AGENT_ASCII,
    Settings,
    console,
    settings,
)
from ptc_cli.core.state import ReconnectStateManager, SessionState
from ptc_cli.core.theme import (
    ThemeManager,
    get_syntax_theme,
    get_theme,
    get_toolbar_styles,
)

__all__ = [
    "COLORS",
    "COMMANDS",
    "MAX_ARG_LENGTH",
    "MAX_ERROR_LENGTH",
    "PTC_AGENT_ASCII",
    "ReconnectStateManager",
    "SessionState",
    "Settings",
    "ThemeManager",
    "console",
    "get_syntax_theme",
    "get_theme",
    "get_toolbar_styles",
    "settings",
]
