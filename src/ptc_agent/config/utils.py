"""Shared configuration utilities.

This module provides common helpers for env loading and config validation.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog
from dotenv import load_dotenv

if TYPE_CHECKING:
    from ptc_agent.config.core import (
        DaytonaConfig,
        FilesystemConfig,
        LoggingConfig,
        MCPConfig,
    )


async def load_dotenv_async(env_file: Path | None = None) -> None:
    """Load environment variables from .env file asynchronously.

    Args:
        env_file: Optional path to .env file. If None, searches default locations.
    """
    if env_file:
        await asyncio.to_thread(load_dotenv, env_file)
    else:
        await asyncio.to_thread(load_dotenv)


def validate_required_sections(
    config_data: dict[str, Any],
    required_sections: list[str],
    config_name: str = "agent_config.yaml"
) -> None:
    """Validate that all required sections exist in config data.

    Args:
        config_data: Parsed config dictionary
        required_sections: List of required section names
        config_name: Name of config file for error messages

    Raises:
        ValueError: If any required sections are missing
    """
    missing = [s for s in required_sections if s not in config_data]
    if missing:
        raise ValueError(
            f"Missing required sections in {config_name}: {', '.join(missing)}\n"
            f"Please add these sections to your agent_config.yaml file."
        )


def validate_section_fields(
    section_data: dict[str, Any],
    required_fields: list[str],
    section_name: str
) -> None:
    """Validate that all required fields exist in a config section.

    Args:
        section_data: Section dictionary
        required_fields: List of required field names
        section_name: Name of section for error messages

    Raises:
        ValueError: If any required fields are missing
    """
    missing = [f for f in required_fields if f not in section_data]
    if missing:
        raise ValueError(
            f"Missing required fields in {section_name} section: {', '.join(missing)}"
        )


# Common field requirements for shared config sections
DAYTONA_REQUIRED_FIELDS = [
    "base_url",
    "auto_stop_interval",
    "auto_archive_interval",
    "auto_delete_interval",
    "python_version",
]

MCP_REQUIRED_FIELDS = ["servers", "tool_discovery_enabled"]

LOGGING_REQUIRED_FIELDS = ["level", "file"]

FILESYSTEM_REQUIRED_FIELDS = ["allowed_directories"]


# Factory functions for creating config objects from dictionaries


def create_daytona_config(data: dict[str, Any]) -> DaytonaConfig:
    """Create DaytonaConfig from config data dictionary.

    Args:
        data: Daytona section from agent_config.yaml

    Returns:
        Configured DaytonaConfig object
    """
    import os

    from ptc_agent.config.core import DaytonaConfig

    validate_section_fields(data, DAYTONA_REQUIRED_FIELDS, "daytona")
    return DaytonaConfig(
        api_key=os.getenv("DAYTONA_API_KEY", ""),
        base_url=data["base_url"],
        auto_stop_interval=data["auto_stop_interval"],
        auto_archive_interval=data["auto_archive_interval"],
        auto_delete_interval=data["auto_delete_interval"],
        python_version=data["python_version"],
        snapshot_enabled=data.get("snapshot_enabled", True),
        snapshot_name=data.get("snapshot_name"),
        snapshot_auto_create=data.get("snapshot_auto_create", True),
    )


def create_mcp_config(data: dict[str, Any]) -> MCPConfig:
    """Create MCPConfig from config data dictionary.

    Args:
        data: MCP section from agent_config.yaml

    Returns:
        Configured MCPConfig object
    """
    from ptc_agent.config.core import MCPConfig, MCPServerConfig

    validate_section_fields(data, MCP_REQUIRED_FIELDS, "mcp")
    mcp_servers = [MCPServerConfig(**server) for server in data["servers"]]
    return MCPConfig(
        servers=mcp_servers,
        tool_discovery_enabled=data["tool_discovery_enabled"],
        lazy_load=data.get("lazy_load", True),
        cache_duration=data.get("cache_duration"),
        tool_exposure_mode=data.get("tool_exposure_mode", "summary"),
    )


def create_logging_config(data: dict[str, Any]) -> LoggingConfig:
    """Create LoggingConfig from config data dictionary.

    Args:
        data: Logging section from agent_config.yaml

    Returns:
        Configured LoggingConfig object
    """
    from ptc_agent.config.core import LoggingConfig

    validate_section_fields(data, LOGGING_REQUIRED_FIELDS, "logging")
    return LoggingConfig(
        level=data["level"],
        file=data["file"],
    )


def create_filesystem_config(data: dict[str, Any]) -> FilesystemConfig:
    """Create FilesystemConfig from config data dictionary.

    Args:
        data: Filesystem section from agent_config.yaml

    Returns:
        Configured FilesystemConfig object
    """
    from ptc_agent.config.core import FilesystemConfig

    validate_section_fields(data, FILESYSTEM_REQUIRED_FIELDS, "filesystem")
    return FilesystemConfig(
        allowed_directories=data["allowed_directories"],
        denied_directories=data.get("denied_directories", []),
        enable_path_validation=data.get("enable_path_validation", True),
    )


def configure_logging(level: str = "INFO") -> None:
    """Configure structlog to respect log level from config.

    This function configures log level filtering

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        cache_logger_on_first_use=True,
    )
