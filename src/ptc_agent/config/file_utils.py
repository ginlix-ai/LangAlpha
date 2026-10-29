"""Config file discovery and YAML loading utilities.

This module provides self-contained utilities for finding and loading
configuration files, with no dependencies on the server package (src.*).

Functions:
- Config file search: find_config_file, get_config_search_paths, find_project_root
- Config directory: get_default_config_dir, ensure_config_dir
- YAML loading: load_yaml_config, load_agent_config, clear_config_cache
- Env var substitution: substitute_env_vars
"""

from __future__ import annotations

import logging
import os
import re
from enum import Enum
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

# Config filenames
AGENT_CONFIG_FILE = "agent_config.yaml"


class ConfigContext(str, Enum):
    """Context for configuration loading behavior."""

    SDK = "sdk"  # CWD → git root → ~/.ptc-agent/
    CLI = "cli"  # ~/.ptc-agent/ → CWD (home first)


# =============================================================================
# Environment Variable Substitution
# =============================================================================


def substitute_env_vars(value: str) -> str:
    """Replace environment variables in string values.

    Supports both formats:
    - $VAR - Simple format
    - ${VAR} - Bash-style format with braces
    """
    if not isinstance(value, str):
        return value

    # Handle ${VAR} format first (bash style)
    pattern = r"\$\{([^}]+)\}"
    result = re.sub(
        pattern,
        lambda m: os.getenv(m.group(1), m.group(0)),
        value,
    )

    # Handle $VAR format (simple)
    if result.startswith("$") and not result.startswith("${"):
        env_var = result[1:]
        if env_var.isidentifier():
            return os.getenv(env_var, env_var)

    return result


def _process_dict(config: dict[str, Any]) -> dict[str, Any]:
    """Recursively process dictionary to replace environment variables."""
    if not config:
        return {}
    result = {}
    for key, value in config.items():
        if isinstance(value, dict):
            result[key] = _process_dict(value)
        elif isinstance(value, list):
            result[key] = _process_list(value)
        elif isinstance(value, str):
            result[key] = substitute_env_vars(value)
        else:
            result[key] = value
    return result


def _process_list(config_list: list[Any]) -> list[Any]:
    """Recursively process list to replace environment variables."""
    result = []
    for item in config_list:
        if isinstance(item, dict):
            result.append(_process_dict(item))
        elif isinstance(item, list):
            result.append(_process_list(item))
        elif isinstance(item, str):
            result.append(substitute_env_vars(item))
        else:
            result.append(item)
    return result


# =============================================================================
# Config File Search
# =============================================================================


def find_project_root(start_path: Path | None = None) -> Path | None:
    """Find git repository root by walking up from start_path."""
    current = start_path or Path.cwd()
    while current != current.parent:
        if (current / ".git").exists():
            return current
        current = current.parent
    return None


def get_default_config_dir() -> Path:
    """Get the default config directory (~/.ptc-agent/)."""
    return Path.home() / ".ptc-agent"


def ensure_config_dir() -> Path:
    """Ensure the default config directory exists."""
    config_dir = get_default_config_dir()
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir


def get_config_search_paths(
    start_path: Path | None = None,
    *,
    context: ConfigContext = ConfigContext.SDK,
) -> list[Path]:
    """Get ordered list of config search paths.

    Search order:
    - SDK: CWD → project root → ~/.ptc-agent/
    - CLI: ~/.ptc-agent/ → CWD
    """
    cwd = start_path or Path.cwd()
    home = get_default_config_dir()

    if context == ConfigContext.CLI:
        return [home, cwd]

    paths = [cwd]
    project_root = find_project_root(cwd)
    if project_root and project_root != cwd:
        paths.append(project_root)
    paths.append(home)
    return paths


def find_config_file(
    filename: str,
    search_paths: list[Path] | None = None,
    env_var: str | None = None,
    *,
    context: ConfigContext = ConfigContext.SDK,
) -> Path | None:
    """Find first existing config file in search paths."""
    if env_var:
        env_path = os.getenv(env_var)
        if env_path:
            path = Path(env_path)
            if path.exists():
                return path

    if search_paths is None:
        search_paths = get_config_search_paths(context=context)

    for search_path in search_paths:
        candidate = search_path / filename
        if candidate.exists():
            return candidate

    return None


# =============================================================================
# YAML Loading with Caching
# =============================================================================

_config_cache: dict[str, dict[str, Any]] = {}


def _read_yaml(file_path: str | Path) -> Any:
    # Only an empty file reads as {}: a falsey non-mapping (`[]`, `false`) has to
    # reach the overlay check intact, or it passes as an empty overlay.
    with open(file_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return {} if data is None else data


def _deep_merge(base: Any, overlay: Any) -> Any:
    """Nested maps merge key by key; lists replace outright, because they are
    ordered preference chains (llm.fallback, fetch_chain) where appending would
    keep an entry the overlay meant to drop. A null sets nothing, at any depth:
    a section whose keys are all commented out parses as null and must not wipe
    the base or reach a validator."""
    if not isinstance(overlay, dict):
        return overlay
    merged = dict(base) if isinstance(base, dict) else {}
    for key, value in overlay.items():
        if value is not None:
            merged[key] = _deep_merge(merged.get(key), value)
    return merged


def refuse_renamed_llm_keys(config: Any, source: str | Path) -> None:
    """Fail on ``llm.flash``, the Background model's name before ``llm.background``.

    Read as an unknown key it would be ignored, and the Background model would
    quietly become the main one, so a file still on it stops the load instead.
    Run on each file, so the error names the one to fix.
    """
    llm = config.get("llm") if isinstance(config, dict) else None
    if isinstance(llm, dict) and "flash" in llm:
        raise ValueError(f"llm.flash was renamed to llm.background (in {source})")


def warn_retired_compaction_keys(config: Any, source: str | Path) -> None:
    """Warn on ``compaction.keep_messages``, which a summary no longer reads.

    Ignored as an unknown key, it would change what a deployment keeps with
    no word of it. Warned, not refused: the token tail that replaces it is a
    safe default, and a file kept from before it should still load.
    """
    compaction = config.get("compaction") if isinstance(config, dict) else None
    if isinstance(compaction, dict) and "keep_messages" in compaction:
        logger.warning(
            f"compaction.keep_messages is no longer read (in {source}); a summary "
            "keeps the newest compaction.keep_tokens instead"
        )


def load_yaml_config(file_path: str, use_cache: bool = True) -> dict[str, Any]:
    """Load and process YAML configuration file with env var substitution.

    APP_ENV layers a sibling overlay on top: APP_ENV=production reads
    agent_config.production.yaml beside agent_config.yaml. Applied at load
    rather than at discovery, so every caller picks it up without knowing it
    exists; an overlay is never loaded without its base beside it.
    """
    if not os.path.exists(file_path):
        logger.warning(f"Configuration file not found: {file_path}")
        return {}

    app_env = os.getenv("APP_ENV", "").strip()
    cache_key = f"{file_path}|{app_env}"
    if use_cache and cache_key in _config_cache:
        return _config_cache[cache_key]

    raw_config = _read_yaml(file_path)
    refuse_renamed_llm_keys(raw_config, file_path)
    warn_retired_compaction_keys(raw_config, file_path)

    if app_env:
        base = Path(file_path)
        overlay = base.with_name(f"{base.stem}.{app_env}{base.suffix}")
        if overlay.is_file():
            overlay_config = _read_yaml(overlay)
            if not isinstance(overlay_config, dict):
                raise ValueError(f"{overlay} must be a mapping of config keys")
            refuse_renamed_llm_keys(overlay_config, overlay)
            warn_retired_compaction_keys(overlay_config, overlay)
            raw_config = _deep_merge(raw_config, overlay_config)
            logger.info(f"Applied {overlay.name} over {file_path}")
        else:
            logger.warning(f"APP_ENV={app_env} but no {overlay.name} beside {file_path}; using the base file only")

    if not raw_config:
        logger.warning(f"Empty configuration file: {file_path}")
        return {}

    processed_config = _process_dict(raw_config)

    logger.debug(f"Loaded configuration from {file_path} (settings: {len(processed_config)})")

    if use_cache:
        _config_cache[cache_key] = processed_config
    return processed_config


def load_agent_config() -> dict[str, Any]:
    """agent_config.yaml as a raw dict, from the same file the agent's own
    loader picks: PTC_CONFIG_FILE first, then the search paths."""
    path = find_config_file(AGENT_CONFIG_FILE, env_var="PTC_CONFIG_FILE")
    return load_yaml_config(str(path)) if path else {}


def clear_config_cache() -> None:
    """Clear the configuration cache."""
    global _config_cache
    _config_cache = {}
    logger.debug("Configuration cache cleared")
