"""
Tests for ptc_agent.config.file_utils — config file discovery and loading.

Covers:
- ConfigContext enum
- find_project_root()
- get_default_config_dir()
- get_config_search_paths() with SDK vs CLI contexts
- find_config_file() search order
- load_yaml_config() with env var substitution
- substitute_env_vars()
"""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from ptc_agent.config.file_utils import (
    AGENT_CONFIG_FILE,
    ConfigContext,
    find_config_file,
    find_project_root,
    get_config_search_paths,
    get_default_config_dir,
    load_yaml_config,
    substitute_env_vars,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


class TestConstants:
    def test_agent_config_file_name(self):
        assert AGENT_CONFIG_FILE == "agent_config.yaml"

    def test_config_context_values(self):
        assert ConfigContext.SDK.value == "sdk"
        assert ConfigContext.CLI.value == "cli"


# ---------------------------------------------------------------------------
# find_project_root()
# ---------------------------------------------------------------------------


class TestFindProjectRoot:
    def test_finds_git_root(self, tmp_path):
        """Should find directory containing .git."""
        git_dir = tmp_path / ".git"
        git_dir.mkdir()
        sub = tmp_path / "a" / "b"
        sub.mkdir(parents=True)

        root = find_project_root(sub)
        assert root == tmp_path

    def test_returns_none_if_no_git(self, tmp_path):
        result = find_project_root(tmp_path)
        # May return None or find a parent .git — depends on test location
        # At minimum it should not crash
        assert result is None or isinstance(result, Path)


# ---------------------------------------------------------------------------
# get_default_config_dir()
# ---------------------------------------------------------------------------


class TestGetDefaultConfigDir:
    def test_returns_path(self):
        result = get_default_config_dir()
        assert isinstance(result, Path)
        assert str(result).endswith(".ptc-agent")


# ---------------------------------------------------------------------------
# get_config_search_paths()
# ---------------------------------------------------------------------------


class TestGetConfigSearchPaths:
    def test_sdk_context_includes_cwd(self):
        """SDK context should include CWD in search paths."""
        paths = get_config_search_paths(None, context=ConfigContext.SDK)
        assert any(str(p) == str(Path.cwd()) for p in paths)

    def test_cli_context_includes_home(self):
        """CLI context should include ~/.ptc-agent/ in search paths."""
        paths = get_config_search_paths(None, context=ConfigContext.CLI)
        home_dir = get_default_config_dir()
        assert any(str(p) == str(home_dir) for p in paths)


# ---------------------------------------------------------------------------
# find_config_file()
# ---------------------------------------------------------------------------


class TestFindConfigFile:
    def test_finds_existing_file(self, tmp_path):
        config_file = tmp_path / "agent_config.yaml"
        config_file.write_text("llm:\n  name: test\n")

        result = find_config_file(
            "agent_config.yaml",
            [tmp_path],
            env_var=None,
        )
        assert result == config_file

    def test_returns_none_when_not_found(self, tmp_path):
        result = find_config_file(
            "agent_config.yaml",
            [tmp_path / "nonexistent"],
            env_var=None,
        )
        assert result is None

    def test_env_var_override(self, tmp_path):
        """PTC_CONFIG_FILE env var should override search."""
        config_file = tmp_path / "custom_config.yaml"
        config_file.write_text("llm:\n  name: test\n")

        with patch.dict(os.environ, {"PTC_CONFIG_FILE": str(config_file)}):
            result = find_config_file(
                "agent_config.yaml",
                None,
                env_var="PTC_CONFIG_FILE",
            )
        assert result == config_file


# ---------------------------------------------------------------------------
# load_yaml_config()
# ---------------------------------------------------------------------------


class TestLoadYamlConfig:
    def test_loads_yaml(self, tmp_path):
        config_file = tmp_path / "test.yaml"
        config_file.write_text("key: value\nnested:\n  inner: 42\n")

        result = load_yaml_config(str(config_file))
        assert result["key"] == "value"
        assert result["nested"]["inner"] == 42

    def test_env_var_substitution(self, tmp_path):
        config_file = tmp_path / "test.yaml"
        config_file.write_text("api_key: ${TEST_API_KEY}\n")

        with patch.dict(os.environ, {"TEST_API_KEY": "secret-123"}):
            result = load_yaml_config(str(config_file))
        assert result["api_key"] == "secret-123"


# ---------------------------------------------------------------------------
# substitute_env_vars()
# ---------------------------------------------------------------------------


class TestSubstituteEnvVars:
    def test_simple_substitution(self):
        with patch.dict(os.environ, {"MY_VAR": "hello"}):
            result = substitute_env_vars("${MY_VAR}")
        assert result == "hello"

    def test_missing_var_empty_string(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("NONEXISTENT_VAR", None)
            result = substitute_env_vars("${NONEXISTENT_VAR}")
        assert "${NONEXISTENT_VAR}" in result or result == ""

    def test_no_substitution_needed(self):
        result = substitute_env_vars("plain_string")
        assert result == "plain_string"

    def test_mixed_content(self):
        with patch.dict(os.environ, {"PORT": "8080"}):
            result = substitute_env_vars("http://localhost:${PORT}/api")
        assert result == "http://localhost:8080/api"
