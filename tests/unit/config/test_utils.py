"""
Tests for ptc_agent.config.utils — config factory functions and validators.

Covers:
- validate_required_sections()
- validate_section_fields()
- create_daytona_config()
- create_mcp_config()
- create_logging_config()
- create_filesystem_config() — including the working_directory bug
"""

import os
from unittest.mock import patch

import pytest

from ptc_agent.config.utils import (
    create_daytona_config,
    create_filesystem_config,
    create_logging_config,
    create_mcp_config,
    validate_required_sections,
    validate_section_fields,
)


# ---------------------------------------------------------------------------
# validate_required_sections
# ---------------------------------------------------------------------------


class TestValidateRequiredSections:
    def test_all_present(self):
        data = {"a": 1, "b": 2, "c": 3}
        validate_required_sections(data, ["a", "b", "c"])  # Should not raise

    def test_missing_one(self):
        data = {"a": 1, "b": 2}
        with pytest.raises(ValueError, match="Missing required sections.*c"):
            validate_required_sections(data, ["a", "b", "c"])

    def test_missing_multiple(self):
        data = {"a": 1}
        with pytest.raises(ValueError, match="b.*c|c.*b"):
            validate_required_sections(data, ["a", "b", "c"])

    def test_custom_config_name(self):
        data = {}
        with pytest.raises(ValueError, match="custom.yaml"):
            validate_required_sections(data, ["x"], config_name="custom.yaml")


class TestValidateSectionFields:
    def test_all_present(self):
        data = {"x": 1, "y": 2}
        validate_section_fields(data, ["x", "y"], "test")

    def test_missing_field(self):
        data = {"x": 1}
        with pytest.raises(ValueError, match="Missing required fields in test.*y"):
            validate_section_fields(data, ["x", "y"], "test")


# ---------------------------------------------------------------------------
# create_daytona_config
# ---------------------------------------------------------------------------


class TestCreateDaytonaConfig:
    def test_creates_config(self):
        data = {
            "base_url": "https://test.daytona.io/api",
            "auto_stop_interval": 1800,
            "auto_archive_interval": 43200,
            "auto_delete_interval": 302400,
            "python_version": "3.11",
        }
        with patch.dict(os.environ, {"DAYTONA_API_KEY": "test-key"}):
            cfg = create_daytona_config(data)
        assert cfg.base_url == "https://test.daytona.io/api"
        assert cfg.api_key == "test-key"
        assert cfg.python_version == "3.11"
        assert cfg.auto_stop_interval == 1800

    def test_missing_env_key_defaults_empty(self):
        """If DAYTONA_API_KEY is not set, api_key defaults to empty string."""
        data = {
            "base_url": "https://test.io/api",
            "auto_stop_interval": 3600,
            "auto_archive_interval": 86400,
            "auto_delete_interval": 604800,
            "python_version": "3.12",
        }
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("DAYTONA_API_KEY", None)
            cfg = create_daytona_config(data)
        assert cfg.api_key == ""

    def test_snapshot_defaults(self):
        data = {
            "base_url": "https://test.io/api",
            "auto_stop_interval": 3600,
            "auto_archive_interval": 86400,
            "auto_delete_interval": 604800,
            "python_version": "3.12",
        }
        cfg = create_daytona_config(data)
        assert cfg.snapshot_enabled is True
        assert cfg.snapshot_name is None
        assert cfg.snapshot_auto_create is True

    def test_snapshot_custom(self):
        data = {
            "base_url": "https://test.io/api",
            "auto_stop_interval": 3600,
            "auto_archive_interval": 86400,
            "auto_delete_interval": 604800,
            "python_version": "3.12",
            "snapshot_enabled": False,
            "snapshot_name": "my-snapshot",
            "snapshot_auto_create": False,
        }
        cfg = create_daytona_config(data)
        assert cfg.snapshot_enabled is False
        assert cfg.snapshot_name == "my-snapshot"

    def test_missing_required_field(self):
        data = {"base_url": "https://test.io/api"}
        with pytest.raises(ValueError, match="Missing required fields in daytona"):
            create_daytona_config(data)


# ---------------------------------------------------------------------------
# create_mcp_config
# ---------------------------------------------------------------------------


class TestCreateMcpConfig:
    def test_empty_servers(self):
        data = {"servers": [], "tool_discovery_enabled": True}
        cfg = create_mcp_config(data)
        assert cfg.servers == []
        assert cfg.tool_discovery_enabled is True
        assert cfg.lazy_load is True

    def test_with_servers(self):
        data = {
            "servers": [
                {"name": "test", "command": "node", "args": ["server.js"]},
                {"name": "other", "command": "python", "description": "Other server"},
            ],
            "tool_discovery_enabled": False,
            "lazy_load": False,
            "tool_exposure_mode": "detailed",
        }
        cfg = create_mcp_config(data)
        assert len(cfg.servers) == 2
        assert cfg.servers[0].name == "test"
        assert cfg.servers[1].description == "Other server"
        assert cfg.tool_discovery_enabled is False
        assert cfg.lazy_load is False
        assert cfg.tool_exposure_mode == "detailed"

    def test_missing_required_field(self):
        data = {"servers": []}
        with pytest.raises(ValueError, match="tool_discovery_enabled"):
            create_mcp_config(data)


# ---------------------------------------------------------------------------
# create_logging_config
# ---------------------------------------------------------------------------


class TestCreateLoggingConfig:
    def test_creates_config(self):
        data = {"level": "DEBUG", "file": "logs/debug.log"}
        cfg = create_logging_config(data)
        assert cfg.level == "DEBUG"
        assert cfg.file == "logs/debug.log"

    def test_missing_required_field(self):
        data = {"level": "INFO"}
        with pytest.raises(ValueError, match="file"):
            create_logging_config(data)


# ---------------------------------------------------------------------------
# create_filesystem_config
# ---------------------------------------------------------------------------


class TestCreateFilesystemConfig:
    def test_creates_config(self):
        data = {
            "allowed_directories": ["/workspace"],
            "denied_directories": ["/secret"],
            "enable_path_validation": False,
        }
        cfg = create_filesystem_config(data)
        assert cfg.allowed_directories == ["/workspace"]
        assert cfg.denied_directories == ["/secret"]
        assert cfg.enable_path_validation is False

    def test_defaults(self):
        data = {"allowed_directories": ["/home/daytona"]}
        cfg = create_filesystem_config(data)
        assert cfg.denied_directories == []
        assert cfg.enable_path_validation is True

    def test_working_directory_applied(self):
        """working_directory from config data should be applied."""
        data = {
            "working_directory": "/custom/workdir",
            "allowed_directories": ["/home/daytona"],
        }
        cfg = create_filesystem_config(data)
        assert cfg.working_directory == "/custom/workdir"

    def test_missing_required_field(self):
        data = {}
        with pytest.raises(ValueError, match="allowed_directories"):
            create_filesystem_config(data)
