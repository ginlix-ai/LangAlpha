"""
Tests for ptc_agent.config.core — core configuration models and defaults.

Covers:
- SecurityConfig default values vs DEFAULT_ALLOWED_IMPORTS/DEFAULT_BLOCKED_PATTERNS
- create_default_security_config() consistency
- CoreConfig construction and validate_api_keys()
- DaytonaConfig, MCPConfig, MCPServerConfig, FilesystemConfig, LoggingConfig defaults
"""

import pytest

from ptc_agent.config.core import (
    DEFAULT_ALLOWED_IMPORTS,
    DEFAULT_BLOCKED_PATTERNS,
    CoreConfig,
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    MCPServerConfig,
    SecurityConfig,
    create_default_security_config,
)


# ---------------------------------------------------------------------------
# SecurityConfig defaults
# ---------------------------------------------------------------------------


class TestSecurityConfig:
    def test_field_defaults(self):
        """SecurityConfig() uses its own field-level defaults."""
        cfg = SecurityConfig()
        assert cfg.max_execution_time == 300
        assert cfg.max_code_length == 10000
        assert cfg.max_file_size == 10485760
        assert cfg.enable_code_validation is True
        assert isinstance(cfg.allowed_imports, list)
        assert isinstance(cfg.blocked_patterns, list)

    def test_field_defaults_independent_copies(self):
        """Each instance gets independent list copies."""
        cfg1 = SecurityConfig()
        cfg2 = SecurityConfig()
        cfg1.allowed_imports.append("extra_module")
        assert "extra_module" not in cfg2.allowed_imports

    def test_module_constants_exist(self):
        """Module-level DEFAULT_* constants are populated."""
        assert len(DEFAULT_ALLOWED_IMPORTS) > 0
        assert len(DEFAULT_BLOCKED_PATTERNS) > 0
        assert "os" in DEFAULT_ALLOWED_IMPORTS
        assert "eval(" in DEFAULT_BLOCKED_PATTERNS


class TestCreateDefaultSecurityConfig:
    def test_returns_security_config(self):
        cfg = create_default_security_config()
        assert isinstance(cfg, SecurityConfig)

    def test_uses_module_constants(self):
        """create_default_security_config() should use DEFAULT_* constants."""
        cfg = create_default_security_config()
        assert cfg.allowed_imports == DEFAULT_ALLOWED_IMPORTS
        assert cfg.blocked_patterns == DEFAULT_BLOCKED_PATTERNS

    def test_field_defaults_match_constants(self):
        """SecurityConfig() and create_default_security_config() should produce identical results."""
        field_default = SecurityConfig()
        factory_default = create_default_security_config()

        assert set(field_default.allowed_imports) == set(factory_default.allowed_imports)
        assert set(field_default.blocked_patterns) == set(factory_default.blocked_patterns)


# ---------------------------------------------------------------------------
# DaytonaConfig
# ---------------------------------------------------------------------------


class TestDaytonaConfig:
    def test_defaults(self):
        cfg = DaytonaConfig()
        assert cfg.api_key == ""
        assert cfg.base_url == "https://app.daytona.io/api"
        assert cfg.python_version == "3.12"
        assert cfg.auto_stop_interval == 3600
        assert cfg.snapshot_enabled is True


# ---------------------------------------------------------------------------
# MCPServerConfig / MCPConfig
# ---------------------------------------------------------------------------


class TestMCPServerConfig:
    def test_minimal(self):
        cfg = MCPServerConfig(name="test")
        assert cfg.enabled is True
        assert cfg.transport == "stdio"
        assert cfg.args == []
        assert cfg.env == {}

    def test_full(self):
        cfg = MCPServerConfig(
            name="tavily",
            description="Web search",
            command="npx",
            args=["-y", "tavily-mcp"],
            env={"API_KEY": "xxx"},
        )
        assert cfg.command == "npx"
        assert len(cfg.args) == 2


class TestMCPConfig:
    def test_defaults(self):
        cfg = MCPConfig()
        assert cfg.servers == []
        assert cfg.tool_discovery_enabled is True
        assert cfg.lazy_load is True


# ---------------------------------------------------------------------------
# FilesystemConfig
# ---------------------------------------------------------------------------


class TestFilesystemConfig:
    def test_defaults(self):
        cfg = FilesystemConfig()
        assert cfg.working_directory == "/home/daytona"
        assert cfg.allowed_directories == ["/home/daytona", "/tmp"]
        assert cfg.denied_directories == []
        assert cfg.enable_path_validation is True


# ---------------------------------------------------------------------------
# LoggingConfig
# ---------------------------------------------------------------------------


class TestLoggingConfig:
    def test_defaults(self):
        cfg = LoggingConfig()
        assert cfg.level == "INFO"
        assert cfg.file == "logs/ptc.log"


# ---------------------------------------------------------------------------
# CoreConfig
# ---------------------------------------------------------------------------


class TestCoreConfig:
    def _make_core(self, **overrides) -> CoreConfig:
        defaults = dict(
            daytona=DaytonaConfig(api_key="test-key"),
            security=SecurityConfig(),
            mcp=MCPConfig(),
            logging=LoggingConfig(),
            filesystem=FilesystemConfig(),
        )
        defaults.update(overrides)
        return CoreConfig(**defaults)

    def test_construction(self):
        cfg = self._make_core()
        assert cfg.daytona.api_key == "test-key"
        assert cfg.config_file_dir is None

    def test_validate_api_keys_valid(self):
        cfg = self._make_core()
        cfg.validate_api_keys()  # Should not raise

    def test_validate_api_keys_missing(self):
        cfg = self._make_core(daytona=DaytonaConfig(api_key=""))
        with pytest.raises(ValueError, match="DAYTONA_API_KEY"):
            cfg.validate_api_keys()
