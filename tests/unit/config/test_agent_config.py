"""
Tests for ptc_agent.config.agent — AgentConfig, LLMConfig, and related models.

Covers:
- AgentConfig.create() with various arguments
- AgentConfig.validate_api_keys()
- AgentConfig.to_core_config()
- AgentConfig.get_llm_client() dispatch (direct client vs factory)
- LLMConfig fields and defaults
- SubagentConfig / SubagentsConfig
- SkillsConfig path resolution
- FlashConfig defaults
"""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ptc_agent.config.agent import (
    AgentConfig,
    FlashConfig,
    LLMConfig,
    LLMDefinition,
    SkillsConfig,
    SubagentConfig,
    SubagentsConfig,
    CompactionConfig,
)
from ptc_agent.config.core import (
    CoreConfig,
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    MCPServerConfig,
    SandboxConfig,
    SecurityConfig,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _minimal_config(**overrides) -> AgentConfig:
    """Create a minimal AgentConfig for testing."""
    defaults = dict(
        llm=LLMConfig(name="test-model"),
        security=SecurityConfig(),
        logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        mcp=MCPConfig(),
        filesystem=FilesystemConfig(),
    )
    defaults.update(overrides)
    return AgentConfig(**defaults)


# ---------------------------------------------------------------------------
# LLMConfig
# ---------------------------------------------------------------------------


class TestLLMConfig:
    def test_defaults(self):
        cfg = LLMConfig(name="gpt-4o")
        assert cfg.name == "gpt-4o"
        assert cfg.flash is None
        assert cfg.compaction is None
        assert cfg.fetch is None
        assert cfg.fallback is None

    def test_all_fields(self):
        cfg = LLMConfig(
            name="claude-sonnet-4-5",
            flash="claude-haiku-4-5",
            compaction="claude-haiku-4-5",
            fetch="claude-haiku-4-5",
            fallback=["gpt-4o", "gpt-4o-mini"],
        )
        assert cfg.flash == "claude-haiku-4-5"
        assert cfg.fallback == ["gpt-4o", "gpt-4o-mini"]


# ---------------------------------------------------------------------------
# AgentConfig.create()
# ---------------------------------------------------------------------------


class TestAgentConfigCreate:
    def test_minimal_create(self):
        """Minimal create() with just an LLM client."""
        mock_llm = MagicMock()
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SANDBOX_PROVIDER", None)
            config = AgentConfig.create(
                llm=mock_llm,
                daytona_api_key="test-key-123",
            )
        assert config.llm.name == "custom"
        assert config.llm_client is mock_llm
        assert config.daytona.api_key == "test-key-123"

    def test_create_uses_env_var(self):
        """create() should fall back to DAYTONA_API_KEY env var."""
        mock_llm = MagicMock()
        with patch.dict(os.environ, {"DAYTONA_API_KEY": "env-key-456"}, clear=False):
            os.environ.pop("SANDBOX_PROVIDER", None)
            config = AgentConfig.create(llm=mock_llm)
        assert config.daytona.api_key == "env-key-456"

    def test_create_auto_detects_docker_without_api_key(self):
        """create() should auto-detect docker provider when no DAYTONA_API_KEY."""
        mock_llm = MagicMock()
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("DAYTONA_API_KEY", None)
            os.environ.pop("SANDBOX_PROVIDER", None)
            config = AgentConfig.create(llm=mock_llm)
        assert config.sandbox.provider == "docker"

    def test_create_raises_when_daytona_explicit_without_key(self):
        """create() should raise if daytona provider is explicit but no API key."""
        mock_llm = MagicMock()
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("DAYTONA_API_KEY", None)
            with pytest.raises(ValueError, match="DAYTONA_API_KEY"):
                AgentConfig.create(llm=mock_llm, provider="daytona")

    def test_create_with_mcp_servers(self):
        mock_llm = MagicMock()
        servers = [
            MCPServerConfig(name="test-server", command="node", args=["server.js"]),
        ]
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SANDBOX_PROVIDER", None)
            config = AgentConfig.create(
                llm=mock_llm,
                daytona_api_key="key",
                mcp_servers=servers,
            )
        assert len(config.mcp.servers) == 1
        assert config.mcp.servers[0].name == "test-server"

    def test_create_with_custom_kwargs(self):
        """create() should accept and apply optional kwargs."""
        mock_llm = MagicMock()
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SANDBOX_PROVIDER", None)
            config = AgentConfig.create(
                llm=mock_llm,
                daytona_api_key="key",
                python_version="3.11",
                log_level="DEBUG",
                enable_view_image=False,
                background_auto_wait=True,
                subagents_enabled=["general-purpose", "research"],
            )
        assert config.daytona.python_version == "3.11"
        assert config.logging.level == "DEBUG"
        assert config.enable_view_image is False
        assert config.background_auto_wait is True
        assert config.subagents.enabled == ["general-purpose", "research"]

    def test_create_with_allowed_directories(self):
        mock_llm = MagicMock()
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SANDBOX_PROVIDER", None)
            config = AgentConfig.create(
                llm=mock_llm,
                daytona_api_key="key",
                allowed_directories=["/workspace", "/data"],
            )
        assert config.filesystem.allowed_directories == ["/workspace", "/data"]


# ---------------------------------------------------------------------------
# AgentConfig.validate_api_keys()
# ---------------------------------------------------------------------------


class TestAgentConfigValidateApiKeys:
    def test_valid_key(self):
        config = _minimal_config()
        config.validate_api_keys()  # Should not raise

    def test_missing_daytona_key(self):
        config = _minimal_config(
            sandbox=SandboxConfig(daytona=DaytonaConfig(api_key=""))
        )
        with pytest.raises(ValueError, match="DAYTONA_API_KEY"):
            config.validate_api_keys()

    def test_docker_provider_skips_daytona_key(self):
        """Docker provider should not require DAYTONA_API_KEY."""
        config = _minimal_config(
            sandbox=SandboxConfig(provider="docker", daytona=DaytonaConfig(api_key=""))
        )
        config.validate_api_keys()  # Should not raise


# ---------------------------------------------------------------------------
# AgentConfig.to_core_config()
# ---------------------------------------------------------------------------


class TestAgentConfigToCoreConfig:
    def test_converts_correctly(self):
        config = _minimal_config()
        config.config_file_dir = Path("/test/dir")
        core = config.to_core_config()

        assert isinstance(core, CoreConfig)
        assert core.daytona.api_key == "test-key"
        assert core.config_file_dir == Path("/test/dir")

    def test_preserves_all_sections(self):
        config = _minimal_config()
        core = config.to_core_config()
        assert core.security is config.security
        assert core.mcp is config.mcp
        assert core.logging is config.logging
        assert core.filesystem is config.filesystem


# ---------------------------------------------------------------------------
# AgentConfig.get_llm_client()
# ---------------------------------------------------------------------------


class TestAgentConfigGetLlmClient:
    def test_returns_direct_client(self):
        """When llm_client is set (via create()), return it directly."""
        mock_llm = MagicMock()
        config = _minimal_config()
        config.llm_client = mock_llm
        assert config.get_llm_client() is mock_llm

    def test_falls_back_to_factory(self):
        """When no llm_client, use src/llms factory."""
        config = _minimal_config()
        mock_created = MagicMock()
        with patch("src.llms.create_llm", return_value=mock_created):
            result = config.get_llm_client()
        assert result is mock_created


# ---------------------------------------------------------------------------
# SubagentConfig / SubagentsConfig
# ---------------------------------------------------------------------------


class TestSubagentConfig:
    def test_defaults(self):
        cfg = SubagentConfig(description="Test agent")
        assert cfg.mode == "ptc"
        assert cfg.model is None
        assert cfg.tools == ["execute_code", "filesystem"]
        assert cfg.max_iterations == 15

    def test_custom_fields(self):
        cfg = SubagentConfig(
            description="Research",
            mode="flash",
            model="claude-haiku-4-5",
            tools=["web_search"],
            max_iterations=5,
        )
        assert cfg.mode == "flash"
        assert cfg.model == "claude-haiku-4-5"


class TestSubagentsConfig:
    def test_defaults(self):
        cfg = SubagentsConfig()
        assert cfg.enabled == ["general-purpose"]
        assert cfg.definitions == {}


# ---------------------------------------------------------------------------
# SkillsConfig
# ---------------------------------------------------------------------------


class TestSkillsConfig:
    def test_defaults(self):
        cfg = SkillsConfig()
        assert cfg.enabled is True
        assert cfg.user_skills_dir == "~/.ptc-agent/skills"
        assert cfg.project_skills_dir == "skills"

    def test_local_skill_dirs_with_sandbox(self):
        cfg = SkillsConfig()
        cwd = Path("/test/project")
        dirs = cfg.local_skill_dirs_with_sandbox(cwd=cwd)
        assert len(dirs) == 2
        # User dir first (lower priority), project dir second (higher priority)
        user_dir, user_sandbox = dirs[0]
        project_dir, project_sandbox = dirs[1]
        assert "ptc-agent/skills" in user_dir
        assert "/test/project/skills" in project_dir
        assert user_sandbox == "/home/workspace/.agents/skills"


# ---------------------------------------------------------------------------
# FlashConfig
# ---------------------------------------------------------------------------


class TestFlashConfig:
    def test_defaults(self):
        cfg = FlashConfig()
        assert cfg.enabled is True


# ---------------------------------------------------------------------------
# LLMDefinition
# ---------------------------------------------------------------------------


class TestLLMDefinition:
    def test_construction(self):
        defn = LLMDefinition(
            model_id="gpt-4o",
            provider="openai",
            sdk="langchain_openai.ChatOpenAI",
            api_key_env="OPENAI_API_KEY",
        )
        assert defn.model_id == "gpt-4o"
        assert defn.base_url is None
        assert defn.parameters == {}


# ---------------------------------------------------------------------------
# CompactionConfig
# ---------------------------------------------------------------------------


class TestCompactionConfig:
    def test_defaults(self):
        cfg = CompactionConfig()
        assert cfg.enabled is True
        assert cfg.token_threshold == 120000
        assert cfg.keep_messages == 5
        assert cfg.truncate_args_trigger_messages is None
        assert cfg.truncate_args_keep_messages == 20
        assert cfg.truncate_args_max_length == 2000

    def test_custom_values(self):
        cfg = CompactionConfig(
            enabled=False,
            token_threshold=80000,
            keep_messages=3,
            truncate_args_trigger_messages=15,
        )
        assert cfg.enabled is False
        assert cfg.token_threshold == 80000
        assert cfg.keep_messages == 3
        assert cfg.truncate_args_trigger_messages == 15


# ---------------------------------------------------------------------------
# AgentConfig — compaction + search_api fields
# ---------------------------------------------------------------------------


class TestAgentConfigNewFields:
    def test_default_compaction(self):
        """AgentConfig should have CompactionConfig with defaults."""
        config = _minimal_config()
        assert isinstance(config.compaction, CompactionConfig)
        assert config.compaction.enabled is True
        assert config.compaction.token_threshold == 120000

    def test_default_search_api(self):
        """AgentConfig should have search_api defaulting to 'tavily'."""
        config = _minimal_config()
        assert config.search_api == "tavily"

    def test_custom_compaction(self):
        compaction = CompactionConfig(enabled=False, token_threshold=50000)
        config = _minimal_config(compaction=compaction)
        assert config.compaction.enabled is False
        assert config.compaction.token_threshold == 50000

    def test_custom_search_api(self):
        config = _minimal_config(search_api="serper")
        assert config.search_api == "serper"
