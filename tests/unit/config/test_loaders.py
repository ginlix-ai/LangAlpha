"""
Tests for ptc_agent.config.loaders — load_from_dict() and load_from_files().

Covers:
- load_from_dict() with full config dict
- load_from_dict() with string LLM format
- load_from_dict() with dict LLM format
- Missing required sections
- Optional sections (agent, subagents, skills, flash)
- Subagent validation against builtins
- load_from_files() config search behavior
- generate_config_template()
"""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.config.agent import AgentConfig, LLMConfig
from ptc_agent.config.loaders import generate_config_template, load_from_dict


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _full_config_dict(**overrides) -> dict:
    """Return a valid full config dictionary matching agent_config.yaml structure."""
    base = {
        "llm": {
            "name": "test-model",
            "flash": "test-flash",
            "compaction": "test-summary",
            "fetch": "test-fetch",
            "fallback": ["fallback-1"],
        },
        "daytona": {
            "base_url": "https://test.daytona.io/api",
            "auto_stop_interval": 3600,
            "auto_archive_interval": 86400,
            "auto_delete_interval": 604800,
            "python_version": "3.12",
        },
        "mcp": {
            "servers": [],
            "tool_discovery_enabled": True,
        },
        "logging": {
            "level": "INFO",
            "file": "logs/test.log",
        },
        "filesystem": {
            "allowed_directories": ["/home/workspace", "/tmp"],
        },
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# load_from_dict — LLM config
# ---------------------------------------------------------------------------


class TestLoadFromDictLLM:
    def test_dict_llm_format(self):
        """LLM section as dict with name and optional fields."""
        config = load_from_dict(_full_config_dict())
        assert config.llm.name == "test-model"
        assert config.llm.flash == "test-flash"
        assert config.llm.compaction == "test-summary"
        assert config.llm.fetch == "test-fetch"
        assert config.llm.fallback == ["fallback-1"]

    def test_string_llm_format(self):
        """String LLM format should produce LLMConfig with name only."""
        data = _full_config_dict(llm="claude-sonnet-4-5")
        config = load_from_dict(data)
        assert config.llm.name == "claude-sonnet-4-5"
        assert config.llm.flash is None
        assert config.llm.compaction is None
        assert config.llm.fetch is None
        assert config.llm.fallback is None

    def test_dict_llm_missing_name_raises(self):
        data = _full_config_dict(llm={"flash": "model"})
        with pytest.raises(ValueError, match="llm.name is required"):
            load_from_dict(data)

    def test_dict_llm_empty_name_raises(self):
        data = _full_config_dict(llm={"name": ""})
        with pytest.raises(ValueError, match="llm.name is required"):
            load_from_dict(data)

    def test_invalid_llm_type_raises(self):
        data = _full_config_dict(llm=42)
        with pytest.raises(ValueError, match="llm section must be"):
            load_from_dict(data)

    def test_null_llm_allowed(self):
        """llm: null produces config with llm=None."""
        data = _full_config_dict(llm=None)
        config = load_from_dict(data)
        assert config.llm is None

    def test_missing_llm_allowed(self):
        """Omitting llm section entirely produces config with llm=None."""
        data = _full_config_dict()
        del data["llm"]
        config = load_from_dict(data)
        assert config.llm is None

    def test_dict_llm_minimal(self):
        """Dict with only name — others default to None."""
        data = _full_config_dict(llm={"name": "gpt-4o"})
        config = load_from_dict(data)
        assert config.llm.name == "gpt-4o"
        assert config.llm.flash is None
        assert config.llm.compaction is None


# ---------------------------------------------------------------------------
# load_from_dict — required sections
# ---------------------------------------------------------------------------


class TestLoadFromDictRequiredSections:
    @pytest.mark.parametrize("missing_key", ["mcp", "logging", "filesystem"])
    def test_missing_required_section(self, missing_key):
        data = _full_config_dict()
        del data[missing_key]
        with pytest.raises(ValueError, match=f"Missing required sections.*{missing_key}"):
            load_from_dict(data)

    def test_missing_sandbox_and_daytona(self):
        """Missing both 'sandbox' and 'daytona' keys should raise."""
        data = _full_config_dict()
        del data["daytona"]
        with pytest.raises(ValueError, match="sandbox.*daytona"):
            load_from_dict(data)


# ---------------------------------------------------------------------------
# load_from_dict — optional sections
# ---------------------------------------------------------------------------


class TestLoadFromDictOptionalSections:
    def test_no_agent_section(self):
        """Missing agent section uses defaults."""
        config = load_from_dict(_full_config_dict())
        assert config.enable_view_image is True
        assert config.background_auto_wait is False

    def test_agent_section_with_values(self):
        data = _full_config_dict(agent={"enable_view_image": False, "background_auto_wait": True})
        config = load_from_dict(data)
        assert config.enable_view_image is False
        assert config.background_auto_wait is True

    def test_agent_section_none(self):
        """YAML sections with only comments parse as None."""
        data = _full_config_dict(agent=None)
        config = load_from_dict(data)
        assert config.enable_view_image is True

    def test_no_skills_section(self):
        config = load_from_dict(_full_config_dict())
        assert config.skills.enabled is True
        assert config.skills.user_skills_dir == "~/.ptc-agent/skills"

    def test_custom_skills_section(self):
        data = _full_config_dict(skills={
            "enabled": False,
            "user_skills_dir": "/custom/skills",
            "project_skills_dir": "my-skills",
            "sandbox_skills_base": "/sandbox/skills",
        })
        config = load_from_dict(data)
        assert config.skills.enabled is False
        assert config.skills.user_skills_dir == "/custom/skills"

    def test_no_flash_section(self):
        config = load_from_dict(_full_config_dict())
        assert config.flash.enabled is True

    def test_flash_disabled(self):
        data = _full_config_dict(flash={"enabled": False})
        config = load_from_dict(data)
        assert config.flash.enabled is False


# ---------------------------------------------------------------------------
# load_from_dict — subagents
# ---------------------------------------------------------------------------


class TestLoadFromDictSubagents:
    def test_default_subagents(self):
        config = load_from_dict(_full_config_dict())
        assert config.subagents.enabled == ["general-purpose"]
        assert config.subagents.definitions == {}

    def test_custom_enabled_subagents(self):
        """Enable a built-in subagent by name."""
        data = _full_config_dict(subagents={
            "enabled": ["general-purpose"],
        })
        config = load_from_dict(data)
        assert "general-purpose" in config.subagents.enabled

    def test_unknown_subagent_raises(self):
        """Enabling an undefined subagent should raise."""
        data = _full_config_dict(subagents={
            "enabled": ["nonexistent-agent"],
        })
        with pytest.raises(ValueError, match="not defined"):
            load_from_dict(data)

    def test_user_defined_subagent(self):
        """User can define and enable custom subagents."""
        data = _full_config_dict(subagents={
            "enabled": ["general-purpose", "my-agent"],
            "definitions": {
                "my-agent": {
                    "description": "Custom agent",
                    "mode": "flash",
                    "tools": ["web_search"],
                },
            },
        })
        config = load_from_dict(data)
        assert "my-agent" in config.subagents.enabled
        assert config.subagents.definitions["my-agent"].mode == "flash"


# ---------------------------------------------------------------------------
# load_from_dict — MCP servers
# ---------------------------------------------------------------------------


class TestLoadFromDictMCP:
    def test_empty_servers(self):
        config = load_from_dict(_full_config_dict())
        assert config.mcp.servers == []

    def test_with_mcp_servers(self):
        data = _full_config_dict()
        data["mcp"]["servers"] = [
            {"name": "tavily", "command": "npx", "args": ["-y", "tavily-mcp"]},
            {"name": "finance", "command": "python", "args": ["server.py"], "enabled": False},
        ]
        config = load_from_dict(data)
        assert len(config.mcp.servers) == 2
        assert config.mcp.servers[0].name == "tavily"
        assert config.mcp.servers[1].enabled is False


# ---------------------------------------------------------------------------
# load_from_dict — filesystem
# ---------------------------------------------------------------------------


class TestLoadFromDictFilesystem:
    @pytest.fixture(autouse=True)
    def _clean_env(self, monkeypatch):
        monkeypatch.delenv("SANDBOX_PROVIDER", raising=False)

    def test_filesystem_defaults(self):
        config = load_from_dict(_full_config_dict())
        assert config.filesystem.working_directory == "/home/workspace"
        assert config.filesystem.allowed_directories == ["/home/workspace", "/tmp"]

    def test_filesystem_with_custom_working_dir(self):
        """Custom working_directory from config should be applied."""
        data = _full_config_dict()
        data["filesystem"]["working_directory"] = "/workspace"
        config = load_from_dict(data)
        assert config.filesystem.working_directory == "/workspace"

    def test_filesystem_denied_directories(self):
        data = _full_config_dict()
        data["filesystem"]["denied_directories"] = ["/secret"]
        config = load_from_dict(data)
        assert config.filesystem.denied_directories == ["/secret"]


# ---------------------------------------------------------------------------
# load_from_dict — security
# ---------------------------------------------------------------------------


class TestLoadFromDictSecurity:
    def test_uses_create_default_security_config(self):
        """load_from_dict uses create_default_security_config(), not SecurityConfig()."""
        config = load_from_dict(_full_config_dict())
        # create_default_security_config uses DEFAULT_ALLOWED_IMPORTS
        from ptc_agent.config.core import DEFAULT_ALLOWED_IMPORTS
        assert config.security.allowed_imports == DEFAULT_ALLOWED_IMPORTS


# ---------------------------------------------------------------------------
# load_from_dict — compaction + search_api
# ---------------------------------------------------------------------------


class TestLoadFromDictCompaction:
    def test_default_compaction(self):
        """No compaction section should produce defaults."""
        config = load_from_dict(_full_config_dict())
        assert config.compaction.enabled is True
        assert config.compaction.token_threshold == 120000
        assert config.compaction.keep_messages == 5

    def test_custom_compaction(self):
        data = _full_config_dict(compaction={
            "enabled": False,
            "token_threshold": 80000,
            "keep_messages": 3,
            "truncate_args_trigger_messages": 15,
            "truncate_args_keep_messages": 10,
            "truncate_args_max_length": 1000,
        })
        config = load_from_dict(data)
        assert config.compaction.enabled is False
        assert config.compaction.token_threshold == 80000
        assert config.compaction.keep_messages == 3
        assert config.compaction.truncate_args_trigger_messages == 15

    def test_partial_compaction(self):
        """Partial compaction section uses defaults for missing fields."""
        data = _full_config_dict(compaction={"enabled": True, "keep_messages": 10})
        config = load_from_dict(data)
        assert config.compaction.keep_messages == 10
        assert config.compaction.token_threshold == 120000  # default

    def test_default_search_api(self):
        config = load_from_dict(_full_config_dict())
        assert config.search_api == "tavily"

    def test_custom_search_api(self):
        data = _full_config_dict(search_api="serper")
        config = load_from_dict(data)
        assert config.search_api == "serper"


# ---------------------------------------------------------------------------
# load_from_files — config search
# ---------------------------------------------------------------------------


class TestLoadFromFiles:
    @pytest.mark.asyncio
    async def test_raises_when_not_found(self):
        """Should raise FileNotFoundError with searched paths when config not found."""
        from ptc_agent.config.loaders import load_from_files

        with patch(
            "ptc_agent.config.loaders.find_config_file",
            return_value=None,
        ):
            with pytest.raises(FileNotFoundError, match="agent_config.yaml not found"):
                await load_from_files(search_paths=True)

    @pytest.mark.asyncio
    async def test_loads_from_explicit_path(self, tmp_path):
        """Should load config from explicit file path."""
        import yaml
        from ptc_agent.config.loaders import load_from_files

        config_data = _full_config_dict()
        config_file = tmp_path / "agent_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        config = await load_from_files(config_file=config_file, search_paths=False)
        assert config.llm.name == "test-model"
        assert config.config_file_dir == tmp_path

    @pytest.mark.asyncio
    async def test_auto_generate(self, tmp_path):
        """auto_generate=True should create template when config not found."""
        from ptc_agent.config.loaders import load_from_files

        with (
            patch("ptc_agent.config.loaders.find_config_file", return_value=None),
            patch(
                "ptc_agent.config.loaders.get_default_config_dir",
                return_value=tmp_path,
            ),
        ):
            # auto_generate creates a template, but it has placeholder values
            # so load_from_dict will likely fail or succeed depending on the template
            # The test verifies the auto_generate path creates a file
            try:
                await load_from_files(auto_generate=True)
            except (ValueError, KeyError):
                pass  # Template has placeholder values that may not validate

            # Template file should have been created
            assert (tmp_path / "agent_config.yaml").exists()


# ---------------------------------------------------------------------------
# generate_config_template
# ---------------------------------------------------------------------------


class TestGenerateConfigTemplate:
    def test_creates_config_file(self, tmp_path):
        result = generate_config_template(tmp_path)
        assert "agent_config.yaml" in result
        assert result["agent_config.yaml"].exists()

    def test_raises_on_existing_no_overwrite(self, tmp_path):
        generate_config_template(tmp_path)
        with pytest.raises(FileExistsError):
            generate_config_template(tmp_path, overwrite=False)

    def test_overwrites_when_requested(self, tmp_path):
        generate_config_template(tmp_path)
        result = generate_config_template(tmp_path, overwrite=True)
        assert result["agent_config.yaml"].exists()

    def test_template_content_is_valid_yaml(self, tmp_path):
        import yaml

        result = generate_config_template(tmp_path)
        content = result["agent_config.yaml"].read_text()
        data = yaml.safe_load(content)
        # Template should have required sections
        assert "llm" in data
        assert "daytona" in data
        assert "mcp" in data
        assert "logging" in data
        assert "filesystem" in data
