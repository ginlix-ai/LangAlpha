"""Tests for SubagentCompiler credential injection (Task 7).

Verifies that a resolved BaseChatModel client from AgentConfig.subsidiary_llm_clients
reaches the compiled SubAgent dict that subagent.py:432 consumes.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace

import pytest

from ptc_agent.agent.subagents.compiler import SubagentCompiler
from ptc_agent.agent.subagents.definition import SubagentDefinition
from ptc_agent.config import AgentConfig, LLMConfig
from ptc_agent.config.core import (
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)


# ── Helpers ───────────────────────────────────────────────────────────────────


@dataclass
class _FakeClient:
    """Minimal BaseChatModel stand-in with ``.model_copy()``."""

    name: str
    _tag: object = field(default_factory=object)

    def model_copy(self) -> "_FakeClient":
        return replace(self, _tag=object())


def _config(**overrides) -> AgentConfig:
    base = dict(
        llm=LLMConfig(name="main-placeholder"),
        security=SecurityConfig(),
        logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        mcp=MCPConfig(),
        filesystem=FilesystemConfig(),
    )
    base.update(overrides)
    return AgentConfig(**base)


def _defn(name: str = "research", model: str | None = "cheap-placeholder") -> SubagentDefinition:
    return SubagentDefinition(
        name=name,
        description="test subagent",
        custom_prompt="stub prompt",
        model=model,
    )


def _compiler(**kwargs) -> SubagentCompiler:
    """Build a compiler with no sandbox/mcp so compile() never touches FS."""
    return SubagentCompiler(**kwargs)


# ── Tests ─────────────────────────────────────────────────────────────────────


class TestSubagentCompilerClientInjection:
    def test_instance_injection(self):
        """Resolved client instance replaces the string model name."""
        stored = _FakeClient(name="byok-subagent-model")
        cfg = _config(subsidiary_llm_clients={"subagent:research": stored})
        compiler = _compiler(config=cfg)

        result = compiler.compile(_defn(name="research", model="cheap-placeholder"))

        # model key must be a client instance, not the string
        assert isinstance(result["model"], _FakeClient)
        assert result["model"] is not stored  # model_copy -> distinct object
        assert result["model"].name == stored.name

    def test_string_fallback_when_no_subagent_client(self):
        """Falls back to definition.model string when subsidiary_llm_clients has no entry."""
        cfg = _config(subsidiary_llm_clients={})
        compiler = _compiler(config=cfg)

        result = compiler.compile(_defn(name="research", model="cheap-placeholder"))

        assert result["model"] == "cheap-placeholder"

    def test_back_compat_no_config(self):
        """SubagentCompiler(config=None) behaves exactly as before — string passthrough."""
        compiler = _compiler()  # no config kwarg

        result = compiler.compile(_defn(name="research", model="cheap-placeholder"))

        assert result["model"] == "cheap-placeholder"

    def test_back_compat_no_model_no_config(self):
        """definition.model=None and no config → no 'model' key."""
        compiler = _compiler()

        result = compiler.compile(_defn(name="research", model=None))

        assert "model" not in result

    def test_consumption_shape(self):
        """agent_.get('model', default_model) returns the injected instance (exact consumer expression)."""
        stored = _FakeClient(name="byok-subagent-model")
        cfg = _config(subsidiary_llm_clients={"subagent:worker": stored})
        compiler = _compiler(config=cfg)

        agent_ = compiler.compile(_defn(name="worker", model="cheap-placeholder"))
        default_model = "default-placeholder"

        # This is the exact expression from subagent.py:432
        subagent_model = agent_.get("model", default_model)

        assert isinstance(subagent_model, _FakeClient)
        assert subagent_model.name == stored.name


# ── Prompt feature gating ─────────────────────────────────────────────────────


def _base_path_defn() -> SubagentDefinition:
    """Definition with no custom prompt → base template + tool_guide render."""
    return SubagentDefinition(
        name="research",
        description="test subagent",
        role_prompt="Do research.",
    )


class TestSubagentPromptFeatureGating:
    """The prompt must mirror the tool binding: watch_market is gated out of
    the finance tool set per user (agent.py), so a subagent prompt advertising
    it with the flag off would drive calls to a missing tool."""

    def test_watch_market_absent_when_flag_off(self):
        compiler = _compiler(config=_config(features={"market_watch": False}))
        result = compiler.compile(_base_path_defn())
        assert "watch_market" not in result["system_prompt"]

    def test_watch_market_present_when_flag_on(self):
        compiler = _compiler(config=_config(features={"market_watch": True}))
        result = compiler.compile(_base_path_defn())
        assert "watch_market" in result["system_prompt"]

    def test_no_config_fails_closed(self):
        compiler = _compiler()
        result = compiler.compile(_base_path_defn())
        assert "watch_market" not in result["system_prompt"]
