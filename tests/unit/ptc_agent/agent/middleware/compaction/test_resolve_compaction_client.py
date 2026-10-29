"""Unit tests for ``resolve_compaction_client``.

Covers the four cases documented on the helper:
1. Compaction model set + role client stored → returns a distinct copy of it.
2. Compaction model set + no role client → None (platform users keep name-based model).
3. No compaction model + main llm_client set → copy of main client.
4. No compaction model + llm_client None → None.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace

from ptc_agent.config import AgentConfig, LLMConfig
from ptc_agent.config.core import (
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)
from ptc_agent.agent.middleware.compaction.model import resolve_compaction_client


@dataclass
class _FakeClient:
    """Minimal stand-in for a BaseChatModel with ``.model_copy()``."""

    name: str
    _tag: object = field(default_factory=object)

    def model_copy(self) -> "_FakeClient":
        return replace(self, _tag=object())


def _config(**overrides) -> AgentConfig:
    base = dict(
        llm=LLMConfig(name="main-model"),
        security=SecurityConfig(),
        logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        mcp=MCPConfig(),
        filesystem=FilesystemConfig(),
    )
    base.update(overrides)
    return AgentConfig(**base)


class TestResolveCompactionClient:
    def test_compaction_model_with_role_client_returns_distinct_copy(self):
        stored = _FakeClient(name="compaction-model")
        cfg = _config(
            llm=LLMConfig(name="main-model", compaction="some-compaction-model"),
            subsidiary_llm_clients={"compaction": stored},
        )

        result = resolve_compaction_client(cfg)

        assert result is not None
        assert result is not stored  # model_copy -> distinct identity
        assert result.name == stored.name

    def test_compaction_model_without_role_client_returns_none(self):
        # Platform users: compaction model name set but no pre-resolved client;
        # they should use the name-based path, not the main model.
        cfg = _config(
            llm=LLMConfig(name="main-model", compaction="some-compaction-model"),
            subsidiary_llm_clients={},
            llm_client=_FakeClient(name="main-client"),
        )

        result = resolve_compaction_client(cfg)

        assert result is None

    def test_no_compaction_model_with_main_client_returns_copy_of_main(self):
        main = _FakeClient(name="main-client")
        cfg = _config(
            llm=LLMConfig(name="main-model", compaction=None),
            subsidiary_llm_clients={},
            llm_client=main,
        )

        result = resolve_compaction_client(cfg)

        assert result is not None
        assert result is not main  # model_copy -> distinct identity
        assert result.name == main.name

    def test_no_compaction_model_no_main_client_returns_none(self):
        cfg = _config(
            llm=LLMConfig(name="main-model", compaction=None),
            subsidiary_llm_clients={},
            llm_client=None,
        )

        result = resolve_compaction_client(cfg)

        assert result is None
