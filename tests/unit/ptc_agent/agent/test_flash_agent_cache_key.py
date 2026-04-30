"""Regression test: ``FlashAgent`` must forward ``config.cache_key`` to
``create_llm`` on the lazy platform path.

When no OAuth/BYOK/reasoning branch pre-built ``llm_client``, the FlashAgent
init path falls through to ``create_llm(config.llm.flash)``. Without
forwarding ``cache_key``, flash-mode chats silently omit ``prompt_cache_key``
on OpenAI/Codex providers while PTC mode (via ``AgentConfig.get_llm_client()``)
gets it — flash and PTC must behave the same way.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from ptc_agent.config import AgentConfig, LLMConfig
from ptc_agent.config.core import (
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)


def _flash_config(cache_key: str | None) -> AgentConfig:
    """Minimal AgentConfig with flash model set and llm_client=None so the
    FlashAgent init path falls through to ``create_llm(...)``."""
    return AgentConfig(
        llm=LLMConfig(name="flash-model", flash="flash-model"),
        security=SecurityConfig(),
        logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        mcp=MCPConfig(),
        filesystem=FilesystemConfig(),
        cache_key=cache_key,
    )


class TestFlashAgentForwardsCacheKey:
    def test_flash_platform_path_forwards_cache_key(self):
        """Lazy create_llm path must receive ``cache_key=config.cache_key``."""
        from ptc_agent.agent.flash.agent import FlashAgent

        config = _flash_config(cache_key="thread-flash-1")
        mock_llm = MagicMock(name="flash-llm-client")
        with (
            patch(
                "src.llms.llm.ensure_model_in_manifest", return_value=None
            ),
            patch("src.llms.create_llm", return_value=mock_llm) as mock_create,
        ):
            FlashAgent(config)

        mock_create.assert_called_once()
        args, kwargs = mock_create.call_args
        # The model name is positional in the existing call shape.
        assert args[0] == "flash-model"
        assert kwargs.get("cache_key") == "thread-flash-1"

    def test_flash_platform_path_with_no_cache_key_passes_none(self):
        """When no cache_key was stashed (e.g. utility path), still forward None
        so factory behavior is identical to the pre-fix baseline."""
        from ptc_agent.agent.flash.agent import FlashAgent

        config = _flash_config(cache_key=None)
        mock_llm = MagicMock(name="flash-llm-client")
        with (
            patch(
                "src.llms.llm.ensure_model_in_manifest", return_value=None
            ),
            patch("src.llms.create_llm", return_value=mock_llm) as mock_create,
        ):
            FlashAgent(config)

        _args, kwargs = mock_create.call_args
        assert kwargs.get("cache_key") is None

    def test_flash_with_prebuilt_client_skips_factory(self):
        """If OAuth/BYOK already populated llm_client, FlashAgent must use it
        directly and not call create_llm at all."""
        from ptc_agent.agent.flash.agent import FlashAgent

        config = _flash_config(cache_key="thread-flash-2")
        prebuilt = MagicMock(name="prebuilt-flash-llm")
        config.llm_client = prebuilt

        with patch("src.llms.create_llm") as mock_create:
            agent = FlashAgent(config)

        assert agent.llm is prebuilt
        mock_create.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
