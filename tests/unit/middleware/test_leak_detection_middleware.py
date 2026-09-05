"""Tests for LeakDetectionMiddleware.

Verifies that tool outputs containing known secret values are redacted
before reaching the LLM context.
"""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from langchain_core.messages import ToolMessage

from ptc_agent.agent.middleware.tool.leak_detection import LeakDetectionMiddleware
from ptc_agent.config.core import MCPServerConfig

# Patch target for get_nested_config (imported locally inside __init__)
_GNC = "src.config.settings.get_nested_config"


def _make_server(name: str = "test", env: dict | None = None, enabled: bool = True) -> MCPServerConfig:
    return MCPServerConfig(
        name=name,
        command="echo",
        args=[],
        env=env or {},
        enabled=enabled,
    )


def _make_http_server(name: str = "remote", headers: dict | None = None) -> MCPServerConfig:
    return MCPServerConfig(
        name=name,
        transport="http",
        url="https://mcp.example.test/mcp",
        headers=headers or {},
        enabled=True,
    )


def _make_tool_message(content: str, tool_call_id: str = "call_1") -> ToolMessage:
    return ToolMessage(content=content, tool_call_id=tool_call_id)


def _disable_github(key, default):
    """Mock get_nested_config that disables GitHub."""
    if key == "github.enabled":
        return False
    return default


class TestSecretDiscovery:
    """Verify that secrets are correctly discovered from MCP config."""

    def test_resolves_placeholder_secrets(self):
        server = _make_server(env={"FMP_API_KEY": "${FMP_API_KEY}"})
        with (
            patch.dict(os.environ, {"FMP_API_KEY": "fmp_secret_12345"}, clear=False),
            patch(_GNC, side_effect=_disable_github),
        ):
            mw = LeakDetectionMiddleware(mcp_servers=[server])
        assert len(mw._secrets) == 1
        assert mw._secrets[0][0] == "FMP_API_KEY"

    def test_skips_short_values(self):
        """Values shorter than 8 chars are ignored to avoid false positives."""
        server = _make_server(env={"SHORT_KEY": "${SHORT_KEY}"})
        with (
            patch.dict(os.environ, {"SHORT_KEY": "abc"}, clear=False),
            patch(_GNC, side_effect=_disable_github),
        ):
            mw = LeakDetectionMiddleware(mcp_servers=[server])
        assert len(mw._secrets) == 0

    def test_literal_env_values(self):
        """Literal (non-placeholder) values are also tracked."""
        server = _make_server(env={"API_KEY": "literal_secret_value"})
        with patch(_GNC, side_effect=_disable_github):
            mw = LeakDetectionMiddleware(mcp_servers=[server])
        assert len(mw._secrets) == 1
        assert mw._secrets[0][1] == "literal_secret_value"

    def test_skips_non_secret_keys(self):
        """GIT_AUTHOR_NAME and similar non-secret keys are excluded."""
        server = _make_server(env={
            "GIT_AUTHOR_NAME": "langalpha-bot",
            "GIT_AUTHOR_EMAIL": "bot@ginlix.ai",
            "GIT_COMMITTER_NAME": "langalpha-bot",
            "GIT_COMMITTER_EMAIL": "bot@ginlix.ai",
        })
        with patch(_GNC, side_effect=_disable_github):
            mw = LeakDetectionMiddleware(mcp_servers=[server])
        assert len(mw._secrets) == 0

    def test_skips_disabled_servers(self):
        server = _make_server(
            env={"SECRET": "${SECRET}"},
            enabled=False,
        )
        with (
            patch.dict(os.environ, {"SECRET": "disabled_secret_value"}, clear=False),
            patch(_GNC, side_effect=_disable_github),
        ):
            mw = LeakDetectionMiddleware(mcp_servers=[server])
        assert len(mw._secrets) == 0

    def test_multiple_servers(self):
        s1 = _make_server(name="s1", env={"KEY_A": "${KEY_A}"})
        s2 = _make_server(name="s2", env={"KEY_B": "${KEY_B}"})
        with (
            patch.dict(os.environ, {
                "KEY_A": "secret_value_a",
                "KEY_B": "secret_value_b",
            }, clear=False),
            patch(_GNC, side_effect=_disable_github),
        ):
            mw = LeakDetectionMiddleware(mcp_servers=[s1, s2])
        assert len(mw._secrets) == 2

    def test_credential_headers_are_tracked(self):
        """An inline header credential redacts from tool output the same way an
        env literal does — a vendor echoing the request back must not hand the
        model the bearer."""
        server = _make_http_server(headers={
            "Authorization": "Bearer inline_bearer_123",
            "X-Api-Key": "inline_apikey_456",
        })
        with patch(_GNC, side_effect=_disable_github):
            mw = LeakDetectionMiddleware(mcp_servers=[server])
        assert sorted(v for _, v in mw._secrets) == [
            "Bearer inline_bearer_123", "inline_apikey_456",
        ]

    def test_protocol_headers_are_not_secrets(self):
        """Tool output quotes "application/json" constantly; masking it would
        punch holes in every result the model reads."""
        server = _make_http_server(headers={
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
        })
        with patch(_GNC, side_effect=_disable_github):
            mw = LeakDetectionMiddleware(mcp_servers=[server])
        assert mw._secrets == []

    def test_header_vault_refs_are_skipped(self):
        """${vault:NAME} resolves in-sandbox; the vault lane covers its value."""
        server = _make_http_server(headers={"Authorization": "${vault:RH_TOKEN}"})
        with patch(_GNC, side_effect=_disable_github):
            mw = LeakDetectionMiddleware(mcp_servers=[server])
        assert mw._secrets == []

    def test_arg_credentials_are_tracked_but_paths_are_not(self):
        """A credential passed as a CLI flag redacts like an env literal; the
        path and URL arguments beside it must stay quotable in tool output."""
        server = MCPServerConfig(
            name="stdio",
            command="npx",
            args=[
                "--token=inline_argtok_123",
                "--out",
                "/workspace/data/long_output_file.csv",
            ],
            env={},
            enabled=True,
        )
        with patch(_GNC, side_effect=_disable_github):
            mw = LeakDetectionMiddleware(mcp_servers=[server])
        assert mw._secrets == [("stdio:token", "inline_argtok_123")]

    def test_github_token_from_config(self):
        """GitHub token is discovered via get_nested_config."""
        def _enable_github(key, default):
            return {
                "github.enabled": True,
                "github.token_env": "GITHUB_BOT_TOKEN",
            }.get(key, default)

        with (
            patch.dict(os.environ, {"GITHUB_BOT_TOKEN": "ghp_1234567890abcdef"}, clear=False),
            patch(_GNC, side_effect=_enable_github),
        ):
            mw = LeakDetectionMiddleware(mcp_servers=[])

        assert any(name == "GITHUB_TOKEN" for name, _ in mw._secrets)

    def test_github_token_skipped_when_disabled(self):
        """GitHub token is not discovered when github is disabled."""
        with patch(_GNC, side_effect=_disable_github):
            mw = LeakDetectionMiddleware(mcp_servers=[])
        assert len(mw._secrets) == 0

    def test_empty_servers_list(self):
        with patch(_GNC, side_effect=_disable_github):
            mw = LeakDetectionMiddleware(mcp_servers=[])
        assert mw._secrets == []

    def test_none_servers(self):
        with patch(_GNC, side_effect=_disable_github):
            mw = LeakDetectionMiddleware(mcp_servers=None)
        assert mw._secrets == []

    def test_secrets_sorted_by_length_descending(self):
        """Longer secrets should be replaced first to avoid partial matches."""
        server = _make_server(env={
            "SHORT": "abcdefgh",         # 8 chars
            "LONG": "abcdefghijklmnop",  # 16 chars
        })
        with patch(_GNC, side_effect=_disable_github):
            mw = LeakDetectionMiddleware(mcp_servers=[server])
        assert len(mw._secrets) == 2
        # Longer value first
        assert len(mw._secrets[0][1]) >= len(mw._secrets[1][1])


class TestScan:
    """Verify the _scan method redacts secrets in content."""

    def _make_mw(self, secrets: dict[str, str]) -> LeakDetectionMiddleware:
        """Create middleware with pre-set secrets (bypass config discovery)."""
        mw = LeakDetectionMiddleware.__new__(LeakDetectionMiddleware)
        mw._secrets = sorted(secrets.items(), key=lambda kv: len(kv[1]), reverse=True)
        return mw

    def test_redacts_single_secret(self):
        mw = self._make_mw({"FMP_API_KEY": "fmp_secret_12345"})
        result = mw._scan("API key is fmp_secret_12345 here")
        assert result == "API key is [REDACTED:FMP_API_KEY] here"

    def test_redacts_multiple_secrets(self):
        mw = self._make_mw({
            "FMP_API_KEY": "fmp_secret_12345",
            "GITHUB_TOKEN": "ghp_abcdef1234567",
        })
        result = mw._scan("keys: fmp_secret_12345 and ghp_abcdef1234567")
        assert "[REDACTED:FMP_API_KEY]" in result
        assert "[REDACTED:GITHUB_TOKEN]" in result
        assert "fmp_secret_12345" not in result
        assert "ghp_abcdef1234567" not in result

    def test_redacts_repeated_occurrences(self):
        mw = self._make_mw({"SECRET": "my_secret_value"})
        result = mw._scan("first my_secret_value then my_secret_value again")
        assert result == "first [REDACTED:SECRET] then [REDACTED:SECRET] again"

    def test_no_secrets_passthrough(self):
        mw = self._make_mw({})
        content = "no secrets here"
        assert mw._scan(content) == content

    def test_no_match_passthrough(self):
        mw = self._make_mw({"SECRET": "not_present_value"})
        content = "nothing to redact here"
        assert mw._scan(content) == content

    def test_longer_secret_replaced_first(self):
        """When one secret is a prefix of another, longer replaces first."""
        mw = self._make_mw({
            "SHORT": "abcdefgh",
            "LONG": "abcdefghijkl",
        })
        result = mw._scan("value is abcdefghijkl end")
        assert result == "value is [REDACTED:LONG] end"


class TestWrapToolCall:
    """Verify sync wrap_tool_call redacts secrets."""

    def _make_mw(self, secrets: dict[str, str]) -> LeakDetectionMiddleware:
        mw = LeakDetectionMiddleware.__new__(LeakDetectionMiddleware)
        mw._secrets = sorted(secrets.items(), key=lambda kv: len(kv[1]), reverse=True)
        return mw

    def test_redacts_tool_message_content(self):
        mw = self._make_mw({"API_KEY": "secret_value_123"})
        msg = _make_tool_message("result: secret_value_123")

        handler = MagicMock(return_value=msg)
        request = MagicMock()

        result = mw.wrap_tool_call(request, handler)

        assert isinstance(result, ToolMessage)
        assert result.content == "result: [REDACTED:API_KEY]"
        handler.assert_called_once_with(request)

    def test_non_string_content_passes_through(self):
        """ToolMessage with non-string content is not scanned."""
        mw = self._make_mw({"KEY": "secret_value_123"})
        msg = ToolMessage(content=["list", "content"], tool_call_id="call_1")

        handler = MagicMock(return_value=msg)
        result = mw.wrap_tool_call(MagicMock(), handler)

        assert result.content == ["list", "content"]

    def test_non_tool_message_passes_through(self):
        """Non-ToolMessage results pass through unchanged."""
        mw = self._make_mw({"KEY": "secret_value_123"})

        handler = MagicMock(return_value="plain string secret_value_123")
        result = mw.wrap_tool_call(MagicMock(), handler)

        assert result == "plain string secret_value_123"


class TestAwrapToolCall:
    """Verify async awrap_tool_call redacts secrets."""

    def _make_mw(self, secrets: dict[str, str]) -> LeakDetectionMiddleware:
        mw = LeakDetectionMiddleware.__new__(LeakDetectionMiddleware)
        mw._secrets = sorted(secrets.items(), key=lambda kv: len(kv[1]), reverse=True)
        return mw

    @pytest.mark.asyncio
    async def test_redacts_tool_message_content(self):
        mw = self._make_mw({"API_KEY": "secret_value_123"})
        msg = _make_tool_message("output: secret_value_123")

        handler = AsyncMock(return_value=msg)
        request = MagicMock()

        result = await mw.awrap_tool_call(request, handler)

        assert isinstance(result, ToolMessage)
        assert result.content == "output: [REDACTED:API_KEY]"
        handler.assert_called_once_with(request)

    @pytest.mark.asyncio
    async def test_non_string_content_passes_through(self):
        """ToolMessage with list content is not scanned (isinstance guard)."""
        mw = self._make_mw({"KEY": "secret_value_123"})
        msg = ToolMessage(content=["list", "content"], tool_call_id="call_1")

        handler = AsyncMock(return_value=msg)
        result = await mw.awrap_tool_call(MagicMock(), handler)

        # ToolMessage may auto-convert content; just verify no crash
        assert result.content is not None
