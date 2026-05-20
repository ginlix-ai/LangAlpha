"""Tests for the RuntimeContextMiddleware.

Covers context block construction with/without user profile,
system message injection via awrap_model_call, and sync passthrough.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import SystemMessage

from ptc_agent.agent.middleware.runtime_context import RuntimeContextMiddleware


# ---------------------------------------------------------------------------
# Tests for _build_context_block
# ---------------------------------------------------------------------------


class TestBuildContextBlock:
    """Tests for _build_context_block."""

    def _make_middleware(self, *, current_time="3:42 PM EST, Monday, April 5, 2026", user_profile=None):
        return RuntimeContextMiddleware(
            current_time=current_time,
            user_profile=user_profile,
        )

    @patch("ptc_agent.agent.middleware.runtime_context.get_loader")
    def test_time_only(self, mock_get_loader):
        """Without user_profile, only time_awareness block is rendered."""
        loader = MagicMock()
        loader.render.return_value = "**Current Date/Time:** 3:42 PM EST"
        mock_get_loader.return_value = loader

        mw = self._make_middleware()

        assert "<time_awareness>" in mw._context_block
        assert "3:42 PM EST" in mw._context_block
        assert "<user_profile>" not in mw._context_block
        loader.render.assert_called_once_with(
            "components/time_awareness.md.j2",
            current_time="3:42 PM EST, Monday, April 5, 2026",
        )

    @patch("ptc_agent.agent.middleware.runtime_context.get_loader")
    def test_time_and_profile(self, mock_get_loader):
        """With user_profile, both blocks are rendered."""
        loader = MagicMock()
        loader.render.side_effect = [
            "**Current Date/Time:** 3:42 PM EST",
            "# User Profile\n- **Name**: Alice",
        ]
        mock_get_loader.return_value = loader

        profile = {"name": "Alice", "timezone": "US/Eastern", "locale": "en-US"}
        mw = self._make_middleware(user_profile=profile)

        assert "<time_awareness>" in mw._context_block
        assert "<user_profile>" in mw._context_block
        assert "Alice" in mw._context_block
        assert loader.render.call_count == 2

    @patch("ptc_agent.agent.middleware.runtime_context.get_loader")
    def test_empty_profile_excluded(self, mock_get_loader):
        """Empty dict user_profile is falsy — no profile block rendered."""
        loader = MagicMock()
        loader.render.return_value = "time content"
        mock_get_loader.return_value = loader

        mw = self._make_middleware(user_profile={})

        assert "<user_profile>" not in mw._context_block
        loader.render.assert_called_once()

    @patch("ptc_agent.agent.middleware.runtime_context.get_loader")
    def test_user_data_counts_only_renders_profile_block(self, mock_get_loader):
        """user_data_counts triggers the profile block even with no user_profile."""
        loader = MagicMock()
        loader.render.side_effect = ["time content", "rendered profile"]
        mock_get_loader.return_value = loader

        mw = RuntimeContextMiddleware(
            current_time="now",
            user_profile=None,
            user_data_counts={"portfolio_count": 5, "watchlist_summary": "2:12", "prefs_set": True},
        )

        assert "<user_profile>" in mw._context_block
        # The template render was called with the counts dict
        _, kwargs = loader.render.call_args_list[1]
        assert kwargs["user_data_counts"] == {
            "portfolio_count": 5, "watchlist_summary": "2:12", "prefs_set": True,
        }

    @patch("ptc_agent.agent.middleware.runtime_context.get_loader")
    def test_no_profile_no_counts_skips_block(self, mock_get_loader):
        """Without profile or counts, the user_profile block is omitted."""
        loader = MagicMock()
        loader.render.return_value = "time content"
        mock_get_loader.return_value = loader

        mw = RuntimeContextMiddleware(
            current_time="now",
            user_profile=None,
            user_data_counts=None,
        )

        assert "<user_profile>" not in mw._context_block
        loader.render.assert_called_once()


# ---------------------------------------------------------------------------
# Tests for awrap_model_call
# ---------------------------------------------------------------------------


class TestAwrapModelCall:
    """Tests for awrap_model_call system message injection."""

    @pytest.mark.asyncio
    @patch("ptc_agent.agent.middleware.runtime_context.get_loader")
    async def test_appends_context_to_system_message(self, mock_get_loader):
        loader = MagicMock()
        loader.render.return_value = "time content"
        mock_get_loader.return_value = loader

        mw = RuntimeContextMiddleware(current_time="now")

        mock_request = MagicMock()
        modified_request = MagicMock()
        mock_request.override = MagicMock(return_value=modified_request)
        mock_request.system_message = SystemMessage(content="base prompt")

        handler = AsyncMock(return_value="model_response")
        result = await mw.awrap_model_call(mock_request, handler)

        # override should have been called with a new system message
        mock_request.override.assert_called_once()
        call_kwargs = mock_request.override.call_args
        assert "system_message" in call_kwargs.kwargs
        new_sys = call_kwargs.kwargs["system_message"]
        assert isinstance(new_sys, SystemMessage)

        # handler should have been called with modified request
        handler.assert_called_once_with(modified_request)
        assert result == "model_response"

    @pytest.mark.asyncio
    @patch("ptc_agent.agent.middleware.runtime_context.get_loader")
    async def test_appends_to_none_system_message(self, mock_get_loader):
        """Works when system_message is None (creates a new one)."""
        loader = MagicMock()
        loader.render.return_value = "time content"
        mock_get_loader.return_value = loader

        mw = RuntimeContextMiddleware(current_time="now")

        mock_request = MagicMock()
        modified_request = MagicMock()
        mock_request.override = MagicMock(return_value=modified_request)
        mock_request.system_message = None

        handler = AsyncMock(return_value="model_response")
        await mw.awrap_model_call(mock_request, handler)

        call_kwargs = mock_request.override.call_args
        new_sys = call_kwargs.kwargs["system_message"]
        assert isinstance(new_sys, SystemMessage)


# ---------------------------------------------------------------------------
# Tests for wrap_model_call (sync passthrough)
# ---------------------------------------------------------------------------


class TestWrapModelCall:
    """Tests for sync wrap_model_call passthrough."""

    @patch("ptc_agent.agent.middleware.runtime_context.get_loader")
    def test_sync_passthrough(self, mock_get_loader):
        """Sync path passes request through without modification."""
        mw = RuntimeContextMiddleware(current_time="now")

        mock_request = MagicMock()
        handler = MagicMock(return_value="response")

        result = mw.wrap_model_call(mock_request, handler)

        handler.assert_called_once_with(mock_request)
        assert result == "response"
