"""Tests for src/tools/search.py — search engine selection and tool creation.

Tests the get_web_search_tool factory function's routing logic and
validation, plus the ToolUsageTracker used by search tool wrappers.
"""

import logging
from unittest.mock import MagicMock, patch

import pytest

from src.config.tools import SearchEngine
from src.tools.decorators import (
    ToolUsageTracker,
    start_tool_tracking,
    stop_tool_tracking,
    get_tool_tracker,
)


# ---------------------------------------------------------------------------
# Tests for SearchEngine enum
# ---------------------------------------------------------------------------


class TestSearchEngineEnum:
    """Tests for SearchEngine enum values."""

    def test_tavily_value(self):
        assert SearchEngine.TAVILY.value == "tavily"

    def test_serper_value(self):
        assert SearchEngine.SERPER.value == "serper"

    def test_bocha_value(self):
        assert SearchEngine.BOCHA.value == "bocha"

    def test_all_members(self):
        members = [e.value for e in SearchEngine]
        assert "tavily" in members
        assert "serper" in members
        assert "bocha" in members


# ---------------------------------------------------------------------------
# Tests for get_web_search_tool routing
# ---------------------------------------------------------------------------


class TestGetWebSearchToolRouting:
    """Tests for get_web_search_tool engine selection routing."""

    def test_serper_engine_calls_serper_configure(self):
        """When SELECTED_SEARCH_ENGINE is serper, serper configure is called."""
        mock_configure = MagicMock()
        mock_web_search = MagicMock()
        mock_tool = MagicMock()

        mock_serper_module = MagicMock(
            configure=mock_configure,
            web_search=mock_web_search,
        )

        with (
            patch("src.tools.search.SELECTED_SEARCH_ENGINE", SearchEngine.SERPER.value),
            patch.dict("sys.modules", {"src.tools.search_services.serper": mock_serper_module}),
            patch("src.tools.search.create_logged_tool", return_value=mock_tool) as mock_create,
        ):
            from src.tools.search import get_web_search_tool
            result = get_web_search_tool(max_search_results=5, time_range="w")

        mock_configure.assert_called_once_with(max_results=5, default_time_range="w")
        mock_create.assert_called_once()
        assert result == mock_tool

    def test_unsupported_engine_raises(self):
        """An unknown engine string raises ValueError."""
        with patch("src.tools.search.SELECTED_SEARCH_ENGINE", "unknown_engine"):
            from src.tools.search import get_web_search_tool
            with pytest.raises(ValueError, match="Unsupported search engine"):
                get_web_search_tool(max_search_results=5)

    def test_tavily_engine_calls_tavily_configure(self):
        """When SELECTED_SEARCH_ENGINE is tavily, tavily configure is called."""
        mock_configure = MagicMock()
        mock_web_search = MagicMock()
        mock_tool = MagicMock()

        mock_tavily_module = MagicMock(
            configure=mock_configure,
            web_search=mock_web_search,
        )

        with (
            patch("src.tools.search.SELECTED_SEARCH_ENGINE", SearchEngine.TAVILY.value),
            patch.dict("sys.modules", {"src.tools.search_services.tavily": mock_tavily_module}),
            patch("src.tools.search.create_logged_tool", return_value=mock_tool) as mock_create,
        ):
            from src.tools.search import get_web_search_tool
            result = get_web_search_tool(
                max_search_results=10, time_range="m", verbose=False
            )

        mock_configure.assert_called_once_with(
            max_results=10, default_time_range="m", verbose=False
        )


# ---------------------------------------------------------------------------
# Tests for the per-user `provider` override
# ---------------------------------------------------------------------------


class TestGetWebSearchToolProviderOverride:
    """The ``provider`` arg overrides ``SELECTED_SEARCH_ENGINE`` per request.

    A valid engine selects that provider's branch; an unknown string logs a
    warning and falls back to the deployment default; ``None`` is a no-op so
    the default engine is used. Provider modules read API keys lazily at call
    time, so building the tools needs no API keys (the modules are mocked here
    regardless, to assert which branch ran).
    """

    def _make_provider_module(self):
        """Build a mock provider module (configure + web_search).

        Returns ``(mock_configure, mock_module)``; the caller installs the
        module in sys.modules under the right import path.
        """
        mock_configure = MagicMock()
        mock_web_search = MagicMock()
        mock_module = MagicMock(configure=mock_configure, web_search=mock_web_search)
        return mock_configure, mock_module

    def test_provider_serper_selects_serper_branch(self):
        """provider='serper' (default engine is tavily) routes to serper."""
        mock_configure, mock_module = self._make_provider_module()
        mock_tool = MagicMock()
        with (
            patch("src.tools.search.SELECTED_SEARCH_ENGINE", SearchEngine.TAVILY.value),
            patch.dict("sys.modules", {"src.tools.search_services.serper": mock_module}),
            patch("src.tools.search.create_logged_tool", return_value=mock_tool) as mock_create,
        ):
            from src.tools.search import get_web_search_tool

            result = get_web_search_tool(max_search_results=5, provider="serper")

        mock_configure.assert_called_once_with(max_results=5, default_time_range=None)
        # The serper branch tags the tool with SerperSearchTool tracking name.
        assert mock_create.call_args.kwargs["tracking_name"] == "SerperSearchTool"
        assert mock_create.call_args.kwargs["name"] == "WebSearch"
        assert result is mock_tool

    def test_provider_tavily_selects_tavily_branch(self):
        """provider='tavily' (default engine is serper) routes to tavily."""
        mock_configure, mock_module = self._make_provider_module()
        mock_tool = MagicMock()
        with (
            patch("src.tools.search.SELECTED_SEARCH_ENGINE", SearchEngine.SERPER.value),
            patch.dict("sys.modules", {"src.tools.search_services.tavily": mock_module}),
            patch("src.tools.search.create_logged_tool", return_value=mock_tool) as mock_create,
        ):
            from src.tools.search import get_web_search_tool

            result = get_web_search_tool(max_search_results=7, provider="tavily")

        mock_configure.assert_called_once_with(
            max_results=7, default_time_range=None, verbose=True
        )
        assert mock_create.call_args.kwargs["tracking_name"] == "TavilySearchTool"
        assert result is mock_tool

    def test_provider_bocha_selects_bocha_branch(self):
        """provider='bocha' (default engine is tavily) routes to bocha."""
        mock_configure, mock_module = self._make_provider_module()
        mock_tool = MagicMock()
        with (
            patch("src.tools.search.SELECTED_SEARCH_ENGINE", SearchEngine.TAVILY.value),
            patch.dict("sys.modules", {"src.tools.search_services.bocha": mock_module}),
            patch("src.tools.search.create_logged_tool", return_value=mock_tool) as mock_create,
        ):
            from src.tools.search import get_web_search_tool

            result = get_web_search_tool(max_search_results=3, provider="bocha")

        mock_configure.assert_called_once_with(
            max_results=3, default_time_range=None, verbose=True
        )
        assert mock_create.call_args.kwargs["tracking_name"] == "BochaSearchTool"
        assert result is mock_tool

    def test_invalid_provider_falls_back_to_default_and_warns(self, caplog):
        """An unknown provider string logs a warning and falls back to the
        deployment default engine — it must not raise."""
        mock_configure, mock_module = self._make_provider_module()
        mock_tool = MagicMock()
        with (
            patch("src.tools.search.SELECTED_SEARCH_ENGINE", SearchEngine.SERPER.value),
            patch.dict("sys.modules", {"src.tools.search_services.serper": mock_module}),
            patch("src.tools.search.create_logged_tool", return_value=mock_tool) as mock_create,
            caplog.at_level(logging.WARNING, logger="src.tools.search"),
        ):
            from src.tools.search import get_web_search_tool

            result = get_web_search_tool(
                max_search_results=5, provider="not-a-real-engine"
            )

        # Fell back to the default (serper) branch.
        assert mock_create.call_args.kwargs["tracking_name"] == "SerperSearchTool"
        assert result is mock_tool
        # And warned about the unknown provider.
        assert any(
            "not-a-real-engine" in rec.getMessage() for rec in caplog.records
        )

    def test_provider_none_uses_default_engine(self):
        """provider=None behaves exactly as before — the default engine is used
        with no fallback warning."""
        mock_configure, mock_module = self._make_provider_module()
        mock_tool = MagicMock()
        with (
            patch("src.tools.search.SELECTED_SEARCH_ENGINE", SearchEngine.TAVILY.value),
            patch.dict("sys.modules", {"src.tools.search_services.tavily": mock_module}),
            patch("src.tools.search.create_logged_tool", return_value=mock_tool) as mock_create,
        ):
            from src.tools.search import get_web_search_tool

            result = get_web_search_tool(max_search_results=5, provider=None)

        mock_configure.assert_called_once_with(
            max_results=5, default_time_range=None, verbose=True
        )
        assert mock_create.call_args.kwargs["tracking_name"] == "TavilySearchTool"
        assert result is mock_tool


# ---------------------------------------------------------------------------
# Tests for ToolUsageTracker
# ---------------------------------------------------------------------------


class TestToolUsageTracker:
    """Tests for the ToolUsageTracker used by search tool wrappers."""

    def test_record_usage_increments(self):
        tracker = ToolUsageTracker()
        tracker.record_usage("SerperSearchTool", count=1)
        tracker.record_usage("SerperSearchTool", count=2)
        assert tracker.usage["SerperSearchTool"] == 3

    def test_get_summary(self):
        tracker = ToolUsageTracker()
        tracker.record_usage("ToolA", count=5)
        summary = tracker.get_summary()
        assert isinstance(summary, dict)
        assert summary["ToolA"] == 5

    def test_reset_clears_usage(self):
        tracker = ToolUsageTracker()
        tracker.record_usage("ToolA", count=3)
        tracker.reset()
        assert tracker.get_summary() == {}

    def test_zero_count_not_recorded(self):
        tracker = ToolUsageTracker()
        tracker.record_usage("ToolA", count=0)
        assert "ToolA" not in tracker.usage

    def test_repr(self):
        tracker = ToolUsageTracker()
        tracker.record_usage("A", 2)
        tracker.record_usage("B", 3)
        r = repr(tracker)
        assert "tools=2" in r
        assert "total_calls=5" in r


class TestToolTrackingContextVar:
    """Tests for start/stop/get tool tracking via ContextVar."""

    def test_start_and_get(self):
        tracker = start_tool_tracking()
        assert get_tool_tracker() is tracker
        # Cleanup
        stop_tool_tracking()

    def test_stop_returns_summary(self):
        tracker = start_tool_tracking()
        tracker.record_usage("SearchTool", 2)
        summary = stop_tool_tracking()
        assert summary == {"SearchTool": 2}
        # After stop, tracker should be gone
        assert get_tool_tracker() is None

    def test_stop_without_start_returns_none(self):
        # Ensure no tracker is active
        stop_tool_tracking()
        result = stop_tool_tracking()
        assert result is None
