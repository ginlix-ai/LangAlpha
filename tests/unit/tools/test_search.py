"""Tests for src/tools/search.py — search engine selection and tool creation.

Tests the get_web_search_tool factory function's routing logic, depth
resolution, and tracking-name assignment, plus the ToolUsageTracker used by
search tool wrappers.
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

    def test_manifest_matches_enum(self):
        """Every manifest provider has an enum member and vice versa."""
        from src.tools.search_manifest import get_search_providers

        assert set(get_search_providers()) == {e.value for e in SearchEngine}

    def test_every_manifest_provider_has_a_builder(self):
        from src.tools.search import _PROVIDER_BUILDERS
        from src.tools.search_manifest import get_search_providers

        assert set(get_search_providers()) == set(_PROVIDER_BUILDERS)


# ---------------------------------------------------------------------------
# Helpers for routing tests
# ---------------------------------------------------------------------------


def _make_provider_module():
    """Build a mock provider module exposing build_web_search_tool.

    Returns ``(mock_build, mock_module)``; the caller installs the module in
    sys.modules under the right import path.
    """
    mock_build = MagicMock(return_value=MagicMock(name="tool_fn"))
    mock_module = MagicMock(build_web_search_tool=mock_build)
    return mock_build, mock_module


# ---------------------------------------------------------------------------
# Tests for get_web_search_tool routing
# ---------------------------------------------------------------------------


class TestGetWebSearchToolRouting:
    """Tests for get_web_search_tool engine selection routing."""

    def test_serper_engine_calls_serper_builder(self):
        """When SELECTED_SEARCH_ENGINE is serper, the serper builder runs."""
        mock_build, mock_module = _make_provider_module()
        mock_tool = MagicMock()

        with (
            patch("src.tools.search.SELECTED_SEARCH_ENGINE", SearchEngine.SERPER.value),
            patch.dict("sys.modules", {"src.tools.search_services.serper": mock_module}),
            patch("src.tools.search.create_logged_tool", return_value=mock_tool) as mock_create,
        ):
            from src.tools.search import get_web_search_tool
            result = get_web_search_tool(max_search_results=5, time_range="w")

        mock_build.assert_called_once_with(
            max_results=5, default_time_range="w", verbose=True
        )
        mock_create.assert_called_once()
        assert result == mock_tool

    def test_unsupported_engine_raises(self):
        """An unknown deployment-default engine string raises ValueError."""
        with patch("src.tools.search.SELECTED_SEARCH_ENGINE", "unknown_engine"):
            from src.tools.search import get_web_search_tool
            with pytest.raises(ValueError, match="Unsupported search engine"):
                get_web_search_tool(max_search_results=5)

    def test_tavily_engine_calls_tavily_builder_with_default_depth(self):
        """Tavily without an explicit depth gets the manifest default level's
        native params (standard → search_depth='basic')."""
        mock_build, mock_module = _make_provider_module()
        mock_tool = MagicMock()

        with (
            patch("src.tools.search.SELECTED_SEARCH_ENGINE", SearchEngine.TAVILY.value),
            patch.dict("sys.modules", {"src.tools.search_services.tavily": mock_module}),
            patch("src.tools.search.create_logged_tool", return_value=mock_tool),
        ):
            from src.tools.search import get_web_search_tool
            result = get_web_search_tool(
                max_search_results=10, time_range="m", verbose=False
            )

        mock_build.assert_called_once_with(
            max_results=10, default_time_range="m", verbose=False, search_depth="basic"
        )
        assert result is mock_tool


# ---------------------------------------------------------------------------
# Tests for the per-user `provider` override
# ---------------------------------------------------------------------------


class TestGetWebSearchToolProviderOverride:
    """The ``provider`` arg overrides ``SELECTED_SEARCH_ENGINE`` per request.

    A valid engine selects that provider's builder; an unknown string logs a
    warning and falls back to the deployment default; ``None`` is a no-op so
    the default engine is used. Provider modules read API keys lazily at call
    time, so building the tools needs no API keys (the modules are mocked here
    regardless, to assert which builder ran).
    """

    def test_provider_serper_selects_serper_builder(self):
        """provider='serper' (default engine is tavily) routes to serper."""
        mock_build, mock_module = _make_provider_module()
        mock_tool = MagicMock()
        with (
            patch("src.tools.search.SELECTED_SEARCH_ENGINE", SearchEngine.TAVILY.value),
            patch.dict("sys.modules", {"src.tools.search_services.serper": mock_module}),
            patch("src.tools.search.create_logged_tool", return_value=mock_tool) as mock_create,
        ):
            from src.tools.search import get_web_search_tool

            result = get_web_search_tool(max_search_results=5, provider="serper")

        mock_build.assert_called_once_with(
            max_results=5, default_time_range=None, verbose=True
        )
        # Single-depth providers keep the bare tracking key.
        assert mock_create.call_args.kwargs["tracking_name"] == "SerperSearchTool"
        assert mock_create.call_args.kwargs["name"] == "WebSearch"
        assert result is mock_tool

    def test_provider_tavily_selects_tavily_builder(self):
        """provider='tavily' (default engine is serper) routes to tavily."""
        mock_build, mock_module = _make_provider_module()
        mock_tool = MagicMock()
        with (
            patch("src.tools.search.SELECTED_SEARCH_ENGINE", SearchEngine.SERPER.value),
            patch.dict("sys.modules", {"src.tools.search_services.tavily": mock_module}),
            patch("src.tools.search.create_logged_tool", return_value=mock_tool) as mock_create,
        ):
            from src.tools.search import get_web_search_tool

            result = get_web_search_tool(max_search_results=7, provider="tavily")

        mock_build.assert_called_once_with(
            max_results=7, default_time_range=None, verbose=True, search_depth="basic"
        )
        # Multi-depth providers get a depth-qualified tracking key.
        assert mock_create.call_args.kwargs["tracking_name"] == "TavilySearchTool:standard"
        assert result is mock_tool

    def test_provider_bocha_selects_bocha_builder(self):
        """provider='bocha' (default engine is tavily) routes to bocha."""
        mock_build, mock_module = _make_provider_module()
        mock_tool = MagicMock()
        with (
            patch("src.tools.search.SELECTED_SEARCH_ENGINE", SearchEngine.TAVILY.value),
            patch.dict("sys.modules", {"src.tools.search_services.bocha": mock_module}),
            patch("src.tools.search.create_logged_tool", return_value=mock_tool) as mock_create,
        ):
            from src.tools.search import get_web_search_tool

            result = get_web_search_tool(max_search_results=3, provider="bocha")

        mock_build.assert_called_once_with(
            max_results=3, default_time_range=None, verbose=True
        )
        assert mock_create.call_args.kwargs["tracking_name"] == "BochaSearchTool"
        assert result is mock_tool

    def test_invalid_provider_falls_back_to_default_and_warns(self, caplog):
        """An unknown provider string logs a warning and falls back to the
        deployment default engine — it must not raise."""
        mock_build, mock_module = _make_provider_module()
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

    def test_manifest_provider_without_builder_falls_back(self, caplog):
        """A provider present in the manifest but missing from
        _PROVIDER_BUILDERS (deployment-edited manifest ahead of the module)
        degrades to the default engine instead of raising per request."""
        mock_build, mock_module = _make_provider_module()
        mock_tool = MagicMock()
        builders = {"serper": __import__("src.tools.search", fromlist=["x"])._build_serper}
        with (
            patch("src.tools.search.SELECTED_SEARCH_ENGINE", SearchEngine.SERPER.value),
            patch("src.tools.search._PROVIDER_BUILDERS", builders),
            patch.dict("sys.modules", {"src.tools.search_services.serper": mock_module}),
            patch("src.tools.search.create_logged_tool", return_value=mock_tool) as mock_create,
            caplog.at_level(logging.WARNING, logger="src.tools.search"),
        ):
            from src.tools.search import get_web_search_tool

            # "tavily" is in the manifest, but not in the patched builders.
            result = get_web_search_tool(max_search_results=5, provider="tavily")

        assert mock_create.call_args.kwargs["tracking_name"] == "SerperSearchTool"
        assert result is mock_tool
        assert any("tavily" in rec.getMessage() for rec in caplog.records)

    def test_provider_none_uses_default_engine(self):
        """provider=None behaves exactly as before — the default engine is used
        with no fallback warning."""
        mock_build, mock_module = _make_provider_module()
        mock_tool = MagicMock()
        with (
            patch("src.tools.search.SELECTED_SEARCH_ENGINE", SearchEngine.TAVILY.value),
            patch.dict("sys.modules", {"src.tools.search_services.tavily": mock_module}),
            patch("src.tools.search.create_logged_tool", return_value=mock_tool) as mock_create,
        ):
            from src.tools.search import get_web_search_tool

            result = get_web_search_tool(max_search_results=5, provider=None)

        mock_build.assert_called_once_with(
            max_results=5, default_time_range=None, verbose=True, search_depth="basic"
        )
        assert mock_create.call_args.kwargs["tracking_name"] == "TavilySearchTool:standard"
        assert result is mock_tool


# ---------------------------------------------------------------------------
# Tests for the per-user `depth` override
# ---------------------------------------------------------------------------


class TestGetWebSearchToolDepth:
    """``depth`` selects a manifest level: native params flow to the builder
    and the tracking name is depth-qualified for multi-depth providers."""

    @pytest.mark.parametrize(
        ("depth", "expected_native", "expected_tracking"),
        [
            ("ultra_fast", "ultra-fast", "TavilySearchTool:ultra_fast"),
            ("fast", "fast", "TavilySearchTool:fast"),
            ("standard", "basic", "TavilySearchTool:standard"),
            ("deep", "advanced", "TavilySearchTool:deep"),
        ],
    )
    def test_tavily_depth_levels(self, depth, expected_native, expected_tracking):
        mock_build, mock_module = _make_provider_module()
        mock_tool = MagicMock()
        with (
            patch("src.tools.search.SELECTED_SEARCH_ENGINE", SearchEngine.TAVILY.value),
            patch.dict("sys.modules", {"src.tools.search_services.tavily": mock_module}),
            patch("src.tools.search.create_logged_tool", return_value=mock_tool) as mock_create,
        ):
            from src.tools.search import get_web_search_tool

            get_web_search_tool(max_search_results=5, depth=depth)

        assert mock_build.call_args.kwargs["search_depth"] == expected_native
        assert mock_create.call_args.kwargs["tracking_name"] == expected_tracking

    def test_unknown_depth_falls_back_to_provider_default(self):
        """A depth the provider doesn't offer degrades to its default level."""
        mock_build, mock_module = _make_provider_module()
        mock_tool = MagicMock()
        with (
            patch("src.tools.search.SELECTED_SEARCH_ENGINE", SearchEngine.TAVILY.value),
            patch.dict("sys.modules", {"src.tools.search_services.tavily": mock_module}),
            patch("src.tools.search.create_logged_tool", return_value=mock_tool) as mock_create,
        ):
            from src.tools.search import get_web_search_tool

            get_web_search_tool(max_search_results=5, depth="warp9")

        assert mock_build.call_args.kwargs["search_depth"] == "basic"
        assert mock_create.call_args.kwargs["tracking_name"] == "TavilySearchTool:standard"

    def test_depth_ignored_for_single_depth_provider(self):
        """Single-depth providers ignore the depth name and keep the bare
        tracking key (no native depth params exist for them)."""
        mock_build, mock_module = _make_provider_module()
        mock_tool = MagicMock()
        with (
            patch("src.tools.search.SELECTED_SEARCH_ENGINE", SearchEngine.SERPER.value),
            patch.dict("sys.modules", {"src.tools.search_services.serper": mock_module}),
            patch("src.tools.search.create_logged_tool", return_value=mock_tool) as mock_create,
        ):
            from src.tools.search import get_web_search_tool

            get_web_search_tool(max_search_results=5, depth="deep")

        mock_build.assert_called_once_with(
            max_results=5, default_time_range=None, verbose=True
        )
        assert mock_create.call_args.kwargs["tracking_name"] == "SerperSearchTool"


# ---------------------------------------------------------------------------
# Tests for per-request builders (cross-user race fix)
# ---------------------------------------------------------------------------


class TestPerRequestBuilders:
    """Builders return fresh tools with independent closure state per call."""

    def test_tavily_builder_returns_fresh_tools(self):
        from src.tools.search_services.tavily import build_web_search_tool

        t1 = build_web_search_tool(max_results=1, search_depth="basic")
        t2 = build_web_search_tool(max_results=2, search_depth="advanced")
        assert t1 is not t2

    def test_serper_builder_returns_fresh_tools(self):
        from src.tools.search_services.serper import build_web_search_tool

        assert build_web_search_tool() is not build_web_search_tool()

    def test_bocha_builder_returns_fresh_tools(self):
        from src.tools.search_services.bocha import build_web_search_tool

        assert build_web_search_tool() is not build_web_search_tool()

    @pytest.mark.asyncio
    async def test_missing_tavily_api_key_is_a_per_call_error(self, monkeypatch):
        """The API wrapper is created lazily inside the tool call, so a
        missing key surfaces as an error result — never a build-time raise."""
        monkeypatch.delenv("TAVILY_API_KEY", raising=False)
        from src.tools.search_services.tavily import build_web_search_tool

        tool = build_web_search_tool(max_results=1)  # must not raise
        content, artifact = await tool.coroutine(query="anything")
        assert "error" in artifact
        assert isinstance(content, str)


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
