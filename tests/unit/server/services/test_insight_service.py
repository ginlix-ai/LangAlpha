"""
Tests for InsightService.

Covers structured extraction, fallback logic, per-user generation,
user context building, and schedule computation.
"""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from src.server.services.insight_service import (
    DEFAULT_MAX_SYMBOLS,
    ET,
    InsightAlreadyGeneratingError,
    InsightService,
    _extract_json_string,
    _extract_structured_output,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _valid_output(**overrides) -> dict:
    """Return a minimal valid structured output dict."""
    data = {
        "headline": "Markets rally on strong earnings",
        "summary": "US equities closed higher. Tech led the advance.",
        "news_items": [
            {"title": "AAPL beats estimates", "body": "Apple reported Q3 earnings above expectations.", "url": "https://example.com/aapl"},
        ],
        "topics": [
            {"text": "Earnings", "trend": "up"},
        ],
    }
    data.update(overrides)
    return data


def _valid_json_str(**overrides) -> str:
    return json.dumps(_valid_output(**overrides))


def _make_watchlist_item(symbol: str, name: str = "", instrument_type: str = "stock") -> dict:
    return {"symbol": symbol, "name": name, "instrument_type": instrument_type}


def _make_holding(symbol: str, name: str = "", quantity: int = 10, average_cost: float = 100.0) -> dict:
    return {"symbol": symbol, "name": name, "quantity": quantity, "average_cost": average_cost}


# ---------------------------------------------------------------------------
# _extract_structured_output
# ---------------------------------------------------------------------------

class TestExtractStructuredOutput:
    """Test _extract_structured_output (module-level function).

    extract_json_from_content is imported inside the function body, so we
    patch it at its source module (src.llms.content_utils).
    """

    @patch("src.llms.content_utils.extract_json_from_content", side_effect=lambda x: x)
    def test_valid_json_all_fields(self, _mock_extract):
        raw = _valid_json_str()
        result = _extract_structured_output(raw)

        assert result["headline"] == "Markets rally on strong earnings"
        assert result["summary"].startswith("US equities")
        assert len(result["news_items"]) == 1
        assert result["news_items"][0]["title"] == "AAPL beats estimates"
        assert len(result["topics"]) == 1
        assert result["topics"][0]["text"] == "Earnings"

    @patch("src.llms.content_utils.extract_json_from_content", side_effect=lambda x: x)
    def test_missing_headline_raises(self, _mock_extract):
        data = _valid_output()
        del data["headline"]
        raw = json.dumps(data)

        with pytest.raises(ValueError, match="caller should invoke LLM fallback"):
            _extract_structured_output(raw)

    @patch("src.llms.content_utils.extract_json_from_content", side_effect=lambda x: x)
    def test_missing_news_items_raises(self, _mock_extract):
        data = _valid_output()
        del data["news_items"]
        raw = json.dumps(data)

        with pytest.raises(ValueError, match="caller should invoke LLM fallback"):
            _extract_structured_output(raw)

    @patch("src.llms.content_utils.extract_json_from_content", side_effect=lambda x: x)
    def test_invalid_json_raises(self, _mock_extract):
        with pytest.raises(ValueError, match="caller should invoke LLM fallback"):
            _extract_structured_output("this is not json at all {{{")

    @patch("src.llms.content_utils.extract_json_from_content", side_effect=lambda x: x)
    def test_json_in_markdown_fence_stripped(self, _mock_extract):
        inner = _valid_json_str()
        fenced = f"```json\n{inner}\n```"
        result = _extract_structured_output(fenced)

        assert result["headline"] == "Markets rally on strong earnings"
        assert len(result["news_items"]) == 1

    @patch("src.llms.content_utils.extract_json_from_content", side_effect=lambda x: x)
    def test_empty_string_raises(self, _mock_extract):
        with pytest.raises(ValueError):
            _extract_structured_output("")


# ---------------------------------------------------------------------------
# _extract_with_fallback
# ---------------------------------------------------------------------------

class TestExtractWithFallback:
    """Test InsightService._extract_with_fallback."""

    def setup_method(self):
        InsightService._instance = None

    def teardown_method(self):
        InsightService._instance = None

    @pytest.mark.asyncio
    @patch(
        "src.server.services.insight_service._extract_structured_output",
        return_value=_valid_output(),
    )
    async def test_direct_parse_succeeds(self, mock_extract):
        svc = InsightService()
        result = await svc._extract_with_fallback("some raw text")

        assert result["headline"] == "Markets rally on strong earnings"
        mock_extract.assert_called_once_with("some raw text")

    @pytest.mark.asyncio
    @patch(
        "src.server.services.insight_service._llm_extract_fallback",
        new_callable=AsyncMock,
        return_value=_valid_output(headline="Fallback headline"),
    )
    @patch(
        "src.server.services.insight_service._extract_structured_output",
        side_effect=ValueError("Direct JSON parsing failed"),
    )
    async def test_direct_fails_llm_fallback_succeeds(self, mock_extract, mock_fallback):
        svc = InsightService()
        result = await svc._extract_with_fallback("bad raw text")

        assert result["headline"] == "Fallback headline"
        mock_extract.assert_called_once_with("bad raw text")
        mock_fallback.assert_awaited_once_with("bad raw text", user_id=None)


# ---------------------------------------------------------------------------
# generate_for_user
# ---------------------------------------------------------------------------

class TestGenerateForUser:
    """Test InsightService.generate_for_user."""

    def setup_method(self):
        InsightService._instance = None

    def teardown_method(self):
        InsightService._instance = None

    @pytest.mark.asyncio
    @patch("src.server.services.insight_service.insight_db")
    @patch("src.server.services.insight_service._run_flash_agent", new_callable=AsyncMock)
    async def test_user_with_watchlist_and_portfolio(self, mock_agent, mock_db):
        """Personalized prompt includes symbols and holdings."""
        user_id = "user-123"
        valid = _valid_output()

        mock_db.get_user_recent_completed_insight = AsyncMock(return_value=None)
        mock_db.create_market_insight_if_not_generating = AsyncMock(
            return_value={"market_insight_id": "ins-1", "created_at": datetime.now(timezone.utc), "status": "generating", "type": "personalized"}
        )

        mock_agent.return_value = _valid_json_str()

        svc = InsightService()

        with patch.object(svc, "_extract_with_fallback", new_callable=AsyncMock, return_value=valid), \
             patch.object(svc, "_build_user_context", new_callable=AsyncMock, return_value="- AAPL (Apple Inc) [stock] [watchlist]\n- TSLA (Tesla) [portfolio: 50 shares @ $200.0]"):
            result = await svc.generate_for_user(user_id)

        assert result["market_insight_id"] == "ins-1"
        mock_db.create_market_insight_if_not_generating.assert_awaited_once()
        call_kwargs = mock_db.create_market_insight_if_not_generating.call_args
        assert call_kwargs.kwargs["type"] == "personalized"
        assert call_kwargs.kwargs["metadata"]["has_context"] is True

    @pytest.mark.asyncio
    @patch("src.server.services.insight_service.insight_db")
    @patch("src.server.services.insight_service._run_flash_agent", new_callable=AsyncMock)
    async def test_empty_watchlist_falls_back_to_generic(self, mock_agent, mock_db):
        """Empty watchlist/portfolio falls back to generic market_update prompt."""
        user_id = "user-456"
        valid = _valid_output()

        mock_db.get_user_recent_completed_insight = AsyncMock(return_value=None)
        mock_db.create_market_insight_if_not_generating = AsyncMock(
            return_value={"market_insight_id": "ins-2", "created_at": datetime.now(timezone.utc), "status": "generating", "type": "personalized"}
        )

        mock_agent.return_value = _valid_json_str()

        svc = InsightService()

        with patch.object(svc, "_extract_with_fallback", new_callable=AsyncMock, return_value=valid), \
             patch.object(svc, "_build_user_context", new_callable=AsyncMock, return_value=""):
            result = await svc.generate_for_user(user_id)

        assert result["market_insight_id"] == "ins-2"
        call_kwargs = mock_db.create_market_insight_if_not_generating.call_args
        assert call_kwargs.kwargs["metadata"]["has_context"] is False

    @pytest.mark.asyncio
    @patch("src.server.services.insight_service.insight_db")
    async def test_existing_generating_raises(self, mock_db):
        """In-progress insight raises InsightAlreadyGeneratingError."""
        existing = {"market_insight_id": "ins-existing", "status": "generating"}
        mock_db.get_user_recent_completed_insight = AsyncMock(return_value=None)
        # Atomic insert returns None (conflict from partial unique index)
        mock_db.create_market_insight_if_not_generating = AsyncMock(return_value=None)
        # Fallback query finds the existing row
        mock_db.get_user_generating_insight = AsyncMock(return_value=existing)

        svc = InsightService()

        with patch.object(svc, "_build_user_context", new_callable=AsyncMock, return_value=""):
            with pytest.raises(InsightAlreadyGeneratingError) as exc_info:
                await svc.generate_for_user("user-789")

        assert exc_info.value.existing_insight is existing

    @pytest.mark.asyncio
    @patch("src.server.services.insight_service.insight_db")
    async def test_conflict_then_completed_returns_recent(self, mock_db):
        """Race: generating row completed between conflict and lookup → return recent."""
        recent = {"market_insight_id": "ins-just-done", "status": "completed"}
        mock_db.get_user_recent_completed_insight = AsyncMock(
            side_effect=[None, recent]  # First call: dedup check, second: race recovery
        )
        mock_db.create_market_insight_if_not_generating = AsyncMock(return_value=None)
        mock_db.get_user_generating_insight = AsyncMock(return_value=None)

        svc = InsightService()

        with patch.object(svc, "_build_user_context", new_callable=AsyncMock, return_value=""):
            result = await svc.generate_for_user("user-race")

        assert result["market_insight_id"] == "ins-just-done"

    @pytest.mark.asyncio
    @patch("src.server.services.insight_service.insight_db")
    async def test_conflict_no_generating_no_recent_raises_retry(self, mock_db):
        """Race: no generating and no recent → raises with retry flag."""
        mock_db.get_user_recent_completed_insight = AsyncMock(return_value=None)
        mock_db.create_market_insight_if_not_generating = AsyncMock(return_value=None)
        mock_db.get_user_generating_insight = AsyncMock(return_value=None)

        svc = InsightService()

        with patch.object(svc, "_build_user_context", new_callable=AsyncMock, return_value=""):
            with pytest.raises(InsightAlreadyGeneratingError) as exc_info:
                await svc.generate_for_user("user-unknown")

        assert exc_info.value.existing_insight.get("retry") is True

    @pytest.mark.asyncio
    @patch("src.server.services.insight_service.insight_db")
    async def test_recent_completed_returns_existing(self, mock_db):
        """Recently completed insight is returned directly (dedup)."""
        recent = {"market_insight_id": "ins-recent", "status": "completed", "headline": "Old news"}
        mock_db.get_user_generating_insight = AsyncMock(return_value=None)
        mock_db.get_user_recent_completed_insight = AsyncMock(return_value=recent)

        svc = InsightService()
        result = await svc.generate_for_user("user-dedup")

        assert result is recent
        assert result["market_insight_id"] == "ins-recent"

    @pytest.mark.asyncio
    @patch("src.server.services.insight_service.insight_db")
    @patch("src.server.services.insight_service._run_flash_agent", new_callable=AsyncMock)
    async def test_generate_returns_immediately(self, mock_agent, mock_db):
        """generate_for_user returns the generating row without waiting."""
        row = {"market_insight_id": "ins-async", "created_at": datetime.now(timezone.utc), "status": "generating"}
        mock_db.get_user_recent_completed_insight = AsyncMock(return_value=None)
        mock_db.create_market_insight_if_not_generating = AsyncMock(return_value=row)

        # Agent should NOT be awaited during generate_for_user
        async def hang_forever(*args, **kwargs):
            await asyncio.sleep(9999)
        mock_agent.side_effect = hang_forever

        svc = InsightService()
        with patch.object(svc, "_build_user_context", new_callable=AsyncMock, return_value=""):
            result = await svc.generate_for_user("user-async")

        assert result["market_insight_id"] == "ins-async"
        assert result["status"] == "generating"

    @pytest.mark.asyncio
    @patch("src.server.services.insight_service.insight_db")
    @patch("src.server.services.insight_service._run_flash_agent", new_callable=AsyncMock)
    async def test_background_timeout_marks_failed(self, mock_agent, mock_db):
        """Background task marks DB row as failed on timeout."""
        mock_db.fail_market_insight = AsyncMock()

        async def hang_forever(*args, **kwargs):
            await asyncio.sleep(9999)
        mock_agent.side_effect = hang_forever

        svc = InsightService()
        svc._generation_timeout = 0.01

        await svc._run_personalized_generation("user-timeout", "ins-timeout", "prompt")

        mock_db.fail_market_insight.assert_awaited_once()
        args = mock_db.fail_market_insight.call_args
        assert args[0][0] == "ins-timeout"
        assert "Timeout" in args[0][1]

    @pytest.mark.asyncio
    @patch("src.server.services.insight_service.insight_db")
    @patch("src.server.services.insight_service._run_flash_agent", new_callable=AsyncMock)
    async def test_background_extraction_failure_marks_failed(self, mock_agent, mock_db):
        """Background task marks DB row as failed when extraction fails."""
        mock_db.fail_market_insight = AsyncMock()
        mock_agent.return_value = "totally garbage output"

        svc = InsightService()

        with patch.object(
            svc, "_extract_with_fallback", new_callable=AsyncMock,
            side_effect=ValueError("Could not extract structured output"),
        ):
            await svc._run_personalized_generation("user-garbage", "ins-bad", "prompt")

        mock_db.fail_market_insight.assert_awaited_once()
        args = mock_db.fail_market_insight.call_args
        assert args[0][0] == "ins-bad"


# ---------------------------------------------------------------------------
# _build_user_context
# ---------------------------------------------------------------------------

class TestBuildUserContext:
    """Test InsightService._build_user_context."""

    def setup_method(self):
        InsightService._instance = None

    def teardown_method(self):
        InsightService._instance = None

    @pytest.mark.asyncio
    async def test_large_watchlist_capped_at_max(self):
        """30+ watchlist symbols are capped at DEFAULT_MAX_SYMBOLS (20)."""
        symbols = [_make_watchlist_item(f"SYM{i:02d}", f"Company {i}") for i in range(35)]
        watchlist = {"watchlist_id": "wl-1"}

        svc = InsightService()
        assert svc._max_symbols == DEFAULT_MAX_SYMBOLS  # 20

        with patch("src.server.database.watchlist.get_user_watchlists", new_callable=AsyncMock, return_value=[watchlist]), \
             patch("src.server.database.watchlist.get_watchlist_items", new_callable=AsyncMock, return_value=symbols), \
             patch("src.server.database.portfolio.get_user_portfolio", new_callable=AsyncMock, return_value=[]):
            result = await svc._build_user_context("user-big")

        lines = [line for line in result.strip().split("\n") if line.startswith("- ")]
        assert len(lines) == DEFAULT_MAX_SYMBOLS

    @pytest.mark.asyncio
    async def test_symbols_deduplicated_across_watchlist_and_portfolio(self):
        """Symbol in both watchlist and portfolio appears once in main list, with portfolio annotation."""
        watchlist = {"watchlist_id": "wl-1"}
        wl_items = [_make_watchlist_item("AAPL", "Apple Inc")]
        holdings = [_make_holding("AAPL", "Apple Inc", quantity=50, average_cost=150.0)]

        svc = InsightService()

        with patch("src.server.database.watchlist.get_user_watchlists", new_callable=AsyncMock, return_value=[watchlist]), \
             patch("src.server.database.watchlist.get_watchlist_items", new_callable=AsyncMock, return_value=wl_items), \
             patch("src.server.database.portfolio.get_user_portfolio", new_callable=AsyncMock, return_value=holdings):
            result = await svc._build_user_context("user-dup")

        lines = result.strip().split("\n")
        # First line: watchlist entry
        assert "AAPL" in lines[0]
        assert "[watchlist]" in lines[0]
        # Second line: portfolio annotation (starts with "  ^")
        assert lines[1].strip().startswith("^")
        assert "portfolio" in lines[1]
        assert "50" in lines[1]
        # Only two lines total -- no duplicate "- AAPL" entry
        main_entries = [l for l in lines if l.startswith("- AAPL")]
        assert len(main_entries) == 1


# ---------------------------------------------------------------------------
# Schedule computation
# ---------------------------------------------------------------------------

class TestSchedule:
    """Test schedule computation methods."""

    def setup_method(self):
        InsightService._instance = None

    def teardown_method(self):
        InsightService._instance = None

    def test_todays_schedule_weekday(self):
        """Weekday schedule includes pre_market + market_updates + post_market."""
        svc = InsightService()
        # 2025-01-06 is a Monday
        from zoneinfo import ZoneInfo
        et = ZoneInfo("America/New_York")
        monday = datetime(2025, 1, 6, 8, 0, tzinfo=et)

        jobs = svc._todays_schedule(monday)
        types = [jtype for _, jtype in jobs]

        assert types[0] == "pre_market"
        assert types[-1] == "post_market"
        assert "market_update" in types
        # Weekday: pre + post + 11 hourly updates (10:00..20:00 inclusive, 60 min interval)
        market_updates = [t for t in types if t == "market_update"]
        assert len(market_updates) == 11

    def test_todays_schedule_weekend(self):
        """Weekend schedule has pre_market and post_market only (no market_update)."""
        svc = InsightService()
        from zoneinfo import ZoneInfo
        et = ZoneInfo("America/New_York")
        # 2025-01-04 is a Saturday
        saturday = datetime(2025, 1, 4, 8, 0, tzinfo=et)

        jobs = svc._todays_schedule(saturday)
        types = [jtype for _, jtype in jobs]

        assert "pre_market" in types
        assert "post_market" in types
        assert "market_update" not in types
        assert len(jobs) == 2

    @pytest.mark.asyncio
    @patch("src.server.services.insight_service.insight_db")
    async def test_is_duplicate_detects_stale(self, mock_db):
        """Recent insight within staleness window is detected as duplicate."""
        svc = InsightService()
        # Completed 10 minutes ago -- within the 4-hour pre_market window
        mock_db.get_latest_completed_at = AsyncMock(
            return_value=datetime.now(timezone.utc) - timedelta(minutes=10)
        )

        assert await svc._is_duplicate("pre_market") is True

    @pytest.mark.asyncio
    @patch("src.server.services.insight_service.insight_db")
    async def test_is_duplicate_allows_old(self, mock_db):
        """Insight older than the staleness window is NOT a duplicate."""
        svc = InsightService()
        # Completed 5 hours ago -- outside the 4-hour pre_market window
        mock_db.get_latest_completed_at = AsyncMock(
            return_value=datetime.now(timezone.utc) - timedelta(hours=5)
        )

        assert await svc._is_duplicate("pre_market") is False

    @pytest.mark.asyncio
    @patch("src.server.services.insight_service.insight_db")
    async def test_is_duplicate_none_means_no_duplicate(self, mock_db):
        """No previous insight means not a duplicate."""
        svc = InsightService()
        mock_db.get_latest_completed_at = AsyncMock(return_value=None)

        assert await svc._is_duplicate("market_update") is False

    def test_next_job_wraps_to_tomorrow(self):
        """When no jobs remain today, _next_job returns first job tomorrow."""
        svc = InsightService()
        from zoneinfo import ZoneInfo
        et = ZoneInfo("America/New_York")
        # 11 PM -- after all jobs for the day
        late_night = datetime(2025, 1, 6, 23, 0, tzinfo=et)

        result = svc._next_job(late_night)

        assert result is not None
        run_at, job_type = result
        # Should be tomorrow's first job (pre_market)
        assert run_at.date() == (late_night.date() + timedelta(days=1))
        assert job_type == "pre_market"


# ---------------------------------------------------------------------------
# _catchup_if_needed
# ---------------------------------------------------------------------------


class TestCatchupIfNeeded:
    """Test catch-up generates an insight when no recent one exists."""

    @pytest.mark.asyncio
    async def test_catchup_generates_when_no_recent(self):
        svc = InsightService()

        with (
            patch.object(svc, "_is_duplicate", new_callable=AsyncMock, return_value=False),
            patch.object(svc, "_generate_insight", new_callable=AsyncMock) as mock_gen,
        ):
            await svc._catchup_if_needed()
            mock_gen.assert_called_once()
            call_args = mock_gen.call_args
            assert call_args[0][0] in ("pre_market", "market_update", "post_market")

    @pytest.mark.asyncio
    async def test_catchup_skips_when_recent_exists(self):
        svc = InsightService()

        with (
            patch.object(svc, "_is_duplicate", new_callable=AsyncMock, return_value=True),
            patch.object(svc, "_generate_insight", new_callable=AsyncMock) as mock_gen,
        ):
            await svc._catchup_if_needed()
            mock_gen.assert_not_called()

    @pytest.mark.asyncio
    async def test_catchup_failure_is_nonfatal(self):
        svc = InsightService()

        with patch.object(
            svc, "_is_duplicate", new_callable=AsyncMock, side_effect=Exception("DB down")
        ):
            # Should not raise — catch-up failure is non-fatal
            await svc._catchup_if_needed()

    @pytest.mark.asyncio
    async def test_catchup_picks_pre_market_before_10(self):
        svc = InsightService()
        morning = datetime(2025, 3, 15, 8, 0, tzinfo=ET)

        with (
            patch("src.server.services.insight_service.datetime") as mock_dt,
            patch.object(svc, "_is_duplicate", new_callable=AsyncMock, return_value=False),
            patch.object(svc, "_generate_insight", new_callable=AsyncMock) as mock_gen,
        ):
            mock_dt.now.return_value = morning
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            await svc._catchup_if_needed()
            assert mock_gen.call_args[0][0] == "pre_market"

    @pytest.mark.asyncio
    async def test_catchup_picks_market_update_midday(self):
        svc = InsightService()
        midday = datetime(2025, 3, 15, 14, 0, tzinfo=ET)

        with (
            patch("src.server.services.insight_service.datetime") as mock_dt,
            patch.object(svc, "_is_duplicate", new_callable=AsyncMock, return_value=False),
            patch.object(svc, "_generate_insight", new_callable=AsyncMock) as mock_gen,
        ):
            mock_dt.now.return_value = midday
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            await svc._catchup_if_needed()
            assert mock_gen.call_args[0][0] == "market_update"

    @pytest.mark.asyncio
    async def test_catchup_picks_post_market_evening(self):
        svc = InsightService()
        evening = datetime(2025, 3, 15, 21, 0, tzinfo=ET)

        with (
            patch("src.server.services.insight_service.datetime") as mock_dt,
            patch.object(svc, "_is_duplicate", new_callable=AsyncMock, return_value=False),
            patch.object(svc, "_generate_insight", new_callable=AsyncMock) as mock_gen,
        ):
            mock_dt.now.return_value = evening
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            await svc._catchup_if_needed()
            assert mock_gen.call_args[0][0] == "post_market"


# ---------------------------------------------------------------------------
# _generate_insight (system scheduled)
# ---------------------------------------------------------------------------

class TestGenerateInsight:
    """Test scheduled system insight generation."""

    def setup_method(self):
        InsightService._instance = None

    def teardown_method(self):
        InsightService._instance = None

    @pytest.mark.asyncio
    @patch("src.server.services.insight_service._run_flash_agent", new_callable=AsyncMock)
    @patch("src.server.services.insight_service.insight_db")
    async def test_timeout_marks_row_failed(self, mock_db, mock_agent):
        """A hung flash agent triggers timeout and marks the DB row as failed."""
        svc = InsightService()
        svc._generation_timeout = 0.1  # 100ms timeout

        mock_db.create_market_insight = AsyncMock(
            return_value={"market_insight_id": "test-id", "status": "generating"}
        )
        mock_db.fail_market_insight = AsyncMock()

        # Agent hangs forever
        async def hang_forever(*a, **kw):
            await asyncio.sleep(999)

        mock_agent.side_effect = hang_forever

        now_et = datetime(2025, 3, 15, 14, 0, tzinfo=ET)
        await svc._generate_insight("market_update", now_et)

        mock_db.fail_market_insight.assert_called_once()
        call_args = mock_db.fail_market_insight.call_args
        assert call_args[0][0] == "test-id"
        assert "Timeout" in call_args[0][1]


# ---------------------------------------------------------------------------
# _extract_json_string (utility)
# ---------------------------------------------------------------------------

class TestExtractJsonString:
    """Test _extract_json_string helper."""

    def test_plain_json(self):
        raw = '{"key": "value"}'
        assert _extract_json_string(raw) == raw

    def test_json_in_markdown_fence(self):
        inner = '{"key": "value"}'
        assert _extract_json_string(f"```json\n{inner}\n```") == inner

    def test_json_in_bare_fence(self):
        inner = '{"key": "value"}'
        assert _extract_json_string(f"```\n{inner}\n```") == inner

    def test_json_embedded_in_reasoning(self):
        inner = '{"headline": "Test", "summary": "S"}'
        text = f"Let me think about this...\n\n```json\n{inner}\n```\n\nThat looks right."
        assert _extract_json_string(text) == inner

    def test_json_no_fence_in_reasoning(self):
        """Falls back to outermost braces when no fence."""
        inner = '{"key": "value"}'
        text = f"Here is the result: {inner} done."
        assert _extract_json_string(text) == inner

    def test_nested_json_in_fence(self):
        """Fenced JSON with nested objects (topics, news_items) parses correctly."""
        inner = '{"headline": "Test", "topics": [{"text": "AI", "trend": "up"}]}'
        text = f"```json\n{inner}\n```"
        result = _extract_json_string(text)
        parsed = json.loads(result)
        assert parsed["topics"][0]["text"] == "AI"

    def test_no_json_returns_none(self):
        assert _extract_json_string("no json here") is None

    def test_whitespace_stripped(self):
        inner = '{"key": "value"}'
        assert _extract_json_string(f"  {inner}  ") == inner
