"""
Tests for InsightService.

Covers structured extraction, fallback logic, per-user generation,
user context building, and schedule computation.
"""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.database.market_insight import SystemInsightClaim
from src.server.services.insight_service import (
    DEFAULT_MAX_SYMBOLS,
    ET,
    InsightAlreadyGeneratingError,
    IN_FLIGHT_SLACK,
    STALENESS_WINDOWS,
    InsightService,
    _extract_json_string,
    _extract_structured_output,
    _llm_extract_fallback,
    _research,
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
        result = await svc._extract_with_fallback("some raw text", user_id=None)

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
        result = await svc._extract_with_fallback("bad raw text", user_id=None)

        assert result["headline"] == "Fallback headline"
        mock_extract.assert_called_once_with("bad raw text")
        mock_fallback.assert_awaited_once_with("bad raw text", user_id=None)

    @pytest.mark.asyncio
    @patch(
        "src.server.services.insight_service._llm_extract_fallback",
        new_callable=AsyncMock,
        return_value=_valid_output(headline="Fallback user headline"),
    )
    @patch(
        "src.server.services.insight_service._extract_structured_output",
        side_effect=ValueError("Direct JSON parsing failed"),
    )
    async def test_extract_with_fallback_threads_user_id(self, mock_extract, mock_fallback):
        """user_id passed to _extract_with_fallback is forwarded to _llm_extract_fallback."""
        svc = InsightService()
        result = await svc._extract_with_fallback("bad raw text", user_id="usr-abc")

        assert result["headline"] == "Fallback user headline"
        mock_fallback.assert_awaited_once_with("bad raw text", user_id="usr-abc")

    @pytest.mark.asyncio
    @patch(
        "src.server.services.insight_service._llm_extract_fallback",
        new_callable=AsyncMock,
        return_value=_valid_output(headline="Fallback none headline"),
    )
    @patch(
        "src.server.services.insight_service._extract_structured_output",
        side_effect=ValueError("Direct JSON parsing failed"),
    )
    async def test_extract_with_fallback_none_user_id(self, mock_extract, mock_fallback):
        """user_id=None is forwarded unchanged (system/scheduled path)."""
        svc = InsightService()
        result = await svc._extract_with_fallback("bad raw text", user_id=None)

        assert result["headline"] == "Fallback none headline"
        mock_fallback.assert_awaited_once_with("bad raw text", user_id=None)


# ---------------------------------------------------------------------------
# _llm_extract_fallback
# ---------------------------------------------------------------------------

class TestLLMExtractFallback:
    """Test _llm_extract_fallback routes through LLMService.complete."""

    @pytest.mark.asyncio
    async def test_calls_llm_service_complete_with_user_id(self):
        """_llm_extract_fallback delegates to LLMService.complete with the supplied user_id."""
        from src.server.models.market_insight import InsightOutputSchema

        stub_result = MagicMock(spec=InsightOutputSchema)
        stub_result.model_dump.return_value = _valid_output()

        mock_agent_config = MagicMock()

        with (
            patch("src.server.app.setup") as mock_setup,
            patch("src.server.services.llm.service.LLMService") as MockLLMService,
        ):
            mock_setup.agent_config = mock_agent_config
            mock_instance = MagicMock()
            mock_instance.complete = AsyncMock(return_value=stub_result)
            MockLLMService.return_value = mock_instance

            result = await _llm_extract_fallback("some raw text", user_id="usr-x")

        # Constructor must receive agent_config from setup
        ctor_kwargs = MockLLMService.call_args.kwargs
        assert ctor_kwargs["agent_config"] is mock_agent_config
        # complete() must be called with user_id, response_schema, mode
        mock_instance.complete.assert_awaited_once()
        call_kwargs = mock_instance.complete.call_args.kwargs
        assert call_kwargs["user_id"] == "usr-x"
        assert call_kwargs["response_schema"] is InsightOutputSchema
        assert call_kwargs["slot"] == "background"
        assert result == _valid_output()

    @pytest.mark.asyncio
    async def test_calls_llm_service_complete_with_none_user_id(self):
        """user_id=None is passed through to LLMService.complete (system/scheduled path)."""
        from src.server.models.market_insight import InsightOutputSchema

        stub_result = MagicMock(spec=InsightOutputSchema)
        stub_result.model_dump.return_value = _valid_output()

        mock_agent_config = MagicMock()

        with (
            patch("src.server.app.setup") as mock_setup,
            patch("src.server.services.llm.service.LLMService") as MockLLMService,
        ):
            mock_setup.agent_config = mock_agent_config
            mock_instance = MagicMock()
            mock_instance.complete = AsyncMock(return_value=stub_result)
            MockLLMService.return_value = mock_instance

            result = await _llm_extract_fallback("some raw text", user_id=None)

        call_kwargs = mock_instance.complete.call_args.kwargs
        assert call_kwargs["user_id"] is None
        assert call_kwargs["slot"] == "background"
        assert result == _valid_output()

    @pytest.mark.asyncio
    async def test_no_create_llm_imported(self):
        """_llm_extract_fallback must not import or call create_llm."""
        stub_result = MagicMock()
        stub_result.model_dump.return_value = _valid_output()

        mock_agent_config = MagicMock()

        with (
            patch("src.server.app.setup") as mock_setup,
            patch("src.server.services.llm.service.LLMService") as MockLLMService,
            patch("src.llms.llm.create_llm") as mock_create_llm,
        ):
            mock_setup.agent_config = mock_agent_config
            mock_instance = MagicMock()
            mock_instance.complete = AsyncMock(return_value=stub_result)
            MockLLMService.return_value = mock_instance

            await _llm_extract_fallback("some text", user_id="usr-y")

        mock_create_llm.assert_not_called()


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
    @patch("src.server.services.insight_service._research", new_callable=AsyncMock)
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
    @patch("src.server.services.insight_service._research", new_callable=AsyncMock)
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
    @patch("src.server.services.insight_service._research", new_callable=AsyncMock)
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
    @patch("src.server.services.insight_service._research", new_callable=AsyncMock)
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
    @patch("src.server.services.insight_service._research", new_callable=AsyncMock)
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

class TestResearch:
    @pytest.mark.asyncio
    async def test_web_fetch_extracts_with_the_users_fetch_model_for_that_run_only(self):
        """A brief's WebFetch runs on the user's fetch model and client, as a
        chat turn's does, and the scheduler's next brief starts without them."""
        from langchain_core.messages import AIMessage

        from src.tools.web import fetch

        seen = {}

        class Graph:
            async def ainvoke(self, state, config):
                seen["model"] = fetch._get_extraction_model()
                seen["client"] = fetch.fetch_llm_client_override.get()
                return {"messages": [AIMessage(content="brief")]}

        client = object()
        resolved = MagicMock()
        resolved.llm.fetch_name = "user-fetch-model"
        resolved.subsidiary_llm_clients = {"fetch": client}

        with patch("src.server.app.setup.agent_config", MagicMock()), \
             patch("src.server.services.llm.config.resolve_llm_config", AsyncMock(return_value=resolved)), \
             patch("ptc_agent.agent.insight_graph.build_insight_graph", return_value=Graph()):
            assert await _research("prompt", user_id="user-1", insight_type="personalized") == "brief"

        assert seen == {"model": "user-fetch-model", "client": client}
        assert fetch.fetch_model_override.get() is None
        assert fetch.fetch_llm_client_override.get() is None


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
    """Catch-up runs the job type for the hour, if today's schedule has it."""

    async def _catchup_at(self, now_et, **gen_kwargs):
        svc = InsightService()
        with (
            patch("src.server.services.insight_service.datetime") as mock_dt,
            patch.object(
                svc, "_generate_insight", new_callable=AsyncMock, **gen_kwargs
            ) as mock_gen,
        ):
            mock_dt.now.return_value = now_et
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            mock_dt.combine = datetime.combine
            await svc._catchup_if_needed()
        return mock_gen

    @pytest.mark.asyncio
    async def test_catchup_failure_is_nonfatal(self):
        friday = datetime(2025, 3, 14, 14, 0, tzinfo=ET)
        # Should not raise: catch-up failure is non-fatal
        mock_gen = await self._catchup_at(friday, side_effect=Exception("DB down"))
        mock_gen.assert_called_once()

    @pytest.mark.asyncio
    async def test_catchup_picks_pre_market_before_10(self):
        mock_gen = await self._catchup_at(datetime(2025, 3, 14, 8, 0, tzinfo=ET))
        assert mock_gen.call_args[0][0] == "pre_market"

    @pytest.mark.asyncio
    async def test_catchup_picks_market_update_midday(self):
        mock_gen = await self._catchup_at(datetime(2025, 3, 14, 14, 0, tzinfo=ET))
        assert mock_gen.call_args[0][0] == "market_update"

    @pytest.mark.asyncio
    async def test_catchup_picks_post_market_evening(self):
        mock_gen = await self._catchup_at(datetime(2025, 3, 15, 21, 0, tzinfo=ET))
        assert mock_gen.call_args[0][0] == "post_market"

    @pytest.mark.asyncio
    async def test_catchup_skips_market_update_on_a_weekend(self):
        """A Saturday afternoon restart runs nothing: the weekend has no hourly update."""
        mock_gen = await self._catchup_at(datetime(2025, 3, 15, 14, 0, tzinfo=ET))
        mock_gen.assert_not_called()


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
    @patch("src.server.services.insight_service._research", new_callable=AsyncMock)
    @patch("src.server.services.insight_service.insight_db")
    async def test_timeout_marks_row_failed(self, mock_db, mock_agent):
        """A hung research run triggers timeout and marks the DB row as failed."""
        svc = InsightService()
        svc._generation_timeout = 0.1  # 100ms timeout

        mock_db.claim_system_insight = AsyncMock(
            return_value=SystemInsightClaim(
                row={"market_insight_id": "test-id", "status": "generating"}
            )
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

    @pytest.mark.asyncio
    @patch("src.server.services.insight_service._research", new_callable=AsyncMock)
    @patch("src.server.services.insight_service.insight_db")
    async def test_claims_with_the_type_window_and_the_generation_bound(
        self, mock_db, mock_agent
    ):
        svc = InsightService()
        svc._generation_timeout = 600
        mock_db.claim_system_insight = AsyncMock(
            return_value=SystemInsightClaim(holder_id="other")
        )

        await svc._generate_insight("post_market", datetime(2025, 3, 14, 20, 30, tzinfo=ET))

        call = mock_db.claim_system_insight.call_args
        assert call.args == ("post_market",)
        assert call.kwargs["fresh_within"] == STALENESS_WINDOWS["post_market"]
        assert call.kwargs["in_flight_within"] == timedelta(seconds=600) + IN_FLIGHT_SLACK

    @pytest.mark.asyncio
    @patch("src.server.services.insight_service._research", new_callable=AsyncMock)
    @patch("src.server.services.insight_service.insight_db")
    async def test_a_fresh_completed_insight_runs_nothing(self, mock_db, mock_agent):
        svc = InsightService()
        mock_db.claim_system_insight = AsyncMock(
            return_value=SystemInsightClaim(holder_id="other")
        )

        await svc._generate_insight("market_update", datetime(2025, 3, 14, 14, 0, tzinfo=ET))

        mock_db.claim_system_insight.assert_awaited_once()
        mock_db.get_market_insight.assert_not_called()
        mock_agent.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("outcome", "runs"),
        [
            (SystemInsightClaim(holder_id="winner"), 0),
            (SystemInsightClaim(row={"market_insight_id": "takeover"}), 1),
            (SystemInsightClaim(holder_id="other", holder_in_flight=True), 0),
        ],
        ids=["holder-completes", "holder-fails-or-expires", "another-worker-takes-over"],
    )
    @patch("src.server.services.insight_service.HOLDER_POLL_SECONDS", 0)
    @patch("src.server.services.insight_service._research", new_callable=AsyncMock)
    @patch("src.server.services.insight_service.insight_db")
    async def test_losing_to_a_run_in_flight_claims_until_it_ends(
        self, mock_db, mock_agent, outcome, runs
    ):
        """The worker that lost runs only if its own later claim succeeds."""
        svc = InsightService()
        in_flight = SystemInsightClaim(holder_id="winner", holder_in_flight=True)
        mock_db.claim_system_insight = AsyncMock(side_effect=[in_flight, in_flight, outcome])
        mock_db.complete_market_insight = AsyncMock()
        mock_agent.return_value = "{}"
        svc._extract_with_fallback = AsyncMock(
            return_value={"headline": "h", "summary": "s", "news_items": [], "topics": []}
        )

        await svc._generate_insight("market_update", datetime(2025, 3, 14, 14, 0, tzinfo=ET))

        assert mock_db.claim_system_insight.await_count == 3
        assert mock_agent.await_count == runs

    @pytest.mark.asyncio
    @patch("src.server.services.insight_service._research", new_callable=AsyncMock)
    @patch("src.server.services.insight_service.insight_db")
    async def test_a_slow_extraction_times_out_with_the_research(self, mock_db, mock_agent):
        """The timeout bounds the whole run, so it cannot outlive its claim's hold."""
        svc = InsightService()
        svc._generation_timeout = 0.1
        mock_db.claim_system_insight = AsyncMock(
            return_value=SystemInsightClaim(row={"market_insight_id": "test-id"})
        )
        mock_db.fail_market_insight = AsyncMock()
        mock_agent.return_value = "not json"

        async def slow_extract(raw_text, user_id):
            await asyncio.sleep(999)

        svc._extract_with_fallback = slow_extract

        await svc._generate_insight("market_update", datetime(2025, 3, 14, 14, 0, tzinfo=ET))

        mock_db.complete_market_insight.assert_not_called()
        assert "Timeout" in mock_db.fail_market_insight.call_args[0][1]


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
