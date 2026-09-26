"""
Tests for src/server/handlers/chat/request_prep.py: the thread row and the
turn context built from it.

Covers:
- ensure_thread: correct DB call with kwargs, title generation, and the
  prior-turn snapshot
- build_turn_context: origin and surface from the request, and the turn's zones
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.handlers.chat.request_prep import PriorThread, build_turn_context

PREP = "src.server.handlers.chat.request_prep"


# ---------------------------------------------------------------------------
# ensure_thread
# ---------------------------------------------------------------------------


class TestEnsureThread:
    @pytest.fixture(autouse=True)
    def _no_prior_row(self):
        """Default every case to a thread that does not exist yet."""
        with patch(
            f"{PREP}.qr_db.get_thread_by_id",
            new_callable=AsyncMock,
            return_value=None,
        ) as mock_read:
            yield mock_read

    @pytest.mark.asyncio
    async def test_basic_call(self):
        from src.server.handlers.chat.request_prep import ensure_thread

        request = MagicMock()
        request.external_thread_id = None
        request.platform = None
        request.origin = None

        with patch(f"{PREP}.qr_db.ensure_thread_exists", new_callable=AsyncMock) as mock_db:
            await ensure_thread(
                request, "t-1", "ws-1", "u-1", msg_type="flash", initial_query="hello"
            )

        mock_db.assert_called_once_with(
            workspace_id="ws-1",
            conversation_thread_id="t-1",
            user_id="u-1",
            initial_query="hello",
            initial_status="in_progress",
            msg_type="flash",
        )

    @pytest.mark.asyncio
    async def test_with_external_thread(self):
        from src.server.handlers.chat.request_prep import ensure_thread

        request = MagicMock()
        request.external_thread_id = "ext-123"
        request.platform = "slack"
        request.origin = None

        with patch(f"{PREP}.qr_db.ensure_thread_exists", new_callable=AsyncMock) as mock_db:
            await ensure_thread(
                request, "t-1", "ws-1", "u-1", msg_type="ptc", initial_query=""
            )

        call_kwargs = mock_db.call_args.kwargs
        assert call_kwargs["external_id"] == "ext-123"
        assert call_kwargs["platform"] == "slack"

    @pytest.mark.asyncio
    async def test_default_initial_query(self):
        from src.server.handlers.chat.request_prep import ensure_thread

        request = MagicMock()
        request.external_thread_id = None
        request.platform = None
        request.origin = None

        with patch(f"{PREP}.qr_db.ensure_thread_exists", new_callable=AsyncMock) as mock_db:
            await ensure_thread(request, "t-1", "ws-1", "u-1", msg_type="flash")

        call_kwargs = mock_db.call_args.kwargs
        assert call_kwargs["initial_query"] == ""

    @pytest.mark.asyncio
    async def test_origin_passed_as_metadata(self):
        """request.origin lands in thread metadata under the 'origin' key."""
        from src.server.handlers.chat.request_prep import ensure_thread
        from src.server.models.chat import ThreadOrigin

        request = MagicMock()
        request.external_thread_id = None
        request.platform = None
        request.origin = ThreadOrigin(type="agent", id="flash-t-1")

        with patch(
            f"{PREP}.qr_db.ensure_thread_exists",
            new_callable=AsyncMock,
            return_value=False,
        ) as mock_db:
            await ensure_thread(
                request, "t-1", "ws-1", "u-1", msg_type="ptc", initial_query="hi"
            )

        assert mock_db.call_args.kwargs["metadata"] == {
            "origin": {"type": "agent", "id": "flash-t-1"}
        }

    @pytest.mark.asyncio
    async def test_title_generation_on_create(self):
        """A newly created thread with a first query spawns title generation."""
        from src.server.handlers.chat.request_prep import ensure_thread

        request = MagicMock()
        request.external_thread_id = None
        request.platform = None
        request.origin = None

        with (
            patch(
                f"{PREP}.qr_db.ensure_thread_exists",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "src.server.services.thread_title.schedule_title_generation"
            ) as mock_schedule,
        ):
            await ensure_thread(
                request, "t-1", "ws-1", "u-1", msg_type="ptc", initial_query="hello"
            )

        mock_schedule.assert_called_once()
        assert mock_schedule.call_args.kwargs["thread_id"] == "t-1"
        assert mock_schedule.call_args.kwargs["expected_title"] == "hello"

    @pytest.mark.asyncio
    async def test_no_title_generation_on_existing_thread(self):
        """created=False (pre-created or follow-up turn) must not re-title."""
        from src.server.handlers.chat.request_prep import ensure_thread

        request = MagicMock()
        request.external_thread_id = None
        request.platform = None
        request.origin = None

        with (
            patch(
                f"{PREP}.qr_db.ensure_thread_exists",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch(
                "src.server.services.thread_title.schedule_title_generation"
            ) as mock_schedule,
        ):
            await ensure_thread(
                request, "t-1", "ws-1", "u-1", msg_type="ptc", initial_query="hello"
            )

        mock_schedule.assert_not_called()

    @pytest.mark.asyncio
    async def test_prior_row_read_before_the_ensure_stamps_it(self):
        """The read has to see the row as it was, so it runs first."""
        from src.server.handlers.chat.request_prep import ensure_thread

        request = MagicMock()
        request.external_thread_id = None
        request.platform = None
        request.origin = None

        calls: list[str] = []
        stamp = datetime(2026, 3, 1, 14, 30, tzinfo=UTC)

        async def _read(_thread_id):
            calls.append("read")
            return {"updated_at": stamp, "metadata": {"origin": {"type": "automation"}}}

        async def _ensure(**_kwargs):
            calls.append("ensure")
            return False

        with (
            patch(f"{PREP}.qr_db.get_thread_by_id", new=_read),
            patch(f"{PREP}.qr_db.ensure_thread_exists", new=_ensure),
            patch(
                f"{PREP}.tl_db.get_latest_attempt",
                new_callable=AsyncMock,
                return_value={"created_at": stamp},
            ),
        ):
            prior = await ensure_thread(
                request, "t-1", "ws-1", "u-1", msg_type="ptc", initial_query="hi"
            )

        assert calls == ["read", "ensure"]
        assert prior.last_turn_at == stamp

    @pytest.mark.asyncio
    async def test_the_prior_turn_is_the_last_attempt_not_the_thread_stamp(self):
        """A rename or a share bumps ``updated_at`` between turns, and a losing
        concurrent POST bumps it on another worker; an attempt row is written
        only by an admitted turn."""
        from src.server.handlers.chat.request_prep import ensure_thread

        request = MagicMock()
        request.external_thread_id = None
        request.platform = None
        request.origin = None
        renamed_at = datetime(2026, 3, 4, 9, 0, tzinfo=UTC)
        last_turn = datetime(2026, 3, 1, 14, 30, tzinfo=UTC)

        with (
            patch(
                f"{PREP}.qr_db.get_thread_by_id",
                new_callable=AsyncMock,
                return_value={"updated_at": renamed_at},
            ),
            patch(f"{PREP}.qr_db.ensure_thread_exists", new_callable=AsyncMock),
            patch(
                f"{PREP}.tl_db.get_latest_attempt",
                new_callable=AsyncMock,
                return_value={"created_at": last_turn},
            ),
        ):
            prior = await ensure_thread(
                request, "t-1", "ws-1", "u-1", msg_type="ptc", initial_query=""
            )

        assert prior.last_turn_at == last_turn

    @pytest.mark.asyncio
    async def test_naive_prior_stamp_becomes_aware(self):
        """A naive stamp would raise against the envelope's aware clock."""
        from src.server.handlers.chat.request_prep import ensure_thread

        request = MagicMock()
        request.external_thread_id = None
        request.platform = None
        request.origin = None

        with (
            patch(
                f"{PREP}.qr_db.get_thread_by_id",
                new_callable=AsyncMock,
                return_value={"updated_at": datetime(2026, 3, 1, 14, 30)},
            ),
            patch(f"{PREP}.qr_db.ensure_thread_exists", new_callable=AsyncMock),
            patch(
                f"{PREP}.tl_db.get_latest_attempt",
                new_callable=AsyncMock,
                return_value={"created_at": datetime(2026, 3, 1, 14, 30)},
            ),
        ):
            prior = await ensure_thread(
                request, "t-1", "ws-1", "u-1", msg_type="ptc", initial_query=""
            )

        assert prior.last_turn_at == datetime(2026, 3, 1, 14, 30, tzinfo=UTC)

    @pytest.mark.asyncio
    async def test_a_fork_measures_from_the_turn_before_the_fork(self):
        """An edit of turn 3 discards turns 3 and later; the history the model
        reads ends at turn 2, so that is the prior turn."""
        from src.server.handlers.chat.request_prep import ensure_thread

        request = MagicMock()
        request.external_thread_id = None
        request.platform = None
        request.origin = None
        request.fork_from_turn = 3
        request.checkpoint_id = "ckpt-3"
        before = datetime(2026, 3, 1, 9, 0, tzinfo=UTC)

        with (
            patch(
                f"{PREP}.qr_db.get_thread_by_id",
                new_callable=AsyncMock,
                return_value={"updated_at": datetime(2026, 3, 4, 9, 0, tzinfo=UTC)},
            ),
            patch(f"{PREP}.qr_db.ensure_thread_exists", new_callable=AsyncMock),
            patch(
                f"{PREP}.tl_db.get_latest_attempt",
                new_callable=AsyncMock,
                return_value={"created_at": before},
            ) as latest,
        ):
            prior = await ensure_thread(
                request, "t-1", "ws-1", "u-1", msg_type="ptc", initial_query=""
            )

        latest.assert_awaited_once_with("t-1", before_turn=3)
        assert prior.last_turn_at == before

    @pytest.mark.asyncio
    async def test_a_plain_turn_reads_the_newest_attempt(self):
        """Without a checkpoint there is no fork, whatever ``fork_from_turn`` says."""
        from src.server.handlers.chat.request_prep import ensure_thread

        request = MagicMock()
        request.external_thread_id = None
        request.platform = None
        request.origin = None
        request.fork_from_turn = None
        request.checkpoint_id = None

        with (
            patch(
                f"{PREP}.qr_db.get_thread_by_id",
                new_callable=AsyncMock,
                return_value={"updated_at": datetime(2026, 3, 4, 9, 0, tzinfo=UTC)},
            ),
            patch(f"{PREP}.qr_db.ensure_thread_exists", new_callable=AsyncMock),
            patch(
                f"{PREP}.tl_db.get_latest_attempt", new_callable=AsyncMock, return_value=None
            ) as latest,
        ):
            await ensure_thread(
                request, "t-1", "ws-1", "u-1", msg_type="ptc", initial_query=""
            )

        latest.assert_awaited_once_with("t-1", before_turn=None)

    @pytest.mark.asyncio
    async def test_a_thread_that_never_ran_has_no_prior_turn(self):
        """The create-first web flow leaves a thread behind when the send never
        happened; its ``updated_at`` is not a turn."""
        from src.server.handlers.chat.request_prep import ensure_thread

        request = MagicMock()
        request.external_thread_id = None
        request.platform = None
        request.origin = None

        with (
            patch(
                f"{PREP}.qr_db.get_thread_by_id",
                new_callable=AsyncMock,
                return_value={"updated_at": datetime(2026, 3, 1, 14, 30, tzinfo=UTC)},
            ),
            patch(f"{PREP}.qr_db.ensure_thread_exists", new_callable=AsyncMock),
            patch(f"{PREP}.tl_db.get_latest_attempt", new_callable=AsyncMock, return_value=None),
        ):
            prior = await ensure_thread(
                request, "t-1", "ws-1", "u-1", msg_type="ptc", initial_query=""
            )

        assert prior.last_turn_at is None

    @pytest.mark.asyncio
    async def test_first_turn_has_no_prior(self):
        from src.server.handlers.chat.request_prep import ensure_thread

        request = MagicMock()
        request.external_thread_id = None
        request.platform = None
        request.origin = None

        with patch(f"{PREP}.qr_db.ensure_thread_exists", new_callable=AsyncMock):
            prior = await ensure_thread(
                request, "t-1", "ws-1", "u-1", msg_type="ptc", initial_query="hi"
            )

        assert prior.last_turn_at is None

    @pytest.mark.asyncio
    async def test_failed_prior_read_still_starts_the_turn(self):
        """Context, not correctness: a read failure degrades to no prior."""
        from src.server.handlers.chat.request_prep import ensure_thread

        request = MagicMock()
        request.external_thread_id = None
        request.platform = None
        request.origin = None

        with (
            patch(
                f"{PREP}.qr_db.get_thread_by_id",
                new_callable=AsyncMock,
                side_effect=RuntimeError("pool down"),
            ),
            patch(
                f"{PREP}.qr_db.ensure_thread_exists", new_callable=AsyncMock
            ) as mock_db,
        ):
            prior = await ensure_thread(
                request, "t-1", "ws-1", "u-1", msg_type="ptc", initial_query="hi"
            )

        assert prior == PriorThread()
        mock_db.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_title_generation_without_llm_service(self):
        """The scheduler owns llm_service resolution, so neither creation door
        carries a `getattr(setup, ...)` locator that could drift."""
        from src.server.services.thread_title import schedule_title_generation

        with patch("src.server.app.setup") as mock_setup:
            mock_setup.llm_service = None
            assert (
                schedule_title_generation(
                    thread_id="t-1",
                    user_id="u-1",
                    first_query="hello",
                    expected_title="hello",
                )
                is None
            )


# ---------------------------------------------------------------------------
# build_turn_context
# ---------------------------------------------------------------------------


class TestBuildTurnContext:
    def _request(self, origin=None, platform=None, surface_rules=None):
        request = MagicMock(timezone="UTC", locale="en-US")
        request.origin = origin
        request.platform = platform
        request.surface_rules = surface_rules
        return request

    def test_origin_comes_from_the_request(self):
        from src.server.models.chat import ThreadOrigin

        ctx = build_turn_context(
            self._request(origin=ThreadOrigin(type="agent", id="flash-t-1")),
            PriorThread(),
            user_profile=None,
        )

        assert ctx.origin == "agent"

    def test_a_manual_follow_up_carries_no_origin(self):
        """A person replying in an automation's thread is waiting; the thread's origin must not say otherwise."""
        ctx = build_turn_context(self._request(), PriorThread(), user_profile=None)

        assert ctx.origin is None

    def test_surface_comes_from_the_request_alone(self):
        stamp = datetime(2026, 3, 1, 14, 30, tzinfo=UTC)
        ctx = build_turn_context(
            self._request(platform="slack", surface_rules="reply in one block"),
            PriorThread(last_turn_at=stamp),
            user_profile=None,
        )

        assert (ctx.platform, ctx.surface_rules) == ("slack", "reply in one block")
        assert ctx.last_turn_at == stamp


# ---------------------------------------------------------------------------
# build_turn_context: the turn's zones
# ---------------------------------------------------------------------------


class TestTurnZones:
    def _zones(self, request_tz, locale="en-US", profile=None):
        request = MagicMock(timezone=request_tz, locale=locale)
        request.origin = None
        ctx = build_turn_context(request, PriorThread(), user_profile=profile)
        return ctx.timezone, ctx.tool_timezone

    def test_valid_request_zone(self):
        assert self._zones("America/New_York") == (
            "America/New_York",
            "America/New_York",
        )

    def test_invalid_zone_falls_back_to_the_locale(self):
        with patch(
            f"{PREP}.get_locale_config",
            return_value={"timezone": "Asia/Shanghai"},
        ):
            assert self._zones("Invalid/Zone", "zh-CN") == (None, "Asia/Shanghai")

    @pytest.mark.parametrize("malformed", ["../etc/passwd", "America", "x" * 300])
    def test_a_malformed_zone_falls_back_like_an_unknown_one(self, malformed):
        """ZoneInfo refuses these as ValueError or OSError, not as an unknown
        name; the turn must still get a clock rather than fail."""
        with patch(f"{PREP}.get_locale_config", return_value={"timezone": "UTC"}):
            assert self._zones(malformed) == (None, "UTC")

    def test_none_locale_uses_default(self):
        with patch(
            f"{PREP}.get_locale_config",
            return_value={"timezone": "UTC"},
        ) as mock_locale:
            assert self._zones(None, None) == (None, "UTC")

        mock_locale.assert_called_once_with("en-US", "en")

    def test_profile_zone_wins_over_the_request(self):
        zones = self._zones("America/New_York", profile={"timezone": "Asia/Shanghai"})
        assert zones == ("Asia/Shanghai", "Asia/Shanghai")

    def test_invalid_profile_zone_falls_to_the_request(self):
        zones = self._zones("Europe/London", profile={"timezone": "Not/AZone"})
        assert zones == ("Europe/London", "Europe/London")

    def test_a_channel_turn_with_no_zone_gets_the_profile_zone(self):
        """The gateway and the automation executor send neither zone nor
        locale; the agent's clock is on the profile zone, so tools must be."""
        zones = self._zones(None, None, profile={"timezone": "Asia/Shanghai"})
        assert zones == ("Asia/Shanghai", "Asia/Shanghai")

    def test_an_unnamed_zone_reaches_tools_but_not_the_stamp(self):
        """An automation's request names no zone, and a failed profile read
        answers None: tools read the locale default, but the stamp is given no
        zone, so it keeps the one the frozen identity states."""
        with patch(
            f"{PREP}.get_locale_config",
            return_value={"timezone": "America/New_York"},
        ):
            assert self._zones(None, None, profile=None) == (None, "America/New_York")
