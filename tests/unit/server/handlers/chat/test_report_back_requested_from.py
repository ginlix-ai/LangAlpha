"""Where a hand-off was asked for, kept for the turn that reports it back.

A dispatch records the place its turn came from in the report-back record it
reserves: an automation run held by the messaging service, with the targets
its agent was told, or the chat app the turn arrived on. The report-back turn
is reminded of it, and the run rides into that turn's config. A record from
before this was kept, or a request from the web app, reminds of nothing.
"""

from __future__ import annotations

import pytest

from src.server.services import automation_delivery
from src.server.services.automation_delivery import Delivery, Target
from src.server.services.report_back.flash import requested_from as RF
from src.server.services.report_back.flash import reserve as R
from src.server.services.report_back.flash.keys import flash_rb_run_key, ptc_origin_key
from tests.unit.server.handlers.chat.redis_fakes import FakeCache

FLASH = "flash-1"
PTC = "ptc-1"

DESK = Target(entry="slack:T/C", address="slack:T/C", name="#desk", ok=True)
DISCORD_DM = Target(entry="discord", address="discord:@me", name=None, ok=True)
UNLINKED = Target(
    entry="telegram", address=None, name=None, ok=False, message="Telegram isn't linked"
)


def _run_turn(*targets: Target, run_id: str = "exec-1") -> dict:
    """The config of a held automation run's turn."""
    return automation_delivery.turn_configurable(Delivery(run_id, list(targets)))


def _record(*entries: dict) -> dict:
    return {"flash_thread_id": FLASH, RF.KEY: list(entries)}


class TestWhatADispatchRecords:
    def test_a_held_runs_turn_records_the_run_and_its_reached_targets(self):
        entry = RF.of_turn(
            {**_run_turn(DESK, UNLINKED), "platform": "slack"}, "Value NVDA"
        )
        assert entry == {
            "asked": "Value NVDA",
            "delivery": {"id": "exec-1", "targets": [DESK.as_dict()]},
        }

    @pytest.mark.parametrize(
        "platform", ["slack", "telegram", "imessage", "discord:AAPL"]
    )
    def test_a_chat_apps_turn_records_the_app(self, platform):
        entry = RF.of_turn({"platform": platform}, "Value NVDA")
        assert entry == {"asked": "Value NVDA", "surface": platform.split(":")[0]}

    @pytest.mark.parametrize(
        "configurable",
        [
            {"platform": "web"},
            {"platform": "market_view:AAPL"},
            {},
            {"platform": None},
        ],
        ids=["web", "market view", "no platform", "null platform"],
    )
    def test_a_turn_from_the_web_app_or_nowhere_records_only_the_request(
        self, configurable
    ):
        assert RF.of_turn(configurable, "Value NVDA") == {"asked": "Value NVDA"}

    def test_a_run_whose_turn_carried_no_targets_goes_by_its_surface(self):
        """A turn started before its targets were carried names the run's id
        alone, which says nowhere to send."""
        entry = RF.of_turn(
            {"automation_execution_id": "exec-1", "platform": "slack"}, "Value NVDA"
        )
        assert entry == {"asked": "Value NVDA", "surface": "slack"}

    def test_a_stamp_for_another_run_is_not_this_runs(self):
        configurable = {
            **_run_turn(DESK, run_id="exec-old"),
            "automation_execution_id": "exec-1",
        }
        assert "delivery" not in RF.of_turn(configurable, "Value NVDA")

    def test_the_request_is_one_short_line(self):
        entry = RF.of_turn({}, "Value `NVDA`\nthen AMD " + "x" * 200)
        assert "\n" not in entry["asked"] and "`" not in entry["asked"]
        assert len(entry["asked"]) == 60
        assert entry["asked"].startswith("Value NVDA then AMD")


class TestTheRecordKeepsTheRequestsStillWaiting:
    def test_a_first_dispatch_records_its_own(self):
        assert RF.recorded(
            {"asked": "a"}, None, flash_thread_id=FLASH, reported=False
        ) == [{"asked": "a"}]

    def test_an_unreported_record_keeps_its_requests_first(self):
        previous = _record({"asked": "a", "surface": "slack"})
        kept = RF.recorded(
            {"asked": "b"}, previous, flash_thread_id=FLASH, reported=False
        )
        assert kept == [{"asked": "a", "surface": "slack"}, {"asked": "b"}]

    def test_a_reported_record_keeps_nothing(self):
        previous = _record({"asked": "a", "surface": "slack"})
        kept = RF.recorded(
            {"asked": "b"}, previous, flash_thread_id=FLASH, reported=True
        )
        assert kept == [{"asked": "b"}]

    def test_another_threads_record_keeps_nothing(self):
        previous = {**_record({"asked": "a"}), "flash_thread_id": "flash-2"}
        kept = RF.recorded(
            {"asked": "b"}, previous, flash_thread_id=FLASH, reported=False
        )
        assert kept == [{"asked": "b"}]

    def test_the_same_request_from_the_same_place_is_kept_once_at_its_latest(self):
        previous = _record({"asked": "a"}, {"asked": "b"})
        kept = RF.recorded(
            {"asked": "a"}, previous, flash_thread_id=FLASH, reported=False
        )
        assert kept == [{"asked": "b"}, {"asked": "a"}]

    def test_the_oldest_go_first_past_the_cap(self):
        previous = _record(*({"asked": str(i)} for i in range(RF.MAX_ENTRIES)))
        kept = RF.recorded(
            {"asked": "new"}, previous, flash_thread_id=FLASH, reported=False
        )
        assert len(kept) == RF.MAX_ENTRIES
        assert kept[0] == {"asked": "1"} and kept[-1] == {"asked": "new"}

    def test_an_unreadable_list_is_none(self):
        assert RF.entries({RF.KEY: {}}) == []
        assert RF.entries({RF.KEY: ["x", {"asked": "a"}]}) == [{"asked": "a"}]


class TestTheReminder:
    def test_an_automation_run_with_one_target(self):
        record = _record(RF.of_turn(_run_turn(DESK), "Value NVDA"))
        assert RF.reminder(record) == (
            "You handed this analysis off during an automation run that sends its "
            "results to: #desk (Slack) `slack:T/C`. If the outcome still belongs "
            "there, send it there with send_message. Write it for chat; attach "
            "files for detail."
        )

    def test_an_automation_run_with_two_targets(self):
        record = _record(
            RF.of_turn(_run_turn(DESK, DISCORD_DM, UNLINKED), "Value NVDA")
        )
        assert RF.reminder(record) == (
            "You handed this analysis off during an automation run that sends its "
            "results to: #desk (Slack) `slack:T/C`, your Discord DM `discord:@me`. "
            "If the outcome still belongs there, send it there with send_message. "
            "Write it for chat; attach files for detail."
        )

    def test_a_chat_app(self):
        record = _record(RF.of_turn({"platform": "slack"}, "Value NVDA"))
        assert RF.reminder(record) == (
            "You handed this analysis off from a Slack conversation. If the "
            "outcome still belongs there, deliver it there as that conversation's "
            "delivery rules say."
        )

    def test_an_app_named_with_a_vowel(self):
        record = _record(RF.of_turn({"platform": "imessage"}, "Value NVDA"))
        assert RF.reminder(record).startswith(
            "You handed this analysis off from an iMessage conversation."
        )

    @pytest.mark.parametrize(
        "record",
        [
            _record(RF.of_turn({"platform": "web"}, "Value NVDA")),
            _record(RF.of_turn({}, "Value NVDA"), RF.of_turn({}, "And AMD")),
            {"flash_thread_id": FLASH, "report_back": True, "dispatch_gen": "g1"},
            None,
        ],
        ids=["web", "web twice", "a record from before", "no record"],
    )
    def test_nothing_when_every_request_came_from_this_conversation(self, record):
        assert RF.reminder(record) is None

    def test_requests_from_one_place_get_one_sentence(self):
        record = _record(
            RF.of_turn({"platform": "slack"}, "Value NVDA"),
            RF.of_turn({"platform": "slack"}, "And AMD"),
        )
        assert RF.reminder(record).startswith(
            "You handed this analysis off from a Slack"
        )

    def test_requests_from_several_places_get_a_line_each(self):
        record = _record(
            RF.of_turn(_run_turn(DESK), "Draft the NVDA note"),
            RF.of_turn({"platform": "slack"}, "Compare margins with AMD"),
            RF.of_turn({"platform": "web"}, "Check the guidance"),
        )
        assert RF.reminder(record) == (
            "You handed this analysis off more than once. Where each request came from:\n"
            '- "Draft the NVDA note": an automation run that sends its results to: '
            "#desk (Slack) `slack:T/C`\n"
            '- "Compare margins with AMD": a Slack conversation\n'
            '- "Check the guidance": this conversation\n'
            "Deliver each outcome where its request came from, if it still belongs "
            "there. Send to an automation run's targets with send_message, written "
            "for chat with files attached for detail. Deliver to a conversation as "
            "its delivery rules say."
        )

    def test_a_request_is_quoted_as_one_plain_line_or_not_at_all(self):
        """The record is read back from storage, so what it quotes is made
        plain again before the agent is told it."""
        record = _record(
            {"asked": "Value `NVDA`\n- then: AMD", "surface": "slack"},
            {"surface": "telegram"},
        )
        lines = RF.reminder(record).splitlines()
        assert lines[1] == '- "Value NVDA - then: AMD": a Slack conversation'
        assert lines[2] == "- A request: a Telegram conversation"

    def test_two_runs_keep_their_targets_apart(self):
        record = _record(
            RF.of_turn(_run_turn(DESK, run_id="exec-1"), "Morning brief"),
            RF.of_turn(_run_turn(DISCORD_DM, run_id="exec-2"), "Evening brief"),
        )
        lines = RF.reminder(record).splitlines()
        assert lines[1] == (
            '- "Morning brief": an automation run that sends its results to: '
            "#desk (Slack) `slack:T/C`"
        )
        assert lines[2] == (
            '- "Evening brief": an automation run that sends its results to: '
            "your Discord DM `discord:@me`"
        )
        assert "Deliver to a conversation" not in lines[3]


class TestTheRunTheTurnSendsFor:
    def test_the_latest_run_a_request_came_from(self):
        record = _record(
            RF.of_turn(_run_turn(DESK, run_id="exec-1"), "a"),
            RF.of_turn(_run_turn(DISCORD_DM, run_id="exec-2"), "b"),
            RF.of_turn({"platform": "slack"}, "c"),
        )
        assert RF.delivery(record) == Delivery("exec-2", [DISCORD_DM])

    @pytest.mark.parametrize(
        "record",
        [
            _record(RF.of_turn({"platform": "slack"}, "a")),
            {"flash_thread_id": FLASH},
            None,
            _record({"delivery": {"id": "exec-1", "targets": [UNLINKED.as_dict()]}}),
        ],
        ids=["chat", "a record from before", "no record", "no target reached"],
    )
    def test_none_without_a_run(self, record):
        assert RF.delivery(record) is None


@pytest.fixture
def cache(monkeypatch):
    c = FakeCache()
    monkeypatch.setattr("src.utils.cache.redis_cache.get_cache_client", lambda: c)
    return c


async def _reserve(entry: dict | None) -> str:
    kwargs = {} if entry is None else {"requested_from": entry}
    async with R.reserve(FLASH, PTC, "ws-1", "fws-1", "u-1", **kwargs) as slot:
        assert slot.error is None
        slot.commit()
        return slot.dispatch_gen


class TestTheReservedRecord:
    @pytest.mark.asyncio
    async def test_it_records_where_the_dispatch_came_from(self, cache):
        entry = RF.of_turn(_run_turn(DESK), "Value NVDA")
        await _reserve(entry)
        assert cache.kv[ptc_origin_key(PTC)][RF.KEY] == [entry]

    @pytest.mark.asyncio
    async def test_a_reservation_naming_nothing_records_nothing(self, cache):
        await _reserve(None)
        assert RF.KEY not in cache.kv[ptc_origin_key(PTC)]

    @pytest.mark.asyncio
    async def test_a_continuation_before_the_report_keeps_the_earlier_request(
        self, cache
    ):
        first = RF.of_turn(_run_turn(DESK), "Value NVDA")
        second = RF.of_turn({"platform": "slack"}, "And AMD")
        await _reserve(first)
        await _reserve(second)
        assert cache.kv[ptc_origin_key(PTC)][RF.KEY] == [first, second]

    @pytest.mark.asyncio
    async def test_a_continuation_after_the_report_started_keeps_only_its_own(
        self, cache
    ):
        first = RF.of_turn(_run_turn(DESK), "Value NVDA")
        second = RF.of_turn({"platform": "slack"}, "And AMD")
        gen = await _reserve(first)
        cache.kv[flash_rb_run_key(FLASH, PTC)] = {
            "run_id": "run-1",
            "dispatch_gen": gen,
        }
        await _reserve(second)
        assert cache.kv[ptc_origin_key(PTC)][RF.KEY] == [second]

    @pytest.mark.asyncio
    async def test_a_report_of_an_older_dispatch_does_not_count(self, cache):
        """The pointer can outlive its report; one claimed for an earlier
        generation says nothing about the requests since."""
        first = RF.of_turn(_run_turn(DESK), "Value NVDA")
        second = RF.of_turn({"platform": "slack"}, "And AMD")
        await _reserve(first)
        cache.kv[flash_rb_run_key(FLASH, PTC)] = {
            "run_id": "run-0",
            "dispatch_gen": "g-old",
        }
        await _reserve(second)
        assert cache.kv[ptc_origin_key(PTC)][RF.KEY] == [first, second]

    @pytest.mark.asyncio
    async def test_a_record_from_before_adds_only_this_request(self, cache):
        cache.kv[ptc_origin_key(PTC)] = {
            "flash_thread_id": FLASH,
            "ptc_thread_id": PTC,
            "report_back": True,
            "user_id": "u-1",
            "dispatch_gen": "g-0",
        }
        entry = RF.of_turn({"platform": "slack"}, "And AMD")
        await _reserve(entry)
        assert cache.kv[ptc_origin_key(PTC)][RF.KEY] == [entry]

    @pytest.mark.asyncio
    async def test_a_failed_read_records_this_request_alone(self, cache, monkeypatch):
        first = RF.of_turn(_run_turn(DESK), "Value NVDA")
        await _reserve(first)
        real = cache.get_strict

        async def flaky(key):
            if key == flash_rb_run_key(FLASH, PTC):
                raise ConnectionError("blip")
            return await real(key)

        monkeypatch.setattr(cache, "get_strict", flaky)
        second = RF.of_turn({"platform": "slack"}, "And AMD")
        await _reserve(second)
        assert cache.kv[ptc_origin_key(PTC)][RF.KEY] == [second]
