"""Settled contract of the turn's anchor row.

The row is the one piece of runtime context that is written once and read for
the rest of the thread, which is what makes its shape worth pinning. Facts
only, because a countdown written at 09:00 is a lie by 09:05 and nothing
rewrites it. The same bytes at both guidance levels and no framing of its own,
because a persisted row outlives the model that wrote it and may be replayed to
one that reads a different carrier shape. And it is context, so a failure
anywhere in it costs the row, never the turn.
"""

import re
from datetime import UTC, date, datetime, timedelta

import pytest
from langchain_core.messages import HumanMessage

from ptc_agent.agent.middleware.runtime_context import (
    ELAPSED_MIN_GAP,
    RUNTIME_UPDATE_KEY,
    RUNTIME_UPDATE_SOURCE,
    TURN_ROW_KIND,
    TURN_SCHEMA_VERSION,
    TurnContextMiddleware,
    render_update_row,
    runtime_update_from_message,
)
from ptc_agent.agent.middleware.runtime_context.clock import MARKET_CLOCK
from ptc_agent.agent.middleware.runtime_context.state import STATE_BASELINE

# A Wednesday, 13:00 ET: the US market is open, so the session line has a
# same-day close to name and the weekend branches stay out of it.
NOW = datetime(2026, 9, 9, 17, 0, tzinfo=UTC)

# "in 3h", "in 45m": the shape a countdown takes in the relative line.
COUNTDOWN = r"\bin \d+[hm]\b"

# What a channel gateway sends for its own surface. langalpha ships no wording
# of its own for a channel, so this text is the whole of what `slack` states.
SLACK_RULES = (
    "Surface slack: plain text or the channel's limited markdown, a few short "
    "paragraphs, one message per answer."
)
REWORDED_SLACK_RULES = (
    "Surface slack: plain text only, one message per answer, and no widgets."
)


async def _row(*, now: datetime = NOW, state: dict | None = None, **kwargs):
    written = await TurnContextMiddleware(now=now, **kwargs).abefore_agent(state or {})
    assert written is not None
    messages = written["messages"]
    assert len(messages) == 1
    return messages[0]


def _provenance(message: HumanMessage) -> dict:
    return message.additional_kwargs[RUNTIME_UPDATE_KEY]["provenance"]


def _state(rows: list, *, cutoff_index: int | None = None) -> dict:
    """The state a later turn opens against: the rows history already holds.

    A cutoff is the compaction boundary, so a row at a lower index is one the
    model can no longer read.
    """
    state: dict = {"messages": list(rows)}
    if cutoff_index is not None:
        state["_summarization_event"] = {"cutoff_index": cutoff_index}
    return state


class TestTheRowIsAPersistedMessage:
    @pytest.mark.asyncio
    async def test_it_is_a_human_message_tagged_as_a_runtime_update(self):
        """The one role every provider accepts at any position, so a thread can
        be replayed to a model it did not start on."""
        message = await _row(timezone="America/New_York", preferred_market="US")

        assert isinstance(message, HumanMessage)
        assert message.additional_kwargs["lc_source"] == RUNTIME_UPDATE_SOURCE
        meta = message.additional_kwargs[RUNTIME_UPDATE_KEY]
        assert meta["kind"] == TURN_ROW_KIND
        assert meta["schema_version"] == TURN_SCHEMA_VERSION
        assert meta["provenance"] == {
            "source": "harness",
            "surface": None,
            "symbol": None,
            "origin": None,
            "market": "US",
            "zone": "America/New_York",
        }
        assert meta["created_at"] == NOW.isoformat()

    @pytest.mark.asyncio
    async def test_no_id_is_minted(self):
        """The row is returned from a middleware hook, so the Pregel path stamps
        it; an id minted here would re-roll on every replay."""
        assert (await _row()).id is None

    @pytest.mark.asyncio
    async def test_the_row_reads_back_as_the_update_that_wrote_it(self):
        update = runtime_update_from_message(await _row(preferred_market="US"))

        assert update is not None
        assert update.kind == TURN_ROW_KIND
        assert update.text == (await _row(preferred_market="US")).content


class TestTheRowIsFactForm:
    @pytest.mark.asyncio
    async def test_it_carries_the_opening_time_and_the_session_as_facts(self):
        text = (await _row(timezone="America/New_York", preferred_market="US")).content

        assert "1:00 PM EDT, Wednesday, September 9, 2026" in text
        assert "US: regular hours, closes Wed 16:00 ET" in text

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "kwargs",
        [
            {"preferred_market": "US"},
            {"preferred_market": "CN", "timezone": "Asia/Shanghai"},
            {"preferred_market": "US", "timezone": "Asia/Shanghai"},
        ],
    )
    async def test_no_line_ever_counts_down(self, kwargs):
        """A countdown is true for a minute; this row is read for the thread."""
        assert not re.search(COUNTDOWN, (await _row(**kwargs)).content)

    @pytest.mark.asyncio
    async def test_an_unknown_market_drops_the_session_line(self):
        text = (await _row(preferred_market="JP")).content

        assert "JP" not in text
        assert text.strip().splitlines() == [
            "5:00 PM UTC, Wednesday, September 9, 2026"
        ]

    @pytest.mark.asyncio
    async def test_no_line_wears_a_label(self):
        """A stamp, a session line and a gap each say what they are in their own
        words, so a heading would only spend tokens naming the line twice."""
        text = (
            await _row(
                timezone="America/New_York",
                preferred_market="US",
                last_turn_at=NOW - timedelta(days=1),
                platform="web",
                origin="automation",
            )
        ).content

        assert "**" not in text
        assert text.splitlines()[:5] == [
            "1:00 PM EDT, Wednesday, September 9, 2026 (America/New_York)",
            "US: regular hours, closes Wed 16:00 ET",
            "1d since your last turn (1 US session closed in between)",
            "Surface `web`, started by automation",
            # The delivery rules are a paragraph, not a fifth unlabelled line.
            "",
        ]


class TestTheGapSinceTheLastTurn:
    @pytest.mark.asyncio
    async def test_a_short_gap_says_nothing(self):
        """Below the threshold the gap is conversational pacing, not absence."""
        text = (
            await _row(
                preferred_market="US",
                last_turn_at=NOW - ELAPSED_MIN_GAP + timedelta(minutes=1),
            )
        ).content

        assert "since your last turn" not in text

    @pytest.mark.asyncio
    async def test_the_threshold_itself_renders(self):
        text = (
            await _row(preferred_market="US", last_turn_at=NOW - ELAPSED_MIN_GAP)
        ).content

        assert "15m since your last turn" in text

    @pytest.mark.asyncio
    async def test_a_long_gap_counts_the_sessions_that_closed_inside_it(self):
        """Five days back is a Friday, and Labor Day sits in the middle, so the
        count is two sessions rather than the three a day count would suggest."""
        text = (
            await _row(preferred_market="US", last_turn_at=NOW - timedelta(days=5))
        ).content

        assert "5d since your last turn (2 US sessions closed in between)" in text

    @pytest.mark.asyncio
    async def test_one_closed_session_is_singular(self):
        text = (
            await _row(preferred_market="US", last_turn_at=NOW - timedelta(days=1))
        ).content

        assert "(1 US session closed in between)" in text

    @pytest.mark.asyncio
    async def test_a_first_turn_has_no_gap_line(self):
        assert "since your last turn" not in (await _row()).content


class TestTheMarketLineOnlyRepeatsWhenItMoved:
    """The row is history: a session line the previous row already stated is a
    repeat the model reads on every later call, so it is written only when the
    state moved, or when the gap line is telling the model to re-fetch."""

    @pytest.mark.asyncio
    async def test_the_first_turn_of_a_thread_states_it(self):
        """Nothing earlier said it, so it has to be said here."""
        text = (await _row(preferred_market="US")).content

        assert "US: regular hours, closes Wed 16:00 ET" in text

    @pytest.mark.asyncio
    async def test_an_unchanged_line_under_the_threshold_drops(self):
        first = await _row(preferred_market="US")
        text = (
            await _row(
                preferred_market="US",
                last_turn_at=NOW - timedelta(minutes=14),
                state=_state([first]),
            )
        ).content

        assert "US:" not in text
        assert text.strip().splitlines() == [
            "5:00 PM UTC, Wednesday, September 9, 2026"
        ]

    @pytest.mark.asyncio
    async def test_a_zone_change_under_the_threshold_states_the_line_again(self):
        """The same phase reads as a different hour in another zone, so the
        line the model can still read is stale the moment the zone moves."""
        first = await _row(preferred_market="US", timezone="Europe/London")
        text = (
            await _row(
                preferred_market="US",
                timezone="Asia/Shanghai",
                last_turn_at=NOW - timedelta(minutes=14),
                state=_state([first]),
            )
        ).content

        assert "US: regular hours" in text
        assert _provenance(first)["zone"] == "Europe/London"

    @pytest.mark.asyncio
    async def test_a_compaction_that_took_every_row_states_the_line_again(self):
        """An unchanged line is a repeat only of a line the model can still read."""
        first = await _row(preferred_market="US")
        text = (
            await _row(
                preferred_market="US",
                last_turn_at=NOW - timedelta(minutes=14),
                state=_state([first], cutoff_index=1),
            )
        ).content

        assert "US: regular hours" in text

    @pytest.mark.asyncio
    async def test_a_thread_with_no_row_in_view_states_the_line(self):
        """A thread from before rows existed has no session line to repeat."""
        text = (
            await _row(preferred_market="US", last_turn_at=NOW - timedelta(minutes=14))
        ).content

        assert "US: regular hours" in text

    @pytest.mark.asyncio
    async def test_a_line_that_moved_is_stated_again(self):
        """Ten minutes that cross 16:00 ET turn regular hours into after-hours."""
        text = (
            await _row(
                now=datetime(2026, 9, 9, 20, 5, tzinfo=UTC),
                preferred_market="US",
                last_turn_at=datetime(2026, 9, 9, 19, 55, tzinfo=UTC),
            )
        ).content

        assert "US: after-hours, next open Thu 09:30 ET" in text

    @pytest.mark.asyncio
    async def test_the_whole_line_decides_not_the_session_name(self):
        """Friday midnight ET stays CLOSED across the boundary, but the reason
        changes from an ordinary evening to the weekend. Comparing the session
        name alone would swallow that."""
        friday_night = datetime(2026, 9, 12, 3, 55, tzinfo=UTC)
        saturday = datetime(2026, 9, 12, 4, 5, tzinfo=UTC)
        assert MARKET_CLOCK.session_at("US", friday_night).name == (
            MARKET_CLOCK.session_at("US", saturday).name
        )

        text = (
            await _row(now=saturday, preferred_market="US", last_turn_at=friday_night)
        ).content

        assert "US: closed (weekend), next open Mon 09:30 ET" in text

    @pytest.mark.asyncio
    async def test_a_market_change_states_the_line_even_in_the_same_phase(self):
        """US and HK are both closed at 5 PM UTC on a weekday evening in HK terms
        only by phase; the last row spoke for another market, so the line is news."""
        first = await _row(preferred_market="US")
        text = (
            await _row(
                preferred_market="HK",
                last_turn_at=NOW - timedelta(minutes=5),
                state=_state([first]),
            )
        ).content

        assert "HK:" in text

    @pytest.mark.asyncio
    async def test_the_same_market_under_the_threshold_still_drops(self):
        first = await _row(preferred_market="US")
        text = (
            await _row(
                preferred_market="US",
                last_turn_at=NOW - timedelta(minutes=5),
                state=_state([first]),
            )
        ).content

        assert "US:" not in text

    @pytest.mark.asyncio
    async def test_a_blind_build_takes_the_market_the_identity_block_states(self):
        """A profile read that did not answer is not a US user."""
        text = (
            await _row(
                preferred_market=None,
                state={STATE_BASELINE: {"identity": {"preferred_market": "HK"}}},
            )
        ).content

        assert "HK:" in text
        assert "US:" not in text

    @pytest.mark.asyncio
    async def test_a_blind_build_takes_the_zone_the_identity_block_states(self):
        text = (
            await _row(
                timezone=None,
                preferred_market=None,
                state={STATE_BASELINE: {"identity": {"timezone": "Asia/Shanghai", "preferred_market": "CN"}}},
            )
        ).content

        assert "Asia/Shanghai" in text
        assert "UTC" not in text

    @pytest.mark.asyncio
    async def test_a_blind_build_with_no_epoch_falls_to_the_default(self):
        text = (await _row(preferred_market=None)).content

        assert "US:" in text

    @pytest.mark.asyncio
    async def test_a_subagent_has_no_market_line(self):
        text = (await _row(preferred_market=None, is_subagent=True)).content

        assert "US:" not in text

    @pytest.mark.asyncio
    async def test_the_gap_line_brings_an_unchanged_line_back(self):
        """When the model is told to re-fetch, the state it should re-fetch
        against belongs next to the cue rather than a scroll away."""
        text = (
            await _row(preferred_market="US", last_turn_at=NOW - timedelta(minutes=20))
        ).content

        assert "US: regular hours, closes Wed 16:00 ET" in text
        assert "20m since your last turn" in text


class TestTheRunLine:
    @pytest.mark.asyncio
    async def test_a_subagent_gets_the_run_line_and_nothing_it_cannot_use(self):
        """A subagent is handed no market and no previous turn of its own, so
        the row shrinks to the stamp plus where it is running."""
        text = (await _row(is_subagent=True)).content

        assert "You are a subagent reporting to a parent agent" in text
        assert "US:" not in text
        assert "since your last turn" not in text

    @pytest.mark.asyncio
    async def test_the_surface_symbol_and_origin_compose_into_one_line(self):
        text = (await _row(platform="market_view:AAPL", origin="automation")).content

        assert "Surface `market_view`, symbol AAPL, started by automation" in text

    @pytest.mark.asyncio
    async def test_nothing_to_say_drops_the_line(self):
        text = (await _row()).content

        assert "Surface `" not in text
        assert "started by" not in text
        assert "subagent reporting to a parent agent" not in text


class TestTheDeliveryRulesRideWhenTheyAreNews:
    """What the reply has to look like used to be a static table of every
    surface, carried in the cached prefix on every turn of every thread. In the
    row it is the rules for the surface in hand, written when the model cannot
    still see them on the wire: the first turn, a change of surface, a rewording
    of what the caller sent, the first turn after a compaction dropped the row
    that stated them, and every subagent run, which starts with no history of
    its own.
    """

    @pytest.mark.asyncio
    async def test_the_first_turn_on_a_surface_states_the_rules_it_was_sent(self):
        message = await _row(platform="slack", surface_rules=SLACK_RULES)

        assert _provenance(message)["rules_key"].startswith("slack#")
        assert SLACK_RULES in message.content

    @pytest.mark.asyncio
    async def test_a_second_turn_with_the_same_text_does_not_repeat_it(self):
        """The rules are still on the wire, so restating them is rent."""
        first = await _row(platform="slack", surface_rules=SLACK_RULES)

        text = (
            await _row(
                platform="slack", surface_rules=SLACK_RULES, state=_state([first])
            )
        ).content

        assert SLACK_RULES not in text
        assert "Surface `slack`" in text

    @pytest.mark.asyncio
    async def test_a_reworded_text_is_stated_again(self):
        """The gateway owns the wording, so it may change it without changing
        surface. The turn after it does is the one that has to restate."""
        first = await _row(platform="slack", surface_rules=SLACK_RULES)

        message = await _row(
            platform="slack",
            surface_rules=REWORDED_SLACK_RULES,
            state=_state([first]),
        )

        assert REWORDED_SLACK_RULES in message.content
        assert _provenance(message)["rules_key"] != _provenance(first)["rules_key"]

    @pytest.mark.asyncio
    async def test_a_change_of_surface_states_the_new_rules(self):
        """A thread asked in Slack and followed up in the web app."""
        first = await _row(platform="slack", surface_rules=SLACK_RULES)

        text = (await _row(platform="web", state=_state([first]))).content

        assert "Surface web: full markdown, no length cap" in text

    @pytest.mark.asyncio
    async def test_a_compaction_past_the_stated_row_states_them_again(self):
        """The row is behind the cutoff, so the rules went with it."""
        first = await _row(platform="slack", surface_rules=SLACK_RULES)

        text = (
            await _row(
                platform="slack",
                surface_rules=SLACK_RULES,
                state=_state([first], cutoff_index=1),
            )
        ).content

        assert SLACK_RULES in text

    @pytest.mark.asyncio
    async def test_a_drifted_cutoff_is_re_found_by_its_anchor(self):
        """The stored index points at a row the model still reads once the
        list shifted under it; the anchor id says where the boundary really is."""
        first = await _row(platform="slack", surface_rules=SLACK_RULES)
        state = _state([first], cutoff_index=1)
        # A message injected ahead of the row: the row now sits at index 1,
        # so the positional cutoff would slice it away, but the anchor names
        # it as the first retained message.
        first.id = "row-1"
        state["messages"] = [HumanMessage(content="injected"), first]
        state["_summarization_event"]["anchor_message_id"] = "row-1"

        text = (await _row(platform="slack", surface_rules=SLACK_RULES, state=state)).content

        assert SLACK_RULES not in text

    @pytest.mark.asyncio
    async def test_a_surface_with_no_caller_rules_states_nothing_of_its_own(self):
        """langalpha ships no wording for a channel. Until the gateway sends
        one, the row names the surface and states no rules under it."""
        message = await _row(platform="slack")

        assert _provenance(message)["rules_key"] == "slack"
        assert "Surface `slack`" in message.content
        assert message.content.strip().endswith("Surface `slack`")

    @pytest.mark.asyncio
    async def test_caller_rules_replace_the_built_in_line(self):
        """The client that renders the reply is the authority on it, even on a
        surface langalpha has a line for."""
        text = (
            await _row(platform="web", surface_rules="Surface web: keep it to a page.")
        ).content

        assert "Surface web: keep it to a page." in text
        assert "full markdown, no length cap" not in text

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sent", ["", "   \n  "])
    async def test_blank_caller_rules_fall_back_to_the_built_in_line(self, sent):
        """Nothing to say and no key to say it under: a digest of whitespace
        would restate the web line on the first blank turn and never again."""
        message = await _row(platform="web", surface_rules=sent)

        assert _provenance(message)["rules_key"] == "web"
        assert "Surface web: full markdown, no length cap" in message.content

    @pytest.mark.asyncio
    async def test_an_automation_adds_its_own_paragraph(self):
        message = await _row(platform="web", origin="automation")

        assert _provenance(message)["rules_key"] == "web+automation"
        assert "Surface web: full markdown, no length cap" in message.content
        assert "This thread was started by an automation." in message.content

    @pytest.mark.asyncio
    async def test_a_manual_follow_up_takes_the_thread_back_from_the_automation(self):
        """The automation row says nobody is waiting; the person's turn has to say otherwise, once."""
        auto = await _row(platform="web", origin="automation")
        manual = await _row(platform="web", state=_state([auto]))

        assert _provenance(manual)["rules_key"] == "web"
        assert "A person sent this turn and is waiting for the reply." in manual.content
        assert "Surface web: full markdown" in manual.content
        assert "started by an automation" not in manual.content

        again = await _row(platform="web", state=_state([auto, manual]))
        assert "waiting for the reply" not in again.content

    @pytest.mark.asyncio
    async def test_an_agent_origin_after_an_automation_restates_without_the_handoff(self):
        """Only a turn with no origin is a person's."""
        auto = await _row(platform="web", origin="automation")
        agent = await _row(platform="web", origin="agent", state=_state([auto]))

        assert _provenance(agent)["rules_key"] == "web"
        assert "Surface web: full markdown" in agent.content
        assert "waiting for the reply" not in agent.content

    @pytest.mark.asyncio
    async def test_a_manual_follow_up_with_no_surface_still_states_the_handoff(self):
        auto = await _row(origin="automation")
        manual = await _row(state=_state([auto]))

        assert _provenance(manual)["rules_key"] == "attended"
        assert "A person sent this turn and is waiting for the reply." in manual.content

        again = await _row(state=_state([auto, manual]))
        assert "rules_key" not in _provenance(again)
        assert "waiting for the reply" not in again.content

    @pytest.mark.asyncio
    async def test_a_turn_with_no_rules_after_a_channel_states_the_return_to_the_default(self):
        """A channel thread continued in the web app arrives with no surface;
        without a row of its own the channel's paragraph would stay the last
        rule in view."""
        slack = await _row(platform="slack", surface_rules=SLACK_RULES)
        web = await _row(state=_state([slack]))

        assert _provenance(web)["rules_key"] == "default"
        assert "no delivery rules of its own" in web.content
        assert SLACK_RULES not in web.content

        again = await _row(state=_state([slack, web]))
        assert _provenance(again)["rules_key"] == "default"
        assert "delivery rules" not in again.content

        back = await _row(platform="slack", surface_rules=SLACK_RULES, state=_state([slack, web, again]))
        assert SLACK_RULES in back.content

    @pytest.mark.asyncio
    async def test_a_channel_that_drops_its_text_states_the_return_to_the_default(self):
        """The gateway may send its name and no rules; the surface line has
        no built-in wording, so the earlier paragraph has to be taken back."""
        ruled = await _row(platform="slack", surface_rules=SLACK_RULES)
        bare = await _row(platform="slack", state=_state([ruled]))

        assert _provenance(bare)["rules_key"] == "slack"
        assert "Surface `slack`" in bare.content
        assert "no delivery rules of its own" in bare.content
        assert SLACK_RULES not in bare.content

        again = await _row(platform="slack", state=_state([ruled, bare]))
        assert "delivery rules" not in again.content

        # A plain turn after a bare surface has nothing to take back either.
        web = await _row(state=_state([ruled, bare, again]))
        assert "rules_key" not in _provenance(web)
        assert "delivery rules" not in web.content

    @pytest.mark.asyncio
    async def test_a_resumed_turn_keeps_the_rules_it_was_interrupted_under(self):
        """The HITL answer and a retry arrive with no surface of their own."""
        chart = await _row(platform="market_view:AAPL")
        assert _provenance(chart)["rules_key"] == "market_view:AAPL"

        mw = TurnContextMiddleware(now=NOW)
        resumed = (await mw.abefore_model(_state([chart])))["messages"][0]
        assert _provenance(resumed)["rules_key"] == "market_view:AAPL"
        assert "delivery rules" not in resumed.content
        assert "Surface" not in resumed.content

        auto = await _row(origin="automation")
        resumed_auto = (
            await TurnContextMiddleware(now=NOW).abefore_model(_state([auto]))
        )["messages"][0]
        assert _provenance(resumed_auto)["rules_key"] == "automation"
        assert "waiting for the reply" not in resumed_auto.content

    @pytest.mark.asyncio
    async def test_a_change_of_chart_states_the_rules_again(self):
        aapl = await _row(platform="market_view:AAPL")
        tsla = await _row(platform="market_view:TSLA", state=_state([aapl]))

        assert _provenance(tsla)["rules_key"] == "market_view:TSLA"
        assert "price chart for TSLA" in tsla.content

        same = await _row(platform="market_view:TSLA", state=_state([aapl, tsla]))
        assert "price chart" not in same.content

    @pytest.mark.asyncio
    async def test_a_subagent_states_who_it_reports_to_and_no_surface(self):
        """A subagent has no surface, so the report rules replace them."""
        message = await _row(is_subagent=True)

        assert _provenance(message)["rules_key"] == "subagent"
        assert "You are reporting to the parent agent, not to a person." in (
            message.content
        )
        assert "Surface " not in message.content

    @pytest.mark.asyncio
    async def test_nothing_to_state_carries_no_rules_and_no_key(self):
        """A turn with no surface, no automation and no parent has no delivery
        rules of its own, and a key stamped anyway would read as the last word
        on what the model was told."""
        message = await _row(timezone="America/New_York", preferred_market="US")

        assert "rules_key" not in _provenance(message)
        assert message.content.strip().splitlines() == [
            "1:00 PM EDT, Wednesday, September 9, 2026 (America/New_York)",
            "US: regular hours, closes Wed 16:00 ET",
        ]

    @pytest.mark.asyncio
    async def test_a_row_from_the_earlier_build_never_counts_as_stated(self):
        """Rows written before the rules moved into the row carry no rules_key.
        Reading one as "already said" would leave every live thread without
        them for the rest of its life."""
        legacy = await _row(platform="slack", surface_rules=SLACK_RULES)
        legacy.additional_kwargs[RUNTIME_UPDATE_KEY]["provenance"] = {
            "source": "harness"
        }

        text = (
            await _row(
                platform="slack", surface_rules=SLACK_RULES, state=_state([legacy])
            )
        ).content

        assert SLACK_RULES in text


class TestTheLowDiskLineIsTakenBack:
    """The low-disk line is an instruction in a row nothing rewrites, so the
    first turn after it stops applying has to say so."""

    LOW = "The computer's disk is nearly full"
    RECOVERED = "no longer reported as nearly full"

    @pytest.mark.asyncio
    async def test_the_turn_after_a_low_one_states_the_recovery(self):
        low = await _row(disk_free_mb=180)
        assert _provenance(low)["disk_low"] is True

        cleared = await _row(disk_known=True, state=_state([low]))

        assert self.RECOVERED in cleared.content
        assert self.LOW not in cleared.content
        assert _provenance(cleared)["disk_low"] is False

    @pytest.mark.asyncio
    async def test_the_recovery_is_stated_once(self):
        low = await _row(disk_free_mb=180)
        cleared = await _row(disk_known=True, state=_state([low]))

        again = await _row(disk_known=True, state=_state([low, cleared]))

        assert self.RECOVERED not in again.content

    @pytest.mark.asyncio
    async def test_a_turn_without_a_reading_takes_nothing_back(self):
        """A failed read or a reading cleared by a spec change is unknown, not
        healthy: the recovery waits for a reading that shows it."""
        low = await _row(disk_free_mb=180)

        unknown = await _row(state=_state([low]))
        assert self.RECOVERED not in unknown.content
        assert "disk_low" not in _provenance(unknown)

        known = await _row(disk_known=True, state=_state([low, unknown]))
        assert self.RECOVERED in known.content

    @pytest.mark.asyncio
    async def test_a_disk_that_stays_low_is_stated_again(self):
        low = await _row(disk_free_mb=180)

        still = await _row(disk_free_mb=120, state=_state([low]))

        assert "nearly full: 120 MB free" in still.content
        assert self.RECOVERED not in still.content

    @pytest.mark.asyncio
    async def test_a_low_row_behind_the_cutoff_needs_no_recovery(self):
        low = await _row(disk_free_mb=180)

        cleared = await _row(disk_known=True, state=_state([low], cutoff_index=1))

        assert self.RECOVERED not in cleared.content

    @pytest.mark.asyncio
    async def test_a_subagent_neither_states_nor_stamps_the_disk(self):
        low = await _row(disk_free_mb=180)

        sub = await _row(is_subagent=True, disk_known=True, state=_state([low]))

        assert self.RECOVERED not in sub.content
        assert "disk_low" not in _provenance(sub)


class TestTheRowIsRenderedOnce:
    @pytest.mark.asyncio
    async def test_it_renders_identically_at_both_guidance_levels(self):
        """A row persists across a mid-thread model switch, so a guidance flip
        must not rewrite content the model already read."""
        from ptc_agent.agent.prompts import guidance_template_vars, init_loader

        message = await _row(
            timezone="America/New_York",
            preferred_market="US",
            last_turn_at=NOW - timedelta(days=1),
            platform="web",
            origin="automation",
        )
        update = runtime_update_from_message(message)

        # The row body reaches the model exactly as it was written down.
        assert render_update_row(update) == update.text == message.content

        loader = init_loader()
        levels = {
            level: loader.render(
                "envelope/turn.md.j2",
                **guidance_template_vars(level),
                opened="1:00 PM EDT, Wednesday, September 9, 2026",
                market_line="US: regular hours, closes Wed 16:00 ET",
                elapsed_human="1d",
                sessions_closed=1,
                market="US",
                surface="web",
                origin="automation",
                surface_rules="Surface web: full markdown, no length cap.",
            ).strip()
            for level in ("lean", "detailed")
        }
        assert levels["lean"] == levels["detailed"]

    @pytest.mark.asyncio
    async def test_the_row_body_carries_no_kind_header(self):
        """The anchor is already prose with its own stamp; a kind header would
        label the clock with a second copy of the time."""
        text = (await _row(preferred_market="US")).content

        assert TURN_ROW_KIND not in text
        assert text.startswith("5:00 PM UTC, Wednesday, September 9, 2026")


class TestAResumedTurnOpensOnTheFirstModelCall:
    """A resumed interrupt re-enters the graph past the entry node."""

    @pytest.mark.asyncio
    async def test_the_model_hook_writes_the_row_when_the_entry_hook_did_not_run(self):
        mw = TurnContextMiddleware(now=NOW)
        written = await mw.abefore_model({"messages": []})
        assert written is not None
        assert [runtime_update_from_message(m).kind for m in written["messages"]] == [TURN_ROW_KIND]
        # Once per instance: the next model call of the same turn is quiet.
        assert await mw.abefore_model(written) is None

    @pytest.mark.asyncio
    async def test_the_model_hook_is_quiet_once_the_entry_hook_ran(self):
        mw = TurnContextMiddleware(now=NOW)
        assert await mw.abefore_agent({}) is not None
        assert await mw.abefore_model({}) is None


class TestAFailureCostsTheRowNeverTheTurn:
    @pytest.mark.asyncio
    async def test_a_render_that_raises_returns_no_row(self, monkeypatch):
        import ptc_agent.agent.prompts as prompts

        def _boom(dt, timezone_str=None):
            raise RuntimeError("no clock")

        monkeypatch.setattr(prompts, "format_current_time", _boom)

        assert await TurnContextMiddleware(now=NOW).abefore_agent({}) is None

    @pytest.mark.asyncio
    async def test_the_sync_hook_writes_nothing(self):
        """The async agent never calls it, but the protocol requires it."""
        assert TurnContextMiddleware(now=NOW).before_agent({}) is None


class TestTheFactFormOfTheSessionLine:
    def test_relative_false_names_the_instant_with_no_countdown(self):
        assert MARKET_CLOCK.describe("US", NOW, relative=False) == (
            "US: regular hours, closes Wed 16:00 ET"
        )
        assert (
            MARKET_CLOCK.describe(
                "US", datetime(2026, 9, 9, 21, 0, tzinfo=UTC), relative=False
            )
            == "US: after-hours, next open Thu 09:30 ET"
        )

    def test_relative_true_is_unchanged(self):
        assert MARKET_CLOCK.describe("US", NOW) == (
            "US: regular hours, closes in 3h (Wed 16:00 ET)"
        )
        assert MARKET_CLOCK.describe("US", NOW, relative=True) == MARKET_CLOCK.describe(
            "US", NOW
        )

    def test_the_viewer_clock_and_the_holiday_caveat_survive_the_fact_form(self):
        # A Monday past the published CN calendar, so the caveat is owed. It
        # follows the calendar's horizon, which each release moves, and stays
        # in September so the viewer's clock is on daylight time.
        from market_protocol.calendars import calendar_range

        day = date(calendar_range("XSHG")[1].year + 3, 9, 1)
        day += timedelta(days=-day.weekday() % 7)
        line = MARKET_CLOCK.describe(
            "CN",
            datetime(day.year, day.month, day.day, 2, 0, tzinfo=UTC),
            viewer_tz="America/New_York",
            relative=False,
        )

        assert line == (
            "CN: regular hours, closes Mon 15:00 CST (Mon 03:00 local), "
            "holidays unverified"
        )
