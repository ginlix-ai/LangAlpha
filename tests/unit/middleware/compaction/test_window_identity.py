"""The window trims the main agent's checkpoint without changing anything the
model, the transcript or an edit reads.

One scenario (``window_harness.SCENARIO``) runs twice through the real graph
and middleware, once with the trim and once without, and the two are compared
request by request: every model and summary request must be byte-identical
once runtime-minted ids are normalized. The scenario then edits a turn the
window had trimmed and runs on from there, compared the same way.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from ptc_agent.agent.middleware.compaction import middleware as middleware_module
from ptc_agent.agent.state import DeltaAgentState
from ptc_agent.agent.transcript import Window, build_directory
from tests.unit.middleware.compaction import window_harness as h


@dataclass
class Phase:
    """The two runs' counts at the end of a phase."""

    calls: int
    summaries: int
    exports: int
    transcripts: tuple[tuple[list[str], dict[str, str]], ...]
    decoded: tuple[int, int]


@dataclass
class Compared:
    off: h.ThreadRun
    on: h.ThreadRun
    scenario: Phase
    edited: Phase


def _phase(off: h.ThreadRun, on: h.ThreadRun, decoded: tuple[int, int]) -> Phase:
    assert len(off.model.requests) == len(on.model.requests)
    return Phase(
        calls=len(on.model.requests),
        summaries=len(on.summary_model.requests),
        exports=len(on.exports),
        transcripts=(h.transcript(off.store), h.transcript(on.store)),
        decoded=decoded,
    )


@pytest.fixture(scope="module")
def compared() -> Compared:
    async def run() -> Compared:
        saver_off, saver_on = InMemorySaver(), InMemorySaver()
        off = await h.run_scenario(saver_off, window=False)
        on = await h.run_scenario(saver_on, window=True)

        async def decoded() -> tuple[int, int]:
            return (
                await h.decoded_messages(saver_off, off.graph),
                await h.decoded_messages(saver_on, on.graph),
            )

        scenario = _phase(off, on, await decoded())
        await h.run_edit(off, saver_off, 3, window=False)
        await h.run_edit(on, saver_on, 3, window=True)
        return Compared(off, on, scenario, _phase(off, on, await decoded()))

    return asyncio.run(run())


def _texts(compared: Compared) -> list[str]:
    return h.digests(compared.on.model.requests[: compared.scenario.calls])


# ------------------------------------------------------------ the requests


def test_the_model_is_sent_the_same_requests(compared: Compared) -> None:
    off, on = compared.off, compared.on
    assert compared.scenario.calls > 40
    assert h.digests(on.model.requests) == h.digests(off.model.requests)
    assert h.digests(on.summary_model.requests) == h.digests(off.summary_model.requests)


def test_the_scenario_reaches_every_reading_the_trim_could_change(compared: Compared) -> None:
    """Each count a trim could move has to show in the requests compared, or
    their equality proves nothing about it."""
    texts = "\n".join(_texts(compared))
    on = compared.on
    # Several trims, each moving the base on, and each dropping exactly the
    # start of the whole thread, as the server checks against its slices.
    assert len(on.asked) >= 3
    assert all(whole for _, whole, _ in on.asked)
    assert len({base for _, base in on.held[: len(h.SCENARIO)]} - {0}) >= 3
    # Tier 1 cuts and summaries, whose turn numbers and index lines run past
    # the trimmed runs (the last test breaks each).
    assert texts.count("argument cut here") > 10
    assert len(re.findall(r"Summary \d+:", texts)) > 5
    assert compared.scenario.summaries >= 4
    # The notes check-in, which counts back to the last notes write, made in
    # turn 2 and trimmed before the check-in ran.
    assert re.search(r"(\d+) tool calls have run since", texts)
    assert on.held[1][1] == 0 and on.held[6][1] > 1
    # The subagent notices: off in turns 3-4, back on in 5, after trims.
    assert "turned subagents off" in texts
    assert "turned subagents back on" in texts


def test_the_checkpoint_keeps_only_the_window(compared: Compared) -> None:
    last = len(h.SCENARIO) - 1
    off_held, on_held = compared.off.held[last], compared.on.held[last]
    assert off_held[1] == 0
    assert on_held[1] > 0
    assert on_held[0] * 3 < off_held[0]
    # Loading the tip decodes the window, not the thread.
    decoded_off, decoded_on = compared.scenario.decoded
    assert decoded_on <= on_held[0] + 2
    assert decoded_off >= off_held[0]


# ------------------------------------------------------------ the re-entry


def test_a_reentry_trims_without_touching_its_turn(compared: Compared) -> None:
    on = compared.on
    assert on.reentry is not None and on.reentry_base is not None
    before, after = on.reentry
    assert after[: len(before)] == before
    assert len(after) > len(before)
    base_before, base_after = on.reentry_base
    assert base_after > base_before
    # The turn's own messages were never trimmed.
    assert after[0] == "h-8"


# ------------------------------------------------------------ a refusal


def test_a_refusal_is_asked_once_per_summary_and_sends_the_same(compared: Compared) -> None:
    """A thread whose slices do not hold its head keeps the server's refusal
    in its checkpoint: it asks again only once a new summary moves the
    anchor, and the write changes nothing the model is sent."""
    refused = asyncio.run(h.run_scenario(InMemorySaver(), window=True, covered=False))
    off = compared.off
    assert h.digests(refused.model.requests) == h.digests(
        off.model.requests[: compared.scenario.calls]
    )
    assert h.digests(refused.summary_model.requests) == h.digests(
        off.summary_model.requests[: compared.scenario.summaries]
    )
    assert {base for _, base in refused.held} == {0}
    summaries = [count for _, _, count in refused.asked]
    assert len(summaries) >= 3
    assert len(set(summaries)) == len(summaries)


# ------------------------------------------------------------ a notification turn


def _notified() -> tuple[h.ThreadRun, h.ThreadRun]:
    async def run() -> tuple[h.ThreadRun, h.ThreadRun]:
        off = await h.run_scenario(InMemorySaver(), window=False, turns=h.NOTIFIED)
        on = await h.run_scenario(InMemorySaver(), window=True, turns=h.NOTIFIED)
        return off, on

    return asyncio.run(run())


def test_a_build_without_the_switch_trims_the_same() -> None:
    """A notification turn's build has no subagent switch, and its trims
    still carry the notices they drop, into a field ``MainAgentState``
    declares: the next turn with the switch restates it as the untrimmed
    thread does."""
    off, on = _notified()
    assert h.digests(on.model.requests) == h.digests(off.model.requests)
    assert h.digests(on.summary_model.requests) == h.digests(off.summary_model.requests)
    # Turns 7 and 8, the ones without the switch, trimmed.
    bases = [base for _, base in on.held]
    assert bases[5] < bases[6] < bases[7]


def test_a_schema_without_the_field_would_lose_the_notice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Why every build declares ``MainAgentState``: without the switch's
    field in the schema, a trim in a turn without the switch drops what its
    carry kept, and the comparison sees it."""
    monkeypatch.setattr(h, "MainAgentState", DeltaAgentState)
    off, on = _notified()
    assert h.digests(on.model.requests) != h.digests(off.model.requests)


# ------------------------------------------------------------ the transcript


def test_the_transcript_is_the_whole_threads(compared: Compared) -> None:
    (off_saves, off_files), (on_saves, on_files) = compared.scenario.transcripts
    assert on_files == off_files
    assert on_saves == off_saves
    assert sorted(on_files) == [f"turn-{n:04d}.jsonl" for n in range(1, len(h.SCENARIO) + 1)]


def test_each_turn_end_renders_as_the_whole_thread_would(compared: Compared) -> None:
    """Digests and shape keys included: the carried entries and the chain
    through them are what a render of every message produces."""
    for run in (compared.off, compared.on):
        exports = run.exports[: compared.scenario.exports]
        wholes = h.whole_threads(run)[: compared.scenario.exports]
        for (_, base, manifest), whole in zip(exports, wholes):
            assert manifest == build_directory(whole, window=Window()).manifest, base
    assert any(base for _, base, _ in compared.on.exports[: compared.scenario.exports])


# ------------------------------------------------------------ an edit


def test_an_edit_into_a_trimmed_turn(compared: Compared) -> None:
    """Turn 3 was trimmed long before; editing it forks from the end of
    turn 2, whose checkpoint still holds the whole list, and the thread runs
    on from there exactly as the untrimmed one does, trimming again."""
    on, off = compared.on, compared.off
    assert compared.edited.calls > compared.scenario.calls
    assert h.digests(on.model.requests) == h.digests(off.model.requests)
    assert h.digests(on.summary_model.requests) == h.digests(off.summary_model.requests)
    edited = on.held[len(h.SCENARIO) :]
    assert edited[0][1] == 0
    assert edited[-1][1] > 0
    first = on.model.requests[compared.scenario.calls]
    humans = [m for m in first if m.type == "human" and m.id and m.id.startswith("h-")]
    assert humans[-1].id == "h-3"
    assert "edited" in str(humans[-1].content)
    (off_saves, off_files), (on_saves, on_files) = compared.edited.transcripts
    assert on_files == off_files and on_saves == off_saves


# ------------------------------------------------------------ the comparison


def _without(key: str):
    """``trim``, its window kept without ``key``."""
    real = middleware_module.trim

    def trimmed(state, cut, carries=(), **kwargs):
        update = real(state, cut, carries, **kwargs)
        update["_window"] = {**update["_window"], key: 0 if key == "runs" else []}
        return update

    return trimmed


@pytest.mark.parametrize(
    "broken", ["numbering", "earlier index", "notes count", "subagent notice"]
)
def test_the_comparison_sees_each_reading_the_trim_keeps(
    compared: Compared, monkeypatch: pytest.MonkeyPatch, broken: str
) -> None:
    """A window that lost any one of them sends different requests, so the
    identity above is not one the scenario would show anyway."""
    if broken == "numbering":
        monkeypatch.setattr(middleware_module, "trim", _without("runs"))
    elif broken == "earlier index":
        monkeypatch.setattr(middleware_module, "trim", _without("requests"))
    elif broken == "notes count":
        monkeypatch.setattr(h, "notes_window_carry", lambda thread: lambda trimmed, state: {})
    else:
        monkeypatch.setattr(h, "subagents_window_carry", lambda trimmed, state: {})
    on = asyncio.run(h.run_scenario(InMemorySaver(), window=True))
    expected = h.digests(compared.off.model.requests[: compared.scenario.calls])
    assert h.digests(on.model.requests) != expected
