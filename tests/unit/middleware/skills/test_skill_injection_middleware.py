"""Tests for ``SkillsMiddleware.abefore_agent`` skill-body injection.

The middleware reads ``skill_contexts`` from ``config["configurable"]``, dedups
against bodies already live in the thread (``compute_already_loaded``), appends
fresh bodies to the last user message in place (preserving its id), and returns
the ``messages``/``loaded_skills`` state update.

These tests drive the hook with fake state/config and patch the content layer at
the middleware namespace, so they verify the *wiring* (what gets read from state,
what gets returned) independently of build_skill_content's own logic.
"""

from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from src.ptc_agent.agent.middleware.skills.content import (
    SkillPrefixResult,
    loaded_skill_marker,
)
from src.ptc_agent.agent.middleware.skills.middleware import SkillsMiddleware

MW = "src.ptc_agent.agent.middleware.skills.middleware"


def _config(skill_contexts=None, skill_dirs=None):
    configurable: dict = {}
    if skill_contexts is not None:
        configurable["skill_contexts"] = skill_contexts
    if skill_dirs is not None:
        configurable["skill_dirs"] = skill_dirs
    return {"configurable": configurable}


@pytest.mark.asyncio
async def test_fresh_skill_returns_body_and_loaded_skills():
    mw = SkillsMiddleware(mode="flash")
    hm = HumanMessage(content="annotate the chart", id="u1")
    state = {"messages": [hm], "loaded_skills": []}
    config = _config(
        skill_contexts=[{"name": "chart-annotation", "instruction": "AAPL:1d"}],
        skill_dirs=["/skills"],
    )
    result_obj = SkillPrefixResult(
        content="\n\nBODY-AND-INSTRUCTION", loaded_skill_names=["chart-annotation"]
    )

    with (
        patch(f"{MW}.compute_already_loaded", return_value=set()) as cal,
        patch(f"{MW}.build_skill_content", return_value=result_obj) as bsc,
    ):
        out = await mw.abefore_agent(state, MagicMock(), config=config)

    # Filesystem discovery still runs (empty for flash) alongside injection.
    assert out["discovered_skills"] == []
    assert out["loaded_skills"] == ["chart-annotation"]

    # Body appended to the SAME user message (id preserved -> add_messages replaces).
    assert len(out["messages"]) == 1
    injected = out["messages"][0]
    assert injected.id == "u1"
    assert "annotate the chart" in injected.content
    assert injected.content.endswith("BODY-AND-INSTRUCTION")

    # already_loaded came from compute_already_loaded(state channels).
    ca_args = cal.call_args.args
    assert ca_args[0] == []  # loaded_skills
    assert ca_args[1] == [hm]  # messages

    # build_skill_content received the coerced SkillRequests + config skill_dirs.
    skills_arg = bsc.call_args.args[0]
    assert [s.name for s in skills_arg] == ["chart-annotation"]
    assert skills_arg[0].instruction == "AAPL:1d"
    assert bsc.call_args.kwargs["skill_dirs"] == ["/skills"]
    assert bsc.call_args.kwargs["mode"] == "flash"
    assert bsc.call_args.kwargs["already_loaded"] == set()
    # The last human message's id is threaded through so the marker can bind to it.
    assert bsc.call_args.kwargs["message_id"] == "u1"


@pytest.mark.asyncio
async def test_already_loaded_skips_loaded_skills_keeps_instruction():
    """When the body is deduped (no fresh names), the instruction still rides on
    the message but ``loaded_skills`` is not returned (tools already persist)."""
    mw = SkillsMiddleware(mode="flash")
    hm = HumanMessage(content="annotate", id="u1")
    state = {"messages": [hm], "loaded_skills": ["chart-annotation"]}
    config = _config(
        skill_contexts=[{"name": "chart-annotation", "instruction": "NVDA:1h"}]
    )
    result_obj = SkillPrefixResult(
        content="\n\n[Instruction: NVDA:1h]", loaded_skill_names=[]
    )

    with (
        patch(f"{MW}.compute_already_loaded", return_value={"chart-annotation"}),
        patch(f"{MW}.build_skill_content", return_value=result_obj),
    ):
        out = await mw.abefore_agent(state, MagicMock(), config=config)

    assert "loaded_skills" not in out
    assert out["messages"][0].content.endswith("[Instruction: NVDA:1h]")
    assert out["messages"][0].id == "u1"


@pytest.mark.asyncio
async def test_no_skill_contexts_is_noop_injection():
    mw = SkillsMiddleware(mode="flash")
    state = {"messages": [HumanMessage(content="hi", id="u1")]}

    with patch(f"{MW}.build_skill_content") as bsc:
        out = await mw.abefore_agent(state, MagicMock(), config=_config())

    bsc.assert_not_called()
    assert out == {"discovered_skills": []}


@pytest.mark.asyncio
async def test_config_none_is_noop_injection():
    mw = SkillsMiddleware(mode="flash")
    state = {"messages": [HumanMessage(content="hi", id="u1")]}

    with patch(f"{MW}.build_skill_content") as bsc:
        out = await mw.abefore_agent(state, MagicMock(), config=None)

    bsc.assert_not_called()
    assert out == {"discovered_skills": []}


@pytest.mark.asyncio
async def test_build_returns_none_injects_nothing():
    mw = SkillsMiddleware(mode="flash")
    state = {"messages": [HumanMessage(content="hi", id="u1")], "loaded_skills": []}
    config = _config(skill_contexts=[{"name": "chart-annotation"}])

    with (
        patch(f"{MW}.compute_already_loaded", return_value=set()),
        patch(f"{MW}.build_skill_content", return_value=None),
    ):
        out = await mw.abefore_agent(state, MagicMock(), config=config)

    assert out == {"discovered_skills": []}


@pytest.mark.asyncio
async def test_last_message_not_human_still_returns_loaded_skills():
    """Defense path: if the last message isn't a user turn, the body can't attach
    but the skill's tools must still load via ``loaded_skills``."""
    mw = SkillsMiddleware(mode="flash")
    state = {"messages": [AIMessage(content="assistant", id="a1")], "loaded_skills": []}
    config = _config(skill_contexts=[{"name": "chart-annotation"}])
    result_obj = SkillPrefixResult(content="BODY", loaded_skill_names=["chart-annotation"])

    with (
        patch(f"{MW}.compute_already_loaded", return_value=set()),
        patch(f"{MW}.build_skill_content", return_value=result_obj),
    ):
        out = await mw.abefore_agent(state, MagicMock(), config=config)

    assert out["loaded_skills"] == ["chart-annotation"]
    assert "messages" not in out


@pytest.mark.asyncio
async def test_blank_skill_names_are_dropped():
    """A skill_contexts entry with no name doesn't reach build_skill_content."""
    mw = SkillsMiddleware(mode="flash")
    state = {"messages": [HumanMessage(content="hi", id="u1")], "loaded_skills": []}
    config = _config(skill_contexts=[{"name": "", "instruction": "x"}])

    with (
        patch(f"{MW}.compute_already_loaded", return_value=set()) as cal,
        patch(f"{MW}.build_skill_content") as bsc,
    ):
        out = await mw.abefore_agent(state, MagicMock(), config=config)

    bsc.assert_not_called()
    cal.assert_not_called()
    assert out == {"discovered_skills": []}


@pytest.mark.asyncio
async def test_injected_body_survives_patch_tool_calls_rewrite():
    """Cross-middleware ordering invariant: the injected body survives PatchToolCalls.

    ``SkillsMiddleware.abefore_agent`` appends a skill body (+ ``mid``-bound marker) to
    the last human message in place. deepagents' ``PatchToolCallsMiddleware.before_agent``
    is the one other ``before_agent`` hook in both the PTC and Flash stacks; it runs
    AFTER Skills and rewrites the whole ``messages`` list to patch dangling tool calls.
    This pins that the rewrite preserves the augmented human message verbatim — same id,
    body + marker intact — so injection is never dropped or mis-targeted. If a future
    deepagents version (or a reordered stack) re-ids/truncates that message, this fails.
    """
    from deepagents.middleware.patch_tool_calls import PatchToolCallsMiddleware

    mw = SkillsMiddleware(mode="flash")
    hm = HumanMessage(content="annotate the chart", id="h1")
    state = {"messages": [hm], "loaded_skills": []}
    config = _config(
        skill_contexts=[{"name": "chart-annotation", "instruction": "AAPL:1d"}],
        skill_dirs=["/skills"],
    )

    # Exercise the real append path. The fake body carries the real mid-bound marker
    # for whatever message_id Skills threads through (the last human's id), so this also
    # proves _inject_requested_skills binds the marker to h1 — not a hardcoded constant.
    def _fake_build(skills, *, skill_dirs, mode, already_loaded, message_id):
        block = f"{loaded_skill_marker('chart-annotation', message_id)}\nBODY\n</loaded-skill>"
        return SkillPrefixResult(
            content=f"\n\n{block}\n\n[Instruction: AAPL:1d]",
            loaded_skill_names=["chart-annotation"],
        )

    with patch(f"{MW}.build_skill_content", side_effect=_fake_build):
        out = await mw.abefore_agent(state, MagicMock(), config=config)

    augmented = out["messages"][0]
    assert augmented.id == "h1"
    assert '<loaded-skill name="chart-annotation" mid="h1">' in augmented.content

    # PatchToolCalls runs next, with a DANGLING tool call present so its rewrite branch
    # actually fires (it no-ops when every tool call is answered).
    dangling = AIMessage(
        content="",
        id="a1",
        tool_calls=[{"name": "do_thing", "args": {}, "id": "call_1"}],
    )
    patched = PatchToolCallsMiddleware().before_agent(
        {"messages": [augmented, dangling]}, MagicMock()
    )

    # The rewrite fired (not a no-op) AND preserved the body-augmented human message.
    assert patched is not None
    msgs = patched["messages"]
    assert any(getattr(m, "type", None) == "tool" for m in msgs)  # synthetic patch added
    humans = [m for m in msgs if getattr(m, "type", None) == "human"]
    assert len(humans) == 1
    assert humans[0].id == "h1"
    assert '<loaded-skill name="chart-annotation" mid="h1">' in humans[0].content


def test_runnable_callable_injects_config_into_abefore_agent():
    """The whole feature hinges on LangGraph injecting per-request ``config`` into
    ``abefore_agent`` (that's how skill_contexts reach the hook). RunnableCallable
    only injects ``config`` when it can read the parameter's annotation — adding
    ``from __future__ import annotations`` to the module would stringify it and
    silently drop the injection, turning skill injection into a no-op in prod with
    every direct-call unit test still green. This guards that regression.
    """
    from langgraph._internal._runnable import RunnableCallable

    mw = SkillsMiddleware(mode="flash")
    rc = RunnableCallable(None, mw.abefore_agent)
    assert "config" in rc.func_accepts
