"""When a pinned thread is due for the summary that takes a new system prompt.

Compaction compares the static prompt the pin holds with the build's before
the call that would mark the pin stale, and the pin records the prompt's
sources, so only a change of those (a deploy) may force a rebuild.
"""

from __future__ import annotations

import contextlib
from types import SimpleNamespace

import pytest
from langchain_core.messages import HumanMessage, SystemMessage

from ptc_agent.agent.middleware import model_resilience
from ptc_agent.agent.middleware._utils import append_to_system_message
from ptc_agent.agent.middleware.runtime_context.frozen_prompt import (
    STATE_SYSTEM_PIN,
    _pin,
    due,
    sources_changed,
    static_digest,
)
from ptc_agent.agent.prompts import release
from tests.unit.ptc_agent.agent.prefix_stability import reply

CLAUDE = SimpleNamespace(model_name="claude-opus-5-5")
BASELINE = "<baseline>agent.md</baseline>"


def _request(system: SystemMessage | None, pin: dict | None, *, model=CLAUDE, messages=()):
    return SimpleNamespace(
        model=model,
        system_message=system,
        state={STATE_SYSTEM_PIN: pin} if pin is not None else {},
        messages=list(messages),
    )


def _due(request) -> bool:
    return due(request, request.messages)


def _built(static: str) -> SystemMessage:
    return append_to_system_message(SystemMessage(static), BASELINE)


def _pinned(static: str, **extra) -> dict:
    return {"model": "claude-opus-5-5", "content": _built(static).content, **extra}


def test_the_static_prompt_is_the_first_block_whatever_shape_the_message_has():
    """Compaction sees the static prompt alone, often a string; the pin keeps
    it with the baseline appended and the cache markers on. Both read it as
    their first block."""
    as_built = SystemMessage("Prompt A.")
    as_pinned = append_to_system_message(as_built, BASELINE)
    as_pinned.content[0]["cache_control"] = {"type": "ephemeral"}

    assert static_digest(as_built.content) == static_digest(as_pinned.content)
    assert static_digest(["Prompt A.", BASELINE]) == static_digest("Prompt A.")
    assert static_digest("Prompt B.") != static_digest("Prompt A.")
    assert static_digest([]) is None


def test_due_reads_the_build_against_the_pin_before_the_call_marks_it_stale():
    pinned = _pinned("Prompt A.")
    bound = [HumanMessage("Hi."), reply(1, text="Hi.")]

    assert not _due(_request(SystemMessage("Prompt A."), pinned, messages=bound))
    assert _due(_request(SystemMessage("Prompt B."), pinned, messages=bound))
    assert _due(_request(SystemMessage("Prompt A."), {**pinned, "stale": True}))
    assert not _due(_request(SystemMessage("Prompt B."), None, messages=bound))


def test_a_fallback_leaves_the_pin_to_the_thinking_it_holds():
    """A GPT fallback in a Claude thread, the pin stale with a deploy: Claude's
    thinking is still bound to the kept message, so however many calls the
    fallback answers it leaves the pin, and the call back on Claude sends it
    again."""
    pinned = _pinned("Prompt A.", stale=True)
    bound = [HumanMessage("Hi."), reply(1, text="Hi.")]
    answered = [*bound, _gpt_answer(2), HumanMessage("Go on.")]
    gpt = SimpleNamespace(model_name="gpt-6.2-sol", metadata={"provider_route": "openai"})

    with _as_fallback():
        assert _pin(_request(SystemMessage("Prompt B."), pinned, model=gpt, messages=bound)) == (None, None, None)
        assert _pin(_request(SystemMessage("Prompt B."), pinned, model=gpt, messages=answered)) == (None, None, None)
    kept, _, _ = _pin(_request(SystemMessage("Prompt B."), pinned, messages=answered))
    assert kept is not None and kept.content == pinned["content"]


def test_a_model_the_thread_was_switched_to_takes_the_pin():
    """Not a fallback: the thread goes on with GPT, so its reasoning gets the pin."""
    pinned = _pinned("Prompt A.", stale=True)
    bound = [HumanMessage("Hi."), reply(1, text="Hi.")]
    gpt = SimpleNamespace(model_name="gpt-6.2-sol", metadata={"provider_route": "openai"})

    _, update, change = _pin(_request(SystemMessage("Prompt B."), pinned, model=gpt, messages=bound))
    assert change == "model_changed" and update["model"] == "gpt-6.2-sol"


def test_an_openai_pin_is_not_taken_back_by_the_build_a_deploy_replaced(monkeypatch):
    """While a cutover drains, the old stack still runs turns. The new build
    takes the pin at once; the old one then sends that pin as it is and asks
    for no summary, so the thread pays one drop, not one each way."""
    gpt = SimpleNamespace(model_name="gpt-6.2-sol", metadata={"provider_route": "openai"})
    build = {"digest": "build-a"}
    monkeypatch.setattr(release, "source_digest", lambda: build["digest"])
    pinned = {"model": "gpt-6.2-sol", "content": _built("Prompt A.").content, "source_digest": "build-a"}
    bound = [HumanMessage("Hi."), _gpt_answer(1)]

    build["digest"] = "build-b"
    _, deployed, change = _pin(_request(_built("Prompt B."), pinned, model=gpt, messages=bound))
    assert change == "deployed" and deployed["replaced"] == ["build-a"]

    build["digest"] = "build-a"
    on_old = _request(_built("Prompt A."), deployed, model=gpt, messages=[*bound, _gpt_answer(2)])
    kept, update, _ = _pin(on_old)
    assert kept is not None and kept.content == deployed["content"] and update is None
    assert not _due(on_old)

    build["digest"] = "build-b"
    assert _pin(_request(_built("Prompt B."), deployed, model=gpt, messages=bound)) == (None, None, None)


@contextlib.contextmanager
def _as_fallback():
    token = model_resilience._fallback.set(True)
    try:
        yield
    finally:
        model_resilience._fallback.reset(token)


def _gpt_answer(n: int):
    from langchain_core.messages import AIMessage

    return AIMessage(
        content=[{"type": "reasoning", "id": f"rs_{n}", "summary": []}, {"type": "text", "text": "ok"}],
        id=f"resp_{n}",
        response_metadata={"model_name": "gpt-6.2-sol", "reasoning_origin": "openai"},
    )


def test_a_thread_holding_no_bound_thinking_is_not_due_since_its_call_takes_the_build():
    """The pin follows the build on that thread's next call, so a summary
    forced or widened for it would buy nothing."""
    pinned = _pinned("Prompt A.")
    unbound = [HumanMessage("Hi."), reply(1, text="Hi.", model_name="deepseek-chat")]

    assert not _due(_request(SystemMessage("Prompt B."), pinned, messages=unbound))
    assert _pin(_request(_built("Prompt B."), pinned, messages=unbound))[2] == "no_bound_thinking"


def test_another_model_is_never_due_since_it_takes_the_build_at_once():
    pinned = _pinned("Prompt A.", stale=True)

    assert not _due(_request(SystemMessage("Prompt B."), pinned, model=SimpleNamespace(model_name="claude-sonnet-5-5")))
    assert not _due(_request(SystemMessage("Prompt B."), pinned, model=SimpleNamespace(model_name="deepseek-chat")))


def test_the_sources_hold_the_python_that_writes_static_prompt_text():
    """The subagent roster and the direct tools' lines are written by Python,
    so a deploy that edits only those would otherwise read as an input."""
    agent = release._AGENT
    sources = release._sources()

    for path in (
        agent / "prompts" / "formatter.py",
        agent / "subagents" / "builtins.py",
        agent / "middleware" / "direct_mcp.py",
        agent / "agent.py",
    ):
        assert path in sources and path.exists()
    assert all(path.exists() for path in sources)


def test_only_a_change_of_the_prompt_sources_is_a_deploy():
    assert not sources_changed({STATE_SYSTEM_PIN: _pinned("Prompt A.", source_digest=release.source_digest())})
    assert sources_changed({STATE_SYSTEM_PIN: _pinned("Prompt A.", source_digest="an earlier deploy")})
    assert sources_changed({STATE_SYSTEM_PIN: _pinned("Prompt A.")})


def test_a_pin_records_its_builds_sources_and_keeps_them_while_stale(monkeypatch):
    monkeypatch.setattr(release, "prompt_version", lambda: 4)
    first = _pin(_request(_built("Prompt B."), None, messages=[HumanMessage("Hi.")]))[1]
    assert (first["source_digest"], first["prompt_version"]) == (release.source_digest(), 4)

    stale = _pin(_request(SystemMessage("Prompt C."), first, messages=[reply(1, text="Hi.")]))[1]
    assert stale == {**first, "stale": True}


@pytest.mark.parametrize("stale", [False, True], ids=["fresh", "stale"])
def test_a_pin_the_build_composes_again_takes_its_sources(monkeypatch, stale):
    """Otherwise a thread no deploy touched keeps the sources of the build it
    was pinned on, and its next due from an input alone reads as a deploy, or
    as urgent once ``rebuild_before`` passes the version it recorded. A pin
    an old worker cleared, without a version, is refreshed the same way."""
    pinned = _pinned("Prompt A.", source_digest=release.source_digest(), **({"stale": True} if stale else {}))
    monkeypatch.setattr(release, "source_digest", lambda: "the next deploy")
    monkeypatch.setattr(release, "prompt_version", lambda: 2)

    _, update, change = _pin(_request(_built("Prompt A."), pinned, messages=[reply(1, text="Hi.")]))

    assert update == {
        **{k: v for k, v in pinned.items() if k != "stale"},
        "source_digest": "the next deploy",
        "prompt_version": 2,
    }
    assert change == ("cleared" if stale else None)
    assert not sources_changed({STATE_SYSTEM_PIN: update})
    assert _pin(_request(_built("Prompt A."), update, messages=[reply(1, text="Hi.")])) == (None, None, None)
