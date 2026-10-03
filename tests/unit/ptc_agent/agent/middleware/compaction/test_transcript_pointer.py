"""A compaction summary points at the transcript only once the agent can read it.

The turn-end export runs after the agent finishes, so a pointer kept on a
failed save sent the agent, for the rest of the turn, to history that was not
there, and the trimming note told it the dropped turns existed only there.
A save that lands is still unreadable from a workspace folder whose links to
the mount are not in, since Grep and Read do not wait for them.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from langchain.agents.middleware.types import ModelRequest, ModelResponse
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage

from ptc_agent.agent.middleware.compaction import compact as compact_module
from ptc_agent.agent.middleware.compaction.compact import Summarizer
from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware
from ptc_agent.agent.middleware.compaction.types import CONTEXT_SUMMARY_PREFIX
from ptc_agent.agent.transcript import TranscriptTarget, pointer

TRANSCRIPT = TranscriptTarget("abcd1234-0000-0000-0000-000000000000")


class _Mount:
    def __init__(self, outcome):
        self.outcome = outcome

    async def save_transcript(self, target, messages):
        if self.outcome == "raises":
            raise ConnectionError("store down")
        if self.outcome == "hangs":
            await asyncio.sleep(10)
        return self.outcome


def _backend(outcome, linked: bool):
    mount = _Mount(outcome)
    asked: list = []

    async def settled_livefs(workspace_id=None):
        asked.append(workspace_id)
        return mount if linked else None

    return SimpleNamespace(livefs=mount, settled_livefs=settled_livefs, asked=asked)


def _middleware(monkeypatch, backend) -> CompactionMiddleware:
    mw = CompactionMiddleware(
        Summarizer(
            GenericFakeChatModel(messages=iter([AIMessage("the summary")])),
            limit=100_000,
            counter=len,
        ),
        token_threshold=6,
        keep_messages=2,
        backend=backend,
        workspace_id="ws-1",
    )
    monkeypatch.setattr(mw, "_transcript_target", lambda: TRANSCRIPT)
    return mw


def _request(mw: CompactionMiddleware) -> ModelRequest:
    msgs = [
        m
        for i in range(6)
        for m in (HumanMessage(f"q{i}", id=f"h{i}"), AIMessage(f"a{i}", id=f"a{i}"))
    ]
    return ModelRequest(
        model=mw._summarizer.model,
        messages=msgs,
        system_message=None,
        tool_choice=None,
        tools=[],
        response_format=None,
        state={},
        runtime=None,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "linked"),
    [(True, True), (True, False), (False, True), ("raises", True), ("hangs", True)],
)
async def test_summary_points_at_transcript_only_when_readable(monkeypatch, outcome, linked):
    monkeypatch.setattr(pointer, "_EXPORT_TIMEOUT", 0.05)
    backend = _backend(outcome, linked)
    mw = _middleware(monkeypatch, backend)
    requests: list = []

    def recording(prompt, messages, turns=None):
        requests.append(turns)
        return build_summary_request(prompt, messages, turns)

    build_summary_request = compact_module.build_summary_request
    monkeypatch.setattr(compact_module, "build_summary_request", recording)
    sent: list = []

    async def handler(req):
        sent.append(req.messages)
        return ModelResponse(result=[AIMessage("done")])

    update = (await mw.awrap_model_call(_request(mw), handler)).command.update

    # Compaction goes ahead either way; only the pointer depends on the save.
    summary = sent[0][0].content
    assert summary.startswith(f"{CONTEXT_SUMMARY_PREFIX}the summary")
    landed = outcome is True and linked
    assert (TRANSCRIPT.directory in summary) is landed
    assert (update["_summarization_event"]["file_path"] is not None) is landed
    # Turn files are cited only where the pointer will be: a citation the
    # agent cannot open is a dead end like the pointer.
    assert (requests[0] is not None) is landed
    assert backend.asked == ["ws-1"]


@pytest.mark.asyncio
async def test_a_compaction_timeout_under_the_save_limit_bounds_the_save(monkeypatch):
    # Admission waits about the compaction timeout; a save given its own
    # longer limit would hold the thread past it.
    monkeypatch.setattr(compact_module, "get_compaction_timeout", lambda: 0.05)
    mw = _middleware(monkeypatch, _backend("hangs", True))

    async def handler(req):
        return ModelResponse(result=[AIMessage("done")])

    result = await asyncio.wait_for(mw.awrap_model_call(_request(mw), handler), timeout=5)

    assert result.command.update["_summarization_event"]["file_path"] is None
