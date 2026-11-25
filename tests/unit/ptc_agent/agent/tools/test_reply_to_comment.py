"""What the agent is told after answering a comment, and what the chat's card
carries: a chart is named as a chart, a file by its path."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from ptc_agent.agent.tools.artifact_comments import create_reply_to_comment_tool
from src.server.database import artifacts as artifacts_db
from src.server.database.artifacts import AgentReply
from src.server.services.artifacts import events

_C = "00000000-0000-0000-0000-0000000000c1"
_CONFIG = {"configurable": {"user_id": "u", "thread_id": "t"}}


def _call(resolve: bool = False) -> dict:
    return {
        "type": "tool_call",
        "name": "reply_to_comment",
        "args": {"comment_id": _C, "text": "Drawn as nvda-neckline.", "resolve": resolve},
        "id": "r1",
    }


def _reply(**over) -> AgentReply:
    fields = dict(
        artifact_id="a1", workspace_id="w1", path="NVDA:1day", number=3,
        resolved=False, kept_open=False, kind="chart",
    )
    return AgentReply(**{**fields, **over})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reply", "resolve", "content"),
    [
        (_reply(), False, "Replied to comment 3 on the NVDA:1day chart."),
        (
            _reply(resolved=True),
            True,
            "Replied to comment 3 on the NVDA:1day chart and resolved it.",
        ),
        (
            _reply(kept_open=True),
            True,
            "Replied to comment 3 on the NVDA:1day chart. It stays open: the user "
            "has written more in it since, which reaches you when they send it.",
        ),
        (
            _reply(path="reports/q3.html", kind="file"),
            False,
            "Replied to comment 3 on reports/q3.html.",
        ),
    ],
)
async def test_the_reply_names_what_the_comment_is_on(monkeypatch, reply, resolve, content):
    monkeypatch.setattr(artifacts_db, "add_agent_reply", AsyncMock(return_value=reply))
    published = AsyncMock()
    monkeypatch.setattr(events, "publish_comment_change", published)

    msg = await create_reply_to_comment_tool().ainvoke(_call(resolve), _CONFIG)

    assert msg.content == content
    assert (msg.artifact["kind"], msg.artifact["path"]) == (reply.kind, reply.path)
    assert published.await_args.kwargs["kind"] == reply.kind
