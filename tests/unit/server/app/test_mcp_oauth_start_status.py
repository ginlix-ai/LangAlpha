"""What status the connect route gives a refusal, and why the order matters.

``McpServerMoved`` is an ``McpOAuthError``, so the two ``except`` clauses in
``oauth_start`` are ordered rather than exclusive: swap them and every moved row
answers 422 with the service's own wording, which the page shows verbatim
instead of telling the user to reload. Nothing else notices -- the request still
fails, just uselessly -- so the order is pinned here.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException, Response

from src.server.app import mcp_oauth as mod
from src.server.services.mcp_oauth import McpOAuthError, McpServerMoved


def _request():
    return SimpleNamespace(headers={}, cookies={})


async def _call(monkeypatch, error: Exception) -> HTTPException:
    async def _start(*_args, **_kwargs):
        raise error

    monkeypatch.setattr(mod, "start_connect", _start)
    with pytest.raises(HTTPException) as caught:
        await mod.oauth_start("srv", "user-1", _request(), Response(), {})
    return caught.value


@pytest.mark.asyncio
async def test_a_row_that_moved_is_a_conflict(monkeypatch):
    raised = await _call(monkeypatch, McpServerMoved("the address changed"))

    assert raised.status_code == 409


@pytest.mark.asyncio
async def test_every_other_refusal_still_reads_as_unprocessable(monkeypatch):
    # The control for the ordering above: the general clause is untouched, so a
    # 409 means the specific one was reached rather than that both moved.
    raised = await _call(monkeypatch, McpOAuthError("not a remote server"))

    assert raised.status_code == 422
