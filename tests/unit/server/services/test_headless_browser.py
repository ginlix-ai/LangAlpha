"""The render context's routing: what answers a request, and that one which
fails to be answered is aborted rather than left pending in the page."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.server.services import headless_browser as hb


async def _handler(own=None, gate=None):
    """The route handler ``gated_context`` installs, from a fake browser."""
    context = MagicMock()
    context.route = AsyncMock()
    context.route_web_socket = AsyncMock()
    context.add_init_script = AsyncMock()
    context.close = AsyncMock()
    browser = MagicMock()
    browser.new_context = AsyncMock(return_value=context)
    async with hb.gated_context(browser, gate or MagicMock(answer=AsyncMock()), own=own):
        pass
    context.close.assert_awaited_once()
    return context.route.await_args.args[1]


@pytest.mark.asyncio
async def test_a_request_that_fails_to_be_answered_is_aborted():
    gate = MagicMock(answer=AsyncMock(side_effect=RuntimeError("context closed")))
    handle = await _handler(gate=gate)
    route = MagicMock(abort=AsyncMock())
    await handle(route, MagicMock(url="https://a.example.com/x"))
    route.abort.assert_awaited_once()


@pytest.mark.asyncio
async def test_what_the_render_answers_itself_never_meets_the_gate():
    gate = MagicMock(answer=AsyncMock())
    handle = await _handler(own=AsyncMock(return_value=True), gate=gate)
    await handle(MagicMock(), MagicMock(url="https://own.invalid/a.js"))
    gate.answer.assert_not_awaited()

    handle = await _handler(own=AsyncMock(return_value=False), gate=gate)
    await handle(MagicMock(), MagicMock(url="https://a.example.com/x"))
    gate.answer.assert_awaited_once()


@pytest.mark.asyncio
async def test_no_request_asks_for_a_pdf_not_even_one_the_render_answers_itself():
    """A widget's own file request is forwarded to the serve route, where
    ``format=pdf`` would start a render inside the render."""
    own = AsyncMock(return_value=True)
    gate = MagicMock(answer=AsyncMock())
    handle = await _handler(own=own, gate=gate)
    route = MagicMock(abort=AsyncMock())
    await handle(
        route,
        MagicMock(url="https://workspace.widget-render.invalid/g/t/report.html?format=%70df"),
    )
    route.abort.assert_awaited_once()
    own.assert_not_awaited()
    gate.answer.assert_not_awaited()


def test_a_budget_is_spent_across_calls():
    budget = hb.Budget(10)
    assert budget.spend(6)
    assert not budget.spend(6)
    assert budget.left == -2
