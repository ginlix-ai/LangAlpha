"""Unit tests for the shared request-cancellation wrapper.

``cancellation_as_http`` converts an ``asyncio.CancelledError`` (a user Stop or a
client disconnect) into a clean 409 instead of letting it escape as a raw ASGI
500. It is the single, reusable mechanism for every stoppable endpoint — not a
per-handler patch.
"""

import asyncio

import pytest
from fastapi import HTTPException

from src.server.handlers.cancellation import cancellation_as_http


@pytest.mark.asyncio
async def test_cancellation_converted_to_clean_409():
    @cancellation_as_http("widget")
    async def _handler():
        raise asyncio.CancelledError()

    with pytest.raises(HTTPException) as exc_info:
        await _handler()

    assert exc_info.value.status_code == 409
    detail = exc_info.value.detail
    assert isinstance(detail, dict)
    assert detail["code"] == "request_cancelled"
    assert detail["verb"] == "widget"
    assert "message" in detail


@pytest.mark.asyncio
async def test_successful_return_passes_through():
    @cancellation_as_http("widget")
    async def _handler(value):
        return {"ok": value}

    assert await _handler(7) == {"ok": 7}


@pytest.mark.asyncio
async def test_http_exception_passes_through_unchanged():
    @cancellation_as_http("widget")
    async def _handler():
        raise HTTPException(status_code=400, detail="bad input")

    with pytest.raises(HTTPException) as exc_info:
        await _handler()

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "bad input"


@pytest.mark.asyncio
async def test_regular_exception_passes_through_unchanged():
    @cancellation_as_http("widget")
    async def _handler():
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        await _handler()
