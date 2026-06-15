"""No-delta skip for the provenance re-sync choke point.

``_sync_provenance_for_response`` is re-entered on every ``update_sse_events``
call — including the per-model-call ``context_window`` persistence, whose events
never carry provenance. It must short-circuit (no extract, no delete-then-insert)
when the sse_events contain no provenance entry, and still run when they do.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.database.conversation import _sync_provenance_for_response

_SYNC = "src.server.database.provenance.sync_provenance_for_response"


@pytest.mark.asyncio
async def test_skips_when_no_provenance_event():
    events = [
        {"event": "context_window", "data": {"token_usage": {}}},
        {"event": "message_chunk", "data": {"text": "hi"}},
    ]
    with patch(_SYNC, new=AsyncMock()) as inner:
        await _sync_provenance_for_response(
            MagicMock(),
            conversation_response_id="r",
            conversation_thread_id="t",
            turn_index=0,
            sse_events=events,
        )
    inner.assert_not_awaited()  # no extract + delete-then-insert on context_window


@pytest.mark.asyncio
async def test_skips_on_empty_or_none():
    with patch(_SYNC, new=AsyncMock()) as inner:
        for events in (None, []):
            await _sync_provenance_for_response(
                MagicMock(),
                conversation_response_id="r",
                conversation_thread_id="t",
                turn_index=0,
                sse_events=events,
            )
    inner.assert_not_awaited()


@pytest.mark.asyncio
async def test_runs_when_provenance_event_present():
    events = [
        {"event": "message_chunk", "data": {"text": "hi"}},
        {
            "event": "provenance",
            "source_type": "web_search",
            "identifier": "https://example.test/a",
            "result_sha256": "sha-a",
        },
    ]
    with patch(_SYNC, new=AsyncMock()) as inner:
        await _sync_provenance_for_response(
            MagicMock(),
            conversation_response_id="r",
            conversation_thread_id="t",
            turn_index=0,
            sse_events=events,
        )
    inner.assert_awaited_once()
