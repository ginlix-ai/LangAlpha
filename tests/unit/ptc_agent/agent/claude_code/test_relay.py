"""What the relay makes of a Claude Code request's stream."""

from __future__ import annotations

import pytest

from ptc_agent.agent.claude_code.relay import ClaudeStreamRelay
from src.llms.token_counter import extract_token_usage


@pytest.mark.asyncio
async def test_a_request_is_metered_with_the_cache_it_wrote_split_by_its_life():
    """The message's last usage, which the converter reads, gives the cache
    written as one count; its first splits it, and a 1h write costs more."""
    start = {"input_tokens": 2, "output_tokens": 0, "cache_read_input_tokens": 100, "cache_creation_input_tokens": 500,
             "cache_creation": {"ephemeral_5m_input_tokens": 0, "ephemeral_1h_input_tokens": 500}}
    end = {"input_tokens": 2, "output_tokens": 7, "cache_read_input_tokens": 100, "cache_creation_input_tokens": 500}
    events = [
        {"type": "message_start", "message": {"id": "msg_1", "type": "message", "role": "assistant", "model": "m", "content": [],
                                              "stop_reason": None, "stop_sequence": None, "usage": start}},
        {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}},
        {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "hi"}},
        {"type": "content_block_stop", "index": 0},
        {"type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": None}, "usage": end},
        {"type": "message_stop"},
    ]

    async def stream():
        for event in events:
            yield event

    reply = await ClaudeStreamRelay(model_name="m").feed(stream()).ainvoke([])
    assert {k: extract_token_usage(reply).get(k) for k in ("input_tokens", "cached_tokens", "cache_5m_tokens", "cache_1h_tokens")} == {
        "input_tokens": 602, "cached_tokens": 100, "cache_5m_tokens": None, "cache_1h_tokens": 500,
    }
