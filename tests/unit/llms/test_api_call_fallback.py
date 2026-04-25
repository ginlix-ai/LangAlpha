"""Regression: manual-parse fallback must accept content_blocks list."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest
from pydantic import BaseModel

from src.llms.api_call import make_api_call


class _Schema(BaseModel):
    description: str
    summary: str


class _ListContentMessage:
    def __init__(self, blocks: list[dict[str, Any]]) -> None:
        self.content_blocks = blocks
        self.content = blocks
        self.usage_metadata = None
        self.response_metadata: dict[str, Any] = {}
        self.additional_kwargs: dict[str, Any] = {}


class _StubLLM:
    def __init__(self, message: _ListContentMessage) -> None:
        self._message = message

    def with_structured_output(self, *_args: Any, **_kwargs: Any) -> "_StubLLM":
        return self

    async def ainvoke(self, *_args: Any, **_kwargs: Any) -> Any:
        self._calls = getattr(self, "_calls", 0) + 1
        if self._calls == 1:
            return {"parsed": None, "raw": self._message}
        return self._message


@pytest.mark.asyncio
async def test_fallback_handles_content_blocks_list() -> None:
    blocks = [
        {"type": "thinking", "thinking": "let me think about this"},
        {
            "type": "text",
            "text": (
                'Here is the JSON:\n\n'
                '{"description": "A test memo.", "summary": "Two short paragraphs."}'
            ),
        },
    ]
    llm = _StubLLM(_ListContentMessage(blocks))

    # ``tracing_context`` requires LangSmith plumbing we don't need in a
    # unit test — patch it out.
    with patch("langsmith.tracing_context") as ctx:
        ctx.return_value.__enter__ = lambda self: None
        ctx.return_value.__exit__ = lambda self, *a: None
        result = await make_api_call(
            llm,
            system_prompt="sys",
            user_prompt="user",
            response_schema=_Schema,
            max_parsing_retries=2,
        )

    assert isinstance(result, _Schema)
    assert result.description == "A test memo."
    assert result.summary == "Two short paragraphs."


@pytest.mark.asyncio
async def test_fallback_still_rejects_truly_empty_list() -> None:
    blocks: list[dict[str, Any]] = [
        {"type": "thinking", "thinking": "no answer"},
    ]
    llm = _StubLLM(_ListContentMessage(blocks))

    with patch("langsmith.tracing_context") as ctx:
        ctx.return_value.__enter__ = lambda self: None
        ctx.return_value.__exit__ = lambda self, *a: None
        with pytest.raises(ValueError) as exc_info:
            await make_api_call(
                llm,
                system_prompt="sys",
                user_prompt="user",
                response_schema=_Schema,
                max_parsing_retries=2,
            )

    # The error should be about JSON extraction, not type rejection.
    assert "Response is not a string" not in str(exc_info.value)
