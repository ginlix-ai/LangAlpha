"""Tests for LLMService — the canonical one-shot LLM wrapper.

Covers:
- ``user_id=None`` bypasses ``resolve_llm_config`` and uses ``create_llm``
  on ``agent_config.llm.flash`` (or ``request_model`` when provided).
- ``user_id="..."`` calls ``resolve_llm_config`` with the correct args and
  uses the resolved ``llm_client``.
- ``response_schema=None`` returns the raw string from ``make_api_call``.
- ``response_schema=SomeModel`` returns a pydantic instance.
- ``return_token_usage=True`` returns a tuple.
- ``request_model`` override reaches both code paths (None + user_id).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel

from src.server.services.llm_service import LLMService


class _DummySchema(BaseModel):
    summary: str


def _make_agent_config(flash: str = "claude-haiku-4-5-20251001", name: str = "claude-opus-4") -> SimpleNamespace:
    """Minimal agent_config stub with an ``llm.flash`` / ``llm.name`` shape."""
    return SimpleNamespace(llm=SimpleNamespace(flash=flash, name=name))


# ---------------------------------------------------------------------------
# user_id=None path — no DB lookup, use platform default
# ---------------------------------------------------------------------------


class TestUserIdNone:
    @pytest.mark.asyncio
    async def test_bypasses_resolve_llm_config(self):
        """With ``user_id=None``, ``resolve_llm_config`` must never be called."""
        agent_config = _make_agent_config(flash="flash-default-model")
        service = LLMService(agent_config=agent_config)

        fake_llm = MagicMock(name="fake_llm")

        with (
            patch(
                "src.server.services.llm_service.resolve_llm_config",
                new_callable=AsyncMock,
            ) as mock_resolve,
            patch(
                "src.server.services.llm_service.create_llm",
                return_value=fake_llm,
            ) as mock_create,
            patch(
                "src.server.services.llm_service.make_api_call",
                new_callable=AsyncMock,
                return_value="raw string response",
            ) as mock_call,
        ):
            result = await service.complete(
                user_id=None,
                user_prompt="hi",
            )

        assert result == "raw string response"
        mock_resolve.assert_not_called()
        mock_create.assert_called_once_with(
            "flash-default-model", reasoning_effort=None
        )
        mock_call.assert_awaited_once_with(
            fake_llm,
            system_prompt="",
            user_prompt="hi",
            response_schema=None,
            return_token_usage=False,
            disable_tracing=True,
        )

    @pytest.mark.asyncio
    async def test_request_model_override_forwarded_to_create_llm(self):
        """``request_model`` overrides ``agent_config.llm.flash`` on the None path."""
        agent_config = _make_agent_config(flash="flash-default")
        service = LLMService(agent_config=agent_config)

        fake_llm = MagicMock(name="fake_llm")

        with (
            patch(
                "src.server.services.llm_service.resolve_llm_config",
                new_callable=AsyncMock,
            ) as mock_resolve,
            patch(
                "src.server.services.llm_service.create_llm",
                return_value=fake_llm,
            ) as mock_create,
            patch(
                "src.server.services.llm_service.make_api_call",
                new_callable=AsyncMock,
                return_value="ok",
            ),
        ):
            await service.complete(
                user_id=None,
                user_prompt="hi",
                request_model="gpt-5.4-mini",
                reasoning_effort="low",
            )

        mock_resolve.assert_not_called()
        mock_create.assert_called_once_with("gpt-5.4-mini", reasoning_effort="low")


# ---------------------------------------------------------------------------
# user_id="..." path — full resolution
# ---------------------------------------------------------------------------


class TestUserIdProvided:
    @pytest.mark.asyncio
    async def test_calls_resolve_llm_config_with_correct_args(self):
        """Confirms every kwarg is forwarded verbatim to ``resolve_llm_config``."""
        agent_config = _make_agent_config()
        service = LLMService(agent_config=agent_config)

        resolved_llm = MagicMock(name="resolved_llm")
        resolved_config = SimpleNamespace(
            llm=SimpleNamespace(flash="flash-resolved", name="name-resolved"),
            llm_client=resolved_llm,
        )

        with (
            patch(
                "src.server.services.llm_service.resolve_llm_config",
                new_callable=AsyncMock,
                return_value=resolved_config,
            ) as mock_resolve,
            patch(
                "src.server.services.llm_service.create_llm",
            ) as mock_create,
            patch(
                "src.server.services.llm_service.make_api_call",
                new_callable=AsyncMock,
                return_value="ok",
            ) as mock_call,
        ):
            result = await service.complete(
                user_id="u1",
                user_prompt="prompt",
                system_prompt="sys",
                mode="flash",
                request_model="mymodel",
                is_byok=False,
                reasoning_effort="medium",
            )

        assert result == "ok"
        mock_resolve.assert_awaited_once_with(
            base_config=agent_config,
            user_id="u1",
            request_model="mymodel",
            is_byok=False,
            mode="flash",
            reasoning_effort="medium",
            fast_mode=None,
        )
        # llm_client was present on the resolved config — no create_llm fallback
        mock_create.assert_not_called()
        mock_call.assert_awaited_once_with(
            resolved_llm,
            system_prompt="sys",
            user_prompt="prompt",
            response_schema=None,
            return_token_usage=False,
            disable_tracing=True,
        )

    @pytest.mark.asyncio
    async def test_falls_back_to_create_llm_when_llm_client_none(self):
        """When no BYOK/OAuth: ``resolved_config.llm_client`` is None → use
        ``create_llm(effective_model)`` on the resolved model name."""
        agent_config = _make_agent_config()
        service = LLMService(agent_config=agent_config)

        resolved_config = SimpleNamespace(
            llm=SimpleNamespace(flash="flash-resolved", name="name-resolved"),
            llm_client=None,
        )
        fake_llm = MagicMock(name="fallback_llm")

        with (
            patch(
                "src.server.services.llm_service.resolve_llm_config",
                new_callable=AsyncMock,
                return_value=resolved_config,
            ),
            patch(
                "src.server.services.llm_service.create_llm",
                return_value=fake_llm,
            ) as mock_create,
            patch(
                "src.server.services.llm_service.make_api_call",
                new_callable=AsyncMock,
                return_value="ok",
            ) as mock_call,
        ):
            await service.complete(
                user_id="u1",
                user_prompt="prompt",
                mode="flash",
            )

        # mode=flash → model_field="flash" → picks resolved_config.llm.flash
        mock_create.assert_called_once_with(
            "flash-resolved", reasoning_effort=None
        )
        assert mock_call.await_args.args[0] is fake_llm


# ---------------------------------------------------------------------------
# make_api_call return-shape plumbing
# ---------------------------------------------------------------------------


class TestReturnShapes:
    @pytest.mark.asyncio
    async def test_response_schema_none_returns_string(self):
        agent_config = _make_agent_config()
        service = LLMService(agent_config=agent_config)

        with (
            patch(
                "src.server.services.llm_service.create_llm",
                return_value=MagicMock(),
            ),
            patch(
                "src.server.services.llm_service.make_api_call",
                new_callable=AsyncMock,
                return_value="free-form reply",
            ) as mock_call,
        ):
            result = await service.complete(
                user_id=None,
                user_prompt="hello",
            )

        assert isinstance(result, str)
        assert result == "free-form reply"
        assert mock_call.await_args.kwargs["response_schema"] is None

    @pytest.mark.asyncio
    async def test_response_schema_returns_pydantic_instance(self):
        agent_config = _make_agent_config()
        service = LLMService(agent_config=agent_config)

        instance = _DummySchema(summary="a summary")

        with (
            patch(
                "src.server.services.llm_service.create_llm",
                return_value=MagicMock(),
            ),
            patch(
                "src.server.services.llm_service.make_api_call",
                new_callable=AsyncMock,
                return_value=instance,
            ) as mock_call,
        ):
            result = await service.complete(
                user_id=None,
                user_prompt="extract",
                response_schema=_DummySchema,
            )

        assert isinstance(result, _DummySchema)
        assert result.summary == "a summary"
        assert mock_call.await_args.kwargs["response_schema"] is _DummySchema

    @pytest.mark.asyncio
    async def test_return_token_usage_returns_tuple(self):
        agent_config = _make_agent_config()
        service = LLMService(agent_config=agent_config)

        payload = ("content", {"input_tokens": 10, "output_tokens": 5})

        with (
            patch(
                "src.server.services.llm_service.create_llm",
                return_value=MagicMock(),
            ),
            patch(
                "src.server.services.llm_service.make_api_call",
                new_callable=AsyncMock,
                return_value=payload,
            ) as mock_call,
        ):
            result = await service.complete(
                user_id=None,
                user_prompt="hi",
                return_token_usage=True,
            )

        assert result == payload
        assert mock_call.await_args.kwargs["return_token_usage"] is True


# ---------------------------------------------------------------------------
# request_model override reaches both paths
# ---------------------------------------------------------------------------


class TestRequestModelOverride:
    @pytest.mark.asyncio
    async def test_forwarded_to_resolve_llm_config(self):
        agent_config = _make_agent_config()
        service = LLMService(agent_config=agent_config)

        resolved_config = SimpleNamespace(
            llm=SimpleNamespace(flash="flash-resolved", name="name-resolved"),
            llm_client=MagicMock(),
        )

        with (
            patch(
                "src.server.services.llm_service.resolve_llm_config",
                new_callable=AsyncMock,
                return_value=resolved_config,
            ) as mock_resolve,
            patch(
                "src.server.services.llm_service.make_api_call",
                new_callable=AsyncMock,
                return_value="ok",
            ),
        ):
            await service.complete(
                user_id="u1",
                user_prompt="hi",
                request_model="custom-model-slug",
            )

        assert mock_resolve.await_args.kwargs["request_model"] == "custom-model-slug"
