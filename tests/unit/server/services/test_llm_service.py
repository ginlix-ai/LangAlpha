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
- ``platform_key_fallback`` log fires for PLATFORM/NONE credential sources
  and is suppressed for OAUTH/BYOK and the ``user_id=None`` system path.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel

from ptc_agent.config.agent import CredentialSource
from src.server.services.llm.service import LLMService


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
                "src.server.services.llm.service.resolve_llm_config",
                new_callable=AsyncMock,
            ) as mock_resolve,
            patch(
                "src.server.services.llm.service.create_llm",
                return_value=fake_llm,
            ) as mock_create,
            patch(
                "src.server.services.llm.service.make_api_call",
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
                "src.server.services.llm.service.resolve_llm_config",
                new_callable=AsyncMock,
            ) as mock_resolve,
            patch(
                "src.server.services.llm.service.create_llm",
                return_value=fake_llm,
            ) as mock_create,
            patch(
                "src.server.services.llm.service.make_api_call",
                new_callable=AsyncMock,
                return_value="ok",
            ),
        ):
            await service.complete(
                user_id=None,
                user_prompt="hi",
                request_model="gpt-5.6-terra",
                reasoning_effort="low",
            )

        mock_resolve.assert_not_called()
        mock_create.assert_called_once_with("gpt-5.6-terra", reasoning_effort="low")


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
            credential_source=CredentialSource.BYOK,
        )

        with (
            patch(
                "src.server.services.llm.service.resolve_llm_config",
                new_callable=AsyncMock,
                return_value=resolved_config,
            ) as mock_resolve,
            patch(
                "src.server.services.llm.service.create_llm",
            ) as mock_create,
            patch(
                "src.server.services.llm.service.make_api_call",
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
            credential_source=CredentialSource.NONE,
        )
        fake_llm = MagicMock(name="fallback_llm")

        with (
            patch(
                "src.server.services.llm.service.resolve_llm_config",
                new_callable=AsyncMock,
                return_value=resolved_config,
            ),
            patch(
                "src.server.services.llm.service.create_llm",
                return_value=fake_llm,
            ) as mock_create,
            patch(
                "src.server.services.llm.service.make_api_call",
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
                "src.server.services.llm.service.create_llm",
                return_value=MagicMock(),
            ),
            patch(
                "src.server.services.llm.service.make_api_call",
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
                "src.server.services.llm.service.create_llm",
                return_value=MagicMock(),
            ),
            patch(
                "src.server.services.llm.service.make_api_call",
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
                "src.server.services.llm.service.create_llm",
                return_value=MagicMock(),
            ),
            patch(
                "src.server.services.llm.service.make_api_call",
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
            credential_source=CredentialSource.BYOK,
        )

        with (
            patch(
                "src.server.services.llm.service.resolve_llm_config",
                new_callable=AsyncMock,
                return_value=resolved_config,
            ) as mock_resolve,
            patch(
                "src.server.services.llm.service.make_api_call",
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


# ---------------------------------------------------------------------------
# platform_key_fallback signal — driven by credential_source, not llm is None
# ---------------------------------------------------------------------------


def _make_resolved_config(
    *,
    credential_source: CredentialSource,
    llm_client=None,
    flash: str = "flash-model",
    name: str = "name-model",
) -> SimpleNamespace:
    return SimpleNamespace(
        llm=SimpleNamespace(flash=flash, name=name),
        llm_client=llm_client,
        credential_source=credential_source,
    )


class TestPlatformKeyFallbackSignal:
    """``platform_key_fallback`` must fire for PLATFORM and NONE, not for OAUTH/BYOK."""

    async def _run(self, service, *, credential_source, llm_client):
        """Helper: patch resolve_llm_config with given source/client, run complete()."""
        resolved_config = _make_resolved_config(
            credential_source=credential_source, llm_client=llm_client
        )
        fake_llm = MagicMock(name="fake_llm")
        with (
            patch(
                "src.server.services.llm.service.resolve_llm_config",
                new_callable=AsyncMock,
                return_value=resolved_config,
            ),
            patch(
                "src.server.services.llm.service.create_llm",
                return_value=fake_llm,
            ),
            patch(
                "src.server.services.llm.service.make_api_call",
                new_callable=AsyncMock,
                return_value="ok",
            ),
        ):
            await service.complete(user_id="user-a", user_prompt="test", mode="flash")

    @pytest.mark.asyncio
    async def test_platform_eager_emits_signal(self):
        """PLATFORM + llm_client SET (eager case missed by the old code) → signal fires."""
        agent_config = _make_agent_config()
        mock_logger = MagicMock()
        service = LLMService(agent_config=agent_config, logger=mock_logger)

        eager_client = MagicMock(name="eager_platform_client")
        await self._run(
            service,
            credential_source=CredentialSource.PLATFORM,
            llm_client=eager_client,
        )

        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args
        assert call_args.args[0] == "llm_service.platform_key_fallback"
        extra = call_args.kwargs["extra"]
        assert extra["user_id"] == "user-a"
        assert extra["credential_source"] == str(CredentialSource.PLATFORM)

    @pytest.mark.asyncio
    async def test_none_lazy_emits_signal(self):
        """NONE + llm_client None (lazy create_llm path) → signal fires."""
        agent_config = _make_agent_config()
        mock_logger = MagicMock()
        service = LLMService(agent_config=agent_config, logger=mock_logger)

        await self._run(
            service,
            credential_source=CredentialSource.NONE,
            llm_client=None,
        )

        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args
        assert call_args.args[0] == "llm_service.platform_key_fallback"
        extra = call_args.kwargs["extra"]
        assert extra["credential_source"] == str(CredentialSource.NONE)

    @pytest.mark.asyncio
    async def test_byok_suppresses_signal(self):
        """BYOK → no platform log."""
        agent_config = _make_agent_config()
        mock_logger = MagicMock()
        service = LLMService(agent_config=agent_config, logger=mock_logger)

        await self._run(
            service,
            credential_source=CredentialSource.BYOK,
            llm_client=MagicMock(name="byok_client"),
        )

        mock_logger.info.assert_not_called()

    @pytest.mark.asyncio
    async def test_oauth_suppresses_signal(self):
        """OAUTH → no platform log."""
        agent_config = _make_agent_config()
        mock_logger = MagicMock()
        service = LLMService(agent_config=agent_config, logger=mock_logger)

        await self._run(
            service,
            credential_source=CredentialSource.OAUTH,
            llm_client=MagicMock(name="oauth_client"),
        )

        mock_logger.info.assert_not_called()

    @pytest.mark.asyncio
    async def test_user_id_none_system_path_suppresses_signal(self):
        """user_id=None (system task) → no resolution → no platform log."""
        agent_config = _make_agent_config(flash="sys-flash-model")
        mock_logger = MagicMock()
        service = LLMService(agent_config=agent_config, logger=mock_logger)

        with (
            patch(
                "src.server.services.llm.service.create_llm",
                return_value=MagicMock(),
            ),
            patch(
                "src.server.services.llm.service.make_api_call",
                new_callable=AsyncMock,
                return_value="ok",
            ),
        ):
            await service.complete(user_id=None, user_prompt="system task")

        mock_logger.info.assert_not_called()
