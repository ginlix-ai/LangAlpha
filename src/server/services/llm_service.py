"""Canonical one-shot LLM call wrapper.

``LLMService.complete`` is the single entry point every server-layer utility
should use when it needs a single-shot LLM invocation (structured output or
free-form text). It delegates credential resolution to
``resolve_llm_config`` — the same function the main PTC and Flash chat
workflows use — so BYOK, Anthropic/Codex OAuth, custom providers, and
user-preferred models are all respected automatically. Contrast with direct
``create_llm(model_name)`` calls, which always bill through the platform key
regardless of what the user has configured.

Two code paths:

- ``user_id=None`` — system/scheduled task, no per-user resolution. Uses
  ``agent_config.llm.flash`` (or ``request_model``) with platform
  credentials. No DB hit.
- ``user_id="..."`` — full user-aware resolution through
  ``resolve_llm_config``. If that function returns a config whose
  ``llm_client`` is ``None`` (no BYOK / OAuth configured), we fall back to
  ``create_llm(effective_model)`` on the resolved model name so the call
  still goes through.

Memo metadata generation is the first caller; thread titles, follow-up
suggestions, hint messages, and the insight-service fallback are the
anticipated next callers. The interface accepts both structured
(``response_schema=Type[BaseModel]``) and free-form (``response_schema=None``)
modes; ``return_token_usage`` is plumbed through so a single billing hook
can land here later.
"""

from __future__ import annotations

import logging
from typing import Any, Literal, Type, TypeVar

from pydantic import BaseModel

from src.llms.api_call import make_api_call
from src.llms.llm import create_llm
from src.server.handlers.chat.llm_config import _MODE_MODEL_MAP, resolve_llm_config

T = TypeVar("T", bound=BaseModel)


class LLMService:
    """Generic one-shot LLM call wrapper with user-aware credential resolution."""

    def __init__(self, *, agent_config: Any, logger: logging.Logger | None = None) -> None:
        self._agent_config = agent_config
        self._logger = logger or logging.getLogger(__name__)

    async def complete(
        self,
        *,
        user_id: str | None,
        user_prompt: str,
        system_prompt: str | None = None,
        response_schema: Type[T] | None = None,
        mode: Literal["flash", "ptc"] = "flash",
        request_model: str | None = None,
        is_byok: bool = True,
        reasoning_effort: str | None = None,
        disable_tracing: bool = True,
        return_token_usage: bool = False,
    ) -> Any:
        """Run a single-shot LLM call.

        Returns:
            - ``response_schema=None`` → raw string content.
            - ``response_schema=MyModel`` → validated pydantic instance.
            - ``return_token_usage=True`` → ``(response, token_usage_dict)`` tuple.
        """
        if user_id is None:
            # Scheduled task / system warmup — no BYOK/OAuth lookup, use
            # platform default. ``resolve_llm_config`` is typed ``user_id: str``
            # and makes a DB lookup via ``get_model_preference``; skipping it
            # keeps system callers DB-free.
            model_name = request_model or self._agent_config.llm.flash
            llm = create_llm(model_name, reasoning_effort=reasoning_effort)
        else:
            resolved_config = await resolve_llm_config(
                base_config=self._agent_config,
                user_id=user_id,
                request_model=request_model,
                is_byok=is_byok,
                mode=mode,
                reasoning_effort=reasoning_effort,
                fast_mode=None,
            )
            llm = resolved_config.llm_client
            if llm is None:
                # No BYOK / OAuth configured and no reasoning override — fall
                # back to a platform-keyed client on the resolved model name.
                model_field, _ = _MODE_MODEL_MAP[mode]
                effective_model = (
                    getattr(resolved_config.llm, model_field, None)
                    or resolved_config.llm.name
                )
                self._logger.info(
                    "llm_service.platform_key_fallback",
                    extra={"user_id": user_id, "model": effective_model, "mode": mode},
                )
                llm = create_llm(effective_model, reasoning_effort=reasoning_effort)

        return await make_api_call(
            llm,
            system_prompt=system_prompt or "",
            user_prompt=user_prompt,
            response_schema=response_schema,
            return_token_usage=return_token_usage,
            disable_tracing=disable_tracing,
        )
