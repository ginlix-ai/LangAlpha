"""Middleware for OpenAI explicit prompt-cache breakpoints (GPT-5.6+).

OpenAI analog of ``AnthropicPromptCachingMiddleware``: tags the last system
content block it sees with a ``prompt_cache_breakpoint`` marker so the static
prefix (system prompt + skills) is written to cache at a stable boundary.
Dynamic-context middlewares (workspace, runtime) run innermost and append
after the marker, keeping the cached prefix stable across requests.
"""

from collections.abc import Awaitable, Callable
from typing import Any

from langchain.agents.middleware.types import AgentMiddleware, ModelRequest, ModelResponse
from langchain_core.messages import SystemMessage

from ptc_agent.agent.middleware.provider_cache import breakpoint_marker, tag_last_text_block

# Explicit-breakpoint marker forwarded verbatim onto the wire content part.
_BREAKPOINT: dict[str, Any] = {"mode": "explicit"}


class OpenAIPromptCachingMiddleware(AgentMiddleware):
    """Places an OpenAI prompt-cache breakpoint on the static system prefix.

    Applies only to ``ChatOpenAI`` models constructed with a non-None
    ``prompt_cache_options`` (opted in via the model manifest ``parameters``)
    AND pointed at api.openai.com — other backends (codex, platform proxy,
    OpenAI-compatible endpoints) reject or drop the marker, so anything with
    a non-official base_url passes through untouched. ``create_llm`` applies
    the same gate when attaching ``prompt_cache_options``; this check is
    defense-in-depth for clients constructed outside the factory.
    """

    @staticmethod
    def _should_apply(request: ModelRequest) -> bool:
        # Reuse the shared provider gate: the OpenAI branch of breakpoint_marker
        # is exactly this middleware's (ChatOpenAI + opted-in + official endpoint).
        marker = breakpoint_marker(request.model)
        return marker is not None and marker[0] == "prompt_cache_breakpoint"

    @staticmethod
    def _tag_system_message(
        system_message: SystemMessage | None,
    ) -> SystemMessage | None:
        if system_message is None:
            return None
        new_content = tag_last_text_block(
            system_message.content, "prompt_cache_breakpoint", _BREAKPOINT
        )
        if new_content is None:
            return system_message
        return SystemMessage(content=new_content)

    def _apply(self, request: ModelRequest) -> ModelRequest:
        tagged = self._tag_system_message(request.system_message)
        if tagged is request.system_message:
            return request
        return request.override(system_message=tagged)

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelResponse:
        if not self._should_apply(request):
            return handler(request)
        return handler(self._apply(request))

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        if not self._should_apply(request):
            return await handler(request)
        return await handler(self._apply(request))
