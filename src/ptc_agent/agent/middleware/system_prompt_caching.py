"""Middleware for the static-prefix cache breakpoint outside ``ChatAnthropic``.

Analog of ``AnthropicPromptCachingMiddleware`` for the other clients
``breakpoint_marker`` names: OpenAI's explicit ``prompt_cache_breakpoint``
(GPT-5.6+) and ``cache_control`` for a Claude model over OpenRouter. Tags the
last system content block it sees so the static system prompt is written to
cache at a stable boundary. Dynamic-context middlewares (workspace, runtime)
run innermost and append after the marker, keeping the cached prefix stable
across requests.
"""

from collections.abc import Awaitable, Callable
from typing import Any

from langchain.agents.middleware.types import AgentMiddleware, ModelRequest, ModelResponse
from langchain_core.messages import SystemMessage

from ptc_agent.agent.middleware.provider_cache import static_prefix_marker, tag_last_text_block


class SystemPromptCacheMiddleware(AgentMiddleware):
    """Places a provider prompt-cache breakpoint on the static system prefix.

    Applies to the models ``static_prefix_marker`` gives a marker: every client
    ``breakpoint_marker`` names except ``ChatAnthropic``. For OpenAI that is a
    ``ChatOpenAI`` with a non-None ``prompt_cache_options`` pointed at
    api.openai.com — other backends (codex, platform proxy, OpenAI-compatible
    endpoints) reject or drop the marker, so anything with a non-official
    base_url passes through untouched. ``create_llm`` applies the same gate when
    attaching ``prompt_cache_options``; this check is defense-in-depth for
    clients constructed outside the factory.
    """

    @staticmethod
    def _marker(request: ModelRequest) -> tuple[str, dict[str, Any]] | None:
        return static_prefix_marker(request.model)

    @staticmethod
    def _tag_system_message(
        system_message: SystemMessage | None, key: str, marker: dict[str, Any]
    ) -> SystemMessage | None:
        if system_message is None:
            return None
        new_content = tag_last_text_block(system_message.content, key, marker)
        if new_content is None:
            return system_message
        return SystemMessage(content=new_content)

    def _apply(self, request: ModelRequest, marker: tuple[str, dict[str, Any]]) -> ModelRequest:
        tagged = self._tag_system_message(request.system_message, *marker)
        if tagged is request.system_message:
            return request
        return request.override(system_message=tagged)

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelResponse:
        marker = self._marker(request)
        if marker is None:
            return handler(request)
        return handler(self._apply(request, marker))

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        marker = self._marker(request)
        if marker is None:
            return await handler(request)
        return await handler(self._apply(request, marker))
