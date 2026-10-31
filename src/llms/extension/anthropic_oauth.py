"""ChatAnthropic variant that uses OAuth Bearer auth (auth_token) instead of API key (X-Api-Key)."""

from functools import cached_property
from typing import Any, ClassVar

import anthropic
from langchain_anthropic import ChatAnthropic
from langchain_core.language_models import LanguageModelInput

# Required by Anthropic's API for OAuth subscription tokens to access Sonnet/Opus.
_CLAUDE_CODE_IDENTITY = "You are Claude Code, Anthropic's official CLI for Claude."

#: How long every cache entry this client writes lives. A subscription's turns
#: are paced by a person, and a 5-minute entry has usually expired by the next
#: message, so the whole prefix is written again each time; an hour keeps it.
_CACHE_TTL = "1h"

#: The keys a cache marker can sit under: the request's system, tools and
#: messages, a message's content, and a block's nested content or document
#: source. Nothing else is walked, so a schema, a tool's arguments or examples,
#: and a map keyed by tool name keep a ``cache_control`` key as written.
_MARKER_PATHS = frozenset({"system", "tools", "messages", "content", "source"})


class ChatAnthropicOAuth(ChatAnthropic):
    """ChatAnthropic that sends ``Authorization: Bearer <token>`` instead of ``X-Api-Key``.

    The Anthropic Python SDK supports two auth modes:
      - ``api_key``   → ``X-Api-Key`` header  (standard API keys)
      - ``auth_token`` → ``Authorization: Bearer`` header  (OAuth tokens)

    LangChain's ``ChatAnthropic`` only passes ``api_key``.  This subclass
    intercepts ``_client_params`` so the OAuth token goes through ``auth_token``,
    nulls out ``api_key`` on the constructed client to prevent the SDK from
    falling back to the ``ANTHROPIC_API_KEY`` environment variable, and prepends
    the Claude Code identity to the system prompt (required for Sonnet/Opus access).
    """

    #: The ttl every cache marker it sends asks for (``_with_cache_ttl``).
    cache_ttl: ClassVar[str] = _CACHE_TTL

    @property
    def _client_params(self) -> dict[str, Any]:
        params = super()._client_params
        # Move the value from api_key → auth_token so the SDK sends Bearer auth
        token = params.pop("api_key", None)
        params["api_key"] = None
        params["auth_token"] = token
        return params

    @cached_property
    def _client(self) -> anthropic.Client:
        client = super()._client
        # The SDK constructor replaces api_key=None with ANTHROPIC_API_KEY env var.
        # Null it out so auth_headers only sends Authorization: Bearer, not X-Api-Key.
        client.api_key = None
        return client

    @cached_property
    def _async_client(self) -> anthropic.AsyncClient:
        client = super()._async_client
        client.api_key = None
        return client

    def _get_request_payload(
        self,
        input_: LanguageModelInput,
        *,
        stop: list[str] | None = None,
        **kwargs: dict,
    ) -> dict:
        payload = super()._get_request_payload(input_, stop=stop, **kwargs)
        # Prepend Claude Code identity — required for Sonnet/Opus with OAuth tokens.
        system = payload.get("system")
        identity_block = {"type": "text", "text": _CLAUDE_CODE_IDENTITY}
        if isinstance(system, list):
            payload["system"] = [identity_block, *system]
        elif isinstance(system, str) and system:
            payload["system"] = [identity_block, {"type": "text", "text": system}]
        else:
            payload["system"] = [identity_block]
        return _with_cache_ttl(payload)


def _with_cache_ttl(value: Any) -> Any:
    """``value`` with every cache marker asking for ``_CACHE_TTL``, as a copy.

    Several middlewares that serve every Anthropic route place the markers, and
    Anthropic refuses a request whose lifetimes are out of order, so the route's
    lifetime is applied here, to the whole request at once. The inputs are not
    written to: the payload shares its marker dicts with the request state.
    """
    if isinstance(value, list):
        return [_with_cache_ttl(item) for item in value]
    if not isinstance(value, dict):
        return value
    copied = {key: _with_cache_ttl(item) if key in _MARKER_PATHS else item for key, item in value.items()}
    if isinstance(copied.get("cache_control"), dict):
        copied["cache_control"] = {**copied["cache_control"], "ttl": _CACHE_TTL}
    return copied
