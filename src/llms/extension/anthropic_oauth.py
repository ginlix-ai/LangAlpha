"""ChatAnthropic variant that uses OAuth Bearer auth (auth_token) instead of API key (X-Api-Key)."""

from functools import cached_property
from typing import Any

import anthropic
from langchain_anthropic import ChatAnthropic
from langchain_core.language_models import LanguageModelInput

# Required by Anthropic's API for OAuth subscription tokens to access Sonnet/Opus.
_CLAUDE_CODE_IDENTITY = "You are Claude Code, Anthropic's official CLI for Claude."


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
        return payload
