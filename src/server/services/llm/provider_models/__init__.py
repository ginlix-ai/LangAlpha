"""Which models a user's credential can use, read from the provider's own list.

A connected key or subscription is asked for its model list, which is then
intersected with the manifest: manifest models the list names are ``ready``
(tuned, shown by default), the rest of the list, past a chat-model filter, are
``others`` (shown on request as Not tuned, saved as custom models). OpenRouter
is matched by name instead, every result flagged ``known`` where it is a
manifest model. When the list cannot be read or names no manifest model the
answer is ``fallback``: the manifest list, as before this existed, so a list
never shrinks to nothing on a provider's bad day.

Layered so the request, fetch and normalize steps (``wire``) take a
credential and nothing else, and the match (``matching``) the manifest as
well; only ``service`` reads the database, the OAuth stores and Redis. The
fetched list is cached in Redis per user and provider, keyed by a fingerprint
of the credential; matching runs on every read, so a manifest change applies
without waiting out the cache.
"""

from .matching import (
    EMPTY_LIST,
    NO_MATCH,
    ModelsResult,
    build_result,
    filter_others,
    gateway_slug,
    manifest_slug_index,
    match_endpoint_models,
    served_models,
)
from .service import (
    FORMAT_SDKS,
    NEEDS_REAUTH,
    NO_API_ACCESS,
    REFRESH_UNAVAILABLE,
    Credential,
    NotConnected,
    UnknownProvider,
    UnsupportedProvider,
    available_models,
    available_models_key,
    endpoint_models,
    invalidate_available_models,
    local_server_models,
)
from .wire import (
    ANTHROPIC,
    BAD_RESPONSE,
    CODEX,
    ENDPOINT_REFUSED,
    GEMINI,
    OPENAI,
    OPENROUTER,
    UNAUTHORIZED,
    UNREACHABLE,
    ListFetchError,
    ListRequest,
    build_list_request,
    fetch_list,
    is_chat_model,
    list_shape,
    list_url,
    normalize,
)

__all__ = [
    "ANTHROPIC",
    "BAD_RESPONSE",
    "CODEX",
    "EMPTY_LIST",
    "ENDPOINT_REFUSED",
    "FORMAT_SDKS",
    "GEMINI",
    "NEEDS_REAUTH",
    "NO_API_ACCESS",
    "NO_MATCH",
    "OPENAI",
    "OPENROUTER",
    "REFRESH_UNAVAILABLE",
    "UNAUTHORIZED",
    "UNREACHABLE",
    "Credential",
    "ListFetchError",
    "ListRequest",
    "ModelsResult",
    "NotConnected",
    "UnknownProvider",
    "UnsupportedProvider",
    "available_models",
    "available_models_key",
    "build_list_request",
    "build_result",
    "endpoint_models",
    "fetch_list",
    "filter_others",
    "gateway_slug",
    "invalidate_available_models",
    "is_chat_model",
    "list_shape",
    "list_url",
    "local_server_models",
    "manifest_slug_index",
    "match_endpoint_models",
    "normalize",
    "served_models",
]
