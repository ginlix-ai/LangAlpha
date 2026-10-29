"""The wire half of a provider's model list: the request a credential makes,
the fetch, and each SDK family's answer normalized to one shape,
``{id, display_name, context_window?, input_modalities?, pricing?,
supported_parameters?, reasoning?}``. Takes a credential and nothing else.
"""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx

from src.llms.model_spec import canonical_reasoning_efforts
from src.llms.reasoning import OFF_LEVELS

FETCH_TIMEOUT_S = 8.0
MAX_LIST_BYTES = 8 * 1024 * 1024
# Enough of a refusal to read the provider's message out of it.
ERROR_BODY_BYTES = 64 * 1024

_OPENAI_DEFAULT_BASE = "https://api.openai.com/v1"
_ANTHROPIC_DEFAULT_BASE = "https://api.anthropic.com"
_GEMINI_DEFAULT_BASE = "https://generativelanguage.googleapis.com"
_ANTHROPIC_OAUTH_BETA = "oauth-2025-04-20"

# Shapes of list endpoint, one per SDK family that answers differently.
OPENAI, ANTHROPIC, GEMINI, CODEX, OPENROUTER = (
    "openai", "anthropic", "gemini", "codex", "openrouter",
)

# Why a list could not be read, as the endpoint reports it.
UNAUTHORIZED = "unauthorized"  # the provider refused the credential (401/403)
UNREACHABLE = "unreachable"  # network failure or the fetch deadline
BAD_RESPONSE = "bad_response"  # another status, an unreadable or oversized body
ENDPOINT_REFUSED = "endpoint_refused"  # the egress policy will not dial it


class ListFetchError(Exception):
    def __init__(self, reason: str, status: int | None = None, detail: str = ""):
        super().__init__(reason if status is None else f"{reason} ({status})")
        self.reason = reason
        self.status = status
        #: The provider's own message for a status it answered with, if any.
        self.detail = detail

    @property
    def timed_out(self) -> bool:
        return isinstance(self.__cause__, (TimeoutError, httpx.TimeoutException))


# ---------------------------------------------------------------------------
# Building the request
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ListRequest:
    shape: str
    url: str
    headers: dict[str, str] = field(repr=False)


def list_shape(info: dict) -> str:
    sdk = info.get("sdk")
    if sdk == "codex":
        return CODEX
    if sdk == "openrouter" or info.get("parent_provider") == "openrouter":
        return OPENROUTER
    if sdk == "gemini":
        return GEMINI
    if sdk == "anthropic":
        return ANTHROPIC
    return OPENAI


def list_url(info: dict, shape: str, override: str | None) -> tuple[str, str]:
    """Where the list lives and the shape it answers in.

    A declared ``models_url`` (an OpenAI-shaped list at a vendor whose chat
    endpoint is its Anthropic one) serves the manifest's own base. A saved base
    URL on the same path at another origin (the vendor's other region, a
    gateway in front of it) gets that list at its own origin; any other saved
    base is listed the way its SDK family lists.
    """
    manifest_base = info.get("base_url")
    models_url = info.get("models_url")
    if models_url:
        if not override or override.rstrip("/") == (manifest_base or "").rstrip("/"):
            return models_url, OPENAI
        if manifest_base and _path(override) == _path(manifest_base):
            o = urlsplit(override.strip())
            return urlunsplit(urlsplit(models_url)._replace(scheme=o.scheme, netloc=o.netloc)), OPENAI
    base = (override or manifest_base or "").strip().rstrip("/")
    if shape == ANTHROPIC:
        from src.llms.endpoints import anthropic_sdk_base_url

        root = (anthropic_sdk_base_url(base) or _ANTHROPIC_DEFAULT_BASE).rstrip("/")
        return f"{root}/v1/models?limit=1000", shape
    if shape == GEMINI:
        return f"{base or _GEMINI_DEFAULT_BASE}/v1beta/models?pageSize=1000", shape
    if shape == CODEX:
        from src.llms.extension.codex import codex_client_version

        return f"{base}/models?client_version={codex_client_version()}", shape
    if shape == OPENROUTER:
        return f"{base}/models?supported_parameters=tools", shape
    return f"{base or _OPENAI_DEFAULT_BASE}/models", shape


def _path(url: str) -> str:
    return urlsplit(url.strip()).path.rstrip("/")


def build_list_request(
    info: dict,
    *,
    api_key: str | None,
    base_url: str | None = None,
    token_headers: dict[str, str] | None = None,
) -> ListRequest:
    """The list request a credential makes, with the headers its LLM client sends.

    ``info`` is the provider's manifest entry; ``base_url`` is the one saved
    with the key, None for the manifest's; ``token_headers`` are a
    subscription token's own.
    """
    url, shape = list_url(info, list_shape(info), base_url)
    headers: dict[str, str] = {str(k): str(v) for k, v in (info.get("default_headers") or {}).items()}
    oauth = info.get("access_type") == "oauth"
    if shape == ANTHROPIC:
        headers["anthropic-version"] = "2023-06-01"
        if oauth:
            headers["Authorization"] = f"Bearer {api_key}"
            beta = [b for b in headers.get("anthropic-beta", "").split(",") if b]
            if _ANTHROPIC_OAUTH_BETA not in beta:
                beta.append(_ANTHROPIC_OAUTH_BETA)
            headers["anthropic-beta"] = ",".join(beta)
        elif api_key:
            headers["x-api-key"] = api_key
    elif shape == GEMINI:
        if api_key:
            headers["x-goog-api-key"] = api_key
    else:
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        if shape == CODEX:
            from src.llms.extension.codex import codex_identity_headers

            headers.update(codex_identity_headers())
    headers.update(token_headers or {})
    return ListRequest(shape=shape, url=url, headers=headers)


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------


async def read_capped(resp: httpx.Response, limit: int) -> bytes | None:
    """The body, or None once it runs past ``limit`` bytes."""
    body = bytearray()
    async for chunk in resp.aiter_bytes():
        body += chunk
        if len(body) > limit:
            return None
    return bytes(body)


def error_message(body: bytes) -> str:
    """The message in a refusal: ``error.message``, a bare ``error``, or the text."""
    try:
        error = json.loads(body).get("error", "")
        message = error.get("message", "") if isinstance(error, dict) else str(error)
    except Exception:
        message = body.decode("utf-8", "replace")
    return str(message)[:200]


async def fetch_list(
    req: ListRequest,
    *,
    transport: Any = None,
    timeout: float = FETCH_TIMEOUT_S,
    max_bytes: int = MAX_LIST_BYTES,
) -> Any:
    """GET the list and return its parsed JSON, or raise ``ListFetchError``."""

    async def _get() -> Any:
        async with httpx.AsyncClient(
            timeout=timeout, transport=transport, follow_redirects=False
        ) as client:
            async with client.stream("GET", req.url, headers=req.headers) as resp:
                if resp.status_code != 200:
                    refusal = await read_capped(resp, ERROR_BODY_BYTES) or b""
                    reason = BAD_RESPONSE
                    # xAI and Gemini refuse a bad key with a 400 that says so.
                    if resp.status_code in (401, 403) or (
                        resp.status_code == 400 and b"api key" in refusal[:4096].lower()
                    ):
                        reason = UNAUTHORIZED
                    raise ListFetchError(reason, resp.status_code, error_message(refusal))
                body = await read_capped(resp, max_bytes)
        if body is None:
            raise ListFetchError(BAD_RESPONSE)
        try:
            return json.loads(body)
        except ValueError as e:
            raise ListFetchError(BAD_RESPONSE) from e

    from src.server.services.llm.egress import EndpointRefused

    try:
        return await asyncio.wait_for(_get(), timeout)
    except ListFetchError:
        raise
    except EndpointRefused as e:
        raise ListFetchError(ENDPOINT_REFUSED) from e
    except (TimeoutError, httpx.TransportError) as e:
        raise ListFetchError(UNREACHABLE) from e
    except httpx.HTTPError as e:
        raise ListFetchError(BAD_RESPONSE) from e


# ---------------------------------------------------------------------------
# Normalizing
# ---------------------------------------------------------------------------

# Models that are not chat models, for lists that carry no capability field to
# tell. Matched against whole segments of the id ("gpt-image-1", not "imagine").
_NON_CHAT = re.compile(
    r"(?:^|[-_/.:])(?:"
    r"embed|embeddings?|rerank|moderation"
    r"|tts|speech|audio|asr|transcribe|whisper|voice"
    r"|realtime|live|livetranslate|s2s"
    r"|image|imagen|dall-e|sora|seedream|seedance|video|lyria|nano-banana|wan\d[\d.]*"
    r"|ocr|search-preview|search-api|deep-research|deep-search"
    r"|computer-use|robotics|aqa|router"
    r"|babbage|davinci|turbo-instruct|mt"
    r")(?=$|[-_/.:\d])",
    re.IGNORECASE,
)

# Volcengine labels each model with a ``domain``; these are the chat ones.
_CHAT_DOMAINS = frozenset({"", "LLM", "VLM"})
_DEAD_STATUSES = frozenset({"shutdown"})
_OPENROUTER_DUPLICATE_SUFFIXES = (":batch", ":free")


def is_chat_model(model_id: str) -> bool:
    return not _NON_CHAT.search(model_id)


def _int(*values: Any) -> int | None:
    for v in values:
        if isinstance(v, int) and not isinstance(v, bool) and v > 0:
            return v
    return None


def _strs(value: Any) -> list[str] | None:
    if isinstance(value, list) and value and all(isinstance(v, str) for v in value):
        return list(value)
    return None


def _dig(item: dict, *path: str) -> Any:
    for key in path:
        if not isinstance(item, dict):
            return None
        item = item.get(key)
    return item


def _model(model_id: str, display_name: Any, context: int | None, modalities: list[str] | None) -> dict:
    out: dict[str, Any] = {
        "id": model_id,
        "display_name": display_name if isinstance(display_name, str) and display_name else model_id,
    }
    if context:
        out["context_window"] = context
    if modalities:
        out["input_modalities"] = modalities
    return out


def _items(payload: Any, key: str) -> list[dict]:
    items = payload.get(key) if isinstance(payload, dict) else None
    if not isinstance(items, list):
        raise ListFetchError(BAD_RESPONSE)
    return [i for i in items if isinstance(i, dict)]


def _normalize_openai(payload: Any) -> list[dict]:
    out = []
    for item in _items(payload, "data"):
        model_id = item.get("id")
        if not isinstance(model_id, str) or not model_id:
            continue
        if str(item.get("status") or "").lower() in _DEAD_STATUSES:
            continue
        domain = item.get("domain")
        if isinstance(domain, str) and domain not in _CHAT_DOMAINS:
            continue
        out.append(_model(
            model_id,
            item.get("display_name") or item.get("name"),
            _int(
                item.get("context_window"),
                item.get("context_length"),
                _dig(item, "limit", "context"),
                _dig(item, "token_limits", "context_window"),
            ),
            _strs(item.get("input_modalities"))
            or _strs(_dig(item, "modalities", "input"))
            or _strs(_dig(item, "modalities", "input_modalities")),
        ))
    return out


def _normalize_anthropic(payload: Any) -> list[dict]:
    out = []
    for item in _items(payload, "data"):
        model_id = item.get("id")
        if not isinstance(model_id, str) or not model_id:
            continue
        caps = item.get("capabilities")
        modalities = None
        if isinstance(caps, dict):
            modalities = ["text"]
            if _dig(caps, "image_input", "supported"):
                modalities.append("image")
            if _dig(caps, "pdf_input", "supported"):
                modalities.append("pdf")
        out.append(_model(model_id, item.get("display_name"), _int(item.get("max_input_tokens")), modalities))
    return out


def _normalize_gemini(payload: Any) -> list[dict]:
    out = []
    for item in _items(payload, "models"):
        name = item.get("name")
        methods = item.get("supportedGenerationMethods") or []
        if not isinstance(name, str) or "generateContent" not in methods:
            continue
        out.append(_model(
            name.removeprefix("models/"), item.get("displayName"), _int(item.get("inputTokenLimit")), None,
        ))
    return out


def _normalize_codex(payload: Any) -> list[dict]:
    out = []
    for item in _items(payload, "models"):
        slug = item.get("slug")
        if not isinstance(slug, str) or item.get("visibility") != "list" or not item.get("supported_in_api"):
            continue
        out.append(_model(
            slug, item.get("display_name"), _int(item.get("context_window")), _strs(item.get("input_modalities")),
        ))
    return out


def _per_million(value: Any) -> float | None:
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None
    return round(price * 1_000_000, 6) if price >= 0 else None


#: Where an OpenRouter model's level goes: ``ChatOpenRouter``'s ``reasoning``
#: field, which OpenRouter maps onto each vendor's own control.
OPENROUTER_EFFORT_WRITE = "parameters.reasoning.effort"


def _openrouter_reasoning(item: dict) -> dict | None:
    """The model's ``reasoning`` block, from OpenRouter's own declaration.

    OpenRouter names each model's levels in our vocabulary, so the list is the
    ladder as is. A model whose thinking is ``mandatory`` refuses ``none``, so
    the off rung goes even where the list names it.
    """
    raw = item.get("reasoning")
    if not isinstance(raw, dict):
        return None
    efforts = canonical_reasoning_efforts(raw.get("supported_efforts"))
    if raw.get("mandatory"):
        efforts = tuple(e for e in efforts if e not in OFF_LEVELS)
    if not efforts:
        return None
    block: dict[str, Any] = {"efforts": list(efforts), "write": OPENROUTER_EFFORT_WRITE}
    if raw.get("default_effort") in efforts:
        block["default"] = raw["default_effort"]
    return block


def _normalize_openrouter(payload: Any) -> list[dict]:
    items = _items(payload, "data")
    ids = {i.get("id") for i in items}
    out = []
    for item in items:
        model_id = item.get("id")
        if not isinstance(model_id, str) or not model_id:
            continue
        if model_id.endswith(_OPENROUTER_DUPLICATE_SUFFIXES) and model_id.rsplit(":", 1)[0] in ids:
            continue
        model = _model(
            model_id,
            item.get("name"),
            _int(item.get("context_length"), _dig(item, "top_provider", "context_length")),
            _strs(_dig(item, "architecture", "input_modalities")),
        )
        raw = item.get("pricing") if isinstance(item.get("pricing"), dict) else {}
        pricing = {
            name: price
            for name, field_name in (
                ("input", "prompt"),
                ("output", "completion"),
                ("cache_read", "input_cache_read"),
                ("cache_write", "input_cache_write"),
            )
            if (price := _per_million(raw.get(field_name))) is not None
        }
        if pricing:
            model["pricing"] = pricing  # USD per million tokens
        params = _strs(item.get("supported_parameters"))
        if params:
            model["supported_parameters"] = params
        if reasoning := _openrouter_reasoning(item):
            model["reasoning"] = reasoning
        out.append(model)
    return out


_NORMALIZERS = {
    OPENAI: _normalize_openai,
    ANTHROPIC: _normalize_anthropic,
    GEMINI: _normalize_gemini,
    CODEX: _normalize_codex,
    OPENROUTER: _normalize_openrouter,
}


def normalize(shape: str, payload: Any) -> list[dict]:
    """The chat models in a list response, deduplicated by id, in list order."""
    seen: set[str] = set()
    out = []
    for model in _NORMALIZERS[shape](payload):
        if model["id"] in seen or not is_chat_model(model["id"]):
            continue
        seen.add(model["id"])
        out.append(model)
    return out
