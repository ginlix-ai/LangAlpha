"""Checking a provider key before it is saved.

A key is checked by listing the provider's models with it, the request a
turn's model list makes, and where the endpoint serves no list, by a one-token
call. OpenRouter is checked against ``GET /key`` instead: its list is public.
"""

from __future__ import annotations

import asyncio
import re
import time
from typing import Any

import httpx

from src.server.services.llm.provider_models import (
    ENDPOINT_REFUSED,
    ListFetchError,
    build_list_request,
    fetch_list,
    normalize,
)
from src.server.services.llm.provider_models.wire import ERROR_BODY_BYTES, error_message, read_capped

TEST_TIMEOUT_S = 5.0

# Cheapest model per SDK type, for the one-token call where there is no list.
_TEST_MODELS: dict[str, str] = {
    "anthropic": "claude-haiku-5-5",
    "openai": "gpt-4.1-nano",
    "gemini": "gemini-2.0-flash-lite",
}

_OPENROUTER_HOST = "openrouter.ai"

_NO_ENDPOINT = "Server reachable but could not verify key — no compatible API endpoint found"

# Common key prefixes (sk-, key-, anthropic-, ...) followed by 6+
# alphanumeric/dash chars: what a provider may echo back of the key it refused.
_KEY_FRAGMENT_RE = re.compile(
    r"(?:sk-|key-|anthropic-|ant-|ANTHROPIC_|OPENAI_|AIza|gsk_|or-)[A-Za-z0-9_-]{6,}", re.ASCII
)


class ProbeTimedOut(Exception):
    pass


def sanitize_error(msg: str) -> str:
    """Strip API key fragments from a provider's error message."""
    return _KEY_FRAGMENT_RE.sub("[REDACTED]", msg)


def probe_key(v: str) -> str:
    if not v:
        return v  # Allow empty for local providers (lm-studio, vllm, ollama)
    if len(v) > 4096:
        raise ValueError("API key must be under 4096 characters")
    if not v.isascii():
        raise ValueError("API key must contain only ASCII characters")
    return v


def probe_url(v: str | None) -> str | None:
    if v is not None:
        if not v.startswith(("http://", "https://")):
            raise ValueError("Base URL must start with http:// or https://")
        if len(v) > 512:
            raise ValueError("Base URL must be under 512 characters")
    return v


async def _listed(
    info: dict, api_key: str, base_url: str | None, timeout: float, transport, *, absent=(404, 405)
) -> dict | None:
    """The key's model list, counted, or None where the endpoint has none."""
    req = build_list_request(info, api_key=api_key, base_url=base_url)
    try:
        payload = await fetch_list(req, transport=transport, timeout=timeout)
    except ListFetchError as e:
        if e.status in absent:
            return None
        raise
    try:
        models = normalize(req.shape, payload)
    except ListFetchError:
        models = []  # A 200 that is not a list still took the key.
    return {"models_found": len(models), "model": models[0]["id"] if models else ""}


async def _send(method: str, url: str, timeout: float, transport, **kwargs) -> httpx.Response:
    """The answer with its body read to at most ``ERROR_BODY_BYTES``: a test
    reads only the status and a refusal's message, or OpenRouter's few fields."""
    # Pinned as the list fetch pins it: a redirect would carry the key to a
    # host the user did not name.
    async with httpx.AsyncClient(timeout=timeout, transport=transport, follow_redirects=False) as client:
        async with client.stream(method, url, **kwargs) as resp:
            body = await read_capped(resp, ERROR_BODY_BYTES)
    return httpx.Response(resp.status_code, content=body or b"", request=resp.request)


def _inferred(resp: httpx.Response, model: str) -> dict:
    if resp.status_code in (404, 405):
        raise ValueError(_NO_ENDPOINT)
    resp.raise_for_status()
    return {"models_found": None, "model": model}


def _base(info: dict, base_url: str | None, default: str) -> str:
    return (base_url or info.get("base_url") or default).rstrip("/")


async def _test_anthropic_key(
    api_key: str, model: str, info: dict, base_url: str | None, timeout: float, transport=None
) -> dict:
    listed = await _listed(info, api_key, base_url, timeout, transport, absent=(404, 405, 422))
    if listed is not None:
        return listed
    from src.llms.endpoints import anthropic_sdk_base_url

    base = anthropic_sdk_base_url(_base(info, base_url, "https://api.anthropic.com"))
    resp = await _send(
        "POST", f"{base}/v1/messages", timeout, transport,
        json={"model": model, "max_tokens": 1, "messages": [{"role": "user", "content": "hi"}]},
        headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
    )
    return _inferred(resp, model)


async def _test_openai_key(
    api_key: str, model: str, info: dict, base_url: str | None, timeout: float, transport=None
) -> dict:
    listed = await _listed(info, api_key, base_url, timeout, transport)
    if listed is not None:
        return listed
    base = _base(info, base_url, "https://api.openai.com/v1")
    resp = await _send(
        "POST", f"{base}/chat/completions", timeout, transport,
        json={"model": model, "max_tokens": 1, "messages": [{"role": "user", "content": "hi"}]},
        headers={"Authorization": f"Bearer {api_key}", "content-type": "application/json"},
    )
    return _inferred(resp, model)


async def _test_gemini_key(
    api_key: str, model: str, info: dict, base_url: str | None, timeout: float, transport=None
) -> dict:
    listed = await _listed(info, api_key, base_url, timeout, transport)
    if listed is not None:
        return listed
    base = _base(info, base_url, "https://generativelanguage.googleapis.com")
    resp = await _send(
        "POST", f"{base}/v1beta/models/{model}:generateContent", timeout, transport,
        json={"contents": [{"parts": [{"text": "hi"}]}], "generationConfig": {"maxOutputTokens": 1}},
        headers={"x-goog-api-key": api_key, "content-type": "application/json"},
    )
    return _inferred(resp, model)


def _usd(value) -> float | None:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def openrouter_key_credits(payload: dict) -> dict:
    """The spend on an OpenRouter key, from its ``GET /key`` answer.

    ``limit`` is the cap set on the key, None for a key without one, which is
    then bounded only by the account's balance (readable only with a
    management key, so not here). All amounts are USD.
    """
    data = payload.get("data") if isinstance(payload, dict) else None
    data = data if isinstance(data, dict) else {}
    limit = _usd(data.get("limit"))
    usage = _usd(data.get("usage"))
    remaining = _usd(data.get("limit_remaining"))
    if remaining is None and limit is not None and usage is not None:
        remaining = max(limit - usage, 0.0)
    free = data.get("is_free_tier")
    reset = data.get("limit_reset")
    return {
        "limit": limit,
        "usage": usage,
        "remaining": remaining,
        "is_free_tier": free if isinstance(free, bool) else None,
        "limit_reset": reset if isinstance(reset, str) else None,
    }


async def _test_openrouter_key(
    api_key: str, model: str, info: dict, base_url: str | None, timeout: float, transport=None
) -> dict:
    """Test an OpenRouter key against ``GET /key``.

    ``/models`` is public there and answers 200 to any key, so it proves
    nothing; ``/key`` answers 401 to a bad one, and to a good one with what the
    key may still spend. Only OpenRouter's own host is asked: a gateway set up
    with OpenRouter as its parent may answer that route with a 401 or a page of
    its own, which would read as the key failing, so it gets the
    OpenAI-compatible test, listed the OpenAI way.
    """
    base = _base(info, base_url, "https://openrouter.ai/api/v1")
    host = (httpx.URL(base).host or "").lower()
    if host != _OPENROUTER_HOST and not host.endswith(f".{_OPENROUTER_HOST}"):
        openai_info = {**info, "sdk": "openai", "parent_provider": None}
        return await _test_openai_key(api_key, model, openai_info, base_url, timeout, transport=transport)
    resp = await _send("GET", f"{base}/key", timeout, transport, headers={"Authorization": f"Bearer {api_key}"})
    resp.raise_for_status()
    return {"models_found": None, "model": "", "credits": openrouter_key_credits(resp.json())}


# SDK -> (test, model for its one-token call)
_SDK_TEST_DISPATCH: dict[str, tuple] = {
    "anthropic": (_test_anthropic_key, _TEST_MODELS["anthropic"]),
    "openai": (_test_openai_key, _TEST_MODELS["openai"]),
    "gemini": (_test_gemini_key, _TEST_MODELS["gemini"]),
    "openrouter": (_test_openrouter_key, _TEST_MODELS["openai"]),
}


async def check_key(
    provider: str, info: dict, api_key: str, base_url: str | None, *, transport: Any = None
) -> dict:
    """``POST /keys/test``'s answer for a key to ``provider``, described by ``info``.

    ``success`` with ``models_found``, ``model`` and ``latency_ms`` (and, for
    OpenRouter, ``credits``), or ``success: False`` with the provider's own
    ``error``. ``base_url`` is the one entered with the key, None for the
    manifest's. Raises ``ProbeTimedOut`` past the deadline, and
    ``EndpointRefused`` for an endpoint the egress policy refused at connect time.
    """
    from src.llms.llm import LLM
    from src.server.services.llm.egress import EndpointRefused

    # SDKs with no test of their own (deepseek, qwq, codex, ...) speak OpenAI's.
    test_fn, model = _SDK_TEST_DISPATCH.get(info.get("sdk") or "openai", _SDK_TEST_DISPATCH["openai"])
    # A model this provider serves: its own rows, else those a variant with
    # none of its own (a coding plan or its region) serves (``_add_brand_rows``).
    mc = LLM.get_model_config()
    manifest_models = mc.manifest.get("models", {})
    listed = manifest_models.get(provider) or manifest_models.get(mc.models_from(provider)) or []
    if listed:
        model = listed[0].get("id", model)

    start = time.monotonic()

    def failed(error: str) -> dict:
        # The prefixes ``sanitize_error`` knows guess at a key's shape; the key
        # tested is known, so it goes whatever shape an endpoint echoes it in.
        error = error.replace(api_key, "[REDACTED]") if api_key else error
        return {"success": False, "error": error, "latency_ms": int((time.monotonic() - start) * 1000)}

    def refused(message: str, status: int) -> dict:
        return failed(sanitize_error(message) if message else f"HTTP {status}")

    try:
        # One deadline for the whole test: a list that answers 404 is followed
        # by a second request, and each read is only bounded per operation.
        result = await asyncio.wait_for(
            test_fn(api_key, model, info, base_url, TEST_TIMEOUT_S, transport=transport),
            TEST_TIMEOUT_S,
        )
    except (httpx.TimeoutException, TimeoutError) as e:
        raise ProbeTimedOut from e
    except ListFetchError as e:
        if e.timed_out:
            raise ProbeTimedOut from e
        if e.reason == ENDPOINT_REFUSED and isinstance(e.__cause__, EndpointRefused):
            raise e.__cause__ from None
        if e.status is not None:
            return refused(e.detail, e.status)
        return failed(sanitize_error(str(e.__cause__ or e)))
    except httpx.HTTPStatusError as e:
        return refused(error_message(e.response.content), e.response.status_code)
    except EndpointRefused:
        raise
    except Exception as e:
        return failed(sanitize_error(str(e)))

    response = {
        "success": True,
        "models_found": result.get("models_found", 0),
        "model": result.get("model", ""),
        "latency_ms": int((time.monotonic() - start) * 1000),
    }
    if result.get("credits") is not None:
        response["credits"] = result["credits"]
    return response
