"""ChatOpenRouter held to the contracts the factory keeps for every other client.

``ChatOpenRouter`` talks to OpenRouter through the ``openrouter`` SDK rather
than the OpenAI one, so none of what the OpenAI-family clients give the agent
for free comes with it. This subclass adds the three the request path relies on:

- **A transport of the factory's choosing.** Upstream builds a fresh httpx
  client pair per model instance, and the factory builds an instance per turn
  and never closes it, so each turn would leave its connections open until a
  garbage collection found them. The factory hands in one shared transport
  instead, or the egress hold where a user's own endpoint has to be held to it.
- **A bound on a silent stream.** OpenRouter's keepalive comments reset the
  read timeout, so a stream that stops producing chunks waits out the whole
  call. The bound here raises a ``TimeoutError`` carrying ``chunks_received``,
  which is what ``error_classification.is_stalled_stream`` reads.
- **Typed stream failures.** Upstream raises a bare ``ValueError`` for an error
  event and yields a chunk with ``finish_reason="error"`` as if it were output.
  Both become :class:`OpenRouterStreamError`, with the status the event gave.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import re
from collections.abc import AsyncIterator, Iterator, Mapping
from typing import Any

import httpx
from langchain_core.outputs import ChatGenerationChunk
from langchain_openrouter import ChatOpenRouter
from pydantic import Field

logger = logging.getLogger(__name__)

# Upstream's wording for an error event with no choices (``_stream``/``_astream``).
_UPSTREAM_ERROR_PREFIX = "OpenRouter API returned an error during streaming"
_UPSTREAM_CODE_RE = re.compile(r"\(code: (\d{3})\)\s*$")


class OpenRouterStreamError(RuntimeError):
    """An OpenRouter stream reported failure after the request was accepted.

    ``status_code`` is the code the error event carried, read by
    ``error_classification`` ahead of any number in the message. ``mid_stream``
    is set only once output has reached the caller: before that the failure can
    still be the upstream refusing the payload, which the reasoning-strip retry
    exists to answer.
    """

    def __init__(
        self, message: str, *, status_code: int | None = None, mid_stream: bool = False
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.mid_stream = mid_stream


class OpenRouterStreamStalledError(TimeoutError):
    """No chunk arrived within ``stream_chunk_timeout``."""

    def __init__(self, timeout_s: float, *, model_name: str, chunks_received: int) -> None:
        super().__init__(
            f"OpenRouter stream sent no chunk for {timeout_s:g}s "
            f"(model={model_name}, chunks_received={chunks_received})"
        )
        self.timeout_s = timeout_s
        self.chunks_received = chunks_received


def _typed_error(exc: ValueError, received: int) -> OpenRouterStreamError | None:
    text = str(exc)
    if not text.startswith(_UPSTREAM_ERROR_PREFIX):
        return None
    match = _UPSTREAM_CODE_RE.search(text)
    return OpenRouterStreamError(
        text,
        status_code=int(match.group(1)) if match else None,
        mid_stream=received > 0,
    )


def _failed_chunk(chunk: ChatGenerationChunk, received: int) -> OpenRouterStreamError | None:
    metadata = chunk.message.response_metadata or {}
    if metadata.get("finish_reason") != "error":
        return None
    native = metadata.get("native_finish_reason")
    return OpenRouterStreamError(
        "OpenRouter stream ended with finish_reason=error"
        + (f" (native: {native})" if native else ""),
        mid_stream=received > 0,
    )


def _carries_output(chunk: ChatGenerationChunk) -> bool:
    """Whether the caller saw anything of this chunk. The adapter yields a
    role-only or usage-only delta as an empty chunk, which leaves the
    reasoning-strip retry as safe as before it."""
    message = chunk.message
    extra = message.additional_kwargs or {}
    return bool(
        message.content
        or getattr(message, "tool_call_chunks", None)
        or extra.get("reasoning_content")
        or extra.get("reasoning_details")
    )


@functools.cache
def _shared_transports():
    """One connection pool per worker for every OpenRouter client.

    ``ChatOpenRouter`` otherwise opens a client pair per instance, the factory
    builds one per turn and nothing closes it, so each turn's connections would
    stay open until a garbage collection found them.
    """
    limits = httpx.Limits(max_connections=1000, max_keepalive_connections=100)
    return httpx.HTTPTransport(limits=limits), httpx.AsyncHTTPTransport(limits=limits)


@functools.cache
def _app_url() -> str | None:
    """The app URL OpenRouter attributes usage to: this deployment's public
    web address, when it has one. A local or private address names nothing
    anyone can visit, so none is sent rather than upstream's LangChain default."""
    import ipaddress
    from urllib.parse import urlparse

    from src.config.env import PUBLIC_APP_URL

    parsed = urlparse(PUBLIC_APP_URL)
    host = parsed.hostname or ""
    if parsed.scheme != "https" or not host or host == "localhost" or host.endswith(".local"):
        return None
    try:
        if not ipaddress.ip_address(host).is_global:
            return None
    except ValueError:
        pass
    return PUBLIC_APP_URL


@functools.cache
def _field_names() -> frozenset[str]:
    from langchain_core.utils.utils import get_pydantic_field_names

    return frozenset(get_pydantic_field_names(ChatOpenRouterClient))


# Options ``chat.send`` takes that are not the request body but where and how
# it is sent. A model's ``parameters`` are its user's, and the egress policy
# reads only the client's endpoint, so ``server_url`` there would dial past it.
_SEND_OPTIONS = frozenset({"server_url", "http_headers", "retries", "timeout_ms"})


@functools.cache
def _request_keys() -> frozenset[str]:
    """Body keys the SDK's ``chat.send`` takes. It takes nothing else, so a key
    outside these in ``model_kwargs`` is a TypeError on every call."""
    from openrouter.chat import Chat

    return frozenset(inspect.signature(Chat.send).parameters) - {"self", "messages"} - _SEND_OPTIONS


class ChatOpenRouterClient(ChatOpenRouter):
    """``ChatOpenRouter`` on a supplied transport, with stream bounds and typed failures."""

    stream_chunk_timeout: float | None = None
    """Seconds a stream may go without a chunk; None for no bound."""

    http_transports: tuple[Any, Any] | None = Field(default=None, exclude=True)
    """(sync, async) httpx transports every request is sent through."""

    follow_redirects: bool = Field(default=True, exclude=True)

    @classmethod
    def factory_kwargs(cls, params: dict, extra_body: Mapping | None, model: str) -> dict:
        """Constructor kwargs from the factory's shared shape.

        Manifest entries and saved custom models were written for the OpenAI-family
        clients: an ``extra_body`` (beside ``parameters`` or inside it), a
        ``reasoning_effort`` dial, a timeout in seconds. ``ChatOpenRouter`` has none
        of those, and forwards any key it does not know to the SDK, which takes only
        the keys it names. So each one is moved to the field that sends the same
        thing, and what has no place on this wire is dropped with a warning rather
        than failing the turn.
        """
        params = {"app_title": "LangAlpha", "app_url": _app_url(), **params}
        fields = _field_names()
        request_keys = _request_keys()

        body = dict(params.pop("extra_body", None) or {})
        nested = dict(params.pop("model_kwargs", None) or {})
        body.update(nested.pop("extra_body", None) or {})
        body.update(extra_body or {})

        # Seconds everywhere else in the factory; milliseconds on this client.
        timeout = params.pop("timeout", None)
        if isinstance(timeout, (int, float)) and not isinstance(timeout, bool):
            params["timeout"] = int(timeout * 1000)

        for alias in ("openai_api_base", "api_base", "openrouter_api_base"):
            url = params.pop(alias, None)
            if url and not params.get("base_url"):
                params["base_url"] = url

        if "provider" in body:
            provider = body.pop("provider")
            if params.get("openrouter_provider") is None:
                params["openrouter_provider"] = provider

        # The effort, wherever the entry wrote it, goes to ``reasoning.effort``:
        # one dial on the wire, the one OpenRouter maps for every vendor.
        reasoning: dict = {}
        for value in (body.pop("reasoning", None), params.pop("reasoning", None)):
            if isinstance(value, Mapping):
                reasoning.update(value)
        for source in (body, nested, params):
            effort = source.pop("reasoning_effort", None)
            if effort:
                reasoning["effort"] = effort
        if reasoning:
            params["reasoning"] = reasoning

        # A body key is a request field. Promoting one that is not (an endpoint,
        # a key, a transport) would let the body choose the connection. A field
        # set explicitly keeps its value, as ``provider`` does above: the client
        # refuses a field given both ways.
        dropped = []
        for source in (body, nested):
            for key, value in source.items():
                if key not in request_keys:
                    dropped.append(key)
                elif key in fields:
                    params.setdefault(key, value)
                else:
                    params.setdefault("model_kwargs", {})[key] = value
        for key in list(params):
            if key in fields or key == "model_kwargs":
                continue
            value = params.pop(key)
            if key in request_keys:
                params.setdefault("model_kwargs", {})[key] = value
            else:
                dropped.append(key)
        if dropped:
            logger.warning(
                "Model %r: OpenRouter takes no %s; sending without",
                model,
                ", ".join(sorted(set(dropped))),
            )
        # Replaced by the egress hold where a client must go through it.
        params["http_transports"] = _shared_transports()
        return params

    def _build_client(self) -> Any:
        sdk = super()._build_client()
        if self.http_transports is None:
            return sdk
        config = sdk.sdk_configuration
        if not (hasattr(config, "client") and hasattr(config, "async_client")):
            raise TypeError(
                "openrouter SDK configuration has no client fields; "
                "the transport cannot be attached"
            )
        # The headers upstream put on its own pair (attribution, user headers),
        # carried over so only the transport changes.
        headers = config.client.headers
        sync_transport, async_transport = self.http_transports
        config.client = httpx.Client(
            headers=headers, transport=sync_transport, follow_redirects=self.follow_redirects
        )
        config.async_client = httpx.AsyncClient(
            headers=headers, transport=async_transport, follow_redirects=self.follow_redirects
        )
        # Ours to keep: the shared transport outlives this client.
        config.client_supplied = True
        config.async_client_supplied = True
        return sdk

    def _stream(self, messages, stop=None, run_manager=None, **kwargs) -> Iterator[ChatGenerationChunk]:
        received = 0
        try:
            for chunk in super()._stream(messages, stop=stop, run_manager=run_manager, **kwargs):
                if failed := _failed_chunk(chunk, received):
                    raise failed
                received += _carries_output(chunk)
                yield chunk
        except ValueError as exc:
            if typed := _typed_error(exc, received):
                raise typed from exc
            raise

    async def _astream(
        self, messages, stop=None, run_manager=None, **kwargs
    ) -> AsyncIterator[ChatGenerationChunk]:
        stream = super()._astream(messages, stop=stop, run_manager=run_manager, **kwargs)
        received = 0
        try:
            while True:
                try:
                    async with asyncio.timeout(self.stream_chunk_timeout) as deadline:
                        chunk = await anext(stream)
                except StopAsyncIteration:
                    return
                except TimeoutError:
                    if deadline.expired():
                        raise OpenRouterStreamStalledError(
                            self.stream_chunk_timeout or 0.0,
                            model_name=self.model_name,
                            chunks_received=received,
                        ) from None
                    raise
                except ValueError as exc:
                    if typed := _typed_error(exc, received):
                        raise typed from exc
                    raise
                if failed := _failed_chunk(chunk, received):
                    raise failed
                received += _carries_output(chunk)
                yield chunk
        finally:
            await stream.aclose()
