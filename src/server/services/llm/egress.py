"""Egress policy for LLM endpoints a user chooses.

In platform mode the server dials whatever base URL a user saves with a key or
writes into a custom model, so that URL must not reach the server's own
network. Two layers. A check where the URL is saved, and again where a client
is built, gives the user a refusal worded for them. The held transport then
validates every connection the client opens, where its socket connects, and
dials only the addresses that passed, which is what holds for a row saved
before the check existed and for a name that resolves somewhere else by the
time it is dialed.

OSS mode is exempt: a self-hoster's model server is on localhost by design. So
is any URL the manifest names for a provider (see ``is_operator_endpoint``).
"""

from __future__ import annotations

import asyncio
import errno
import functools
import importlib
import ipaddress
import os
import selectors
import socket
import threading
import time
from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from urllib.parse import urlsplit

import anthropic
import anyio
import httpcore
import httpcore2
import httpx
import httpx2
import openai
from fastapi import HTTPException

from src.llms.llm import CONNECTION_PARAM_KEYS, ENDPOINT_PARAM_KEYS, param_levels
from src.server.utils.egress_guard import (
    EgressBlockedError,
    EgressResolutionError,
    resolve_public_ips,
    resolve_public_ips_sync,
)

PRIVATE_ADDRESS = "it points to a private or reserved address. Use a public URL."
UNRESOLVED = "its host could not be resolved. Check the URL."
NOT_HTTP = "it is not a valid http or https URL."

class _LookupPool(ThreadPoolExecutor):
    """A pool that knows when a lookup would have to wait for a thread.

    A lookup cannot be cancelled, so one past its deadline keeps its thread
    until the resolver gives up, and names whose nameserver never answers can
    hold every thread long after their checks have returned.
    """

    def __init__(self, max_workers: int, thread_name_prefix: str):
        super().__init__(max_workers=max_workers, thread_name_prefix=thread_name_prefix)
        self._size = max_workers
        self._taken = 0
        self._taken_lock = threading.Lock()

    def submit(self, fn, /, *args, **kwargs):
        with self._taken_lock:
            self._taken += 1
        try:
            future = super().submit(fn, *args, **kwargs)
        except BaseException:
            self._release()
            raise
        future.add_done_callback(self._release)
        return future

    def _release(self, _future=None) -> None:
        with self._taken_lock:
            self._taken -= 1

    def full(self) -> bool:
        return self._taken >= self._size


# The checks below run inside a request: a key or model save, a key test, a
# client build. Their lookups get these threads, never the loop's default
# pool that every other blocking call shares, and a name not answered in time
# counts as one that does not resolve. One save checks at most so many URLs,
# since a client can send any list it likes; the app sends one or two.
_CHECK_RESOLVER = _LookupPool(max_workers=16, thread_name_prefix="egress-check")
# A held connection's lookup, under the connect timeout, gets threads of its
# own as well: a name that stalls holds one of these, never the default pool,
# and never a thread a save's check is waiting for. It cannot be skipped, so
# the pool is sized for names that stall, not for the lookups a worker makes.
_CONNECT_RESOLVER = ThreadPoolExecutor(max_workers=64, thread_name_prefix="egress-connect")
CHECK_TIMEOUT = 5.0
MAX_SAVE_CHECKS = 20
TOO_MANY_ENDPOINTS = (
    f"At most {MAX_SAVE_CHECKS} new base URLs can be saved at once. Save fewer at a time."
)

_SETTINGS_LINK = {"url": "/settings?tab=model", "label": "Open Settings"}


def enforced() -> bool:
    from src.config import settings

    return settings.HOST_MODE == "platform"


def is_operator_endpoint(url: str) -> bool:
    """Whether ``url`` is the manifest's base URL for a provider users bring keys to.

    Those are dialed as written, internal or not: the operator chose them.
    Local providers are left out because their default is the server's own
    host, and platform routes because no user key is ever meant for them, so a
    user naming either gets the same check as any other URL.
    """
    from src.llms.llm import LLM

    return any(
        info.get("base_url") == url
        and not info.get("platform")
        and info.get("access_type") != "local"
        for info in LLM.get_model_config().flat_providers.values()
    )


# Every reader below goes through ``param_levels``, since a key under
# ``model_kwargs`` reaches the constructor as one at the top does.


def parameter_urls(params: Mapping | None) -> list[str]:
    """The endpoints a model's ``parameters`` name, each of which replaces the
    base URL the client would otherwise get."""
    urls = []
    for _prefix, level in param_levels(params):
        for key in ENDPOINT_PARAM_KEYS:
            value = level.get(key)
            if isinstance(value, Mapping):
                value = value.get("api_endpoint")
            if isinstance(value, str) and value:
                urls.append(value)
    return urls


def _keys_in(params: Mapping | None, keys: frozenset[str]) -> list[str]:
    return sorted(prefix + k for prefix, level in param_levels(params) for k in level if k in keys)


def connection_params(params: Mapping | None) -> list[str]:
    """The keys in ``parameters`` that would route the client some other way
    (a proxy, a client of its own), which no user may set in platform mode."""
    return _keys_in(params, CONNECTION_PARAM_KEYS)


# Keys that move a Gemini client onto Vertex, which signs with the server's
# own Google credentials when the model names none, and where google-genai
# builds the host from ``location`` (``https://{location}-aiplatform...``), so
# a location can name any host. No user may set them in platform mode, on any
# provider: refusing the name is simpler than knowing which SDK reads it.
VERTEX_PARAM_KEYS = frozenset({"vertexai", "project", "location", "credentials"})


def vertex_params(params: Mapping | None) -> list[str]:
    """The keys in ``parameters`` that would put the client on Vertex."""
    return _keys_in(params, VERTEX_PARAM_KEYS)


def routing_params(params: Mapping | None) -> dict:
    """The part of ``parameters`` the policy reads, for telling whether a save
    changed it."""
    keys = {*ENDPOINT_PARAM_KEYS, *CONNECTION_PARAM_KEYS, *VERTEX_PARAM_KEYS}
    return {
        prefix + k: v
        for prefix, level in param_levels(params)
        for k, v in level.items()
        if k in keys
    }


def user_endpoints(base_url: str | None, params: Mapping | None = None) -> list[str]:
    """Every endpoint a client could dial that the operator did not choose."""
    return [
        url
        for url in (base_url, *parameter_urls(params))
        if url and not is_operator_endpoint(url)
    ]


async def endpoint_refusal(url: str, *, require_resolution: bool = True) -> str | None:
    """Why the server will not dial ``url``, worded for the user, or None.

    With ``require_resolution=False`` a host that does not resolve passes: as a
    client is being built that can be a resolver blip, and the transport then
    meets it as a connection error the call may retry. So does a name while
    every check thread is taken, rather than waiting out the deadline behind
    lookups that may never return: the transport resolves it as it connects.
    """
    if is_operator_endpoint(url):
        return None
    from src.config.env import HOST_IP

    # The factory fills this placeholder before dialing, so the check has to
    # see the address it becomes.
    url = url.replace("{HOST_IP}", HOST_IP)
    try:
        parts = urlsplit(url)
        host = parts.hostname
        port = parts.port
    except ValueError:
        return NOT_HTTP
    if parts.scheme not in ("http", "https") or not host:
        return NOT_HTTP
    if host.rstrip(".") == "localhost" or host.rstrip(".").endswith(".localhost"):
        return PRIVATE_ADDRESS
    if not require_resolution and _CHECK_RESOLVER.full() and not _is_address(host):
        return None
    try:
        await asyncio.wait_for(
            resolve_public_ips(
                host,
                port=port or (443 if parts.scheme == "https" else 80),
                executor=_CHECK_RESOLVER,
            ),
            CHECK_TIMEOUT,
        )
    except (EgressResolutionError, TimeoutError):
        return UNRESOLVED if require_resolution else None
    except EgressBlockedError:
        return PRIVATE_ADDRESS
    return None


def _is_address(host: str) -> bool:
    """Whether ``host`` is an IP address, which is checked with no lookup."""
    try:
        ipaddress.ip_address(host)
    except ValueError:
        return False
    return True


async def save_refusals(urls: Iterable[str | None]) -> dict[str, str | None]:
    """``endpoint_refusal`` for each distinct URL one save would store.

    The lookups run side by side, and past ``MAX_SAVE_CHECKS`` the save is
    refused before any of them starts.
    """
    distinct = list(dict.fromkeys(url for url in urls if url))
    if len(distinct) > MAX_SAVE_CHECKS:
        raise HTTPException(status_code=400, detail=TOO_MANY_ENDPOINTS)
    reasons = await asyncio.gather(*(endpoint_refusal(url) for url in distinct))
    return dict(zip(distinct, reasons))


def _model_refusal(message: str) -> HTTPException:
    return HTTPException(
        status_code=400,
        detail={"message": message, "type": "base_url_blocked", "link": _SETTINGS_LINK},
    )


async def refuse_model_endpoints(
    model_name: str, urls: Iterable[str], params: Mapping | None = None
) -> None:
    """Refuse a client before it is built, with the banner a model refusal gets.

    A 400 ``base_url_blocked``, so a turn that only inherited this model from
    its thread runs the account default instead (``thread_model``). Vertex keys
    in ``params`` are refused here too, since they choose the host as a URL does.
    """
    for key in vertex_params(params):
        raise _model_refusal(
            f"Model '{model_name}' cannot be used: parameters.{key} cannot be set. Remove it."
        )
    distinct = list(dict.fromkeys(urls))
    reasons = await asyncio.gather(
        *(endpoint_refusal(url, require_resolution=False) for url in distinct)
    )
    for reason in reasons:
        if reason:
            raise _model_refusal(f"The base URL for model '{model_name}' cannot be used: {reason}")


class EndpointRefused(Exception):
    """A request to a user's endpoint, refused before it was sent.

    ``status_code`` is what the model retry layer reads to give up on it,
    since every retry would be refused the same way.
    """

    status_code = 422


# The SDKs retry any foreign exception out of their transport and rewrap it as
# a connection error; their own base type is the one they pass through as is.
class _OpenAIEndpointRefused(EndpointRefused, openai.OpenAIError):
    pass


class _AnthropicEndpointRefused(EndpointRefused, anthropic.AnthropicError):
    pass


def _keepalive_options() -> list[tuple[int, int, int]]:
    # What the Anthropic SDK sets on its own transport, kept so a long call
    # that idles between chunks is not dropped by a NAT on the way.
    options = [(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)]
    for name, value in (("TCP_KEEPINTVL", 60), ("TCP_KEEPCNT", 5), ("TCP_KEEPIDLE", 60)):
        if hasattr(socket, name):
            options.append((socket.IPPROTO_TCP, getattr(socket, name), value))
    return options


# Attempts on a name's addresses start this far apart (RFC 8305, and what
# anyio's own connect_tcp does), so an address that drops packets delays a
# connection by a fraction of a second, not by the connect timeout.
_STAGGER = 0.25

# A resolver can answer with hundreds of addresses, and each attempt holds a
# socket until it settles, so only the first few are dialed.
_MAX_ADDRESSES = 8

# The SDKs' own defaults, for one pool every held client in the process shares.
_LIMITS = {"max_connections": 1000, "max_keepalive_connections": 100, "keepalive_expiry": 5.0}


def _interleaved(ips: list[str]) -> list[str]:
    """The resolver's order with the address families alternating, from the
    family it put first."""
    v6 = [ip for ip in ips if ":" in ip]
    v4 = [ip for ip in ips if ":" not in ip]
    first, second = (v6, v4) if ips and ":" in ips[0] else (v4, v6)
    order = [ip for pair in zip(first, second) for ip in pair]
    order += first[len(second):] + second[len(first):]
    return order[:_MAX_ADDRESSES]


def _all_failed(hc: Any, host: str, errors: list[Exception]) -> Exception:
    if len(errors) == 1:
        return errors[0]
    return hc.ConnectError(f"could not connect to {host!r}: " + "; ".join(map(str, errors)))


async def _race(hc: Any, host: str, ips: list[str], connect) -> Any:
    """Happy eyeballs over ``ips``: ``connect(ip)`` for each in turn, the next
    started a stagger after the last or as soon as it fails. The first stream
    wins; attempts still in flight are cancelled, and one that connects anyway
    is closed."""
    winner = None
    errors: list[Exception] = []

    async def attempt(ip: str, settled: anyio.Event) -> None:
        nonlocal winner
        try:
            stream = await connect(ip)
        except (hc.ConnectError, hc.ConnectTimeout) as exc:
            errors.append(exc)
        else:
            if winner is None:
                winner = stream
                group.cancel_scope.cancel()
            else:
                with anyio.CancelScope(shield=True):
                    await stream.aclose()
        finally:
            settled.set()

    try:
        async with anyio.create_task_group() as group:
            for ip in ips:
                settled = anyio.Event()
                group.start_soon(attempt, ip, settled)
                with anyio.move_on_after(_STAGGER):
                    await settled.wait()
    except BaseException:
        # The deadline, or a cancelled caller, can land after an attempt won.
        if winner is not None:
            with anyio.CancelScope(shield=True):
                await winner.aclose()
        raise
    if winner is None:
        raise _all_failed(hc, host, errors)
    return winner


def _dial(ip: str, port: int, local_address: str | None, socket_options) -> socket.socket:
    """A non-blocking connect to ``ip``, started and not waited on."""
    sock = socket.socket(socket.AF_INET6 if ":" in ip else socket.AF_INET, socket.SOCK_STREAM)
    try:
        for option in socket_options or ():
            sock.setsockopt(*option)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if local_address:
            sock.bind((local_address, 0))
        sock.setblocking(False)
        error = sock.connect_ex((ip, port))
        if error not in (0, errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EAGAIN):
            raise OSError(error, os.strerror(error))
    except BaseException:
        sock.close()
        raise
    return sock


def _race_sync(hc, host, port, ips, timeout, local_address, socket_options) -> socket.socket:
    """:func:`_race` for a blocking caller. The connects run side by side
    under one selector, so cancelling a loser is closing its socket."""
    deadline = None if timeout is None else time.monotonic() + timeout
    waiting = list(ips)
    selector = selectors.DefaultSelector()
    errors: list[Exception] = []
    next_start = 0.0
    try:
        while waiting or selector.get_map():
            now = time.monotonic()
            if deadline is not None and now >= deadline:
                raise hc.ConnectTimeout(f"timed out connecting to {host!r}")
            if waiting and (now >= next_start or not selector.get_map()):
                try:
                    sock = _dial(waiting.pop(0), port, local_address, socket_options)
                except OSError as exc:
                    errors.append(hc.ConnectError(str(exc)))
                    continue
                selector.register(sock, selectors.EVENT_WRITE)
                next_start = now + _STAGGER
                continue
            wakes = [t for t in (next_start if waiting else None, deadline) if t is not None]
            for key, _events in selector.select(max(min(wakes) - now, 0) if wakes else None):
                sock = key.fileobj
                selector.unregister(sock)
                error = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                if not error:
                    sock.setblocking(True)
                    return sock
                sock.close()
                errors.append(hc.ConnectError(os.strerror(error)))
                next_start = 0.0
        raise _all_failed(hc, host, errors)
    finally:
        for key in list(selector.get_map().values()):
            key.fileobj.close()
        selector.close()


def _held_transport_class(hx: Any, hc: Any) -> type:
    """The held transport for one httpx line and the httpcore under it (the
    OpenAI SDK is on httpx2 and httpcore2, the others on httpx and httpcore).
    One object serves a sync and an async client, as each SDK hands the same
    one to both."""
    # httpcore exports no way to wrap a socket it did not open, and the sync
    # race opens its own; this is the stream its own sync backend returns.
    SyncStream = importlib.import_module(f"{hc.__name__}._backends.sync").SyncStream

    def refusal(refused: type[EndpointRefused], host: str, exc: EgressBlockedError) -> Exception:
        if isinstance(exc, EgressResolutionError):
            return hc.ConnectError(f"could not resolve {host!r}")
        return refused(f"The base URL cannot be used: {PRIVATE_ADDRESS}")

    class AsyncBackend(hc.AsyncNetworkBackend):
        """Opens every connection of the pool on an address validated for it.

        httpcore asks its backend for a socket each time its pool opens a
        connection, a redirect's included, so this is where the check holds:
        the name is resolved, any non-global answer refused, and only the
        validated addresses dialed. The request keeps the name, which TLS SNI
        and the certificate check then use as they would anyway. A refusal is
        raised as is, since httpx maps only httpcore's own errors, and a name
        that does not resolve is a connection error, as it is without this.
        """

        def __init__(self, refused: type[EndpointRefused]) -> None:
            self._refused = refused
            self._inner = hc.AnyIOBackend()

        async def connect_tcp(self, host, port, timeout=None, local_address=None, socket_options=None):
            def connect(ip):
                return self._inner.connect_tcp(
                    ip, port, local_address=local_address, socket_options=socket_options
                )

            try:
                # One deadline for the lookup and the whole race, as httpcore
                # gives anyio's connect_tcp.
                with anyio.fail_after(timeout):
                    try:
                        ips = await resolve_public_ips(host, port=port, executor=_CONNECT_RESOLVER)
                    except EgressBlockedError as exc:
                        raise refusal(self._refused, host, exc) from exc
                    return await _race(hc, host, _interleaved(ips), connect)
            except TimeoutError as exc:
                raise hc.ConnectTimeout(f"timed out connecting to {host!r}") from exc

        async def sleep(self, seconds: float) -> None:
            await self._inner.sleep(seconds)

    class SyncBackend(hc.NetworkBackend):
        """:class:`AsyncBackend` for a blocking client."""

        def __init__(self, refused: type[EndpointRefused]) -> None:
            self._refused = refused

        def connect_tcp(self, host, port, timeout=None, local_address=None, socket_options=None):
            # One deadline for the lookup and the race, as on the async side,
            # though a blocking lookup runs to its end, as in httpcore's own.
            started = time.monotonic()
            try:
                ips = resolve_public_ips_sync(host, port=port)
            except EgressBlockedError as exc:
                raise refusal(self._refused, host, exc) from exc
            if timeout is not None:
                timeout -= time.monotonic() - started
            sock = _race_sync(hc, host, port, _interleaved(ips), timeout, local_address, socket_options)
            return SyncStream(sock)

        def sleep(self, seconds: float) -> None:
            time.sleep(seconds)

    def attached(transport, pool_cls, backend, ssl_context):
        # httpx takes no network backend (no parameter on its transports, as
        # of httpx 0.28 and httpx2 2.9), but httpcore's pool takes one in its
        # constructor. So the pool the transport sends through, ``_pool``, is
        # replaced with one built from the same settings plus the backend.
        # Checked first, so a release that renames or reshapes it is refused
        # here rather than left sending through a pool of its own.
        if type(getattr(transport, "_pool", None)) is not pool_cls:
            raise TypeError(f"{type(transport).__name__} has no {pool_cls.__name__} to hold")
        transport._pool = pool_cls(
            ssl_context=ssl_context,
            max_connections=_LIMITS["max_connections"],
            max_keepalive_connections=_LIMITS["max_keepalive_connections"],
            keepalive_expiry=_LIMITS["keepalive_expiry"],
            socket_options=_keepalive_options(),
            network_backend=backend,
        )
        return transport

    class HeldTransport(hx.BaseTransport, hx.AsyncBaseTransport):
        """Sends each request through a pool whose every connection is opened
        on a validated address (see :class:`AsyncBackend`).

        One ordinary pool per side, keyed by origin as httpcore keys it, so a
        connection is only reused for the name it was opened for, and the
        pool's own limits and keepalive expiry bound what any number of names
        can leave open.
        """

        def __init__(self, refused: type[EndpointRefused], *, shared: bool = False) -> None:
            self._shared = shared
            ssl_context = hx.create_ssl_context()
            self._sync = attached(
                hx.HTTPTransport(verify=ssl_context), hc.ConnectionPool, SyncBackend(refused), ssl_context
            )
            self._async = attached(
                hx.AsyncHTTPTransport(verify=ssl_context),
                hc.AsyncConnectionPool,
                AsyncBackend(refused),
                ssl_context,
            )

        def handle_request(self, request):
            return self._sync.handle_request(request)

        async def handle_async_request(self, request):
            return await self._async.handle_async_request(request)

        def close(self) -> None:
            # A shared transport outlives any one client: another may be
            # mid-request on its pool.
            if not self._shared:
                self._sync.close()

        async def aclose(self) -> None:
            if not self._shared:
                await self._async.aclose()

    return HeldTransport


_HeldHttpx = _held_transport_class(httpx, httpcore)
_HeldHttpx2 = _held_transport_class(httpx2, httpcore2)


@functools.cache
def sdk_transport(family: str):
    """The held transport for one SDK family, as the LLM factory's
    ``egress_transport`` asks for it.

    One per family per worker, shared by every client held to it, as the SDKs
    share their own default: clients are built per turn and never closed, so a
    transport each would leave a connection open behind every turn and reuse
    none.
    """
    if family == "openai":
        return _HeldHttpx2(_OpenAIEndpointRefused, shared=True)
    if family == "anthropic":
        return _HeldHttpx(_AnthropicEndpointRefused, shared=True)
    if family in ("gemini", "openrouter"):
        return _HeldHttpx(EndpointRefused, shared=True)
    raise ValueError(f"No held transport for SDK family {family!r}")


def http_transport():
    """The held transport for a plain ``httpx`` client, its own to close."""
    return _HeldHttpx(EndpointRefused)


async def user_endpoint_transport(base_url: str | None):
    """The transport a plain request to a base URL the user supplied goes out on.

    None where the policy does not apply: outside platform mode, no URL, or an
    operator's endpoint. Otherwise a URL the policy refuses raises
    ``EndpointRefused`` now, and the rest go out pinned, so a name that
    resolves elsewhere by the time it is dialed cannot carry the request inside.
    """
    if not enforced() or not base_url or is_operator_endpoint(base_url):
        return None
    reason = await endpoint_refusal(base_url)
    if reason:
        raise EndpointRefused(f"Base URL cannot be used: {reason}")
    return http_transport()


__all__ = [
    "EndpointRefused",
    "connection_params",
    "endpoint_refusal",
    "enforced",
    "http_transport",
    "is_operator_endpoint",
    "parameter_urls",
    "refuse_model_endpoints",
    "routing_params",
    "sdk_transport",
    "user_endpoint_transport",
    "user_endpoints",
    "vertex_params",
]
