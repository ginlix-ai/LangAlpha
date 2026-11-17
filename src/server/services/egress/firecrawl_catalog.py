"""Firecrawl's data catalog, as the relay needs it: what a capability costs,
and what to call it and its provider when it becomes a source.

Read from Firecrawl's own ``find_tools`` (free) and kept in Redis for an hour
so every worker shares one copy. Its library contracts carry no price, so this
stays the one source of prices, though Firecrawl stopped listing the tool to
agents on 2026-11-16 (it still answers a call). If it ever stops answering,
a catalog call whose provider is not already cached is refused, never run
unpriced. A provider's whole listing is fetched at once:
the next call usually wants another of its capabilities. Providers are fetched
one per request and side by side, because ``find_tools`` refuses a whole batch
over one name it does not list (checked 2026-11-15).

The read runs inside admission, under the relay's wall clock, so it has a
budget of its own well inside that clock: a catalog that hangs refuses the
call as a failed credit check, which the caller retries, rather than eating
the clock.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import weakref
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlparse

import httpx

from src.server.services.egress.jsonrpc import tool_result

if TYPE_CHECKING:
    from src.server.utils.egress_guard import PinnedTarget

logger = logging.getLogger(__name__)

#: How long a provider's listing is kept. A call is held at the cached price
#: and charged at the price its reply reports, so a price Firecrawl raises
#: lets calls past admission underpriced for at most this long.
_CACHE_TTL_S = 3600
#: A name Firecrawl does not list is remembered this long, so a misspelt
#: provider costs one lookup per worker set, not one per call.
_MISS_TTL_S = 600
_CACHE_PREFIX = "firecrawl:catalog:v2:"
#: A provider whose cached listing lacks a capability a call names is read
#: again at most this often, so a new capability is not hidden for an hour.
_REFRESH_PREFIX = "firecrawl:catalog:refreshed:v2:"
_REFRESH_EVERY_MS = 5 * 60 * 1000
_PAGE = 100
#: The whole read, every provider and page. The relay's clock is 55s.
_BUDGET_S = 10.0
#: Providers one read may name: a call carries at most 10 runs.
MAX_PROVIDERS = 10
#: Provider fetches in flight on one worker, two requests each. The relay's
#: client is shared with every user's calls, so the catalog never holds more
#: than a few of its connections.
_FETCHES_AT_ONCE = 4
#: What a provider id looks like (every one of 581 listed on 2026-11-15:
#: lowercase letters, digits and hyphens, at most 50 characters). Anything
#: else is never looked up.
PROVIDER_ID = re.compile(r"[a-z0-9][a-z0-9-]{0,79}")
#: Options through which a per-record capability takes how many records to
#: return, compared without case, ``_`` or ``-``. The catalog names
#: ``limit``, ``size`` and ``page_size`` (2026-11-15); the rest are the
#: usual spellings of the same thing.
_RECORD_COUNT_OPTIONS = frozenset(
    {"limit", "size", "pagesize", "perpage", "maxresults", "numresults", "count"}
)
_NUMERIC = frozenset({"number", "integer"})


class CatalogUnavailable(Exception):
    """Firecrawl's catalog could not be read, so a catalog call cannot be priced."""


@dataclass(frozen=True)
class Capability:
    name: str
    credits_cost: float
    per_record: bool
    #: The option a per-record capability takes its record count from, and
    #: the count it returns when that option is left out (the option's
    #: default, else its maximum); both None when it declares no such option.
    records_option: str | None = None
    records_default: int | None = None


@dataclass(frozen=True)
class Provider:
    name: str
    #: The provider's site, for a source's icon; "" when the catalog names none.
    domain: str
    capabilities: dict[str, Capability] = field(default_factory=dict)

    def to_json(self) -> dict:
        return asdict(self)

    @classmethod
    def from_json(cls, data: dict) -> Provider:
        caps = {k: Capability(**v) for k, v in (data.get("capabilities") or {}).items()}
        return cls(name=data["name"], domain=data.get("domain") or "", capabilities=caps)


def capability(
    catalog: Mapping[str, Provider], provider: object, name: object
) -> Capability | None:
    """``provider``'s capability ``name``, or None if the catalog lists neither."""
    entry = catalog.get(provider) if isinstance(provider, str) else None
    return entry.capabilities.get(name) if entry and isinstance(name, str) else None


def _domain(item: dict) -> str:
    """The provider's site: its attribution's URL, else the domain its favicon
    link names, which is how the catalog points at most providers' sites."""
    url = (item.get("sourceAttribution") or {}).get("url")
    if url:
        return (urlparse(url).hostname or "").removeprefix("www.")
    logo = item.get("logoUrl") or ""
    domain = (parse_qs(urlparse(logo).query).get("domain") or [""])[0]
    return domain.removeprefix("www.")


def _count(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return math.ceil(value) if math.isfinite(value) and value > 0 else None


def _capability(item: Mapping[str, Any]) -> Capability | None:
    cost = item.get("creditsCost")
    if isinstance(cost, bool) or not isinstance(cost, (int, float)):
        return None
    per_record = bool(item.get("perRecord"))
    option = default = None
    if per_record:
        for entry in item.get("options") or []:
            if (
                isinstance(entry, Mapping)
                and isinstance(entry.get("name"), str)
                and re.sub(r"[-_]", "", entry["name"].lower()) in _RECORD_COUNT_OPTIONS
                and entry.get("type") in _NUMERIC
            ):
                option = entry["name"]
                default = _count(entry.get("default")) or _count(entry.get("max"))
                break
    return Capability(
        name=item.get("name") or item["capability"],
        credits_cost=float(cost),
        per_record=per_record,
        records_option=option,
        records_default=default,
    )


class _Unlisted(Exception):
    """Firecrawl answered that it does not list the provider asked about."""


async def _find_tools(
    client: httpx.AsyncClient,
    target: PinnedTarget,
    headers: Mapping[str, str],
    arguments: dict,
    *,
    provider: str | None = None,
) -> dict:
    """One ``find_tools`` read. Raises :class:`_Unlisted` only for the clean
    answer that ``provider`` is not in the catalog (a providers-level read
    rejecting exactly that name); every other refusal or odd shape is
    :class:`CatalogUnavailable`, which is never cached."""
    url, sent, extensions = target.pinned_kwargs(
        {**headers, "Content-Type": "application/json",
         "Accept": "application/json, text/event-stream"}
    )
    response = await client.post(
        url,
        headers=sent,
        extensions=extensions,
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "firecrawl_find_tools", "arguments": arguments},
        },
    )
    if response.status_code != 200:
        raise CatalogUnavailable(f"find_tools answered HTTP {response.status_code}")
    result = tool_result(response.content)
    try:
        run = result["data"]["alexandria"][0]  # type: ignore[index]
    except (KeyError, IndexError, TypeError) as e:
        raise CatalogUnavailable("find_tools answered in an unexpected shape") from e
    error = run.get("error") if isinstance(run, Mapping) else None
    if isinstance(error, Mapping):
        message = str(error.get("message") or "")
        if (
            provider is not None
            and error.get("code") == "invalid_option"
            and "providers" in message.lower()
            and f'Rejected: "{provider}"' in message
        ):
            raise _Unlisted(provider)
        raise CatalogUnavailable(f"find_tools refused: {error.get('code')}")
    data = run.get("data") if isinstance(run, Mapping) else None
    if not isinstance(data, dict):
        raise CatalogUnavailable("find_tools answered in an unexpected shape")
    return data


def _tools_page(name: str, offset: int) -> dict:
    return {
        "providers": [name],
        "level": "tools",
        "includeCosts": True,
        "expand": ["options"],
        "limit": _PAGE,
        "offset": offset,
    }


async def _fetch(
    client: httpx.AsyncClient, target: PinnedTarget, headers: Mapping[str, str], name: str
) -> Provider | None:
    """Provider ``name``'s listing, or None when Firecrawl answers cleanly
    that it does not list it. A degraded answer raises instead."""
    # Side by side, but only the providers-level answer says a name is not
    # listed: the tools page's refusal of the same name is not taken as one.
    listing, page = await asyncio.gather(
        _find_tools(
            client,
            target,
            headers,
            {"providers": [name], "level": "providers", "limit": _PAGE},
            provider=name,
        ),
        _find_tools(client, target, headers, _tools_page(name, 0)),
        return_exceptions=True,
    )
    if isinstance(listing, _Unlisted):
        return None
    for answer in (listing, page):
        if isinstance(answer, BaseException):
            raise answer
    item = next(
        (i for i in listing.get("items") or [] if (i.get("provider") or i.get("id")) == name),
        None,
    )
    items = page.get("items") or []
    if item is None or not items:
        raise CatalogUnavailable(f"find_tools listed {name} without its entry or its tools")
    provider = Provider(name=item.get("name") or name, domain=_domain(item))
    offset = 0
    while True:
        for tool in items:
            if tool.get("provider") == name and (cap := _capability(tool)) is not None:
                provider.capabilities[tool["capability"]] = cap
        if len(items) < _PAGE:
            return provider
        offset += _PAGE
        items = (await _find_tools(client, target, headers, _tools_page(name, offset))).get(
            "items"
        ) or []


# One gate per event loop, so a test's fresh loop never meets another's.
_gates: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Semaphore] = (
    weakref.WeakKeyDictionary()
)


def _gate() -> asyncio.Semaphore:
    loop = asyncio.get_running_loop()
    gate = _gates.get(loop)
    if gate is None:
        gate = _gates[loop] = asyncio.Semaphore(_FETCHES_AT_ONCE)
    return gate


async def _fetch_and_cache(name: str, url: str, headers: Mapping[str, str]) -> Provider | None:
    from src.server.services.egress.relay import get_relay_client
    from src.server.utils.egress_guard import EgressBlockedError, pin_public_url
    from src.utils.cache.redis_cache import get_cache_client

    try:
        async with asyncio.timeout(_BUDGET_S), _gate():
            target = await pin_public_url(url, require_https=True)
            provider = await _fetch(get_relay_client(), target, headers, name)
    except TimeoutError as e:
        raise CatalogUnavailable("find_tools did not answer in time") from e
    except (EgressBlockedError, httpx.HTTPError, ValueError) as e:
        raise CatalogUnavailable(f"find_tools failed: {type(e).__name__}") from e
    if provider is None:
        await get_cache_client().set(_CACHE_PREFIX + name, {"missing": True}, ttl=_MISS_TTL_S)
        logger.info("[firecrawl_catalog] %s is not in the catalog", name)
        return None
    await get_cache_client().set(_CACHE_PREFIX + name, provider.to_json(), ttl=_CACHE_TTL_S)
    logger.info(
        "[firecrawl_catalog] fetched %s",
        json.dumps({name: len(provider.capabilities)}),
    )
    return provider


# One fetch per provider in flight on this worker, which callers join. An
# optimization only: workers do not see each other's, and the Redis copy is
# what they share.
_inflight: dict[str, asyncio.Task[Provider | None]] = {}


def _fetch_once(name: str, url: str, headers: Mapping[str, str]) -> asyncio.Task:
    task = _inflight.get(name)
    if task is None or task.done():
        task = asyncio.ensure_future(_fetch_and_cache(name, url, headers))
        _inflight[name] = task

        def _done(t: asyncio.Task) -> None:
            if _inflight.get(name) is t:
                del _inflight[name]
            # Read here so a fetch every waiter gave up on is not reported
            # as an exception nobody retrieved.
            if not t.cancelled():
                t.exception()

        task.add_done_callback(_done)
    return task


async def providers(
    names: Iterable[str],
    *,
    url: str,
    headers: Mapping[str, str],
    refresh: Iterable[str] = (),
) -> dict[str, Provider]:
    """The catalog entries for ``names``, read with ``headers`` (the user's
    credential); a name Firecrawl does not list, or that is no provider
    id, is left out. ``refresh``
    names cached providers to read again, which each is at most every few
    minutes. Raises :class:`CatalogUnavailable` when a missing entry cannot
    be fetched within the budget."""
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    # A name that cannot be a provider id is never looked up, nor cached.
    wanted = sorted({n for n in names if isinstance(n, str) and PROVIDER_ID.fullmatch(n)})
    if not wanted:
        return {}
    if len(wanted) > MAX_PROVIDERS:
        raise ValueError(f"one catalog read names at most {MAX_PROVIDERS} providers")
    again = set()
    for name in set(refresh) & set(wanted):
        if await cache.acquire_lock(_REFRESH_PREFIX + name, "1", _REFRESH_EVERY_MS):
            again.add(name)
    found: dict[str, Provider] = {}
    missing: list[str] = []
    for name, cached in zip(wanted, await cache.mget([_CACHE_PREFIX + n for n in wanted])):
        if name in again or not isinstance(cached, dict):
            missing.append(name)
        elif not cached.get("missing"):
            found[name] = Provider.from_json(cached)
    if not missing:
        return found
    try:
        async with asyncio.timeout(_BUDGET_S):
            # Shielded: a fetch other callers joined outlives this one's
            # cancellation, and caches its answer for them.
            fetched = await asyncio.gather(
                *(asyncio.shield(_fetch_once(name, url, headers)) for name in missing)
            )
    except TimeoutError as e:
        raise CatalogUnavailable("find_tools did not answer in time") from e
    return {**found, **{n: p for n, p in zip(missing, fetched) if p is not None}}
