"""Firecrawl's hosted MCP tools as the relay meters them: what a call should
cost before it runs, what it cost from its reply, and the sources it carries.

Each kind of call is one class holding all three, so a tool's pricing and its
reply's shape are read side by side. Replies were probed on 2026-10-27:
catalog calls (``alexandria_library_read``) report ``data.creditsCost``; a page scrape
``metadata.creditsUsed``; search and crawl a top-level ``creditsUsed``, a
crawl's being the job's total so far; map reports nothing and costs one credit
a call.

A source is a page (a scrape, or each page of a crawl) as ``web_fetch``, each
search result as ``web_search``, so they join the groups WebFetch and
WebSearch fill, and a catalog run as ``mcp_tool``, named by its capability and
provider. Each carries what it cost in Firecrawl credits; a call that returned
several splits its cost among them, evenly, except catalog runs, which split
it by their prices. Bodies are the text the agent read, whole.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import replace
from typing import Any
from urllib.parse import urlencode, urlparse

from ptc_agent.agent.provenance.types import redact_args
from src.server.services.egress import RelayError, RelayRejection, firecrawl_catalog
from src.server.services.egress.firecrawl_catalog import Capability, Provider
from src.server.services.egress.metering import Charge, MeteredSource

_PROVIDER = "firecrawl"
# Per-page prices Firecrawl publishes (docs.firecrawl.dev/billing, read
# 2026-11-16): 1 a page, 4 more for each model-run format and for a prompt
# injection check, and sites it prices on their own: X at 30 (1 and 29 for
# its query), LinkedIn at what the provider serving it charges, at most 40.
# Every proxy tier costs a basic page's 1 (billed so on 2026-11-16: basic,
# auto, enhanced, stealth). ``summary`` is not listed; it runs a model, so it
# is priced like the formats that do.
_PLUS_FOUR_FORMATS = frozenset(
    {"json", "query", "question", "audio", "video", "highlights", "summary"}
)
_SITE_PRICES = {"x.com": 30.0, "twitter.com": 30.0, "linkedin.com": 40.0}
#: Every branch a search can answer with, each a list of hits with a URL.
_SEARCH_BRANCHES = ("web", "news", "images")
_SEARCH_DEFAULT_LIMIT = 10
#: Every paid call is held at least this much, so one that is charged at its
#: estimate (cut short, unanswered) is never free.
MIN_ESTIMATE = 1.0
#: A crawl answers only once it finishes, so a larger one outlasts the relay's
#: clock and the crawl id with it.
CRAWL_PAGE_CAP = 100
#: Runs one catalog call may carry, as Firecrawl takes them.
_MAX_RUNS = 10
#: The free tool that finds a catalog capability, named in refusals.
_DISCOVERY = "alexandria_library_search"
# Scalar inputs shown in a catalog source's identifier, so two runs of one
# capability (two tickers) stay two sources rather than one deck.
_IDENT_OPTIONS_MAX = 6


def _number(value: Any) -> float | None:
    """``value`` as a finite number, else None. JSON reads ``1e999`` as
    infinity, Python's parser takes ``NaN``, and an integer too long for a
    float would raise."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        number = float(value)
    except OverflowError:
        return None
    return number if math.isfinite(number) else None


def _count(value: Any) -> int | None:
    """``value`` as a count of at least one, else None."""
    number = _number(value)
    return math.ceil(number) if number is not None and number > 0 else None


def _shown(value: Any) -> str:
    """A name the caller sent, as a refusal repeats it: short."""
    text = str(value)
    return text if len(text) <= 80 else text[:77] + "..."


def _charge(value: Any) -> Charge | None:
    """A reported cost, else None. A negative one is unreadable, which settles
    at the estimate: taken as is, it would settle the call free."""
    credits = _number(value)
    return None if credits is None or credits < 0 else Charge(credits)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _canonical(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True, default=str, ensure_ascii=False)


def _page_price(options: Mapping[str, Any], url: Any = None) -> float:
    listed = options.get("formats")
    formats = {
        f if isinstance(f, str) else str(_mapping(f).get("type"))
        for f in (listed if isinstance(listed, list) else [])
    }
    model_runs = 4.0 * len(formats & _PLUS_FOUR_FORMATS)
    host = _host(url)
    for site, price in _SITE_PRICES.items():
        if host == site or host.endswith("." + site):
            return price + model_runs
    pages = _pdf_pages(options)
    redacted = bool(options.get("redactPII"))
    return (
        # A page, or a credit a PDF page in its place: a PDF capped at 3
        # pages billed 3 (checked live 2026-11-16).
        max(1.0, pages)
        + model_runs
        + (4.0 if redacted else 0.0)
        + (4.0 if options.get("lockdown") else 0.0)
        + (4.0 if options.get("checkPromptInjection") else 0.0)
        + (1.0 if options.get("zeroDataRetention") else 0.0)
        # With redaction, 4 more for each PDF page past the first.
        + (4.0 * max(pages - 1.0, 0.0) if redacted else 0.0)
    )


def _pdf_pages(options: Mapping[str, Any]) -> float:
    """The PDF pages a scrape caps its parsing at (``parsers`` with a pdf
    ``maxPages``). Uncapped, a PDF's pages are unknown until its reply,
    which reports what they cost: such a call is held as one page and
    charged in full, so a user near the end of their credits can overshoot
    by that one call, after which paid calls are refused."""
    parsers = options.get("parsers")
    caps = [
        _count(parser.get("maxPages"))
        for parser in (parsers if isinstance(parsers, list) else [])
        if isinstance(parser, Mapping) and parser.get("type") == "pdf"
    ]
    return float(max((c for c in caps if c is not None), default=0))


def _search_sources(listed: Any) -> int:
    """How many of the result kinds a search can return it asks for, each a
    name or ``{"type": name}``; web alone when it names none."""
    kinds = {
        kind if isinstance(kind, str) else _mapping(kind).get("type")
        for kind in (listed if isinstance(listed, list) else [])
    }
    return len(kinds & set(_SEARCH_BRANCHES)) or 1


def _host(url: Any) -> str:
    """``url``'s host, or "" for one that does not parse: the vendor refuses
    such a URL itself, free, so pricing it as an ordinary page is enough."""
    if not isinstance(url, str):
        return ""
    try:
        return (urlparse(url).hostname or "").removeprefix("www.")
    except ValueError:
        return ""


def _source(
    source_type: str,
    identifier: str,
    body: str,
    *,
    title: Any,
    args: Mapping[str, Any],
    cost: float | None,
    site: str | None = None,
) -> MeteredSource:
    return MeteredSource(
        source_type=source_type,
        identifier=identifier,
        body=body,
        title=title if isinstance(title, str) else None,
        provider=_PROVIDER,
        site=site,
        args=redact_args(args),
        vendor_credits=cost,
    )


def _page_source(
    page: Mapping[str, Any], args: Mapping[str, Any], cost: float | None, url: Any = None
) -> list[MeteredSource]:
    metadata = _mapping(page.get("metadata"))
    url = metadata.get("sourceURL") or metadata.get("url") or page.get("url") or url
    if not isinstance(url, str) or not url:
        return []
    body = page.get("markdown")
    if not isinstance(body, str) or not body:
        body = _canonical({k: v for k, v in page.items() if k != "metadata"})
    return [_source("web_fetch", url, body, title=metadata.get("title"), args=args, cost=cost)]


class _Call:
    """One kind of Firecrawl call. The defaults are a call that costs nothing
    up front, reports no cost and returns no sources."""

    def refusal(self, arguments: Mapping[str, Any]) -> RelayRejection | None:
        return None

    def providers(self, arguments: Mapping[str, Any]) -> set[str]:
        """The catalog providers ``estimate`` and ``sources`` need."""
        return set()

    def unlisted(self, arguments: Mapping[str, Any], catalog: Mapping[str, Provider]) -> set[str]:
        """Providers whose cached listing may be too old for this call."""
        return set()

    def estimate(self, arguments: Mapping[str, Any], catalog: Mapping[str, Provider]) -> float:
        return 0.0

    def charge(self, arguments: Mapping[str, Any], result: Mapping[str, Any]) -> Charge | None:
        return None

    def job_ref(self, arguments: Mapping[str, Any], result: Mapping[str, Any]) -> str | None:
        """The job a reply reports on, whose polls repeat its pages and total."""
        return None

    def sources(
        self,
        arguments: Mapping[str, Any],
        result: Mapping[str, Any],
        catalog: Mapping[str, Provider],
        call_cost: float,
    ) -> list[MeteredSource]:
        return []


class _Catalog(_Call):
    """``alexandria_library_read``: a list of 1 to 10 runs in ``calls``, each
    priced by the catalog, some per record returned."""

    @staticmethod
    def _runs(arguments: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        runs = arguments.get("calls")
        return [r for r in runs if isinstance(r, Mapping)] if isinstance(runs, list) else []

    def refusal(self, arguments):
        # Before any catalog lookup: each run names a provider fetched on its
        # own, so a long or junk list would fan out over the relay's shared
        # connections. Firecrawl takes at most 10 runs a call. A frame whose
        # runs the meter cannot all read is refused rather than held at the
        # minimum, since the vendor would run what it can read of it.
        runs = arguments.get("calls")
        if (
            not isinstance(runs, list)
            or not 0 < len(runs) <= _MAX_RUNS
            or len(self._runs(arguments)) != len(runs)
            or "alexandria" in arguments
        ):
            return RelayRejection(
                400,
                RelayError.BAD_REQUEST,
                f"alexandria_library_read takes calls: a list of 1 to {_MAX_RUNS} "
                "{provider, capability, options} runs",
            )
        for run in runs:
            if run.get("version") is not None:
                # The catalog prices a capability's current version only.
                return RelayRejection(
                    400,
                    RelayError.BAD_REQUEST,
                    "a catalog run's version cannot be priced; omit it to run the latest",
                )
            provider = run.get("provider")
            if not isinstance(provider, str) or not firecrawl_catalog.PROVIDER_ID.fullmatch(
                provider
            ):
                return RelayRejection(
                    400,
                    RelayError.BAD_REQUEST,
                    "a catalog run's provider is not a catalog id; "
                    f"find it with {_DISCOVERY}",
                )
        return None

    @staticmethod
    def _records_bound(options: Mapping[str, Any], capability: Capability) -> int | None:
        """How many records a per-record run can return: the count it asks
        for, else the count the catalog says it returns by default, and at
        least its longest list of inputs (a bulk lookup). A run with neither
        a count nor a list returns one record, unless the capability takes a
        count whose default the catalog does not state: None, unbounded."""
        given = _count(options.get(capability.records_option or "limit"))
        lists = max((len(v) for v in options.values() if isinstance(v, list)), default=0)
        if given is None and capability.records_option is not None:
            given = capability.records_default
            if given is None and not lists:
                return None
        return max(given or 1, lists)

    def providers(self, arguments):
        return {r["provider"] for r in self._runs(arguments) if isinstance(r.get("provider"), str)}

    def unlisted(self, arguments, catalog):
        """Providers the catalog lists without a capability a run names."""
        return {
            r["provider"]
            for r in self._runs(arguments)
            if r.get("provider") in catalog
            and firecrawl_catalog.capability(catalog, r["provider"], r.get("capability")) is None
        }

    def estimate(self, arguments, catalog):
        # A run the meter cannot price is refused rather than held at a
        # guess: the vendor bills it at whatever it costs.
        total = 0.0
        for run in self._runs(arguments):
            provider, name = run.get("provider"), run.get("capability")
            capability = firecrawl_catalog.capability(catalog, provider, name)
            if capability is None:
                raise RelayRejection(
                    400,
                    RelayError.BAD_REQUEST,
                    f"{_shown(provider)}/{_shown(name)} is not in Firecrawl's catalog; "
                    f"find its exact provider and capability with {_DISCOVERY}",
                )
            options = _mapping(run.get("options"))
            option = capability.records_option or "limit"
            if (
                capability.per_record
                and options.get(option) is not None
                and _count(options[option]) is None
            ):
                raise RelayRejection(
                    400,
                    RelayError.BAD_REQUEST,
                    f"{_shown(provider)}/{_shown(name)} needs options.{option} "
                    "to be a positive number",
                )
            count = self._records_bound(options, capability) if capability.per_record else 1
            if count is None:
                raise RelayRejection(
                    400,
                    RelayError.BAD_REQUEST,
                    f"{_shown(provider)}/{_shown(name)} is priced per record returned; "
                    f"set options.{capability.records_option} to bound it",
                )
            total += max(0.0, capability.credits_cost) * count
        return total

    def charge(self, arguments, result):
        return _charge(_mapping(result.get("data")).get("creditsCost"))

    def sources(self, arguments, result, catalog, call_cost):
        runs = self._runs(arguments)
        out: list[MeteredSource] = []
        for index, item in enumerate(_mapping(result.get("data")).get("alexandria") or []):
            if not isinstance(item, Mapping) or item.get("error"):
                continue
            provider_key, capability_key = item.get("provider"), item.get("capability")
            if not isinstance(provider_key, str) or not isinstance(capability_key, str):
                continue
            options = _mapping(runs[index].get("options")) if index < len(runs) else {}
            provider = catalog.get(provider_key)
            capability = firecrawl_catalog.capability(catalog, provider_key, capability_key)
            cost = None
            if capability is not None:
                records = item.get("records") if isinstance(item.get("records"), int) else 1
                cost = capability.credits_cost * (records if capability.per_record else 1)
            # Redacted as ``args`` is: the identifier is stored and shown too.
            scalars = sorted(
                (k, v)
                for k, v in (redact_args(dict(options)) or {}).items()
                if isinstance(v, (str, int, float)) and not isinstance(v, bool)
            )[:_IDENT_OPTIONS_MAX]
            identifier = f"{provider_key}/{capability_key}"
            body = item.get("data") if item.get("data") is not None else item.get("records")
            out.append(
                _source(
                    "mcp_tool",
                    f"{identifier}?{urlencode(scalars)}" if scalars else identifier,
                    _canonical(body),
                    title=(
                        f"{capability.name} · {provider.name}"
                        if provider and capability
                        else f"{capability_key} · {provider_key}"
                    ),
                    args=options,
                    cost=cost,
                    site=(provider.domain or None) if provider else None,
                )
            )
        # Split the call's rounded charge by the runs' prices, so the
        # sources add up to what was billed even when a price is fractional;
        # evenly when none is priced above zero.
        total = sum(s.vendor_credits or 0.0 for s in out)
        if total <= 0:
            return [replace(s, vendor_credits=call_cost / len(out)) for s in out]
        return [
            s if s.vendor_credits is None
            else replace(s, vendor_credits=call_cost * s.vendor_credits / total)
            for s in out
        ]


class _Page(_Call):
    """``firecrawl_scrape`` of a URL."""

    def estimate(self, arguments, catalog):
        return _page_price(arguments, arguments.get("url"))

    def charge(self, arguments, result):
        return _charge(_mapping(result.get("metadata")).get("creditsUsed"))

    def sources(self, arguments, result, catalog, call_cost):
        return _page_source(result, arguments, call_cost, arguments.get("url"))


class _Search(_Call):
    def refusal(self, arguments):
        if arguments.get("limit") is not None and _count(arguments["limit"]) is None:
            return RelayRejection(
                400, RelayError.BAD_REQUEST, "firecrawl_search needs limit to be a positive number"
            )
        return None

    def estimate(self, arguments, catalog):
        # ``limit`` applies to each source asked for, and the results are
        # billed together: 2 credits a 10, or 10 with end-to-end ZDR, which
        # also makes each scraped result a ZDR scrape.
        limit = _count(arguments.get("limit")) or _SEARCH_DEFAULT_LIMIT
        results = limit * _search_sources(arguments.get("sources"))
        enterprise = arguments.get("enterprise")
        zdr = isinstance(enterprise, list) and "zdr" in enterprise
        cost = (10.0 if zdr else 2.0) * -(-results // 10)
        scrape = arguments.get("scrapeOptions")
        if isinstance(scrape, Mapping):
            cost += results * _page_price({**scrape, "zeroDataRetention": True} if zdr else scrape)
        return cost

    def charge(self, arguments, result):
        return _charge(result.get("creditsUsed"))

    def sources(self, arguments, result, catalog, call_cost):
        data = _mapping(result.get("data")) or result
        # Every branch: a paid call's relayed sources stand in for its whole
        # trace entry, so a branch left out here would be lost.
        hits = [
            h
            for branch in _SEARCH_BRANCHES
            for h in (data.get(branch) if isinstance(data.get(branch), list) else [])
            if isinstance(h, Mapping) and h.get("url")
        ]
        share = call_cost / len(hits) if hits else None
        return [
            _source(
                "web_search",
                str(hit["url"]),
                _canonical(dict(hit)),
                title=hit.get("title"),
                args=arguments,
                cost=share,
            )
            for hit in hits
        ]


class _Map(_Call):
    def estimate(self, arguments, catalog):
        return 1.0

    def charge(self, arguments, result):
        return Charge(1)


class _Crawl(_Call):
    """``firecrawl_crawl``, which waits for the crawl to finish, and
    ``firecrawl_check_crawl_status``, which polls one; both report the job's
    total so far and list every page so far."""

    def __init__(self, *, starts: bool) -> None:
        self._starts = starts

    def refusal(self, arguments):
        limit = _number(arguments.get("limit"))
        if self._starts and (limit is None or not 0 < limit <= CRAWL_PAGE_CAP):
            return RelayRejection(
                400,
                RelayError.LIMIT_REQUIRED,
                f"firecrawl_crawl needs a page limit of at most {CRAWL_PAGE_CAP}",
            )
        return None

    def estimate(self, arguments, catalog):
        if not self._starts:
            return 0.0
        # Priced by the crawl's own site, as a scrape of it is.
        per_page = _page_price(_mapping(arguments.get("scrapeOptions")), arguments.get("url"))
        return (_count(arguments.get("limit")) or 0) * per_page

    def job_ref(self, arguments, result):
        # A start takes its job from the reply alone: an ``id`` in its
        # arguments names a job the caller chose, whose total it would net
        # against. A status poll names its job.
        job = result.get("id") if self._starts else result.get("id") or arguments.get("id")
        return f"crawl:{job}" if job else None

    def charge(self, arguments, result):
        charge, job_ref = _charge(result.get("creditsUsed")), self.job_ref(arguments, result)
        return None if charge is None or job_ref is None else replace(charge, job_ref=job_ref)

    def sources(self, arguments, result, catalog, call_cost):
        pages = [p for p in result.get("data") or [] if isinstance(p, Mapping)]
        # Each poll lists every page so far beside the job's total so far, so
        # the average is the same on every poll that repeats a page.
        share = call_cost / len(pages) if pages else None
        return [s for page in pages for s in _page_source(page, arguments, share)]


class _ScrapeCatalog(_Call):
    """``firecrawl_scrape`` with ``alexandria`` runs, the catalog's frame
    until Firecrawl moved it to ``alexandria_library_read`` (2026-11-16) and
    took the field off scrape's schema. Its server still runs such a frame,
    but how it bills one sent beside a url was never checked, so it is
    refused rather than priced as either."""

    def refusal(self, arguments):
        return RelayRejection(
            400,
            RelayError.BAD_REQUEST,
            "firecrawl_scrape no longer takes catalog runs; "
            "run them with alexandria_library_read(calls=[...])",
        )


_CALLS: dict[str, _Call] = {
    "alexandria_library_read": _Catalog(),
    "firecrawl_search": _Search(),
    "firecrawl_map": _Map(),
    "firecrawl_crawl": _Crawl(starts=True),
    "firecrawl_check_crawl_status": _Crawl(starts=False),
}
class _Unpriced(_Call):
    """A paid tool with no price here: refused, since the vendor would bill
    it at whatever it costs and its hold would bound nothing."""

    def refusal(self, arguments):
        return RelayRejection(
            403,
            RelayError.TOOL_BLOCKED,
            "the relay has no price for this tool, so it is not called on the user's account",
        )


_SCRAPE_CATALOG, _PAGE, _UNPRICED = _ScrapeCatalog(), _Page(), _Unpriced()


class FirecrawlMeter:
    vendor = "firecrawl"
    tracking = "FirecrawlCredits"
    label = "Firecrawl"
    # Spend not yet on any usage row (calls in flight, and calls with no
    # thread known, which wait for the user's next turn), in Firecrawl
    # credits. With no balance to check, it bounds that spend with the call
    # being admitted counted, so it is the most the deployment can be out if
    # that turn never comes. With a balance, the balance bounds each call and
    # this only pauses paid calls once that spend reaches it.
    owed_cap = 1000

    @staticmethod
    def _call(tool: str, arguments: Mapping[str, Any]) -> _Call:
        if tool == "firecrawl_scrape":
            # Catalog runs only when there are runs to make. An empty or falsy
            # ``alexandria`` beside a url is priced as the page scrape it
            # otherwise is (Firecrawl refused each such shape when checked,
            # 2026-11-15).
            runs = arguments.get("alexandria")
            return _SCRAPE_CATALOG if isinstance(runs, (Mapping, list)) and runs else _PAGE
        return _CALLS.get(tool, _UNPRICED)

    def is_paid(self, tool: str) -> bool:
        return tool != _DISCOVERY

    def refusal(self, tool: str, arguments: Mapping[str, Any]) -> RelayRejection | None:
        """Why a call cannot be priced, or None: a crawl needs a page limit,
        a search's limit has to be a count, and a tool with no price here is
        not called. A catalog run is checked against the catalog in
        ``estimate``."""
        return self._call(tool, arguments).refusal(arguments)

    async def load_catalog(
        self, tool: str, arguments: Mapping[str, Any], *, url: str, headers: Mapping[str, str]
    ) -> dict[str, Provider]:
        """The catalog entries the call's price and sources need, read with
        the credential the relay sends it with."""
        call = self._call(tool, arguments)
        names = call.providers(arguments)
        if not names:
            return {}
        catalog = await firecrawl_catalog.providers(names, url=url, headers=headers)
        stale = call.unlisted(arguments, catalog)
        if stale:
            # A capability added since the provider was cached: read it again
            # (throttled) rather than refuse it for an hour.
            catalog = {
                **catalog,
                **await firecrawl_catalog.providers(
                    stale, url=url, headers=headers, refresh=stale
                ),
            }
        return catalog

    def estimate(
        self, tool: str, arguments: Mapping[str, Any], catalog: Mapping[str, Provider]
    ) -> float:
        """The most a call should cost, before it runs, in Firecrawl credits.
        Never below ``MIN_ESTIMATE``; a price that is not finite is passed
        on, for admission to refuse. Raises ``RelayRejection`` for a catalog
        run the catalog cannot price."""
        value = self._call(tool, arguments).estimate(arguments, catalog)
        return max(MIN_ESTIMATE, value) if math.isfinite(value) else value

    def charge(
        self, tool: str, arguments: Mapping[str, Any], result: Mapping[str, Any]
    ) -> Charge | None:
        return self._call(tool, arguments).charge(arguments, result)

    def job_ref(
        self, tool: str, arguments: Mapping[str, Any], result: Mapping[str, Any]
    ) -> str | None:
        """The vendor job a reply reports on, whose later polls repeat its total."""
        return self._call(tool, arguments).job_ref(arguments, result)

    def sources(
        self,
        tool: str,
        arguments: Mapping[str, Any],
        result: Mapping[str, Any],
        *,
        catalog: Mapping[str, Provider],
        call_cost: float,
    ) -> tuple[list[MeteredSource], str | None]:
        """``(source, body)`` for each thing the reply returned, and the job
        they belong to. ``call_cost`` is what the call cost in Firecrawl
        credits (a crawl's: its total so far)."""
        call = self._call(tool, arguments)
        return call.sources(arguments, result, catalog, call_cost), call.job_ref(arguments, result)
