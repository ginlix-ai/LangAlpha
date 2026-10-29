"""FetchRouter: web-fetch with a single paid provider and an in-house net.

Fetch uses exactly one third-party provider — the first chain entry that
has a key — and degrades **only to in-house**, never to a second paid
provider (every third party covers the same URLs, so cascading through
them just multiplies cost and latency; the free in-house crawler is the
one worthwhile safety net). Extra third-party entries in ``fetch_chain``
are inert unless the earlier ones lack a key.

Semantics per URL (batches split/merge around it):
1. Dedicated-extractor URLs (YouTube/X) are forced to in-house whatever
   the chain — third-party fetchers return junk pages for them.
2. Pick the primary = first chain entry with a key; skip other third
   parties. Skip any provider whose circuit breaker is open.
3. Retryable errors (timeout, anti_bot, rate_limited, provider_error, ...)
   move the URL to in-house.
4. Terminal errors (not_found, paywall, forbidden, unsupported_url,
   budget_exceeded) fail fast — no provider will do better.
5. A URL that fails with anti_bot escalates to the "stealth" fetch level
   on the primary when its capability declares one, before falling to
   in-house.
6. Only the portable request core (urls, objective, mode, freshness)
   crosses a fallback boundary.

Successful per-URL fetches record usage under the capability's
level-qualified tracking name (billing flows through the manifest).
"""

import asyncio
import logging
import os
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import import_module
from typing import Dict, List, Optional, Protocol, Tuple
from urllib.parse import urlparse

from src.observability.langsmith_run import (
    ERROR_CLASS,
    ERROR_TEXT_MAX,
    TOOL_PROVIDER,
    record_child_run,
)
from src.tools.web.breaker import CircuitBreaker
from src.tools.decorators import get_tool_tracker
from src.tools.web.manifest import (
    CAPABILITY_FETCH,
    CapabilitySpec,
    LevelSpec,
    WebProviderSpec,
    get_web_provider_spec,
)
from src.tools.web.types import (
    FetchAttempt,
    FetchRequest,
    FetchResponse,
    FetchResult,
    WebError,
    WebErrorType,
)

logger = logging.getLogger(__name__)

_STEALTH_LEVEL = "stealth"
_INHOUSE = "inhouse"

# URL patterns that must go through the in-house crawler's dedicated
# extractors (YouTube transcripts, X/Twitter posts) regardless of the chain.
_INHOUSE_URL_PATTERNS = re.compile(
    r"(?:^|\.)(?:youtube\.com|youtu\.be|twitter\.com|x\.com)$", re.IGNORECASE
)


def _needs_inhouse(url: str) -> bool:
    host = (urlparse(url).netloc or "").lower().split(":")[0]
    return bool(host and _INHOUSE_URL_PATTERNS.search(host))


# Each provider attempt is its own LangSmith run, as each failed LLM attempt
# is: the in-house fallback turns a failing primary into an ok tool run, so
# only a run per attempt lets a per-provider alarm see the primary is down.
_ATTEMPT_RUN = "web_provider_attempt"


def _trace_attempt(
    provider: str,
    urls: List[str],
    outcome: str,
    started: Optional[datetime] = None,
    error: Optional[str] = None,
    error_class: Optional[str] = None,
) -> None:
    record_child_run(
        _ATTEMPT_RUN,
        start_time=started,
        inputs={"urls": urls},
        outputs={"outcome": outcome},
        error=error[:ERROR_TEXT_MAX] if error else None,
        metadata={
            TOOL_PROVIDER: provider,
            "capability": CAPABILITY_FETCH,
            "attempt_outcome": outcome,
            ERROR_CLASS: error_class,
        },
    )


def _error_kind(error: WebError) -> str:
    return error.native_kind or error.type.value


def _attempt_outcome(
    provider: str, results: List[FetchResult]
) -> Tuple[FetchAttempt, Optional[WebError]]:
    """Summarize one provider's attempt, with the dominant error when all failed.

    The dominant error records why a fallback happened, not just that it did.
    It is counted by the provider's own name for the failure, since the
    normalized type buckets a dead target and a broken provider into the same
    ``provider_error``.
    """
    ok = sum(1 for r in results if r.ok)
    if ok == len(results):
        return FetchAttempt(provider, "ok"), None
    if ok:
        return FetchAttempt(provider, "partial"), None
    errors = [r.error for r in results if r.error]
    top = Counter(_error_kind(e) for e in errors).most_common(1)[0][0]
    return FetchAttempt(provider, top), next(e for e in errors if _error_kind(e) == top)


class FetchAdapter(Protocol):
    """Data-level fetch adapter a provider module implements.

    Batch sizing is owned by the manifest capability (``max_batch_size``),
    not the adapter.
    """

    name: str

    async def fetch(self, req: FetchRequest, native_params: Dict) -> FetchResponse: ...


def _lazy_adapter(provider: str, class_name: str):
    """Deferred import so provider modules load only when chained."""

    def build() -> FetchAdapter:
        return getattr(import_module(f"src.tools.web.providers.{provider}"), class_name)()

    return build


# Provider name -> adapter factory. Adding a fetch provider = one entry here,
# one adapter class, one manifest fetch capability.
_ADAPTER_BUILDERS = {
    provider: _lazy_adapter(provider, class_name)
    for provider, class_name in {
        "exa": "ExaFetchAdapter",
        "parallel": "ParallelFetchAdapter",
        "firecrawl": "FirecrawlFetchAdapter",
        "tavily": "TavilyFetchAdapter",
        "inhouse": "InhouseFetchAdapter",
    }.items()
}


@dataclass
class _ChainEntry:
    provider: WebProviderSpec
    capability: CapabilitySpec
    adapter: FetchAdapter
    breaker: CircuitBreaker

    def available(self) -> bool:
        return self.provider.env_key is None or bool(os.getenv(self.provider.env_key))


def build_chain(providers: List[str]) -> List["_ChainEntry"]:
    """Resolve provider names against the manifest + adapter registry.

    Unknown names or providers without a fetch capability are dropped with
    a warning (deployment-edited config must not take fetch down).
    """
    entries: List[_ChainEntry] = []
    for name in providers:
        spec = get_web_provider_spec(name)
        cap = spec.capability(CAPABILITY_FETCH) if spec else None
        builder = _ADAPTER_BUILDERS.get(name)
        if spec is None or cap is None or builder is None:
            logger.warning("Ignoring unknown fetch provider %r in fetch_chain", name)
            continue
        entries.append(
            _ChainEntry(
                provider=spec,
                capability=cap,
                adapter=builder(),
                breaker=CircuitBreaker(name=f"fetch:{name}"),
            )
        )
    return entries


class FetchRouter:
    """Routes FetchRequests through the configured provider chain."""

    def __init__(self, providers: List[str]):
        self._chain = build_chain(providers)
        if not self._chain:
            logger.warning("fetch_chain resolved empty; falling back to inhouse")
            self._chain = build_chain([_INHOUSE])
        # Forced (dedicated-extractor) URLs always have an in-house entry to
        # land on; when the chain carries one, the same entry — and the same
        # breaker — serves both forced and degraded URLs.
        self._inhouse_entry = next(
            (e for e in self._chain if e.adapter.name == _INHOUSE), None
        )
        if self._inhouse_entry is None:
            extra = build_chain([_INHOUSE])
            self._inhouse_entry = extra[0] if extra else None

    @property
    def provider_names(self) -> List[str]:
        return [e.adapter.name for e in self._chain]

    async def _call_adapter(
        self, entry: _ChainEntry, urls: List[str], req: FetchRequest, level: LevelSpec
    ) -> List[FetchResult]:
        """One provider attempt over urls, split by the capability's batch size.

        Each sub-batch is isolated: an adapter that raises (e.g. a non-JSON
        200 body → JSONDecodeError, which adapters don't catch) fails only its
        own URLs instead of discarding every sibling sub-batch's results.
        """
        size = entry.capability.max_batch_size
        batches = [urls[i : i + size] for i in range(0, len(urls), size)]
        responses = await asyncio.gather(
            *(
                entry.adapter.fetch(
                    FetchRequest(
                        urls=batch,
                        objective=req.objective,
                        mode=req.mode,
                        max_age_seconds=req.max_age_seconds,
                    ),
                    level.native_params,
                )
                for batch in batches
            ),
            return_exceptions=True,
        )
        out: List[FetchResult] = []
        for batch, resp in zip(batches, responses):
            if isinstance(resp, asyncio.CancelledError):
                raise resp
            if isinstance(resp, BaseException):
                logger.error(
                    "fetch adapter %s raised on batch: %r", entry.adapter.name, resp
                )
                out.extend(
                    FetchResult(
                        url=u,
                        error=WebError(
                            type=WebErrorType.PROVIDER_ERROR, message=str(resp)[:300]
                        ),
                    )
                    for u in batch
                )
            else:
                out.extend(resp.results)
        return out

    async def _attempt(
        self, entry: _ChainEntry, urls: List[str], req: FetchRequest, anti_bot: set
    ) -> List[FetchResult]:
        """All attempts on one provider: default-level work plus stealth
        re-queues. Returns the provider's final per-URL outcomes; successful
        fetches record usage at the level they were billed at."""
        stealth = entry.capability.level(_STEALTH_LEVEL)
        default = entry.capability.default_level_spec
        # Work items are (urls, level) attempts on this provider. URLs an
        # earlier provider flagged anti_bot start at the stealth level
        # when offered; an anti_bot failure at a non-stealth level
        # re-queues those URLs as a stealth attempt on this same provider
        # (billed at the stealth rate).
        flagged = [u for u in urls if u in anti_bot]
        normal = [u for u in urls if u not in anti_bot]
        work: List[Tuple[List[str], LevelSpec]] = []
        if normal:
            work.append((normal, default))
        if flagged:
            work.append((flagged, stealth or default))

        results: List[FetchResult] = []  # this provider's final outcome per URL
        while work:
            batch_urls, level = work.pop(0)
            batch = await self._call_adapter(entry, batch_urls, req, level)
            ok_count = sum(1 for r in batch if r.ok)
            if ok_count:
                tracker = get_tool_tracker()
                if tracker and level.credits > 0:
                    tracker.record_usage(
                        entry.capability.tracking_key(level), count=ok_count
                    )
            escalate: List[str] = []
            for result in batch:
                hit_anti_bot = (
                    not result.ok
                    and result.error is not None
                    and result.error.type == WebErrorType.ANTI_BOT
                )
                if hit_anti_bot:
                    anti_bot.add(result.url)
                if hit_anti_bot and stealth is not None and level is not stealth:
                    escalate.append(result.url)
                else:
                    results.append(result)
            if escalate:
                work.append((escalate, stealth))
        return results

    async def fetch(self, req: FetchRequest) -> FetchResponse:
        final: Dict[str, FetchResult] = {}
        last_error: Dict[str, FetchResult] = {}
        pending: List[str] = list(dict.fromkeys(req.urls))  # de-dupe, keep order
        anti_bot: set = set()
        attempts: List[FetchAttempt] = []
        first_ok_provider: Optional[str] = None
        # Without a usable in-house entry, forced URLs degrade to ordinary
        # routing rather than failing outright.
        forced = (
            {u for u in pending if _needs_inhouse(u)}
            if self._inhouse_entry is not None
            else set()
        )

        # The primary is the first keyed third-party entry; fetch degrades only
        # to in-house, so every other third-party entry is inert.
        primary = next(
            (e.adapter.name for e in self._chain
             if e.adapter.name != _INHOUSE and e.available()),
            None,
        )

        # Stages make the routing policy explicit: the primary serves ordinary
        # URLs, an in-house chain entry serves everything, and forced URLs get
        # an in-house stage even when the chain has none (dedicated extractors
        # are unconditional).
        stages: List[Tuple[_ChainEntry, str]] = []
        for entry in self._chain:
            if entry.adapter.name == _INHOUSE:
                stages.append((entry, "all"))
            elif entry.adapter.name == primary:
                stages.append((entry, "ordinary"))
        if forced and all(entry is not self._inhouse_entry for entry, _ in stages):
            stages.append((self._inhouse_entry, "forced"))

        for entry, scope in stages:
            if scope == "ordinary":
                urls_now = [u for u in pending if u not in forced]
            elif scope == "forced":
                urls_now = [u for u in pending if u in forced]
            else:
                urls_now = list(pending)
            if not urls_now:
                continue
            if not entry.available():
                logger.debug("fetch: skipping %s (no API key)", entry.adapter.name)
                continue
            await entry.breaker.check_state()
            if entry.breaker.is_open():
                logger.debug("fetch: skipping %s (breaker open)", entry.adapter.name)
                _trace_attempt(
                    entry.adapter.name,
                    urls_now,
                    WebErrorType.CIRCUIT_OPEN.value,
                    error=f"{entry.adapter.name} skipped: circuit breaker open",
                    error_class=WebErrorType.CIRCUIT_OPEN.value,
                )
                continue
            started = datetime.now(timezone.utc)
            results = await self._attempt(entry, urls_now, req, anti_bot)
            attempt, dominant = _attempt_outcome(entry.adapter.name, results)
            attempts.append(attempt)

            any_ok = False
            for result in results:
                if result.ok:
                    any_ok = True
                    final[result.url] = result
                    if first_ok_provider is None:
                        first_ok_provider = entry.adapter.name
                elif result.error and not result.error.retryable:
                    final[result.url] = result
                else:
                    last_error[result.url] = result

            provider_failure: Optional[str] = None
            failure_class: Optional[str] = None
            if any_ok:
                await entry.breaker.record_success()
            elif results and all(r.error and r.error.provider_side for r in results):
                # Whole attempt failed at the provider level, so the attempt
                # is an errored run. Per-URL failures (timeouts, anti_bot, and
                # target-side 429/5xx) are not: a down target site must not
                # read as a broken provider, nor open its shared breaker.
                provider_failure = "; ".join(str(r.error) for r in results if r.error)
                failure_class = dominant.type.value
                if all(r.error and r.error.provider_fault for r in results):
                    await entry.breaker.record_failure(reason=provider_failure)
            _trace_attempt(
                entry.adapter.name,
                urls_now,
                attempt.outcome,
                started,
                error=provider_failure,
                error_class=failure_class,
            )

            pending = [u for u in pending if u not in final]
            if not pending:
                break

        for url in pending:
            final[url] = last_error.get(
                url,
                FetchResult(
                    url=url,
                    error=WebError(
                        type=WebErrorType.CIRCUIT_OPEN,
                        message="No fetch provider available (keys unset or breakers open)",
                        # Ours, not the page's: no provider could take it.
                        provider_fault=True,
                    ),
                ),
            )

        return FetchResponse(
            results=[final[u] for u in req.urls if u in final],
            provider=first_ok_provider,
            attempts=attempts,
        )
