"""Server-side request meter for one ``get_sec_filing`` call.

A ``CallMeter`` sits in the ``current_meter`` context variable for the length
of a call and records every outbound request the call makes and the outcome of
our own cache layers. ``metered_call`` logs one summary at the end. Nothing
here is ever shown to the model.

Context propagation: asyncio tasks inherit the context, and
``asyncio.to_thread`` copies it, so work started either way reports into the
meter. A bare ``ThreadPoolExecutor.submit`` or ``loop.run_in_executor`` does
NOT copy the context; the requests such a thread makes are silently unmetered.
Submit through ``contextvars.copy_context().run`` (or ``asyncio.to_thread``)
when a worker thread should be measured.

Process state: the only module-level state is the hook-installation flag. The
meter itself lives in the call's context, so workers and concurrent calls
never share one.
"""

from __future__ import annotations

import contextvars
import importlib
import inspect
import logging
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Iterator, Optional, Sequence

from src.observability.langsmith_run import stamp_run

logger = logging.getLogger(__name__)

HTTP_HIT = "hit"
HTTP_REVALIDATED = "revalidated"
HTTP_NETWORK = "network"

current_meter: contextvars.ContextVar[Optional["CallMeter"]] = contextvars.ContextVar(
    "sec_call_meter", default=None
)


def classify_path(host: str, path: str) -> str:
    """Name what a request fetches from its host and path."""
    host = host.lower()
    if host == "www.sec.gov":
        if path.startswith("/files/company_tickers") and path.endswith(".json"):
            return "ticker_map"
        if path == "/include/ticker.txt":
            return "ticker_map"
        if path.startswith("/Archives/"):
            if path.endswith("-index.htm") or path.endswith("index.json"):
                return "filing_index"
            if path.startswith("/Archives/edgar/data/"):
                return "filing_text" if path.endswith(".txt") else "filing_document"
        return "other"
    if host == "data.sec.gov":
        if path.startswith("/submissions/"):
            return "submissions"
        if path.startswith("/api/xbrl/companyfacts/"):
            return "companyfacts"
        return "other"
    if host == "efts.sec.gov":
        return "full_text_search"
    if host.endswith("financialmodelingprep.com"):
        lowered = path.lower()
        if "earning-call-transcript-dates" in lowered or "earning_call_transcript_dates" in lowered:
            return "fmp_call_dates"
        if "earning-call-transcript" in lowered or "earning_call_transcript" in lowered:
            return "fmp_transcript"
        return "fmp_other"
    return "other"


@dataclass
class RequestRecord:
    """One outbound request.

    ``cache``: a hit never left the process; a revalidation went to the network
    and got a 304, so its body came from the HTTP cache; a network response's
    body crossed the wire. Every non-hit is a network request, and only a body
    that crossed the wire counts toward ``bytes_in``. ``started`` is when the
    request went on the wire, after any rate-limiter permit; ``ended`` is when
    the body finished for a network response, when headers arrived otherwise.
    """

    host: str
    path_class: str
    started: float
    status: Optional[int] = None
    bytes: int = 0
    ended: Optional[float] = None
    limiter_wait_ms: Optional[float] = None
    cache: str = HTTP_NETWORK

    @property
    def is_network(self) -> bool:
        return self.cache != HTTP_HIT


def overlap(intervals: Sequence[tuple[float, float]]) -> tuple[float, int]:
    """Union length in ms and peak overlap of (start, end) intervals in seconds.

    One interval ending at t does not overlap one starting at t.
    """
    covered, open_now, peak, last = 0.0, 0, 0, 0.0
    for t, delta in sorted([(s, 1) for s, _ in intervals] + [(e, -1) for _, e in intervals]):
        if open_now:
            covered += t - last
        open_now, last = open_now + delta, t
        peak = max(peak, open_now)
    return covered * 1000.0, peak


def _describe(exc: BaseException) -> str:
    """The exception's type and HTTP status, never its text, which can carry a request URL with credentials."""
    status = getattr(getattr(exc, "response", None), "status_code", None)
    return type(exc).__name__ if status is None else f"{type(exc).__name__} {status}"


class CallMeter:
    """Everything measured during one tool call. Safe to feed from many threads."""

    def __init__(self, case: Optional[dict[str, Any]] = None) -> None:
        self.case = dict(case or {})
        self.started = time.perf_counter()
        self.ended: Optional[float] = None
        self.error: Optional[str] = None
        self.records: list[RequestRecord] = []
        self._cache: dict[str, dict[str, int]] = {}
        self._lock = threading.Lock()

    def add(self, record: RequestRecord) -> RequestRecord:
        self.records.append(record)
        return record

    def cache(self, layer: str, outcome: str) -> None:
        """Count the outcome ("hit", "miss", ...) of one of our own cache layers."""
        with self._lock:
            counts = self._cache.setdefault(layer, {})
            counts[outcome] = counts.get(outcome, 0) + 1

    def fail(self, error: str) -> None:
        """Mark the call failed, for an error the caller handles without raising."""
        self.error = error

    def summary(self) -> dict[str, Any]:
        now = time.perf_counter()
        records = list(self.records)
        by_host: dict[str, dict[str, int]] = {}
        by_class: dict[str, dict[str, int]] = {}
        for r in records:
            for table, key in ((by_host, r.host), (by_class, r.path_class)):
                counts = table.setdefault(key, {})
                counts[r.cache] = counts.get(r.cache, 0) + 1

        network = [r for r in records if r.is_network]
        intervals = [(r.started, r.ended if r.ended is not None else now) for r in network]
        critical_ms, concurrency = overlap(intervals)
        with self._lock:
            cache_layers = {
                layer: {**counts, "hit_rate": round(counts.get("hit", 0) / sum(counts.values()), 3)}
                for layer, counts in self._cache.items()
            }
        summary = {
            "wall_ms": round(((self.ended or now) - self.started) * 1000.0, 1),
            "requests_total": len(records),
            "requests_by_host": by_host,
            "requests_by_path_class": by_class,
            "network_count": len(network),
            "http_cache_hits": len(records) - len(network),
            "revalidations": sum(1 for r in records if r.cache == HTTP_REVALIDATED),
            "cache": cache_layers,
            "network_ms_sum": round(sum(e - s for s, e in intervals) * 1000.0, 1),
            "critical_path_ms": round(critical_ms, 1),
            "max_concurrency": concurrency,
            "limiter_wait_ms_sum": round(sum(r.limiter_wait_ms or 0.0 for r in network), 1),
            "bytes_in": sum(r.bytes for r in records if r.cache == HTTP_NETWORK),
            "bytes_from_http_cache": sum(r.bytes for r in records if r.cache != HTTP_NETWORK),
        }
        if self.error is not None:
            summary["error"] = self.error
        return summary


@contextmanager
def metered_call(**case: Any) -> Iterator[CallMeter]:
    """Meter one tool call: ``with metered_call(symbol="AAPL") as meter:``.

    Installs the transport hooks on first use, sets the meter for everything
    the block starts, and on exit logs one ``sec_filing.metrics`` line and
    stamps the LangSmith run. An exception still logs, with an ``error``
    field naming its type, then propagates.
    """
    install_hooks()
    meter = CallMeter(case)
    token = current_meter.set(meter)
    try:
        yield meter
    except BaseException as exc:
        meter.fail(_describe(exc))
        raise
    finally:
        current_meter.reset(token)
        meter.ended = time.perf_counter()
        summary = meter.summary()
        logger.info("sec_filing.metrics", extra={"sec_filing_case": meter.case, "sec_filing_metrics": summary})
        stamp_run(sec_filing_metrics=summary)


def note_cache(layer: str, outcome: str) -> None:
    """Count a cache outcome on the current call's meter; nothing outside a metered call."""
    meter = current_meter.get()
    if meter is not None:
        meter.cache(layer, outcome)


def _new_record(request: Any, started: float) -> RequestRecord:
    url = request.url
    return RequestRecord(host=url.host, path_class=classify_path(url.host, url.path), started=started)


def _body_length(response: Any) -> int:
    try:
        length = int(response.headers.get("content-length") or 0)
    except (TypeError, ValueError):
        length = 0
    if length:
        return length
    try:
        return len(response.content)
    except Exception:
        return 0


_inflight: contextvars.ContextVar[Optional[RequestRecord]] = contextvars.ContextVar("sec_meter_inflight", default=None)


class _Passage:
    """One request through a hooked transport layer, the core both transport shims share.

    The caching layer (``outer``) opens a record as a cache hit; a layer below it
    only marks that record as having gone to the network, at its dispatch time.
    An inner layer with no caching layer above it opens its own record.
    """

    def __init__(self, request: Any, outer: bool, stream_class: type) -> None:
        self.request, self.outer, self.stream_class = request, outer, stream_class
        self.meter = current_meter.get()
        self.record: Optional[RequestRecord] = None
        self.token: Optional[contextvars.Token] = None

    def __enter__(self) -> "_Passage":
        if self.meter is None:
            return self
        parent = _inflight.get()
        if not self.outer and parent is not None:
            parent.cache, parent.started = HTTP_NETWORK, time.perf_counter()
            return self
        self.record = _new_record(self.request, time.perf_counter())
        self.record.cache = HTTP_HIT if self.outer else HTTP_NETWORK
        self.token = _inflight.set(self.record)
        return self

    def done(self, response: Any) -> Any:
        if self.record is not None:
            self._settle(response)
        return response

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.token is None:
            return
        _inflight.reset(self.token)
        if exc_type is not None:
            self._settle(None)

    def _settle(self, response: Any) -> None:
        record = self.record
        record.ended = time.perf_counter()
        self.meter.add(record)
        if response is None:
            return
        record.status, record.bytes = response.status_code, _body_length(response)
        if record.cache != HTTP_NETWORK:
            return
        if response.status_code == 304 or response.headers.get("x-cache", "").upper() == "HIT":
            record.cache = HTTP_REVALIDATED
            return
        try:
            response.stream = self.stream_class(response.stream, record)
        except Exception:
            logger.debug("sec metering could not wrap a response stream", exc_info=True)


def _make_stream_class(httpx_mod: Any) -> type:
    """A pass-through stream that stamps the record when the body is done.

    Wrapping the iterator never reads it: the caller still decides whether and
    when the body is consumed, so a streaming response stays streaming.
    """

    class _TimedStream(httpx_mod.SyncByteStream, httpx_mod.AsyncByteStream):
        def __init__(self, inner: Any, record: RequestRecord) -> None:
            self._inner, self._record, self._count, self._done = inner, record, 0, False

        def _finish(self) -> None:
            if not self._done:
                self._done = True
                self._record.ended = time.perf_counter()
                self._record.bytes = self._count or self._record.bytes

        def __iter__(self) -> Iterator[bytes]:
            try:
                for chunk in self._inner:
                    self._count += len(chunk)
                    yield chunk
            finally:
                self._finish()

        async def __aiter__(self):
            try:
                async for chunk in self._inner:
                    self._count += len(chunk)
                    yield chunk
            finally:
                self._finish()

        def close(self) -> None:
            try:
                self._inner.close()
            finally:
                self._finish()

        async def aclose(self) -> None:
            try:
                await self._inner.aclose()
            finally:
                self._finish()

    return _TimedStream


def _wrap_transport(original: Callable, outer: bool, stream_class: type) -> Callable:
    if inspect.iscoroutinefunction(original):

        async def handle(self, request, *args, **kwargs):
            with _Passage(request, outer, stream_class) as passage:
                return passage.done(await original(self, request, *args, **kwargs))

    else:

        def handle(self, request, *args, **kwargs):
            with _Passage(request, outer, stream_class) as passage:
                return passage.done(original(self, request, *args, **kwargs))

    handle.__wrapped__ = original  # type: ignore[attr-defined]
    return handle


def _credit(record: RequestRecord, t0: float) -> None:
    now = time.perf_counter()
    record.limiter_wait_ms = (record.limiter_wait_ms or 0.0) + (now - t0) * 1000.0
    record.started = now


async def _credited(awaitable: Awaitable, record: RequestRecord, t0: float) -> Any:
    try:
        return await awaitable
    finally:
        _credit(record, t0)


def _wrap_limiter(sync_original: Callable, async_original: Callable) -> tuple[Callable, Callable]:
    """Time permit acquisition for the request in flight, and move its start past the permit.

    The rate-limiting transports acquire their permit inside ``handle_request``,
    with no seam to time just that step, so the pyrate_limiter calls are timed
    instead and credited to the request in flight.
    """

    def try_acquire(self, *args, **kwargs):
        record = _inflight.get()
        if record is None:
            return sync_original(self, *args, **kwargs)
        t0 = time.perf_counter()
        result = sync_original(self, *args, **kwargs)
        if inspect.isawaitable(result):
            return _credited(result, record, t0)
        _credit(record, t0)
        return result

    async def try_acquire_async(self, *args, **kwargs):
        record = _inflight.get()
        if record is None:
            return await async_original(self, *args, **kwargs)
        return await _credited(async_original(self, *args, **kwargs), record, time.perf_counter())

    try_acquire.__wrapped__ = sync_original  # type: ignore[attr-defined]
    try_acquire_async.__wrapped__ = async_original  # type: ignore[attr-defined]
    return try_acquire, try_acquire_async


_hooks_installed = False
_hooks_lock = threading.Lock()


def install_hooks() -> None:
    """Wrap the edgartools (httpxthrottlecache) transports once per process.

    The caching transport sees every request; the rate-limiting and primp
    transports below it see only the ones that reach the network, so a caching
    request with no inner call is an HTTP cache hit. Classes missing from the
    installed version are skipped. Idempotent and safe to call from any thread.
    """
    global _hooks_installed
    with _hooks_lock:
        if _hooks_installed:
            return
        _hooks_installed = True
        try:
            compat = importlib.import_module("httpxthrottlecache._compat")
        except ImportError:
            return
        stream_class = _make_stream_class(compat.httpx)
        layers = (
            ("httpxthrottlecache.filecache.transport", "CachingTransport", True),
            ("httpxthrottlecache.ratelimiter", "RateLimitingTransport", False),
            ("httpxthrottlecache.ratelimiter", "AsyncRateLimitingTransport", False),
            ("httpxthrottlecache.primp_transport", "PrimpTransport", False),
            ("httpxthrottlecache.primp_transport", "AsyncPrimpTransport", False),
        )
        for module_name, class_name, outer in layers:
            try:
                cls = getattr(importlib.import_module(module_name), class_name, None)
            except ImportError:
                continue
            for name in ("handle_request", "handle_async_request"):
                original = getattr(cls, name, None) if cls is not None else None
                if original is not None and getattr(original, "__wrapped__", None) is None:
                    setattr(cls, name, _wrap_transport(original, outer, stream_class))
        try:
            limiter_cls = importlib.import_module("pyrate_limiter").Limiter
            if getattr(limiter_cls.try_acquire, "__wrapped__", None) is None:
                limiter_cls.try_acquire, limiter_cls.try_acquire_async = _wrap_limiter(
                    limiter_cls.try_acquire, limiter_cls.try_acquire_async
                )
        except (ImportError, AttributeError):
            logger.debug("sec metering: limiter wait not measurable", exc_info=True)


_FMP_RECORD_KEY = "sec_meter_record"


def fmp_event_hooks() -> dict[str, list[Callable]]:
    """``httpx.AsyncClient(event_hooks=...)`` hooks that record FMP requests.

    The response hook reads the body (the client reads it right after anyway,
    and it is memoized) so the end time is exact. A request that fails before a
    response runs no response hook, so it keeps ``status`` None: the next request
    of its kind (FMPClient's retry) ends it, and ``summary`` ends one left open.
    """

    async def on_request(request) -> None:
        meter = current_meter.get()
        if meter is None:
            return
        now, record = time.perf_counter(), _new_record(request, time.perf_counter())
        for failed in meter.records:
            if failed.ended is None and (failed.host, failed.path_class) == (record.host, record.path_class):
                failed.ended = now
        request.extensions[_FMP_RECORD_KEY] = meter.add(record)

    async def on_response(response) -> None:
        record = response.request.extensions.get(_FMP_RECORD_KEY)
        if record is None:
            return
        await response.aread()
        record.status, record.bytes, record.ended = response.status_code, _body_length(response), time.perf_counter()

    return {"request": [on_request], "response": [on_response]}
