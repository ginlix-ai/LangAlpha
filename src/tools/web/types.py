"""Normalized data types for the web provider protocol.

These are the non-negotiable half of the protocol: every provider adapter
maps its raw payloads into these types so the fetch fallback chain, billing,
provenance, and frontend cards never see provider-specific shapes. Request
schemas stay provider-native — only responses and errors are normalized.

Data-only module: stdlib imports only.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Literal, Optional


class WebErrorType(str, Enum):
    """Provider-independent failure taxonomy.

    Each member carries defaults for the two orthogonal routing axes:
    ``retryable_default`` (does the fetch chain try the next provider?) and
    ``provider_fault_default`` (does the failure count against the provider's
    circuit breaker?). Terminal errors (paywall, not_found, ...) exit the
    chain early because no other provider will do better.
    """

    NOT_FOUND = "not_found"
    TIMEOUT = "timeout"
    FORBIDDEN = "forbidden"
    PAYWALL = "paywall"
    ANTI_BOT = "anti_bot"
    UNSUPPORTED_URL = "unsupported_url"
    EMPTY = "empty"
    RATE_LIMITED = "rate_limited"
    BUDGET_EXCEEDED = "budget_exceeded"
    CIRCUIT_OPEN = "circuit_open"
    PROVIDER_ERROR = "provider_error"

    @property
    def retryable_default(self) -> bool:
        return self in _RETRYABLE_TYPES

    @property
    def provider_fault_default(self) -> bool:
        return self in _PROVIDER_FAULT_TYPES


_RETRYABLE_TYPES = frozenset(
    {
        WebErrorType.TIMEOUT,
        WebErrorType.ANTI_BOT,
        WebErrorType.EMPTY,
        WebErrorType.RATE_LIMITED,
        WebErrorType.CIRCUIT_OPEN,
        WebErrorType.PROVIDER_ERROR,
    }
)

_PROVIDER_FAULT_TYPES = frozenset(
    {
        WebErrorType.PROVIDER_ERROR,
        WebErrorType.RATE_LIMITED,
    }
)


@dataclass(frozen=True)
class WebError:
    """One normalized failure. ``retryable`` defaults from the error type."""

    type: WebErrorType
    message: str
    http_status: Optional[int] = None
    retryable: Optional[bool] = None
    # Whether the PROVIDER (not the target page) is at fault — drives circuit
    # breaker accounting. Target-side 429/5xx must not open a provider breaker.
    provider_fault: Optional[bool] = None

    def __post_init__(self) -> None:
        if self.retryable is None:
            object.__setattr__(self, "retryable", self.type.retryable_default)
        if self.provider_fault is None:
            object.__setattr__(self, "provider_fault", self.type.provider_fault_default)

    def __str__(self) -> str:
        status = f" HTTP {self.http_status}" if self.http_status else ""
        return f"[{self.type.value}{status}] {self.message}"


class WebToolError(Exception):
    """Exception wrapper for a call-level failure carrying a normalized WebError.

    Used where an operation has no per-item error slot (e.g. starting or
    polling a crawl job) — per-URL/page failures stay inline in result types.
    """

    def __init__(self, error: WebError):
        self.error = error
        super().__init__(str(error))


@dataclass(frozen=True)
class SearchResult:
    """One normalized search hit.

    Search tools keep the existing dict convention at the tool boundary
    (``content_and_artifact``); ``as_dict()`` serializes the model-facing
    content dict only. Frontend cards are assembled by the tool builders —
    that is where ``favicon`` is consumed, so it is deliberately absent
    from ``as_dict()``.
    """

    title: str
    url: str
    content: str = ""
    excerpts: tuple = ()
    published_date: Optional[str] = None
    author: Optional[str] = None
    favicon: Optional[str] = None
    score: Optional[float] = None
    result_type: str = "page"

    def as_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "type": self.result_type,
            "title": self.title,
            "url": self.url,
            "content": self.content or "\n\n".join(self.excerpts),
        }
        if self.published_date:
            d["date"] = self.published_date
        if self.author:
            d["author"] = self.author
        if self.score is not None:
            d["score"] = self.score
        return d


@dataclass(frozen=True)
class FetchRequest:
    """The portable fetch request core — all that crosses a fallback boundary.

    Provider-native extras (Exa subpages, Firecrawl actions, ...) apply only
    while that provider is primary; they never survive into a fallback call.
    ``max_age_seconds``: None = provider default, 0 = force fresh, -1 = cache only.
    """

    urls: List[str]
    objective: Optional[str] = None
    mode: Literal["excerpts", "full", "both"] = "excerpts"
    max_age_seconds: Optional[int] = None


@dataclass(frozen=True)
class FetchResult:
    """One fetched URL — content or a per-URL error, never an exception."""

    url: str
    final_url: Optional[str] = None
    title: Optional[str] = None
    markdown: Optional[str] = None
    excerpts: tuple = ()
    summary: Optional[str] = None
    published_date: Optional[str] = None
    author: Optional[str] = None
    source: Literal["live", "cache"] = "live"
    error: Optional[WebError] = None

    @property
    def ok(self) -> bool:
        return self.error is None


@dataclass
class FetchResponse:
    """Order-preserving results for a FetchRequest, plus routing telemetry."""

    results: List[FetchResult] = field(default_factory=list)
    provider: Optional[str] = None
    providers_tried: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class CrawlRequest:
    """A site-crawl job request. ``limit`` is the hard page cap (billing cap).

    ``include_paths``/``exclude_paths`` are URL-pathname regex patterns
    (Firecrawl semantics); ``query`` is a natural-language steer the provider
    may use to focus discovery.
    """

    url: str
    limit: int
    max_depth: Optional[int] = None
    include_paths: Optional[List[str]] = None
    exclude_paths: Optional[List[str]] = None
    query: Optional[str] = None


@dataclass(frozen=True)
class MapRequest:
    """A URL-discovery request. ``query`` ranks/filters links by relevance."""

    url: str
    query: Optional[str] = None
    limit: int = 100


@dataclass(frozen=True)
class UrlInfo:
    """One discovered URL from a site map."""

    url: str
    title: Optional[str] = None
    description: Optional[str] = None


@dataclass(frozen=True)
class CrawlJob:
    """Handle for a started crawl job."""

    id: str
    provider: str


@dataclass(frozen=True)
class CrawlPage:
    """One crawled page — content or a per-page error, never an exception."""

    url: str
    title: Optional[str] = None
    markdown: Optional[str] = None
    error: Optional[WebError] = None

    @property
    def ok(self) -> bool:
        return self.error is None


class CrawlState(str, Enum):
    """Provider-independent crawl job lifecycle."""

    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self is not CrawlState.RUNNING


@dataclass
class CrawlStatus:
    """One status poll: job progress plus pages not yet delivered.

    ``pages`` contains only pages past the caller's ``skip`` offset, so a
    polling loop can stream pages out incrementally without re-reading
    already-delivered ones.
    """

    state: CrawlState
    total: int = 0
    completed: int = 0
    pages: List[CrawlPage] = field(default_factory=list)
    error: Optional[WebError] = None


@dataclass(frozen=True)
class ResearchRequest:
    """A provider-side deep research request.

    ``output_schema`` (JSON Schema) requests structured output where the
    provider supports it; None means a plain markdown/text report.
    ``max_wait_seconds`` bounds the adapter's internal polling for async
    providers — a run still incomplete at the deadline raises WebToolError.
    """

    query: str
    output_schema: Optional[Dict[str, Any]] = None
    max_wait_seconds: float = 600.0
    poll_interval: float = 5.0


@dataclass(frozen=True)
class ResearchCitation:
    """One source the provider's research run cited."""

    url: str
    title: Optional[str] = None
    excerpts: tuple = ()
    confidence: Optional[str] = None


@dataclass
class ResearchResult:
    """A completed research run, normalized across providers.

    ``report`` is the synthesized text (or JSON-encoded structured output
    when an output_schema was requested). ``usage`` keeps the provider's raw
    cost/usage telemetry verbatim for billing calibration.
    """

    provider: str
    report: str
    citations: List[ResearchCitation] = field(default_factory=list)
    request_id: Optional[str] = None
    model: Optional[str] = None
    response_time: Optional[float] = None
    usage: Optional[Dict[str, Any]] = None
