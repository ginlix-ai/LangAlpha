"""Web fetch: the shared FetchService and the agent-facing WebFetch tool.

FetchService owns what stays provider-independent: URL normalization and the
Redis markdown cache around the provider chain (FetchRouter, which also owns
routing dedicated-extractor URLs in-house). The tool layer keeps the
agent-facing contract: WebFetch(url, prompt), the extraction policy
(provider-native excerpts when available, otherwise our extraction LLM), and
sitemap-based alternative-URL suggestions.

The chain comes from ``fetch_chain`` in agent_config.yaml and defaults to
``["inhouse"]`` — third-party delegation ships dark until config flips it.
"""

import asyncio
import hashlib
import logging
from contextvars import ContextVar
from typing import Annotated, Any, List, Optional

from langchain_core.tools import StructuredTool

from src.config.core import find_config_file, load_yaml_config
from src.llms import LLM, format_llm_content, make_api_call, maybe_disable_streaming
from src.tools.web.inhouse.sitemap import get_sitemap_summary
from src.tools.decorators import log_io
from src.tools.web.router import FetchRouter
from src.observability import stamp_run
from src.tools.web.types import (
    FetchAttempt,
    FetchRequest,
    FetchResponse,
    FetchResult,
    WebError,
    WebErrorType,
)

logger = logging.getLogger(__name__)

CACHE_TTL = 900  # 15 minutes, same as the legacy web_fetch cache
CACHE_PREFIX = "web_fetch"

_DEFAULT_CHAIN = ["inhouse"]

# Extraction model configuration
EXTRACTION_TIMEOUT = 60.0  # seconds per model attempt

# Per-request overrides for extraction model (set by chat handler from user preferences)
fetch_model_override: ContextVar[str | None] = ContextVar("fetch_model_override", default=None)
fetch_llm_client_override: ContextVar[Any] = ContextVar("fetch_llm_client_override", default=None)

EXTRACTION_SYSTEM_PROMPT = """You are a web content extraction assistant.
Your task is to extract specific information from webpage content based on the user's prompt.

Guidelines:
- Focus only on information relevant to the user's prompt
- Provide concise, well-structured responses
- Preserve important details like numbers, names, and dates
- Format output in a readable manner (use markdown if helpful)
- If the requested information is not found on this page:
  - Clearly state that the information was not found
  - If a site structure is provided, suggest alternative URLs that might contain the information
  - Format suggestions as: "The information might be found at: [URL1], [URL2]"
"""


def _normalize_url(url: str) -> str:
    """Upgrade HTTP to HTTPS."""
    if url.startswith("http://"):
        return "https://" + url[7:]
    return url


def _cache_key(url: str) -> str:
    return f"{CACHE_PREFIX}:{hashlib.md5(url.encode()).hexdigest()}"


def get_fetch_chain() -> List[str]:
    """Read fetch_chain from agent_config.yaml; default ships dark (inhouse)."""
    try:
        from src.config.tool_settings import _get_agent_config_dict

        chain = _get_agent_config_dict().get("fetch_chain")
        if isinstance(chain, list) and chain:
            return [str(p) for p in chain]
    except Exception as e:
        logger.warning(f"Failed to read fetch_chain from agent config: {e}")
    return list(_DEFAULT_CHAIN)


async def _get_cache_client():
    try:
        from src.utils.cache import get_cache_client

        cache = get_cache_client()
        if not cache.client:
            await cache.connect()
        return cache
    except Exception as e:
        logger.debug(f"Cache not available: {e}")
        return None


class FetchService:
    """Fetch URLs through the provider chain with markdown caching."""

    def __init__(self, chain: Optional[List[str]] = None):
        self._router = FetchRouter(chain or get_fetch_chain())

    @property
    def provider_names(self) -> List[str]:
        return self._router.provider_names

    async def fetch(self, req: FetchRequest) -> FetchResponse:
        """Fetch with cache + pre-routing. Results preserve request order.

        Only full markdown is cached (excerpts are objective-specific).
        ``max_age_seconds=0`` bypasses the cache read; ``-1`` is cache-only.
        """
        urls = [_normalize_url(u) for u in req.urls]
        results: dict[str, FetchResult] = {}

        cache = None
        if req.max_age_seconds != 0:
            cache = await _get_cache_client()
            if cache:
                unique = list(dict.fromkeys(urls))
                cached_values = await asyncio.gather(
                    *(cache.get(_cache_key(u)) for u in unique)
                )
                for url, cached in zip(unique, cached_values):
                    if cached:
                        results[url] = FetchResult(url=url, markdown=cached, source="cache")

        remaining = [u for u in urls if u not in results]
        attempts: List[FetchAttempt] = []
        provider: Optional[str] = None

        if remaining and req.max_age_seconds != -1:
            resp = await self._router.fetch(
                FetchRequest(
                    urls=remaining,
                    objective=req.objective,
                    mode=req.mode,
                    max_age_seconds=req.max_age_seconds,
                )
            )
            attempts = resp.attempts
            provider = resp.provider
            for result in resp.results:
                results[result.url] = result

            if cache is None and req.max_age_seconds != 0:
                cache = await _get_cache_client()
            if cache:
                await asyncio.gather(
                    *(
                        cache.set(_cache_key(r.url), r.markdown, ttl=CACHE_TTL)
                        for r in results.values()
                        if r.ok and r.markdown and r.source == "live"
                    )
                )

        for url in urls:
            if url not in results:  # cache-only miss
                results[url] = FetchResult(
                    url=url,
                    error=WebError(
                        type=WebErrorType.EMPTY, message="Not in cache", retryable=False
                    ),
                )

        ordered = [results[u] for u in urls]
        return FetchResponse(results=ordered, provider=provider, attempts=attempts)


_fetch_service: Optional[FetchService] = None


def get_fetch_service() -> FetchService:
    """Process-wide FetchService built from agent config (lazy singleton)."""
    global _fetch_service
    if _fetch_service is None:
        _fetch_service = FetchService()
    return _fetch_service


def reset_fetch_service() -> None:
    """Drop the singleton (tests / config reload)."""
    global _fetch_service
    _fetch_service = None


def _get_extraction_model() -> str:
    """Get the configured extraction model.
    Priority: context override (user pref) > agent_config.yaml llm.fetch > llm.flash > llm.name.
    """
    override = fetch_model_override.get()
    if override:
        return override
    path = find_config_file("agent_config.yaml")
    config = load_yaml_config(str(path)) if path else {}
    llm = config.get("llm", {})
    return llm.get("fetch") or llm.get("flash") or llm.get("name", "")


def _with_sitemap_suggestions(message: str, sitemap_summary: str) -> str:
    """Append alternative-URL suggestions when a fetch came back unusable."""
    if not sitemap_summary:
        return message
    return (
        f"{message}\n\n"
        f"Here are other pages available on this site:\n\n"
        f"{sitemap_summary}\n\n"
        f"Try fetching one of these alternative URLs instead."
    )


def _compose_native_output(result: FetchResult) -> str:
    """Format provider-extracted content (summary/excerpts) — no LLM call."""
    parts = []
    if result.title:
        parts.append(f"# {result.title}")
    if result.summary:
        parts.append(result.summary)
    if result.excerpts:
        parts.append("Relevant excerpts from the page:\n\n" + "\n\n".join(result.excerpts))
    return "\n\n".join(parts)


async def _extract_with_llm(
    markdown: str,
    prompt: str,
    model: str,
    sitemap_summary: str = "",
) -> str:
    """
    Extract information from markdown content using the codebase LLM.

    Args:
        markdown: The webpage content in markdown format
        prompt: The extraction prompt from the user
        model: The LLM model name from models.json
        sitemap_summary: Optional site structure summary for URL suggestions

    Returns:
        Extracted content based on the prompt
    """
    # Build user prompt with optional sitemap context
    if sitemap_summary:
        user_prompt = f"""Extract information from this webpage based on the following prompt.

**Prompt:** {prompt}

**Site Structure:** (other pages available on this domain)
{sitemap_summary}

**Webpage Content:**
{markdown}
"""
    else:
        user_prompt = f"""Extract information from this webpage based on the following prompt.

**Prompt:** {prompt}

**Webpage Content:**
{markdown}
"""

    client_override = fetch_llm_client_override.get()
    if client_override is not None:
        # Deep-copy: maybe_disable_streaming mutates .streaming in place and
        # the override may be a shared instance from config.subsidiary_llm_clients.
        # Mirrors thread_maintenance.py::compact.
        llm = client_override.model_copy()
    else:
        llm = LLM(model).get_llm()

    # Disable streaming to keep extraction chunks off the agent's SSE stream,
    # EXCEPT for Codex whose proxy rejects stream=false outright.
    maybe_disable_streaming(llm)

    # Apply timeout for extraction
    result = await asyncio.wait_for(
        make_api_call(
            llm=llm,
            system_prompt=EXTRACTION_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            disable_tracing=True,
        ),
        timeout=EXTRACTION_TIMEOUT,
    )

    # Normalize content: extract text only, discard reasoning
    formatted = format_llm_content(result)
    return formatted["text"]


async def web_fetch(
    url: str,
    prompt: str,
    model: Optional[str] = None,
    use_cache: bool = True,
    include_sitemap: bool = True,
) -> tuple[str, dict]:
    """
    Fetch content from a URL and process it using an AI model.

    Fully async for true concurrency support. Multiple calls to this
    function will execute in parallel.

    Args:
        url: The URL to fetch content from
        prompt: The prompt to run on the fetched content
        model: LLM model to use for extraction (default: from agent_config.yaml llm.flash)
        use_cache: Whether to use Redis cache (default: True)
        include_sitemap: Include site structure for URL suggestions (default: True)

    Returns:
        ``(content, artifact)``: the model-facing text plus a metadata artifact
        (url, title, serving provider, live/cache source; ``error`` kind on
        failure) consumed by provenance and the frontend, never by the LLM.
    """
    # Use configured model if not specified
    if model is None:
        model = _get_extraction_model()

    artifact: dict = {"type": "web_fetch", "url": url}
    try:
        service = get_fetch_service()
        request = FetchRequest(
            urls=[url],
            objective=prompt,
            # Ask for native extraction: providers that treat the prompt as
            # their server-side objective return targeted excerpts/summary,
            # which skips the extraction LLM call below.
            mode="excerpts",
            max_age_seconds=0 if not use_cache else None,
        )
        logger.debug(
            f"Fetching {url} via chain {service.provider_names} (model: {model})"
        )

        # Start the sitemap lookup alongside the fetch, but await it only on
        # the paths that use it — the native-excerpts fast path must not pay
        # for a sitemap it discards.
        sitemap_task = (
            asyncio.create_task(get_sitemap_summary(url)) if include_sitemap else None
        )

        async def _sitemap() -> str:
            if sitemap_task is None:
                return ""
            try:
                return await sitemap_task
            except Exception as e:
                logger.debug(f"Sitemap fetch failed: {e}")
                return ""

        try:
            response = await service.fetch(request)
            result = response.results[0]
            # The service normalizes URLs (HTTP→HTTPS); adopt its canonical one.
            url = result.url
            artifact["url"] = url
            artifact["provider"] = response.provider
            artifact["attempts"] = [str(a) for a in response.attempts]
            artifact["source"] = result.source

            if result.error is not None:
                kind = result.error.type.value
                artifact["error"] = kind
                return _with_sitemap_suggestions(
                    f"[{kind}] Failed to fetch {url}: {result.error.message}",
                    await _sitemap(),
                ), artifact

            artifact["title"] = result.title

            # Extraction policy: a provider that understood the prompt as its
            # native objective already returned targeted excerpts/summary — use
            # them directly and skip the extraction LLM call.
            if result.excerpts or result.summary:
                return _compose_native_output(result), artifact

            markdown = result.markdown
            if not markdown or len(markdown.strip()) < 50:
                artifact["error"] = "empty"
                sitemap_summary = await _sitemap()
                if sitemap_summary:
                    return _with_sitemap_suggestions(
                        f"The page at {url} appears to be empty or blocked.", sitemap_summary
                    ), artifact
                return (
                    f"Failed to fetch content from {url}. The page may be empty or blocked.",
                    artifact,
                )

            # Extract with LLM (with sitemap context)
            extracted = await _extract_with_llm(markdown, prompt, model, await _sitemap())
            return extracted, artifact
        finally:
            if sitemap_task is not None and not sitemap_task.done():
                sitemap_task.cancel()

    except Exception as e:
        # Catch-all for unexpected errors (LLM extraction, cache)
        # Note: FetchRouter maps provider failures to per-URL errors internally
        logger.error(f"Failed to process {url}. Error: {repr(e)}")
        short_error = str(e).split('\n')[0][:100]
        artifact["error"] = "exception"
        return f"Failed to process {url}: {short_error}", artifact


# Create async tool using StructuredTool.from_function with coroutine
async def _web_fetch_tool_impl(
    url: Annotated[str, "A fully-formed URL. HTTP is upgraded to HTTPS automatically."],
    prompt: Annotated[str, "What information to extract from the page."],
) -> tuple[str, dict]:
    """Delegate to web_fetch(); the agent-facing contract is ``description``.

    Stamps here rather than inside web_fetch() because this is the one point
    every branch — native excerpts, empty page, per-URL error, catch-all —
    passes through on its way out of the tool run.
    """
    content, artifact = await web_fetch(url=url, prompt=prompt)
    provider = artifact.get("provider")
    stamp_run(
        tags=[f"fetch_provider:{provider}"] if provider else None,
        fetch_provider=provider,
        fetch_attempts=artifact.get("attempts") or None,
        fetch_source=artifact.get("source"),
        fetch_error=artifact.get("error"),
    )
    return content, artifact


# Apply decorator and create tool
_decorated_impl = log_io(_web_fetch_tool_impl)

web_fetch_tool = StructuredTool.from_function(
    coroutine=_decorated_impl,
    name="WebFetch",
    response_format="content_and_artifact",
    description="""Fetches a URL and answers a question about its content using a small, fast model.

Handles regular web pages (tiered fetching with anti-bot bypass), PDFs,
YouTube transcripts and X/Twitter posts. Run several in parallel when you
need more than one page.

Returns:
    The model's answer to your prompt, not the raw page. Long content is
    summarised, and when the answer is not on the page you get alternative
    URLs from the site's sitemap instead.

A URL that redirects to a different host returns the redirect URL rather than
content; call again with that URL.""",
)
