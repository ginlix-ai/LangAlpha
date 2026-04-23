"""
Web fetch tool with LLM-based content extraction.

This module provides a web_fetch tool that fetches content from a URL
and processes it using an AI model, similar to Claude's WebFetch tool.

Features:
- Async-first API for true concurrency support
- Redis caching with 15-minute TTL
- Batch fetching with MemoryAdaptiveDispatcher
- Provider-agnostic content extraction
- Sitemap-aware extraction for URL suggestions
"""

import asyncio
import hashlib
import logging
from contextvars import ContextVar
from typing import Annotated, Any, Optional

from langchain_core.tools import StructuredTool

from .decorators import log_io
from .crawler.safe_wrapper import get_safe_crawler_sync, CrawlResult
from .crawler.sitemap import get_sitemap_summary
from src.llms import LLM, make_api_call, maybe_disable_streaming, format_llm_content
from src.config.core import load_yaml_config, find_config_file

logger = logging.getLogger(__name__)

# Cache configuration
CACHE_TTL = 900  # 15 minutes
CACHE_PREFIX = "web_fetch"

# Extraction model configuration
EXTRACTION_TIMEOUT = 60.0  # seconds per model attempt

# Per-request overrides for extraction model (set by chat handler from user preferences)
fetch_model_override: ContextVar[str | None] = ContextVar("fetch_model_override", default=None)
fetch_llm_client_override: ContextVar[Any] = ContextVar("fetch_llm_client_override", default=None)


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


def _get_cache_key(url: str) -> str:
    """Generate cache key for URL."""
    url_hash = hashlib.md5(url.encode()).hexdigest()
    return f"{CACHE_PREFIX}:{url_hash}"


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
    """
    Normalize URL: upgrade HTTP to HTTPS.

    Args:
        url: The original URL

    Returns:
        Normalized URL with HTTPS
    """
    if url.startswith("http://"):
        return "https://" + url[7:]
    return url


async def _get_cache_client():
    """Get and connect cache client if available."""
    try:
        from src.utils.cache import get_cache_client
        cache = get_cache_client()
        if not cache.client:
            await cache.connect()
        return cache
    except Exception as e:
        logger.debug(f"Cache not available: {e}")
        return None


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
        # Mirrors workflow_handler.py::compact. See workflow_handler.py:514-526.
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
) -> str:
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
        The model's response about the content, or error/redirect message
    """
    # Use configured model if not specified
    if model is None:
        model = _get_extraction_model()

    try:
        # Normalize URL (upgrade HTTP to HTTPS)
        url = _normalize_url(url)
        cache_key = _get_cache_key(url)

        # Check cache first
        cache = None
        cached_markdown = None
        if use_cache:
            cache = await _get_cache_client()
            if cache:
                cached_markdown = await cache.get(cache_key)
                if cached_markdown:
                    logger.debug(f"Cache hit for {url}")
                    # Still fetch sitemap for URL suggestions (parallel with extraction prep)
                    sitemap_summary = ""
                    if include_sitemap:
                        sitemap_summary = await get_sitemap_summary(url)
                    return await _extract_with_llm(cached_markdown, prompt, model, sitemap_summary)

        # Crawl first, then extract with LLM
        # Uses SafeCrawlerWrapper for circuit breaker and fault tolerance
        logger.debug(f"Fetching {url} with standard extraction (model: {model})")

        # Get safe crawler with circuit breaker protection
        safe_crawler = get_safe_crawler_sync()

        # Fetch sitemap in parallel with URL crawl (no added latency)
        if include_sitemap:
            sitemap_task = get_sitemap_summary(url)
            crawl_task = safe_crawler.crawl(url)
            sitemap_summary, crawl_result = await asyncio.gather(
                sitemap_task, crawl_task, return_exceptions=True
            )
            # Handle sitemap exceptions
            if isinstance(sitemap_summary, Exception):
                logger.debug(f"Sitemap fetch failed: {sitemap_summary}")
                sitemap_summary = ""

            # Handle crawl exceptions (shouldn't happen with safe wrapper, but be defensive)
            if isinstance(crawl_result, Exception):
                crawl_result = CrawlResult(
                    success=False,
                    error=str(crawl_result)[:200],
                    error_type="unexpected_error",
                )

            # Check crawl result
            if not crawl_result.success:
                # If sitemap available, suggest alternatives
                if sitemap_summary:
                    return (
                        f"Failed to fetch {url}: {crawl_result.error}\n\n"
                        f"However, here are other pages available on this site:\n\n"
                        f"{sitemap_summary}\n\n"
                        f"Try fetching one of these alternative URLs instead."
                    )
                else:
                    return f"Failed to fetch {url}: {crawl_result.error}"

            markdown = crawl_result.markdown
        else:
            crawl_result = await safe_crawler.crawl(url)
            if not crawl_result.success:
                return f"Failed to fetch {url}: {crawl_result.error}"
            markdown = crawl_result.markdown
            sitemap_summary = ""

        if not markdown or len(markdown.strip()) < 50:
            # Empty page - include sitemap suggestions if available
            if sitemap_summary:
                return (
                    f"The page at {url} appears to be empty or blocked.\n\n"
                    f"Here are other pages available on this site:\n\n"
                    f"{sitemap_summary}\n\n"
                    f"Try fetching one of these alternative URLs instead."
                )
            return f"Failed to fetch content from {url}. The page may be empty or blocked."

        # Store in cache
        if cache and use_cache:
            await cache.set(cache_key, markdown, ttl=CACHE_TTL)
            logger.debug(f"Cached content for {url} (TTL: {CACHE_TTL}s)")

        # Extract with LLM (with sitemap context)
        extracted = await _extract_with_llm(markdown, prompt, model, sitemap_summary)
        return extracted

    except Exception as e:
        # Catch-all for unexpected errors (LLM extraction, cache)
        # Note: SafeCrawlerWrapper handles crawl errors internally
        logger.error(f"Failed to process {url}. Error: {repr(e)}")
        short_error = str(e).split('\n')[0][:100]
        return f"Failed to process {url}: {short_error}"


# Create async tool using StructuredTool.from_function with coroutine
async def _web_fetch_tool_impl(
    url: Annotated[str, "The URL to fetch content from"],
    prompt: Annotated[str, "The prompt to run on the fetched content"],
) -> str:
    """
    Fetches content from a specified URL and processes it using an AI model.

    Takes a URL and a prompt as input. Fetches the URL content, converts HTML
    to markdown, then processes the content with the prompt using a small,
    fast model. Returns the model's response about the content.

    Use this tool when you need to retrieve and analyze web content.

    Usage notes:
    - The URL must be a fully-formed valid URL
    - HTTP URLs will be automatically upgraded to HTTPS
    - The prompt should describe what information you want to extract from the page
    - Results may be summarized if the content is very large
    - When a URL redirects to a different host, the tool will inform you and
      provide the redirect URL. You should then make a new request with the
      redirect URL to fetch the content.
    """
    return await web_fetch(url=url, prompt=prompt)


# Apply decorator and create tool
_decorated_impl = log_io(_web_fetch_tool_impl)

web_fetch_tool = StructuredTool.from_function(
    coroutine=_decorated_impl,
    name="WebFetch",
    description="""Fetches content from a specified URL and processes it using an AI model.

Takes a URL and a prompt as input. Fetches the URL content, converts to
markdown, then processes the content with the prompt using a small,
fast model. Returns the model's response about the content.

Supports multiple content types with dedicated extractors:
- Regular web pages: tiered HTML fetching with anti-bot bypass
- URL-based PDF files: text extraction (no LLM needed for parsing)
- YouTube videos: transcript extraction with timestamps
- X/Twitter posts: tweet text, media, and engagement stats


Usage notes:
- Run multiple in parallel if needed
- The URL must be a fully-formed valid URL
- HTTP URLs will be automatically upgraded to HTTPS
- The prompt should describe what information you want to extract from the page
- Results may be summarized if the content is very large
- If the requested information is not found, the tool will suggest alternative
  URLs from the site's sitemap that might contain the information
- When a URL redirects to a different host, the tool will inform you and
  provide the redirect URL. You should then make a new request with the
  redirect URL to fetch the content.""",
)
