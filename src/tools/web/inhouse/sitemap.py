"""
Sitemap fetching and summarization for web_fetch.

Discovers sitemap URLs via robots.txt and well-known paths, parses sitemap XML,
and produces a grouped summary for LLM context injection.

Uses httpx + stdlib xml.etree.ElementTree.
"""

import logging
import xml.etree.ElementTree as ET
from typing import Optional
from urllib.parse import urlparse
from collections import defaultdict

import httpx

logger = logging.getLogger(__name__)

_SITEMAP_NS = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
_USER_AGENT = (
    "Mozilla/5.0 (compatible; SitemapFetcher/1.0; "
    "+https://github.com/ginlix/langalpha)"
)


async def _find_sitemap_url(client: httpx.AsyncClient, domain: str) -> list[str]:
    """Discover sitemap URLs from robots.txt, falling back to well-known paths."""
    sitemap_urls: list[str] = []

    # Try robots.txt first
    for scheme in ("https", "http"):
        try:
            r = await client.get(f"{scheme}://{domain}/robots.txt")
            if 200 <= r.status_code < 300:
                for line in r.text.splitlines():
                    if line.lower().startswith("sitemap:"):
                        url = line.split(":", 1)[1].strip()
                        if url:
                            sitemap_urls.append(url)
                if sitemap_urls:
                    return sitemap_urls
        except httpx.HTTPError:
            continue

    # Fallback: try well-known paths
    for scheme in ("https", "http"):
        for path in ("/sitemap.xml", "/sitemap_index.xml"):
            try:
                r = await client.head(f"{scheme}://{domain}{path}")
                if 200 <= r.status_code < 300:
                    return [f"{scheme}://{domain}{path}"]
            except httpx.HTTPError:
                continue

    return []


async def _parse_sitemap(
    client: httpx.AsyncClient,
    url: str,
    urls: list[str],
    max_urls: int,
    max_depth: int = 2,
) -> None:
    """
    Fetch and parse a sitemap XML, collecting <loc> URLs.

    Handles both regular sitemaps and sitemap indexes (recursive).
    Stops collecting once max_urls is reached.
    """
    if len(urls) >= max_urls or max_depth <= 0:
        return

    try:
        r = await client.get(url)
        if r.status_code < 200 or r.status_code >= 300:
            return
    except httpx.HTTPError:
        return

    try:
        root = ET.fromstring(r.content)
    except ET.ParseError:
        logger.debug(f"Failed to parse sitemap XML from {url}")
        return

    tag = root.tag.lower()

    # Sitemap index — contains <sitemap><loc>...</loc></sitemap> entries
    if "sitemapindex" in tag:
        sub_urls = []
        for sitemap_el in root.iter(f"{_SITEMAP_NS}sitemap"):
            loc = sitemap_el.findtext(f"{_SITEMAP_NS}loc")
            if loc:
                sub_urls.append(loc.strip())
        # Also try without namespace (some sitemaps omit it)
        if not sub_urls:
            for sitemap_el in root.iter("sitemap"):
                loc = sitemap_el.findtext("loc")
                if loc:
                    sub_urls.append(loc.strip())

        # Fetch sub-sitemaps sequentially to avoid orphaned tasks
        for sub_url in sub_urls:
            if len(urls) >= max_urls:
                break
            await _parse_sitemap(client, sub_url, urls, max_urls, max_depth - 1)
    else:
        # Regular sitemap — contains <url><loc>...</loc></url> entries
        for url_el in root.iter(f"{_SITEMAP_NS}url"):
            if len(urls) >= max_urls:
                break
            loc = url_el.findtext(f"{_SITEMAP_NS}loc")
            if loc:
                urls.append(loc.strip())
        # Also try without namespace
        if not urls:
            for url_el in root.iter("url"):
                if len(urls) >= max_urls:
                    break
                loc = url_el.findtext("loc")
                if loc:
                    urls.append(loc.strip())


async def fetch_sitemap_urls(domain: str, max_urls: int = 100, timeout: float = 10.0) -> list[dict]:
    """
    Fetch sitemap URLs for a domain.

    Args:
        domain: Domain to fetch sitemap from (e.g., "example.com")
        max_urls: Maximum number of URLs to return
        timeout: Timeout in seconds for the entire operation (default: 10s)

    Returns:
        List of dicts with "url" key.
        Returns empty list if sitemap unavailable or times out.
    """
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(timeout, connect=5.0),
            follow_redirects=True,
            headers={"User-Agent": _USER_AGENT},
        ) as client:
            sitemap_urls = await _find_sitemap_url(client, domain)
            if not sitemap_urls:
                logger.debug(f"No sitemap found for {domain}")
                return []

            urls: list[str] = []
            for sitemap_url in sitemap_urls:
                if len(urls) >= max_urls:
                    break
                await _parse_sitemap(client, sitemap_url, urls, max_urls)

            logger.debug(f"Fetched {len(urls)} URLs from sitemap for {domain}")
            return [{"url": u} for u in urls]

    except httpx.TimeoutException:
        logger.debug(f"Sitemap fetch timed out for {domain} (>{timeout}s)")
        return []
    except Exception as e:
        logger.debug(f"Sitemap fetch failed for {domain}: {e}")
        return []


def summarize_sitemap(urls: list[dict], max_examples: int = 3) -> str:
    """
    Create high-level summary of sitemap URLs grouped by path prefix.

    Groups URLs by their first path segment and shows representative
    examples with titles if available.

    Args:
        urls: List of dicts with "url" key and optional "title"/"head_data"
        max_examples: Maximum example URLs per path prefix

    Returns:
        Formatted string summary of site structure
    """
    if not urls:
        return ""

    # Group by first path segment
    groups = defaultdict(list)
    for item in urls:
        url = item.get("url", "")
        if not url:
            continue
        path = urlparse(url).path
        # Extract first path segment as prefix
        segments = path.strip("/").split("/")
        prefix = "/" + segments[0] if segments and segments[0] else "/"
        groups[prefix].append(item)

    if not groups:
        return ""

    # Format summary, sorted by count descending
    lines = []
    for prefix, items in sorted(groups.items(), key=lambda x: -len(x[1])):
        examples = items[:max_examples]
        example_strs = []
        for e in examples:
            url_str = e.get("url", "")
            # Try to get title from head_data or direct title field
            title = ""
            if e.get("head_data") and e["head_data"].get("title"):
                title = e["head_data"]["title"][:40]
            elif e.get("title"):
                title = e["title"][:40]

            if title:
                example_strs.append(f"{url_str} ({title})")
            else:
                example_strs.append(url_str)

        lines.append(f"- {prefix} ({len(items)} pages): {', '.join(example_strs)}")

    return "\n".join(lines)


async def get_sitemap_summary(
    url: str,
    max_urls: Optional[int] = None,
    max_examples: Optional[int] = None,
) -> str:
    """
    Get sitemap summary for a URL's domain.

    Fetches sitemap and creates a high-level summary suitable for
    injection into LLM context.

    Args:
        url: Any URL from the target domain
        max_urls: Maximum URLs to fetch (default from config or 100)
        max_examples: Max examples per prefix (default from config or 3)

    Returns:
        Formatted sitemap summary, or empty string if unavailable
    """
    from src.config.tool_settings import (
        is_sitemap_enabled,
        get_sitemap_max_urls,
        get_sitemap_max_examples,
    )

    # Check if sitemap is enabled
    if not is_sitemap_enabled():
        return ""

    # Skip sitemap fetch if crawler circuit breaker is open
    # This reduces load when the crawler is having issues
    try:
        from .safe_wrapper import get_safe_crawler_sync
        safe_crawler = get_safe_crawler_sync()
        if not safe_crawler.is_healthy():
            logger.debug("Skipping sitemap fetch - crawler circuit is open")
            return ""
    except Exception:
        pass  # Continue if safe_wrapper not available

    if max_urls is None:
        max_urls = get_sitemap_max_urls()
    if max_examples is None:
        max_examples = get_sitemap_max_examples()

    try:
        domain = urlparse(url).netloc
        if not domain:
            return ""

        # Fetch sitemap URLs
        urls = await fetch_sitemap_urls(domain, max_urls)
        if not urls:
            return ""

        # Generate summary
        summary = summarize_sitemap(urls, max_examples)
        if summary:
            logger.debug(f"Generated sitemap summary for {domain} ({len(urls)} URLs)")
        return summary

    except Exception as e:
        logger.debug(f"Sitemap summary failed for {url}: {e}")
        return ""
