"""Site crawl tools: WebCrawl (dump-first async crawl) and WebMap (URL discovery).

PTC-only: the tools are built per-session bound to the agent's filesystem
backend so crawled pages dump into the sandbox workspace instead of the
context window. Crawl bills per delivered page (the manifest level's ``credits``)
through the dynamic tool tracker as pages arrive — a cancelled crawl bills
only what was delivered. Tier gating happens at resolve time (server) via the
crawl capability's ``min_tier``.
"""

import asyncio
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from langchain_core.tools import tool

from src.config.tool_settings import get_crawl_provider
from src.tools.decorators import create_logged_tool, get_tool_tracker
from src.tools.web.manifest import (
    CAPABILITY_CRAWL,
    CAPABILITY_MAP,
    get_capability,
    get_web_provider_spec,
)
from src.tools.web.types import CrawlPage, CrawlRequest, CrawlState, MapRequest, WebToolError

logger = logging.getLogger(__name__)

_MAX_CRAWL_PAGES = 100  # hard billing-exposure cap on `limit`
_MAX_MAP_LINKS = 300
_POLL_INTERVAL = 3.0
_CRAWL_TIMEOUT = 600.0  # wall cap on one crawl job
_MAX_POLL_FAILURES = 3  # consecutive status-poll errors before giving up
_DEFAULT_OUTPUT_DIR = "work/crawl"


def _build_firecrawl_adapter():
    from src.tools.web.providers.firecrawl import FirecrawlCrawlAdapter

    return FirecrawlCrawlAdapter()


_ADAPTER_BUILDERS = {"firecrawl": _build_firecrawl_adapter}


def _host_dirname(url: str) -> str:
    netloc = urlparse(url).netloc or "site"
    return re.sub(r"[^A-Za-z0-9.-]+", "_", netloc)


def _page_filename(page_url: str, used: set) -> str:
    """Stable, collision-free .md filename for one crawled page URL."""
    path = urlparse(page_url).path.strip("/")
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", path).strip("-.")[:120] or "index"
    name = f"{slug}.md"
    n = 2
    while name in used:
        name = f"{slug}-{n}.md"
        n += 1
    used.add(name)
    return name


async def _write_workspace_file(backend: Any, rel_path: str, content: str) -> Optional[str]:
    """Write through the agent filesystem backend; returns an error string on failure."""
    normalized = backend.normalize_path(rel_path)
    if backend.filesystem_config.enable_path_validation and not backend.validate_path(normalized):
        return f"Access denied: {rel_path} is not in allowed directories"
    ok = await backend.awrite_text(normalized, content)
    return None if ok else f"Failed to write {rel_path}"


async def _dump_pages(
    backend: Any,
    pages: List[CrawlPage],
    dest_dir: str,
    used_names: set,
    index_entries: List[Dict[str, Any]],
) -> int:
    """Write one .md file per ok page (writes run concurrently) and append
    every page's index entry (failures included). Returns the pages dumped."""
    # Filenames are assigned sequentially — they share the collision set.
    named = {i: _page_filename(p.url, used_names) for i, p in enumerate(pages) if p.ok}
    # A poll window can deliver up to the crawl limit at once; don't slam the
    # sandbox filesystem with 100 concurrent writes.
    write_sem = asyncio.Semaphore(8)

    async def _write(page: CrawlPage, fname: str) -> Optional[str]:
        header = f"# {page.title or page.url}\n\nSource: {page.url}\n\n---\n\n"
        async with write_sem:
            return await _write_workspace_file(
                backend, f"{dest_dir}/{fname}", header + (page.markdown or "")
            )

    results = await asyncio.gather(
        *(_write(pages[i], f) for i, f in named.items()), return_exceptions=True
    )
    errs: Dict[int, Optional[str]] = {}
    for i, r in zip(named, results):
        if isinstance(r, asyncio.CancelledError):
            raise r
        errs[i] = str(r)[:200] if isinstance(r, BaseException) else r
    new_ok = 0
    for i, page in enumerate(pages):
        if not page.ok:
            index_entries.append({"url": page.url, "error": str(page.error)})
        elif errs[i] is None:
            new_ok += 1
            index_entries.append({"url": page.url, "title": page.title, "file": named[i]})
        else:
            index_entries.append({"url": page.url, "error": errs[i]})
    return new_ok


def create_crawl_tools(filesystem_backend: Any) -> List[Any]:
    """Build WebCrawl + WebMap bound to ``filesystem_backend``.

    Returns [] when the configured provider has no crawl capability, no
    adapter, or its API key is unset — deployments without a key never
    register the tools.
    """
    provider = get_crawl_provider()
    cap = get_capability(provider, CAPABILITY_CRAWL)
    spec = get_web_provider_spec(provider)
    builder = _ADAPTER_BUILDERS.get(provider)
    if cap is None or spec is None or builder is None:
        logger.warning(f"Crawl provider {provider!r} has no crawl capability/adapter; skipping")
        return []
    if spec.env_key and not os.getenv(spec.env_key):
        logger.debug(f"Crawl provider {provider!r} key {spec.env_key} unset; skipping tools")
        return []

    adapter = builder()
    level = cap.default_level_spec
    crawl_tracking = cap.tracking_key(level)
    map_cap = get_capability(provider, CAPABILITY_MAP)

    @tool("WebCrawl", response_format="content_and_artifact")
    async def web_crawl(
        url: str,
        limit: int = 25,
        query: Optional[str] = None,
        include_paths: Optional[List[str]] = None,
        exclude_paths: Optional[List[str]] = None,
        max_depth: Optional[int] = None,
        output_dir: Optional[str] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        """Crawl a website and dump each page as markdown into the workspace.

        Starts an asynchronous crawl at ``url``, follows internal links up to
        ``limit`` pages, and writes one .md file per page plus an index.jsonl
        manifest. Read the dumped files with glob/read_file afterwards — page
        content is never returned inline. Scope a site with WebMap first, and
        prefer WebFetch when you already know the handful of URLs you need.
        Crawling bills per delivered page, so keep ``limit`` tight and focus
        the crawl with include_paths/exclude_paths.

        Args:
            url: Starting URL; the crawl stays within its site.
            limit: Hard page cap (default 25, max 100).
            query: Natural-language steer for what the crawl should focus on.
            include_paths: URL-pathname regex patterns to include (e.g. ["^/docs/.*"]).
            exclude_paths: URL-pathname regex patterns to exclude.
            max_depth: Max link-discovery depth from the start URL.
            output_dir: Workspace directory to dump into; pages land in
                <output_dir>/<host>/. Defaults to work/crawl. Pass your task's
                work dir (e.g. work/<task>/crawl) when running a task workflow.
        """
        limit_ = max(1, min(int(limit), _MAX_CRAWL_PAGES))
        base_dir = (output_dir or _DEFAULT_OUTPUT_DIR).rstrip("/")
        dest_dir = f"{base_dir}/{_host_dirname(url)}"
        index_path = f"{dest_dir}/index.jsonl"

        req = CrawlRequest(
            url=url,
            limit=limit_,
            max_depth=max_depth,
            include_paths=include_paths,
            exclude_paths=exclude_paths,
            query=query,
        )
        try:
            job = await adapter.start_crawl(req, dict(level.native_params))
        except WebToolError as e:
            return f"Crawl failed to start: {e}", {"type": "site_crawl", "url": url, "error": str(e)}

        index_entries: List[Dict[str, Any]] = []
        used_names: set = set()
        ok_pages = 0
        skip = 0
        state = CrawlState.RUNNING
        note = ""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _CRAWL_TIMEOUT
        poll_failures = 0

        try:
            while True:
                await asyncio.sleep(_POLL_INTERVAL)
                try:
                    status = await adapter.crawl_status(job.id, skip=skip)
                    poll_failures = 0
                except WebToolError as e:
                    poll_failures += 1
                    if poll_failures >= _MAX_POLL_FAILURES:
                        await adapter.cancel_crawl(job.id)
                        state = CrawlState.CANCELLED
                        note = f"Aborted after repeated status failures: {e}"
                        break
                    continue

                skip += len(status.pages)
                prev_entries = len(index_entries)
                new_ok = await _dump_pages(
                    filesystem_backend, status.pages, dest_dir, used_names, index_entries
                )
                if len(index_entries) > prev_entries:
                    await _write_workspace_file(
                        filesystem_backend,
                        index_path,
                        "\n".join(json.dumps(e, ensure_ascii=False) for e in index_entries) + "\n",
                    )
                ok_pages += new_ok
                # Meter the pages Firecrawl delivered (and billed us for) as they
                # arrive, so a cancelled crawl still bills exactly what was
                # delivered — and a failed local write never yields a free page.
                delivered_ok = sum(1 for p in status.pages if p.ok)
                if delivered_ok:
                    tracker = get_tool_tracker()
                    if tracker:
                        tracker.record_usage(crawl_tracking, count=delivered_ok)

                if status.state.terminal:
                    state = status.state
                    if status.error:
                        note = str(status.error)
                    break
                if loop.time() >= deadline:
                    await adapter.cancel_crawl(job.id)
                    state = CrawlState.CANCELLED
                    note = f"Stopped at the {int(_CRAWL_TIMEOUT)}s crawl time cap"
                    break
        except asyncio.CancelledError:
            # Turn was cancelled — don't leave the provider job running.
            await adapter.cancel_crawl(job.id)
            raise
        except Exception as e:
            # Any other mid-crawl error (bad status body, sandbox write failure):
            # cancel the paid remote job rather than leak it, and return what we
            # dumped so far instead of raising an unhandled tool exception.
            logger.error("WebCrawl %s aborted: %r", job.id, e)
            await adapter.cancel_crawl(job.id)
            state = CrawlState.CANCELLED
            note = f"Aborted: {str(e)[:200]}"

        failures = len(index_entries) - ok_pages
        lines = [
            f"Crawled {ok_pages} page(s) from {url} into {dest_dir}/ "
            f"(crawl {state.value}{f'; {note}' if note else ''}).",
        ]
        if failures:
            lines.append(f"{failures} page(s) failed — see {index_path}.")
        if ok_pages:
            lines.append(
                f"Manifest: {index_path}. Use glob('{dest_dir}/*.md') and read_file to work "
                f"through the pages."
            )
        artifact = {
            "type": "site_crawl",
            "url": url,
            "state": state.value,
            "pages": ok_pages,
            "failures": failures,
            "dir": dest_dir,
            "index": index_path,
            "files": [e["file"] for e in index_entries if "file" in e][:50],
        }
        return "\n".join(lines), artifact

    @tool("WebMapImpl", response_format="content_and_artifact")
    async def web_map(url: str, query: Optional[str] = None, limit: int = 100) -> Tuple[str, Dict[str, Any]]:
        """Discover the URLs of a website without crawling it.

        Maps ``url`` and lists its discovered links, ranked by relevance to
        ``query`` when given. Fast and cheap — use it to scope a site before
        deciding which pages to WebFetch or whether a WebCrawl is worth it.

        Args:
            url: Site to map.
            query: Optional relevance filter/ranking for the returned links.
            limit: Max links to return (default 100, max 300).
        """
        limit_ = max(1, min(int(limit), _MAX_MAP_LINKS))
        try:
            links = await adapter.map_site(MapRequest(url=url, query=query, limit=limit_), {})
        except WebToolError as e:
            return f"Site map failed: {e}", {"type": "site_map", "url": url, "error": str(e)}

        if not links:
            return f"No URLs discovered for {url}.", {"type": "site_map", "url": url, "links": []}
        lines = []
        for info in links:
            desc = f" — {info.title}" if info.title else ""
            lines.append(f"- {info.url}{desc}")
        artifact = {
            "type": "site_map",
            "url": url,
            "links": [
                {"url": i.url, "title": i.title, "description": i.description} for i in links
            ],
        }
        return f"Discovered {len(links)} URL(s) for {url}:\n" + "\n".join(lines), artifact

    tools: List[Any] = [web_crawl]
    if map_cap is not None:
        map_key = map_cap.tracking_key(map_cap.default_level_spec)
        tools.append(create_logged_tool(web_map, name="WebMap", tracking_name=map_key))
    else:
        # Capability-driven: a provider without a manifest map capability
        # simply doesn't offer WebMap (never ship an unmetered tool).
        logger.info(f"Crawl provider {provider!r} has no map capability; WebMap not registered")
    return tools
