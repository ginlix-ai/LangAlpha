"""Tests for the site-crawl layer: FirecrawlCrawlAdapter mapping and the
WebCrawl/WebMap tools (dump-first writes, per-page metering, cancellation).

Provider calls are stubbed at the API-wrapper boundary; fixtures use neutral
placeholder URLs and content per house rule.
"""

import asyncio
import json
from types import SimpleNamespace

import pytest

import src.tools.web.crawl as crawl_mod
from src.tools.decorators import set_tool_tracker, start_tool_tracking
from src.tools.web.types import (
    CrawlJob,
    CrawlPage,
    CrawlRequest,
    CrawlState,
    CrawlStatus,
    MapRequest,
    UrlInfo,
    WebError,
    WebErrorType,
    WebToolError,
)

SITE = "https://docs.site.example"
PAGE_A = f"{SITE}/guide/intro"
PAGE_B = f"{SITE}/guide/setup"


# ---------------------------------------------------------------------------
# FirecrawlCrawlAdapter — payload mapping, pagination, error taxonomy
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestFirecrawlCrawlAdapter:
    def _adapter(self, monkeypatch):
        from src.tools.web.providers import firecrawl

        monkeypatch.setenv("FIRECRAWL_API_KEY", "test-key")
        return firecrawl, firecrawl.FirecrawlCrawlAdapter()

    async def test_start_crawl_maps_request(self, monkeypatch):
        firecrawl, adapter = self._adapter(monkeypatch)
        captured = {}

        async def fake_start(self, payload):
            captured.update(payload)
            return {"success": True, "id": "job-123"}

        monkeypatch.setattr(firecrawl.FirecrawlAPI, "start_crawl", fake_start)
        req = CrawlRequest(
            url=SITE,
            limit=10,
            max_depth=2,
            include_paths=["^/guide/.*"],
            query="setup docs",
        )
        job = await adapter.start_crawl(req, {})

        assert job == CrawlJob(id="job-123", provider="firecrawl")
        assert captured["url"] == SITE
        assert captured["limit"] == 10
        assert captured["maxDiscoveryDepth"] == 2
        assert captured["includePaths"] == ["^/guide/.*"]
        assert captured["prompt"] == "setup docs"
        assert captured["scrapeOptions"]["formats"] == [{"type": "markdown"}]

    async def test_start_crawl_failure_raises(self, monkeypatch):
        firecrawl, adapter = self._adapter(monkeypatch)

        async def fake_start(self, payload):
            return {"success": False, "error": "bad request"}

        monkeypatch.setattr(firecrawl.FirecrawlAPI, "start_crawl", fake_start)
        with pytest.raises(WebToolError, match="bad request"):
            await adapter.start_crawl(CrawlRequest(url=SITE, limit=5), {})

    async def test_missing_key_raises(self, monkeypatch):
        from src.tools.web.providers.firecrawl import FirecrawlCrawlAdapter

        monkeypatch.delenv("FIRECRAWL_API_KEY", raising=False)
        with pytest.raises(WebToolError, match="FIRECRAWL_API_KEY"):
            await FirecrawlCrawlAdapter().start_crawl(CrawlRequest(url=SITE, limit=5), {})

    async def test_status_follows_pagination_and_maps_pages(self, monkeypatch):
        firecrawl, adapter = self._adapter(monkeypatch)
        page_ok = {
            "markdown": "useful content that is long enough",
            "metadata": {"title": "Intro", "url": PAGE_A, "statusCode": 200},
        }
        page_err = {
            "markdown": "",
            "metadata": {"url": PAGE_B, "error": "render timeout"},
        }
        responses = {
            None: {
                "status": "completed",
                "total": 2,
                "completed": 2,
                "creditsUsed": 2,
                "data": [page_ok],
                "next": "https://api.firecrawl.example/v2/crawl/job-123?skip=1",
            },
            "https://api.firecrawl.example/v2/crawl/job-123?skip=1": {
                "data": [page_err],
                "next": None,
            },
        }

        async def fake_status(self, crawl_id, skip=0, page_url=None):
            return responses[page_url]

        monkeypatch.setattr(firecrawl.FirecrawlAPI, "crawl_status", fake_status)
        status = await adapter.crawl_status("job-123")

        assert status.state is CrawlState.COMPLETED
        assert status.total == 2
        ok, err = status.pages
        assert ok.ok and ok.title == "Intro" and "useful content" in ok.markdown
        assert not err.ok and err.error.type == WebErrorType.PROVIDER_ERROR
        assert "render timeout" in err.error.message

    async def test_status_pagination_hop_capped(self, monkeypatch):
        """A `next` cursor that never clears stops after the hop cap instead
        of pinning the poll loop past the crawl deadline."""
        firecrawl, adapter = self._adapter(monkeypatch)
        calls = {"n": 0}

        async def fake_status(self, crawl_id, skip=0, page_url=None):
            calls["n"] += 1
            return {
                "status": "completed",
                "data": [],
                "next": "https://api.firecrawl.example/v2/crawl/job-123?skip=1",
            }

        monkeypatch.setattr(firecrawl.FirecrawlAPI, "crawl_status", fake_status)
        status = await adapter.crawl_status("job-123")

        assert status.state is CrawlState.COMPLETED
        assert calls["n"] == firecrawl._MAX_STATUS_PAGES + 1

    async def test_page_status_code_mapping(self, monkeypatch):
        firecrawl, adapter = self._adapter(monkeypatch)

        async def fake_status(self, crawl_id, skip=0, page_url=None):
            return {
                "status": "scraping",
                "data": [
                    {"markdown": "x" * 50, "metadata": {"url": PAGE_A, "statusCode": 404}},
                    {"markdown": "", "metadata": {"url": PAGE_B, "statusCode": 200}},
                ],
                "next": None,
            }

        monkeypatch.setattr(firecrawl.FirecrawlAPI, "crawl_status", fake_status)
        status = await adapter.crawl_status("job-123")

        assert status.state is CrawlState.RUNNING
        not_found, empty = status.pages
        assert not_found.error.type == WebErrorType.NOT_FOUND
        assert empty.error.type == WebErrorType.EMPTY

    async def test_failed_job_carries_error(self, monkeypatch):
        firecrawl, adapter = self._adapter(monkeypatch)

        async def fake_status(self, crawl_id, skip=0, page_url=None):
            return {"status": "failed", "error": "crawl exploded", "data": [], "next": None}

        monkeypatch.setattr(firecrawl.FirecrawlAPI, "crawl_status", fake_status)
        status = await adapter.crawl_status("job-123")
        assert status.state is CrawlState.FAILED
        assert "crawl exploded" in status.error.message

    async def test_map_site_maps_links_and_tolerates_strings(self, monkeypatch):
        firecrawl, adapter = self._adapter(monkeypatch)
        captured = {}

        async def fake_map(self, payload):
            captured.update(payload)
            return {
                "success": True,
                "links": [
                    {"url": PAGE_A, "title": "Intro", "description": "The intro"},
                    PAGE_B,
                ],
            }

        monkeypatch.setattr(firecrawl.FirecrawlAPI, "map_site", fake_map)
        links = await adapter.map_site(MapRequest(url=SITE, query="setup", limit=50), {})

        assert captured == {"url": SITE, "limit": 50, "search": "setup"}
        assert links[0] == UrlInfo(url=PAGE_A, title="Intro", description="The intro")
        assert links[1] == UrlInfo(url=PAGE_B)

    async def test_cancel_never_raises(self, monkeypatch):
        firecrawl, adapter = self._adapter(monkeypatch)

        async def fake_cancel(self, crawl_id):
            raise firecrawl.httpx.HTTPError("boom")

        monkeypatch.setattr(firecrawl.FirecrawlAPI, "cancel_crawl", fake_cancel)
        await adapter.cancel_crawl("job-123")  # must not raise


# ---------------------------------------------------------------------------
# Tool factory + WebCrawl / WebMap behavior (fake adapter + fake backend)
# ---------------------------------------------------------------------------


class FakeBackend:
    def __init__(self):
        self.files: dict = {}
        self.filesystem_config = SimpleNamespace(enable_path_validation=False)

    def normalize_path(self, p):
        return p

    def validate_path(self, p):
        return True

    async def awrite_text(self, p, content):
        self.files[p] = content
        return True


class FakeAdapter:
    name = "fake"

    def __init__(self, statuses=None, links=None):
        self.statuses = list(statuses or [])
        self.links = links or []
        self.started: CrawlRequest | None = None
        self.cancelled = False
        self.status_skips: list = []

    async def start_crawl(self, req, native_params):
        self.started = req
        return CrawlJob(id="job-1", provider=self.name)

    async def crawl_status(self, job_id, skip=0):
        self.status_skips.append(skip)
        return self.statuses.pop(0)

    async def cancel_crawl(self, job_id):
        self.cancelled = True

    async def map_site(self, req, native_params):
        return self.links


def _make_tools(monkeypatch, adapter):
    monkeypatch.setenv("FIRECRAWL_API_KEY", "test-key")
    monkeypatch.setattr(crawl_mod, "_ADAPTER_BUILDERS", {"firecrawl": lambda: adapter})
    monkeypatch.setattr(crawl_mod, "_POLL_INTERVAL", 0.0)
    backend = FakeBackend()
    tools = crawl_mod.create_crawl_tools(backend)
    return tools, backend


class TestCreateCrawlTools:
    def test_builds_both_tools(self, monkeypatch):
        tools, _ = _make_tools(monkeypatch, FakeAdapter())
        assert [t.name for t in tools] == ["WebCrawl", "WebMap"]

    def test_missing_key_returns_no_tools(self, monkeypatch):
        monkeypatch.delenv("FIRECRAWL_API_KEY", raising=False)
        assert crawl_mod.create_crawl_tools(FakeBackend()) == []

    def test_unknown_provider_returns_no_tools(self, monkeypatch):
        monkeypatch.setattr(crawl_mod, "get_crawl_provider", lambda: "nope")
        assert crawl_mod.create_crawl_tools(FakeBackend()) == []


@pytest.mark.asyncio
class TestWebCrawlTool:
    def _statuses(self):
        return [
            CrawlStatus(
                state=CrawlState.RUNNING,
                total=3,
                completed=1,
                pages=[CrawlPage(url=PAGE_A, title="Intro", markdown="intro body")],
            ),
            CrawlStatus(
                state=CrawlState.COMPLETED,
                total=3,
                completed=2,
                pages=[
                    CrawlPage(url=PAGE_B, title="Setup", markdown="setup body"),
                    CrawlPage(
                        url=f"{SITE}/broken",
                        error=WebError(type=WebErrorType.NOT_FOUND, message="gone"),
                    ),
                ],
            ),
        ]

    async def test_dumps_pages_and_meters_per_page(self, monkeypatch):
        adapter = FakeAdapter(statuses=self._statuses())
        tools, backend = _make_tools(monkeypatch, adapter)
        web_crawl = tools[0]
        tracker = start_tool_tracking()
        try:
            content, artifact = await web_crawl.coroutine(url=SITE, limit=10)
        finally:
            set_tool_tracker(None)

        # Two ok pages written under work/crawl/<host>/ plus the index.
        assert backend.files["work/crawl/docs.site.example/guide-intro.md"].endswith("intro body")
        assert "Source: " + PAGE_A in backend.files["work/crawl/docs.site.example/guide-intro.md"]
        assert "work/crawl/docs.site.example/guide-setup.md" in backend.files
        index_lines = [
            json.loads(line)
            for line in backend.files["work/crawl/docs.site.example/index.jsonl"].strip().splitlines()
        ]
        assert len(index_lines) == 3
        assert index_lines[0] == {"url": PAGE_A, "title": "Intro", "file": "guide-intro.md"}
        assert "error" in index_lines[2]

        # Metered per delivered page, batch by batch.
        assert tracker.get_summary() == {"FirecrawlCrawlTool": 2}
        # Incremental skip offsets passed to the adapter.
        assert adapter.status_skips == [0, 1]

        assert "Crawled 2 page(s)" in content
        assert artifact["pages"] == 2 and artifact["failures"] == 1
        assert artifact["state"] == "completed"

    async def test_limit_clamped_to_cap(self, monkeypatch):
        adapter = FakeAdapter(
            statuses=[CrawlStatus(state=CrawlState.COMPLETED, total=0, completed=0)]
        )
        tools, _ = _make_tools(monkeypatch, adapter)
        await tools[0].coroutine(url=SITE, limit=5000)
        assert adapter.started.limit == crawl_mod._MAX_CRAWL_PAGES

    async def test_output_dir_override(self, monkeypatch):
        adapter = FakeAdapter(
            statuses=[
                CrawlStatus(
                    state=CrawlState.COMPLETED,
                    total=1,
                    completed=1,
                    pages=[CrawlPage(url=PAGE_A, title="Intro", markdown="body")],
                )
            ]
        )
        tools, backend = _make_tools(monkeypatch, adapter)
        await tools[0].coroutine(url=SITE, output_dir="work/mytask/crawl")
        assert "work/mytask/crawl/docs.site.example/guide-intro.md" in backend.files

    async def test_start_failure_returns_error(self, monkeypatch):
        adapter = FakeAdapter()

        async def failing_start(req, native_params):
            raise WebToolError(WebError(type=WebErrorType.FORBIDDEN, message="key rejected"))

        adapter.start_crawl = failing_start
        tools, backend = _make_tools(monkeypatch, adapter)
        content, artifact = await tools[0].coroutine(url=SITE)
        assert "Crawl failed to start" in content and "key rejected" in content
        assert artifact["error"] and not backend.files

    async def test_cancellation_cancels_provider_job(self, monkeypatch):
        adapter = FakeAdapter()

        async def hanging_status(job_id, skip=0):
            raise asyncio.CancelledError()

        adapter.crawl_status = hanging_status
        tools, _ = _make_tools(monkeypatch, adapter)
        with pytest.raises(asyncio.CancelledError):
            await tools[0].coroutine(url=SITE)
        assert adapter.cancelled

    async def test_repeated_poll_failures_abort_and_cancel(self, monkeypatch):
        adapter = FakeAdapter()

        async def broken_status(job_id, skip=0):
            raise WebToolError(WebError(type=WebErrorType.PROVIDER_ERROR, message="poll down"))

        adapter.crawl_status = broken_status
        tools, _ = _make_tools(monkeypatch, adapter)
        content, artifact = await tools[0].coroutine(url=SITE)
        assert adapter.cancelled
        assert "poll down" in content
        assert artifact["pages"] == 0
        # The artifact must not claim the crawl is still running.
        assert artifact["state"] == "cancelled"


@pytest.mark.asyncio
class TestWebMapTool:
    async def test_lists_links_and_bills_flat(self, monkeypatch):
        adapter = FakeAdapter(
            links=[UrlInfo(url=PAGE_A, title="Intro"), UrlInfo(url=PAGE_B)]
        )
        tools, _ = _make_tools(monkeypatch, adapter)
        web_map = tools[1]
        tracker = start_tool_tracking()
        try:
            content, artifact = await web_map.coroutine(url=SITE, query="guide")
        finally:
            set_tool_tracker(None)

        assert tracker.get_summary() == {"FirecrawlMapTool": 1}
        assert "Discovered 2 URL(s)" in content and PAGE_A in content
        assert artifact["links"][0]["title"] == "Intro"

    async def test_empty_map(self, monkeypatch):
        tools, _ = _make_tools(monkeypatch, FakeAdapter(links=[]))
        content, artifact = await tools[1].coroutine(url=SITE)
        assert "No URLs discovered" in content and artifact["links"] == []

    async def test_map_error_surfaces(self, monkeypatch):
        adapter = FakeAdapter()

        async def failing_map(req, native_params):
            raise WebToolError(WebError(type=WebErrorType.RATE_LIMITED, message="slow down"))

        adapter.map_site = failing_map
        tools, _ = _make_tools(monkeypatch, adapter)
        content, artifact = await tools[1].coroutine(url=SITE)
        assert "Site map failed" in content and "slow down" in content
        assert artifact["error"]
