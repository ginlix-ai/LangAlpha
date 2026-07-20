"""Tests for provider fetch adapters — payload mapping and error taxonomy.

Provider API calls are stubbed at the API-wrapper boundary (the adapters'
mapping logic is the unit under test); fixtures use neutral placeholder
URLs and content per house rule.
"""

import httpx
import pytest

from src.tools.web.providers._shared import http_status_error, transport_error
from src.tools.web.types import FetchRequest, WebErrorType


URL_OK = "https://site.example/page"
URL_BAD = "https://site.example/missing"


class TestSharedErrorMapping:
    """One taxonomy for every provider's own-API failures."""

    @pytest.mark.parametrize(
        ("status", "expected"),
        [
            (401, WebErrorType.FORBIDDEN),
            (403, WebErrorType.FORBIDDEN),
            (402, WebErrorType.BUDGET_EXCEEDED),
            (429, WebErrorType.RATE_LIMITED),
            (500, WebErrorType.PROVIDER_ERROR),
        ],
    )
    def test_http_status_taxonomy(self, status, expected):
        err = http_status_error(status, "body")
        assert err.type == expected
        assert err.http_status == status
        # A provider-API failure is provider-scoped, so the router advances
        # to the next provider (a revoked primary key degrades to fallbacks).
        assert err.retryable is True

    def test_overrides_add_provider_quirks(self):
        err = http_status_error(408, "body", {408: WebErrorType.TIMEOUT})
        assert err.type == WebErrorType.TIMEOUT

    def test_transport_timeout_keeps_taxonomy_type(self):
        assert transport_error(httpx.ConnectTimeout("slow")).type == WebErrorType.TIMEOUT
        assert transport_error(httpx.ConnectError("down")).type == WebErrorType.PROVIDER_ERROR


@pytest.mark.asyncio
class TestExaFetchAdapter:
    async def _run(self, monkeypatch, payload):
        from src.tools.web.providers import exa

        monkeypatch.setenv("EXA_API_KEY", "test-key")

        async def fake_post(self, path, body):
            return payload

        monkeypatch.setattr(exa.ExaAPI, "_post", fake_post)
        adapter = exa.ExaFetchAdapter()
        return await adapter.fetch(
            FetchRequest(urls=[URL_OK, URL_BAD], objective="what is it"), {}
        )

    async def test_maps_results_and_statuses(self, monkeypatch):
        payload = {
            "results": [
                {
                    "id": URL_OK,
                    "url": URL_OK,
                    "title": "A Page",
                    "text": "long text",
                    "highlights": ["relevant bit"],
                    "summary": "a summary",
                }
            ],
            "statuses": [
                {"id": URL_OK, "status": "success"},
                {
                    "id": URL_BAD,
                    "status": "error",
                    "error": {"tag": "CRAWL_NOT_FOUND", "httpStatusCode": 404},
                },
            ],
        }
        resp = await self._run(monkeypatch, payload)

        ok, bad = resp.results
        assert ok.ok and ok.title == "A Page" and ok.excerpts == ("relevant bit",)
        assert ok.summary == "a summary"
        assert not bad.ok
        assert bad.error.type == WebErrorType.NOT_FOUND
        assert not bad.error.retryable

    async def test_empty_content_is_empty_error_not_billed_ok(self, monkeypatch):
        """A 200 result with no text/highlights/summary must not count as ok."""
        payload = {
            "results": [{"id": URL_OK, "url": URL_OK, "title": "A Page", "text": "  "}],
            "statuses": [{"id": URL_OK, "status": "success"}],
        }
        resp = await self._run(monkeypatch, payload)
        empty = resp.results[0]
        assert not empty.ok
        assert empty.error.type == WebErrorType.EMPTY

    async def test_url_missing_everywhere_is_empty(self, monkeypatch):
        resp = await self._run(monkeypatch, {"results": [], "statuses": []})
        assert all(r.error.type == WebErrorType.EMPTY for r in resp.results)

    async def test_missing_key_is_retryable_provider_error(self, monkeypatch):
        from src.tools.web.providers.exa import ExaFetchAdapter

        monkeypatch.delenv("EXA_API_KEY", raising=False)
        resp = await ExaFetchAdapter().fetch(FetchRequest(urls=[URL_OK]), {})
        assert resp.results[0].error.type == WebErrorType.PROVIDER_ERROR
        assert resp.results[0].error.retryable

    async def test_request_timeout_maps_to_timeout(self, monkeypatch):
        from src.tools.web.providers import exa

        monkeypatch.setenv("EXA_API_KEY", "test-key")

        async def raise_timeout(self, path, body):
            raise httpx.ConnectTimeout("connect timed out")

        monkeypatch.setattr(exa.ExaAPI, "_post", raise_timeout)
        resp = await exa.ExaFetchAdapter().fetch(FetchRequest(urls=[URL_OK]), {})
        assert resp.results[0].error.type == WebErrorType.TIMEOUT


@pytest.mark.asyncio
class TestParallelFetchAdapter:
    async def _run(self, monkeypatch, payload):
        from src.tools.web.providers import parallel

        monkeypatch.setenv("PARALLEL_API_KEY", "test-key")

        async def fake_post(self, path, body):
            return payload

        monkeypatch.setattr(parallel.ParallelAPI, "_post", fake_post)
        adapter = parallel.ParallelFetchAdapter()
        return await adapter.fetch(
            FetchRequest(urls=[URL_OK, URL_BAD], objective="what is it"), {}
        )

    async def test_maps_results_and_errors(self, monkeypatch):
        payload = {
            "results": [
                {
                    "url": URL_OK,
                    "title": "A Page",
                    "excerpts": ["excerpt one", "excerpt two"],
                    "publish_date": "2026-01-01",
                }
            ],
            "errors": [
                {"url": URL_BAD, "error_type": "http_error", "http_status_code": 404}
            ],
        }
        resp = await self._run(monkeypatch, payload)

        ok, bad = resp.results
        assert ok.ok and len(ok.excerpts) == 2 and ok.published_date == "2026-01-01"
        assert bad.error.type == WebErrorType.NOT_FOUND

    async def test_empty_content_is_empty_error_not_billed_ok(self, monkeypatch):
        """A 200 result with no full_content/excerpts must not count as ok."""
        payload = {
            "results": [{"url": URL_OK, "title": "A Page", "full_content": "", "excerpts": []}],
            "errors": [],
        }
        resp = await self._run(monkeypatch, payload)
        empty = resp.results[0]
        assert not empty.ok
        assert empty.error.type == WebErrorType.EMPTY

    async def test_paywall_status_codes(self, monkeypatch):
        payload = {
            "results": [{"url": URL_OK, "excerpts": ["x"]}],
            "errors": [
                {"url": URL_BAD, "error_type": "http_error", "http_status_code": 403}
            ],
        }
        resp = await self._run(monkeypatch, payload)
        assert resp.results[1].error.type == WebErrorType.PAYWALL
        assert not resp.results[1].error.retryable

    async def test_request_timeout_maps_to_timeout(self, monkeypatch):
        from src.tools.web.providers import parallel

        monkeypatch.setenv("PARALLEL_API_KEY", "test-key")

        async def raise_timeout(self, path, body):
            raise httpx.ReadTimeout("read timed out")

        monkeypatch.setattr(parallel.ParallelAPI, "_post", raise_timeout)
        resp = await parallel.ParallelFetchAdapter().fetch(FetchRequest(urls=[URL_OK]), {})
        assert resp.results[0].error.type == WebErrorType.TIMEOUT


@pytest.mark.asyncio
class TestFirecrawlFetchAdapter:
    async def _run(self, monkeypatch, payload, urls=None):
        from src.tools.web.providers import firecrawl

        monkeypatch.setenv("FIRECRAWL_API_KEY", "test-key")

        async def fake_scrape(self, url, proxy=None, max_age_seconds=None):
            return payload

        monkeypatch.setattr(firecrawl.FirecrawlAPI, "scrape", fake_scrape)
        adapter = firecrawl.FirecrawlFetchAdapter()
        return await adapter.fetch(FetchRequest(urls=urls or [URL_OK]), {"proxy": "auto"})

    async def test_maps_markdown(self, monkeypatch):
        payload = {
            "success": True,
            "data": {
                "markdown": "# Page\n\nsome real content here",
                "metadata": {"title": "A Page", "sourceURL": URL_OK, "statusCode": 200},
            },
        }
        resp = await self._run(monkeypatch, payload)
        assert resp.results[0].ok
        assert resp.results[0].markdown.startswith("# Page")

    async def test_scrape_bounds_provider_cache_age(self, monkeypatch):
        """Omitted max_age must not fall through to Firecrawl's 2-day default;
        explicit 0 still forces a fresh scrape."""
        from src.tools.web.providers import firecrawl

        monkeypatch.setenv("FIRECRAWL_API_KEY", "test-key")
        captured = {}

        async def fake_request(self, method, url, **kw):
            captured.update(kw.get("json") or {})
            return {"success": True, "data": {"markdown": "x", "metadata": {}}}

        monkeypatch.setattr(firecrawl.FirecrawlAPI, "_request", fake_request)
        api = firecrawl.FirecrawlAPI()

        await api.scrape("https://site.example/page")
        assert captured["maxAge"] == firecrawl._DEFAULT_MAX_AGE_SECONDS * 1000

        captured.clear()
        await api.scrape("https://site.example/page", max_age_seconds=0)
        assert captured["maxAge"] == 0

    async def test_target_403_maps_to_anti_bot(self, monkeypatch):
        payload = {
            "success": True,
            "data": {"markdown": "", "metadata": {"statusCode": 403}},
        }
        resp = await self._run(monkeypatch, payload)
        error = resp.results[0].error
        assert error.type == WebErrorType.ANTI_BOT
        assert error.retryable

    async def test_target_404_is_terminal(self, monkeypatch):
        payload = {
            "success": True,
            "data": {"markdown": "", "metadata": {"statusCode": 404}},
        }
        resp = await self._run(monkeypatch, payload)
        assert resp.results[0].error.type == WebErrorType.NOT_FOUND
        assert not resp.results[0].error.retryable

    async def test_success_false_is_provider_error(self, monkeypatch):
        payload = {"success": False, "error": "scrape engine unavailable"}
        resp = await self._run(monkeypatch, payload)
        assert resp.results[0].error.type == WebErrorType.PROVIDER_ERROR


@pytest.mark.asyncio
class TestInhouseFetchAdapter:
    async def _run(self, monkeypatch, crawl_result):
        from src.tools.web.providers import inhouse

        class FakeCrawler:
            async def crawl(self, url):
                return crawl_result

        async def fake_get_crawler():
            return FakeCrawler()

        monkeypatch.setattr(
            "src.tools.web.inhouse.safe_wrapper.get_safe_crawler", fake_get_crawler
        )
        adapter = inhouse.InhouseFetchAdapter()
        return await adapter.fetch(FetchRequest(urls=[URL_OK]), {})

    async def test_success_maps_markdown(self, monkeypatch):
        from src.tools.web.inhouse.safe_wrapper import CrawlResult

        resp = await self._run(
            monkeypatch, CrawlResult(success=True, markdown="content", title="T")
        )
        assert resp.results[0].ok and resp.results[0].markdown == "content"

    @pytest.mark.parametrize(
        ("error_type", "expected", "retryable"),
        [
            ("blocked", WebErrorType.FORBIDDEN, False),
            ("stealth_failed", WebErrorType.ANTI_BOT, True),
            ("timeout", WebErrorType.TIMEOUT, True),
            ("rate_limited", WebErrorType.RATE_LIMITED, True),
            ("circuit_open", WebErrorType.CIRCUIT_OPEN, True),
            ("empty_content", WebErrorType.EMPTY, True),
            ("dns_error", WebErrorType.PROVIDER_ERROR, True),
            ("cancelled", WebErrorType.PROVIDER_ERROR, False),
        ],
    )
    async def test_error_type_mapping(self, monkeypatch, error_type, expected, retryable):
        from src.tools.web.inhouse.safe_wrapper import CrawlResult

        resp = await self._run(
            monkeypatch,
            CrawlResult(success=False, error="scripted", error_type=error_type),
        )
        error = resp.results[0].error
        assert error.type == expected
        assert error.retryable is retryable
