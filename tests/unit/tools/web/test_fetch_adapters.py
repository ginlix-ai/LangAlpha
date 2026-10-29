"""Tests for provider fetch adapters — payload mapping and error taxonomy.

Provider API calls are stubbed at the API-wrapper boundary (the adapters'
mapping logic is the unit under test); fixtures use neutral placeholder
URLs and content per house rule.
"""

import httpx
import pytest
from langsmith.run_helpers import tracing_context
from langsmith.run_trees import RunTree

from src.tools.web.providers._shared import (
    http_status_error,
    mark_web_outcome,
    transport_error,
)
from src.tools.web.types import FetchRequest, WebError, WebErrorType, WebToolError


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

    @pytest.mark.parametrize(("status", "fault"), [(401, True), (402, True), (403, False)])
    def test_only_our_key_or_account_faults_the_provider(self, status, fault):
        """A 403 can be the provider refusing one site, which must not open
        the breaker every other URL shares."""
        assert http_status_error(status, "body").provider_fault is fault


class _NullClient:
    def create_run(self, **kwargs):
        return None

    def update_run(self, **kwargs):
        return None


def _web_error(etype, **kwargs):
    return WebError(type=etype, message="scripted", **kwargs)


class TestWebToolOutcome:
    """Whose failure a web call was: the provider's is an errored run that
    alarms, the target's refusal is ``unavailable`` and does not."""

    @pytest.mark.parametrize(
        ("error", "outcome", "error_class"),
        [
            pytest.param(_web_error(WebErrorType.NOT_FOUND), "unavailable", "not_found", id="not-found"),
            pytest.param(_web_error(WebErrorType.PAYWALL), "unavailable", "paywall", id="paywall"),
            pytest.param(_web_error(WebErrorType.ANTI_BOT), "unavailable", "anti_bot", id="anti-bot"),
            pytest.param(_web_error(WebErrorType.EMPTY), "unavailable", "empty", id="empty"),
            pytest.param(
                _web_error(WebErrorType.UNSUPPORTED_URL), "unavailable", "unsupported_url",
                id="unsupported-url",
            ),
            pytest.param(http_status_error(403, "refused"), "unavailable", "forbidden", id="site-403"),
            pytest.param(_web_error(WebErrorType.TIMEOUT), "unavailable", "timeout", id="timeout"),
            pytest.param(
                _web_error(WebErrorType.PROVIDER_ERROR, provider_fault=False, native_kind="dns_error"),
                "unavailable", "provider_error", id="dead-domain",
            ),
            pytest.param(
                WebToolError(_web_error(WebErrorType.NOT_FOUND)), "unavailable", "not_found",
                id="wrapped-target-refusal",
            ),
            pytest.param(http_status_error(401, "bad key"), "error", "forbidden", id="key-401"),
            pytest.param(http_status_error(402, "no credit"), "error", "budget_exceeded", id="budget-402"),
            pytest.param(http_status_error(429, "slow down"), "error", "rate_limited", id="rate-limited"),
            pytest.param(
                _web_error(WebErrorType.PROVIDER_ERROR), "error", "provider_error",
                id="provider-fault",
            ),
            pytest.param(
                _web_error(
                    WebErrorType.CIRCUIT_OPEN, provider_fault=False, native_kind="host_circuit_open"
                ),
                "unavailable", "circuit_open", id="inhouse-host-breaker",
            ),
            pytest.param(
                _web_error(WebErrorType.CIRCUIT_OPEN, provider_fault=False, native_kind="circuit_open"),
                "error", "circuit_open", id="inhouse-infra-breaker",
            ),
            pytest.param(
                _web_error(WebErrorType.CIRCUIT_OPEN, provider_fault=True), "error",
                "circuit_open", id="no-provider-available",
            ),
            pytest.param(ValueError("bug"), "error", "ValueError", id="our-code"),
        ],
    )
    def test_outcome_table(self, error, outcome, error_class):
        run = RunTree(name="WebFetch", run_type="tool", client=_NullClient())
        with tracing_context(parent=run):
            mark_web_outcome(error, "acme")

        assert run.metadata["tool_outcome"] == outcome
        assert run.metadata["error_class"] == error_class
        assert run.metadata["tool_provider"] == "acme"


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

    async def test_success_false_does_not_fault_the_provider(self, monkeypatch):
        """A 200 with success=false describes one url's scrape, not an outage.
        PROVIDER_ERROR faults the provider by default, so this branch has to
        say otherwise or a handful of dead links disables Firecrawl outright.
        """
        payload = {
            "success": False,
            "code": "SCRAPE_DNS_RESOLUTION_ERROR",
            "error": 'DNS resolution failed for hostname "example.invalid".',
        }
        resp = await self._run(monkeypatch, payload)
        error = resp.results[0].error

        assert error.provider_fault is False
        assert error.retryable  # the chain should still try the next provider
        # The code is the only handle on which failure it was, so it must survive.
        assert "SCRAPE_DNS_RESOLUTION_ERROR" in error.message

    @pytest.mark.parametrize(
        ("paths", "expected", "fault"),
        [
            ([["url"], ["url"]], WebErrorType.UNSUPPORTED_URL, False),
            ([["url"], ["formats"]], WebErrorType.PROVIDER_ERROR, True),
        ],
    )
    async def test_a_400_rejecting_only_the_url_is_the_urls(
        self, monkeypatch, paths, expected, fault
    ):
        """Firecrawl names the field it refused; a request of ours it refuses
        is still a broken provider call."""
        from src.tools.web.providers import firecrawl

        monkeypatch.setenv("FIRECRAWL_API_KEY", "test-key")
        body = {"success": False, "details": [{"path": p, "message": "Invalid"} for p in paths]}
        response = httpx.Response(400, json=body, request=httpx.Request("POST", URL_OK))

        async def rejecting(self, url, proxy=None, max_age_seconds=None):
            raise httpx.HTTPStatusError("bad request", request=response.request, response=response)

        monkeypatch.setattr(firecrawl.FirecrawlAPI, "scrape", rejecting)
        resp = await firecrawl.FirecrawlFetchAdapter().fetch(FetchRequest(urls=[URL_OK]), {})
        error = resp.results[0].error

        assert error.type == expected
        assert error.provider_fault is fault
        assert error.retryable


# Every outcome SafeCrawlerWrapper can emit, with both routing axes.
# provider_fault=True is what counts against the chain's breaker for this
# provider, so it belongs only to failures another URL would not survive
# either — never to a host that blocks, throttles, or fails to resolve.
_CRAWLER_OUTCOMES = [
    ("blocked", WebErrorType.FORBIDDEN, False, False),
    ("stealth_failed", WebErrorType.ANTI_BOT, True, False),
    ("timeout", WebErrorType.TIMEOUT, True, False),
    ("connection_timeout", WebErrorType.TIMEOUT, True, False),
    ("rate_limited", WebErrorType.RATE_LIMITED, True, False),
    ("host_circuit_open", WebErrorType.CIRCUIT_OPEN, True, False),
    ("circuit_open", WebErrorType.CIRCUIT_OPEN, True, False),
    ("empty_content", WebErrorType.EMPTY, True, False),
    ("dns_error", WebErrorType.PROVIDER_ERROR, True, False),
    ("connection_refused", WebErrorType.PROVIDER_ERROR, True, False),
    ("network_error", WebErrorType.PROVIDER_ERROR, True, False),
    ("crawl_error", WebErrorType.PROVIDER_ERROR, True, False),
    ("infra_error", WebErrorType.PROVIDER_ERROR, True, False),
    ("cancelled", WebErrorType.PROVIDER_ERROR, False, False),
    ("queue_full", WebErrorType.PROVIDER_ERROR, True, True),
    ("browser_closed", WebErrorType.PROVIDER_ERROR, True, True),
]


def test_inhouse_mapping_covers_every_crawler_outcome():
    """The map is total. An outcome added to the crawler without a mapping here
    would otherwise fall through to a breaker-tripping default."""
    from src.tools.web.providers import inhouse

    # ``cancelled`` is handled ahead of the map (it also clears retryable).
    assert set(inhouse._ERROR_TYPES) | {"cancelled"} == {
        kind for kind, *_ in _CRAWLER_OUTCOMES
    }


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
        ("error_type", "expected", "retryable", "provider_fault"), _CRAWLER_OUTCOMES
    )
    async def test_error_type_mapping(
        self, monkeypatch, error_type, expected, retryable, provider_fault
    ):
        from src.tools.web.inhouse.safe_wrapper import CrawlResult

        resp = await self._run(
            monkeypatch,
            CrawlResult(success=False, error="scripted", error_type=error_type),
        )
        error = resp.results[0].error
        assert error.type == expected
        assert error.retryable is retryable
        assert error.provider_fault is provider_fault
        # Several outcomes normalize to PROVIDER_ERROR; the crawler's own name
        # is what keeps them apart in the attempt telemetry.
        assert error.native_kind == error_type

    async def test_unmapped_outcome_is_not_a_provider_fault(self, monkeypatch):
        """An unrecognized outcome must not be able to open the breaker."""
        from src.tools.web.inhouse.safe_wrapper import CrawlResult

        resp = await self._run(
            monkeypatch,
            CrawlResult(success=False, error="scripted", error_type="brand_new_kind"),
        )
        error = resp.results[0].error
        assert error.type == WebErrorType.PROVIDER_ERROR
        assert error.provider_fault is False
