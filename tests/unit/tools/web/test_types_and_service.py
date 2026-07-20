"""Tests for src/tools/web/types.py and fetch service plumbing."""

import pytest

from src.tools.web.fetch import FetchService, get_fetch_chain
from src.tools.web.router import _needs_inhouse
from src.tools.web.types import (
    FetchRequest,
    FetchResult,
    SearchResult,
    WebError,
    WebErrorType,
)


class TestWebError:
    def test_retryable_defaults_from_type(self):
        assert WebError(type=WebErrorType.TIMEOUT, message="x").retryable
        assert not WebError(type=WebErrorType.NOT_FOUND, message="x").retryable
        assert not WebError(type=WebErrorType.PAYWALL, message="x").retryable

    def test_explicit_retryable_wins(self):
        error = WebError(type=WebErrorType.TIMEOUT, message="x", retryable=False)
        assert not error.retryable

    def test_str_includes_type_and_status(self):
        error = WebError(type=WebErrorType.FORBIDDEN, message="denied", http_status=403)
        assert "[forbidden HTTP 403] denied" == str(error)


class TestSearchResultDict:
    def test_as_dict_matches_tool_boundary_convention(self):
        result = SearchResult(
            title="T", url="https://site.example", excerpts=("one", "two"),
            published_date="2026-01-01", score=0.9,
        )
        d = result.as_dict()
        assert d["type"] == "page"
        assert d["content"] == "one\n\ntwo"
        assert d["date"] == "2026-01-01"
        assert d["score"] == 0.9


class TestPreRouting:
    @pytest.mark.parametrize(
        ("url", "expected"),
        [
            ("https://www.youtube.com/watch?v=abc", True),
            ("https://youtu.be/abc", True),
            ("https://x.com/user/status/1", True),
            ("https://twitter.com/user/status/1", True),
            ("https://site.example/watch?v=abc", False),
            ("https://site.example/x.com", False),
            ("https://notx.company.example", False),
        ],
    )
    def test_needs_inhouse(self, url, expected):
        assert _needs_inhouse(url) is expected


class TestFetchChainConfig:
    def test_default_ships_dark(self, monkeypatch):
        monkeypatch.setattr(
            "src.config.tool_settings._get_agent_config_dict", lambda: {}
        )
        assert get_fetch_chain() == ["inhouse"]

    def test_configured_chain_wins(self, monkeypatch):
        monkeypatch.setattr(
            "src.config.tool_settings._get_agent_config_dict",
            lambda: {"fetch_chain": ["parallel", "inhouse"]},
        )
        assert get_fetch_chain() == ["parallel", "inhouse"]


@pytest.mark.asyncio
class TestFetchService:
    async def test_normalizes_and_orders(self, monkeypatch):
        import src.tools.web.fetch as fetch_module

        async def no_cache():
            return None

        monkeypatch.setattr(fetch_module, "_get_cache_client", no_cache)

        class FakeRouter:
            provider_names = ["fake"]

            async def fetch(self, req):
                from src.tools.web.types import FetchResponse

                return FetchResponse(
                    results=[FetchResult(url=u, markdown=f"got {u}") for u in req.urls],
                    provider="fake",
                    providers_tried=["fake"],
                )

        service = FetchService(chain=["inhouse"])
        service._router = FakeRouter()

        resp = await service.fetch(
            FetchRequest(urls=["http://a.example/x", "https://b.example/y"])
        )

        assert [r.url for r in resp.results] == [
            "https://a.example/x",  # http upgraded
            "https://b.example/y",
        ]
        assert resp.provider == "fake"

    async def test_cache_only_miss_is_empty_error(self, monkeypatch):
        import src.tools.web.fetch as fetch_module

        async def no_cache():
            return None

        monkeypatch.setattr(fetch_module, "_get_cache_client", no_cache)
        service = FetchService(chain=["inhouse"])

        resp = await service.fetch(
            FetchRequest(urls=["https://a.example/x"], max_age_seconds=-1)
        )

        assert resp.results[0].error.type == WebErrorType.EMPTY

    async def test_cache_hit_short_circuits_router(self, monkeypatch):
        """A cached URL returns source='cache' without ever calling the router;
        only live results are written back (cache reads are not re-cached)."""
        import src.tools.web.fetch as fetch_module

        class FakeCache:
            def __init__(self):
                self.store = {"https://a.example/x": "cached markdown"}
                self.writes: list = []

            async def get(self, key):
                return self.store.get(key)

            async def set(self, key, value, ttl=None):
                self.writes.append((key, value))

        fake = FakeCache()

        async def get_cache():
            return fake

        # Key the cache the same way the service does, so the hit lands.
        fake.store = {fetch_module._cache_key("https://a.example/x"): "cached markdown"}
        monkeypatch.setattr(fetch_module, "_get_cache_client", get_cache)

        class BoomRouter:
            provider_names = ["boom"]

            async def fetch(self, req):
                raise AssertionError("router must not run on a cache hit")

        service = FetchService(chain=["inhouse"])
        service._router = BoomRouter()

        resp = await service.fetch(FetchRequest(urls=["https://a.example/x"]))

        assert resp.results[0].source == "cache"
        assert resp.results[0].markdown == "cached markdown"
        assert fake.writes == []  # cache-sourced result not re-written
