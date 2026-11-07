"""fetch_sitemap_urls bounds the whole walk, not each read."""

import asyncio

import pytest

from src.tools.web.inhouse import sitemap


@pytest.mark.asyncio
async def test_walk_that_never_ends_returns_empty_within_timeout(monkeypatch):
    """httpx's timeout restarts on every byte and every sub-sitemap, so a
    site with thousands of sub-sitemaps (or one dripping bytes) must still
    give up once the overall timeout passes."""

    async def endless_walk(client, domain):
        await asyncio.Event().wait()

    monkeypatch.setattr(sitemap, "_find_sitemap_url", endless_walk)

    urls = await asyncio.wait_for(
        sitemap.fetch_sitemap_urls("site.example", timeout=0.2), timeout=5
    )

    assert urls == []

@pytest.mark.asyncio
async def test_walk_cut_short_keeps_the_urls_it_collected(monkeypatch):
    async def two_sitemaps(client, domain):
        return ["https://site.example/a.xml", "https://site.example/b.xml"]

    async def parse(client, sitemap_url, urls, max_urls):
        if sitemap_url.endswith("a.xml"):
            urls.extend(["https://site.example/one", "https://site.example/two"])
            return
        await asyncio.Event().wait()

    monkeypatch.setattr(sitemap, "_find_sitemap_url", two_sitemaps)
    monkeypatch.setattr(sitemap, "_parse_sitemap", parse)

    urls = await asyncio.wait_for(
        sitemap.fetch_sitemap_urls("site.example", timeout=0.2), timeout=5
    )

    assert urls == [
        {"url": "https://site.example/one"},
        {"url": "https://site.example/two"},
    ]
