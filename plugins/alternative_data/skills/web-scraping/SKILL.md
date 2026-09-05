---
name: web-scraping
description: "Web scraping: scrape_page / scrape_pages MCP tools for fetching pages as markdown, HTML, or text (fast HTTP, browser rendering, anti-bot stealth), plus the direct Scrapling Python API for selectors, sessions, and spiders"
license: MIT
---

# Web Scraping

## Overview

Two ways to scrape in the sandbox:

1. **MCP tools** (`scrape_page`, `scrape_pages`) — recommended for straight "give me this page's content". Synchronous, return dicts.
2. **Direct Scrapling Python API** — for CSS/XPath selectors, sessions, logins, and multi-page spiders. Async, returns Page objects with `.css()` / `.xpath()`.

Quick fetches can run inline via `ExecuteCode`. For spiders, multi-URL crawls, or anything you'll iterate on, write the scraper to `work/<task_name>/scraper.py` and run it via `Bash` — edit-and-rerun beats resubmitting code.

## MCP Tools

Import from `tools.scrape`. **Synchronous** — no `await`.

```python
from tools.scrape import scrape_page, scrape_pages
```

### Signatures

```python
scrape_page(url: str, mode: str = "fast", extraction: str = "markdown",
            timeout: float = 30.0, solve_cloudflare: bool = False) -> dict

scrape_pages(urls: list[str], mode: str = "fast", extraction: str = "markdown",
             timeout: float = 30.0, solve_cloudflare: bool = False) -> dict
```

### Parameters

| Param | Default | Notes |
|---|---|---|
| `mode` | `"fast"` | `"fast"` plain HTTP · `"browser"` JS rendering · `"stealth"` bot-protected sites |
| `extraction` | `"markdown"` | `"markdown"` (article text, cleaned) · `"html"` (raw) · `"text"` (plain) |
| `timeout` | `30.0` | Per-fetch **seconds**, 1–60 — seconds in every mode, not ms |
| `solve_cloudflare` | `False` | Only meaningful with `mode="stealth"` |
| `urls` | — | `scrape_pages` only; **max 10** per call |

Escalate modes only as needed: start `fast`, go to `browser` when the page needs JavaScript, `stealth` when you're getting blocked, and add `solve_cloudflare=True` only if `stealth` still returns a challenge page.

### Return shape

`scrape_page` returns a flat dict:

```python
{
    "url": "https://example.com",
    "status": 200,
    "title": "Example Domain",
    "content": "# Example Domain\n\nThis domain is for use in...",  # str
    "extraction": "markdown",
    "mode": "fast",
}
```

- **`content` is a plain string**, not a list — use it directly, never `content[0]` (that yields a single character).
- `content` is truncated to **400,000 chars**.
- No `.css()` / `.xpath()` / `.body` / `.headers` / `.cookies` — for selectors use the direct Python API below, or parse `extraction="html"` with BeautifulSoup.

`scrape_pages` wraps them:

```python
{
    "results": [ ... ],  # one entry per input URL, in input order
    "count": 3,
}
```

### Errors

Errors are returned, never raised. **Always check for `"error"` before reading `content`.**

```python
res = scrape_page(url="https://example.com")
if "error" in res:
    print(res["error"], res["detail"])
else:
    print(res["content"])
```

Per-URL errors — appear as `{"error", "detail", "url"}` entries inside `scrape_pages["results"]`, or as the whole return of `scrape_page`:

| Code | Meaning |
|---|---|
| `invalid_url` | Not an `http://` / `https://` URL |
| `fetch_failed` | Network, DNS, timeout, or browser failure |
| `extract_failed` | Page fetched but the extractor failed on the markup; the entry still carries `status` |
| `scrape_failed` | Unexpected internal failure for that one URL |

Whole-call errors — the entire return is `{"error", "detail"}`, no `results`:

| Code | Meaning |
|---|---|
| `invalid_mode` / `invalid_extraction` / `invalid_timeout` | Bad argument value |
| `invalid_urls` | `scrape_pages` got an empty list or more than 10 URLs |

**One bad URL never sinks a batch.** `scrape_pages` always returns one entry per input URL, in input order — failures come back as error entries alongside the successes.

### Examples

```python
from tools.scrape import scrape_page, scrape_pages

# Single page → markdown
res = scrape_page(url="https://example.com")
if "error" not in res:
    print(res["title"], res["status"], len(res["content"]))

# JS-rendered page
res = scrape_page(url="https://spa-site.com", mode="browser", timeout=60)

# Bot-protected page
res = scrape_page(url="https://protected-site.com", mode="stealth", solve_cloudflare=True)

# Batch — split successes from failures
batch = scrape_pages(urls=[...], mode="fast")   # <= 10 URLs
pages = [r for r in batch["results"] if "error" not in r]
failed = [(r["url"], r["error"]) for r in batch["results"] if "error" in r]

# Raw HTML when you need to parse structure yourself
res = scrape_page(url="https://example.com", extraction="html")
from bs4 import BeautifulSoup
soup = BeautifulSoup(res["content"], "html.parser")
titles = [h1.get_text() for h1 in soup.find_all("h1")]
```

Batches run concurrently — 8 at a time in `fast` mode, 2 at a time in `browser` / `stealth` (browser sessions are memory-heavy). More than 10 URLs means more than one call.

---

## Direct Python API (Advanced)

For selectors, sessions, spiders, or when you need the full Page object. **Requires imports. Async.**

### Fetcher (Fast HTTP — Tier 1)

```python
from scrapling.fetchers import AsyncFetcher

page = await AsyncFetcher.get("https://example.com", stealthy_headers=True)
print(page.status)       # 200
print(page.body)         # Raw bytes
print(page.headers)      # Response headers

# CSS selectors (Scrapy-style pseudo-elements)
titles = page.css("h1::text").getall()
links = page.css("a::attr(href)").getall()

# XPath
items = page.xpath("//div[@class='item']/text()").getall()

# BeautifulSoup-style
divs = page.find_all("div", class_="content")
```

### DynamicFetcher (Browser — Tier 2)

```python
from scrapling.fetchers import DynamicFetcher

page = await DynamicFetcher.async_fetch(
    "https://spa-website.com",
    headless=True,
    network_idle=True,
    disable_resources=True,
    timeout=30000,          # milliseconds here, unlike the MCP tools
    wait_selector=".data-table",
)
rows = page.css("table.data-table tr")
for row in rows:
    cells = row.css("td::text").getall()
```

### StealthyFetcher (Anti-Bot — Tier 3)

```python
from scrapling.fetchers import StealthyFetcher

page = await StealthyFetcher.async_fetch(
    "https://protected-site.com",
    headless=True,
    solve_cloudflare=True,
    network_idle=True,
)
```

### Sessions (Persistent Connections)

```python
from scrapling.fetchers import FetcherSession

with FetcherSession(impersonate="chrome") as session:
    login_page = session.post("https://site.com/login", data={...})
    dashboard = session.get("https://site.com/dashboard")
    data = dashboard.css(".user-data::text").getall()
```

### Spider (Multi-Page Crawl)

```python
from scrapling.spiders import Spider, Request, Response

class PriceScraper(Spider):
    name = "prices"
    start_urls = ["https://example.com/products"]
    concurrent_requests = 5

    async def parse(self, response: Response):
        for product in response.css(".product"):
            yield {
                "name": product.css(".name::text").get(),
                "price": product.css(".price::text").get(),
            }
        next_page = response.css("a.next::attr(href)").get()
        if next_page:
            yield Request(next_page)

spider = PriceScraper()
result = spider.start()
result.items.to_json("work/<task_name>/data/prices.json")
```

## Converting HTML to Markdown

Only needed when you fetched HTML yourself — `scrape_page(extraction="markdown")` already does this.

```python
import html_to_markdown

markdown = html_to_markdown.convert(
    html_string, html_to_markdown.ConversionOptions(extract_metadata=False)
).content

# Article-only extraction (strips nav/ads/boilerplate)
import trafilatura

article = trafilatura.extract(html_string, output_format="markdown", favor_recall=True)
```

## When to Use Which

| Need | Use |
|------|-----|
| Quick page content as markdown | `scrape_page()` |
| Several known URLs at once | `scrape_pages()` (≤10 per call) |
| Extract specific elements (CSS/XPath) | Direct Python API with selectors |
| Login + scrape authenticated pages | Direct Python API with sessions |
| Crawl many pages with pagination | Direct Python API with Spider |
| Bypass Cloudflare | `scrape_page(mode="stealth", solve_cloudflare=True)` or direct `StealthyFetcher` |
| Save results to file | Direct Python API (spider `.to_json()`) |
