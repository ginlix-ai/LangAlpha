# Scrapling API Cheat Sheet

## Fetchers

### Fetcher (HTTP, no JS)

```python
from scrapling.fetchers import Fetcher
page = Fetcher.get("https://example.com", impersonate="chrome", timeout=15)
page = Fetcher.post("https://example.com/api", json={"key": "val"})
```

| Param | Type | Default | Notes |
|---|---|---|---|
| `url` | `str` | required | |
| `impersonate` | `str\|list` | `"chrome"` | `"chrome110"`, `"firefox102"`, `"safari15_5"` |
| `timeout` | `int` | `30` | seconds |
| `proxy` | `str` | `None` | `"http://user:pass@host:port"` |
| `headers` / `cookies` | `dict` | `None` | |
| `data` / `json` | `dict` | `None` | POST/PUT body |
| `stealthy_headers` | `bool` | `True` | realistic browser headers |
| `follow_redirects` | `bool` | `True` | |
| `retries` | `int` | `3` | |

Also: `.put()`, `.delete()`. Async: `AsyncFetcher` (same API, coroutines).

### FetcherSession (connection pooling, ~10x faster)

```python
from scrapling.fetchers import FetcherSession
with FetcherSession(impersonate="chrome") as session:  # also works as async with
    page = session.get("https://example.com")
```

### DynamicFetcher (browser, JS rendering)

```python
from scrapling.fetchers import DynamicFetcher
page = DynamicFetcher.fetch("https://example.com", disable_resources=True)
page = await DynamicFetcher.async_fetch("https://example.com")  # async variant
```

| Param | Type | Default | Notes |
|---|---|---|---|
| `headless` | `bool` | `True` | |
| `disable_resources` | `bool` | `False` | block fonts/images/css (~25% faster) |
| `network_idle` | `bool` | `False` | wait for zero network activity 500ms |
| `timeout` | `int` | `30000` | **milliseconds** |
| `wait` | `int` | `None` | extra wait after load (ms) |
| `wait_selector` | `str` | `None` | CSS selector to wait for |
| `wait_selector_state` | `str` | `"attached"` | `attached`/`detached`/`visible`/`hidden` |
| `page_action` | `Callable` | `None` | `fn(playwright_page)` for automation |
| `proxy` | `str\|dict` | `None` | |
| `blocked_domains` | `set` | `None` | domain names to block |

Session variants: `DynamicSession` / `AsyncDynamicSession` (keeps browser open across fetches, supports `max_pages` for tab pooling).

### StealthyFetcher (anti-bot bypass)

All `DynamicFetcher` params plus:

```python
from scrapling.fetchers import StealthyFetcher
page = StealthyFetcher.fetch("https://protected-site.com", solve_cloudflare=True)
```

| Extra Param | Default | Notes |
|---|---|---|
| `solve_cloudflare` | `False` | auto-solve Cloudflare challenges |
| `block_webrtc` | `False` | prevent IP leak via WebRTC |
| `hide_canvas` | `False` | anti-fingerprint canvas noise |

Session variants: `StealthySession` / `AsyncStealthySession`.

### When to Use Which

| | Fetcher | DynamicFetcher | StealthyFetcher |
|---|---|---|---|
| JS | No | Yes | Yes |
| Speed | Fast | Medium | Medium |
| Anti-bot | Low | Medium | High |

---

## Response Object

Returned by all fetchers. Inherits all Selector methods below.

| Attribute | Type | Description |
|---|---|---|
| `status` | `int` | HTTP status code |
| `url` | `str` | Response URL |
| `headers` / `cookies` | `dict` | Response headers/cookies |
| `body` | `bytes` | Raw body |
| `text` | `TextHandler` | Text content |
| `json()` | `dict` | Parse body as JSON |
| `urljoin(rel)` | `str` | Resolve relative URL |

---

## Selectors (on Response or Selector)

```python
from scrapling import Selector
sel = Selector(content="<html>...</html>")

# CSS (supports ::text and ::attr(name) pseudo-elements)
titles = page.css("h1::text").getall()
links = page.css("a::attr(href)").getall()
first_title = page.css("h1::text").get()

# XPath
items = page.xpath("//div[@class='item']")

# find / find_all (BeautifulSoup-like)
items = page.find_all("div", class_="item")
first = page.find("div", class_="item")

# Text search
el = page.find_by_text("Click here", partial=True)

# Regex on element text
matches = page.re(r"\$[\d,]+\.?\d*")

# All text content
text = page.get_all_text(separator="\n", strip=True)
```

**DOM navigation:** `parent`, `children`, `siblings`, `next`, `previous`

**Element attributes:** `el["href"]`, `"href" in el`, `el.attrib`, `el.has_class("active")`, `el.tag`, `el.html_content`

### Selectors Collection (returned by css/xpath/find_all)

| Method/Prop | Returns | Description |
|---|---|---|
| `get()` | `TextHandler` | First element serialized |
| `getall()` | `TextHandlers` | All elements serialized |
| `first` / `last` | `Selector` | First/last element |
| `css(sel)` / `xpath(sel)` | `Selectors` | Sub-select across all |
| `re(regex)` | `TextHandlers` | Regex across all |

**TextHandler** extends `str` with `clean()`, `json()`, `re()`, `re_first()`. **TextHandlers** extends `list[TextHandler]` with `get()`, `getall()`, `re()`, `re_first()`.

---

## Spider

```python
from scrapling.spiders import Spider, Response, Request

class QuotesSpider(Spider):
    name = "quotes"
    start_urls = ["https://quotes.toscrape.com"]
    concurrent_requests = 4
    download_delay = 0.5

    async def parse(self, response: Response):
        for quote in response.css("div.quote"):
            yield {
                "text": quote.css("span.text::text").get(""),
                "author": quote.css("small.author::text").get(""),
            }
        next_page = response.css("li.next a::attr(href)").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

result = QuotesSpider().start()
# result.items -> ItemList (list of dicts), result.stats -> CrawlStats
# result.items.to_json("out.json") / result.items.to_jsonl("out.jsonl")
```

### Class Attributes

| Attribute | Default | Description |
|---|---|---|
| `name` | required | Spider identifier |
| `start_urls` | `[]` | Initial URLs |
| `allowed_domains` | `set()` | Domain whitelist |
| `concurrent_requests` | `4` | Max simultaneous requests |
| `download_delay` | `0.0` | Seconds between requests |

### Lifecycle Hooks

| Hook | Description |
|---|---|
| `async parse(self, response)` | **Required.** Yield dicts (items) or Requests. |
| `async on_start(self)` | Before crawling |
| `async on_close(self)` | After crawling |
| `async on_error(self, request, error)` | Handle errors |
| `async is_blocked(self, response) -> bool` | Custom block detection |

### Requests

```python
yield Request("https://example.com/page2", callback=self.parse_detail, meta={"id": 1})
yield response.follow("/page2", callback=self.parse_detail)  # resolves relative URLs
```

---

## MCP Tool Wrappers (auto-generated in sandbox)

The Scrapling MCP server exposes 6 tools, auto-registered as top-level Python functions in the sandbox. **No imports needed. Synchronous — no `await`.**

```python
# No import needed — available as top-level function
result = get(url="https://example.com", extraction_type="markdown")
print(result["content"][0])  # markdown string
```

### Response Format

All tools return a **dict** (not a Page object):

```python
{"status": 200, "url": "https://example.com", "content": ["<text>", ""]}
```

- `content` is a **list** — actual text is `content[0]`
- No `.css()`, `.xpath()` methods — use BeautifulSoup to parse if needed
- `css_selector` param returns raw HTML of matched elements, not parsed text

### `get` — Fast HTTP fetch

| Param | Type | Default | Notes |
|---|---|---|---|
| `url` | `str` | required | |
| `extraction_type` | `str` | `"markdown"` | `"markdown"`, `"HTML"`, `"text"` |
| `css_selector` | `str` | `None` | Returns raw HTML of matched elements |
| `main_content_only` | `bool` | `True` | `<body>` only |
| `impersonate` | `str` | `"chrome"` | Browser fingerprint |
| `timeout` | `int` | `30` | **seconds** |
| `stealthy_headers` | `bool` | `True` | Realistic browser headers |
| `proxy` | `str` | `None` | Proxy URL |
| `follow_redirects` | `bool` | `True` | |
| `retries` | `int` | `3` | |

### `fetch` — Browser fetch (Playwright)

| Param | Type | Default | Notes |
|---|---|---|---|
| `url` | `str` | required | |
| `extraction_type` | `str` | `"markdown"` | |
| `css_selector` | `str` | `None` | |
| `headless` | `bool` | `True` | |
| `network_idle` | `bool` | `False` | Wait for zero network activity |
| `disable_resources` | `bool` | `False` | Block fonts/images (~25% faster) |
| `wait_selector` | `str` | `None` | Wait for CSS selector before extract |
| `timeout` | `int` | `30000` | **milliseconds** |
| `proxy` | `str\|dict` | `None` | |

### `stealthy_fetch` — Anti-bot browser fetch

All `fetch` params plus:

| Param | Type | Default | Notes |
|---|---|---|---|
| `solve_cloudflare` | `bool` | `False` | Auto-solve Cloudflare challenges |
| `hide_canvas` | `bool` | `False` | Canvas fingerprint noise |
| `block_webrtc` | `bool` | `False` | Prevent IP leak |

### Bulk variants

`bulk_get`, `bulk_fetch`, `bulk_stealthy_fetch` — same params but `urls: list[str]` instead of `url`. Returns list of results.
