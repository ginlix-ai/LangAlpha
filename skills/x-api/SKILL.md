---
name: x-api
description: "Search X (Twitter) posts, pull user profiles, fetch specific tweets, and read reply threads for sentiment, news, and event research. Triggers on 'X', 'Twitter', 'tweets about', 'sentiment on', 'what are people saying about', 'historical tweets', or any request to read public X content."
---

# X (Twitter) API

Read-only access to X content via five MCP tools. Use for sentiment on tickers, exec announcements, launches, event tracking, and qualitative research alongside SEC/market data.

> **Not for single-post URL lookups.** If the user hands you a specific X post URL and just wants its text or context, use `web_fetch` on the URL — no vault, no auth, no rate limit. Reach for this skill when the task is **search, aggregation, or thread traversal**.

## Auth

Every tool requires a `bearer_token`. Read it once per code block from the workspace vault:

```python
from vault import get
token = get("X_BEARER_TOKEN")
```

If `token` is `None` or empty, the user hasn't added it yet. For the setup walkthrough and per-error fixes, see [TROUBLESHOOTING.md](./TROUBLESHOOTING.md).

## Tools at a glance

The primary use case is **search** — the first two tools are what you'll reach for most.

| Tool | Use for | Page size | Notes |
|---|---|---|---|
| `search_posts` | Posts from the last ~7 days | default 10, max 100 | Default choice. Query ≤512 chars. |
| `search_all_posts` | Posts older than 7 days (back to 2006) | default 10, max 500 | Paid-tier X plan only. Query ≤1024 chars. |
| `get_conversation` | Reply thread to a root tweet | default 50, max 100 | Uses recent search — thread must be ≤7 days old. Root tweet not included. |
| `get_user_by_username` | A user profile + metrics | — | Handle without `@`, ≤15 chars. |
| `get_tweet_by_id` | Hydrate a single post (mainly to find its `conversation_id` before `get_conversation`) | — | For a one-off URL the user already has, prefer `web_fetch`. |

## Examples

### Recent sentiment on a ticker

```python
res = search_posts(
    query="$NVDA -is:retweet lang:en",
    bearer_token=token,
    max_results=100,
)
posts = res["posts"]
posts.sort(key=lambda p: p["public_metrics"].get("impression_count", 0), reverse=True)
for p in posts[:10]:
    print(p["author"]["username"], p["public_metrics"].get("like_count"), p["text"][:140])
```

### Historical reaction to a past event

```python
res = search_all_posts(
    query="$TSLA earnings -is:retweet lang:en",
    bearer_token=token,
    max_results=500,
    start_time="2020-03-13T00:00:00Z",
    end_time="2020-03-20T00:00:00Z",
)
```

### Full thread on a specific tweet

```python
root = get_tweet_by_id(tweet_id="1700000000000000001", bearer_token=token)
thread = get_conversation(
    conversation_id=root["post"]["conversation_id"],
    bearer_token=token,
    max_results=100,
)
all_posts = [root["post"], *thread["posts"]]
```

### Paginate through a large result

```python
posts, next_tok = [], None
while len(posts) < 500:
    res = search_posts(
        query="from:FedSpeakers",
        bearer_token=token,
        max_results=100,
        next_token=next_tok,
    )
    if "error" in res:
        break
    posts.extend(res["posts"])
    next_tok = res.get("next_token")
    if not next_tok:
        break
```

## Post shape

Each post: `id`, `text`, `created_at`, `lang`, `conversation_id`, `author_id`, `edit_history_tweet_ids`, `public_metrics` (retweet/reply/like/quote/bookmark/impression counts), `author` — which is `{id, username, name, verified}`, `{id, unresolved: true}` for suspended/deleted users, or `None` if the tweet has no author_id.

Full per-tool response schemas (including user shape and error variants): [reference.md](./reference.md).

## Query syntax

`$TSLA` (cashtag), `#hashtag`, `from:elonmusk`, `to:@SEC_News`, `-is:retweet`, `is:verified`, `has:links`, `has:media`, `lang:en`, `"exact phrase"`, parentheses + `OR` for alternation. Full list: https://docs.x.com/x-api/posts/search/introduction.

## Errors

Every tool returns `{"error": "...", ...}` on failure — they never raise. Always check for `error` before accessing `posts` / `user` / `post`. For the per-error playbook (including setup fixes and tier gotchas), read [TROUBLESHOOTING.md](./TROUBLESHOOTING.md).

## Do / Don't

- **Do** read `token = get("X_BEARER_TOKEN")` once and reuse it across calls.
- **Do** cross-reference with `get_daily_prices` and `get_sec_filing` when investigating price moves or disclosures.
- **Don't** hardcode tokens. Ever.
- **Don't** cache `next_token` across sessions — cursors can expire.
- **Don't** assume every author is resolved — check for `{unresolved: true}` before reading `username`.

## Related

- `get_daily_prices` — cross-reference X sentiment with price action
- `get_sec_filing` — pair chatter with official disclosures
- `scrapling` `get` / `fetch` — fallback for public pages when the API is blocked
- `web_search` — broader news search that also indexes X posts
