# X API Response Reference

Schemas observed from live calls against `api.x.com/2` (2026-04-21). Every tool returns either the success shape shown or the common error shape. All fields other than `id`, `text`, `author_id`, `conversation_id`, `created_at` should be treated as best-effort — X occasionally omits a metric on deleted/suspended authors.

## Common error shape

Every tool returns this on failure. Always check for `error` before accessing `posts` / `post` / `user`.

```json
{
  "error": "missing_token | invalid_argument | auth_failed | rate_limited | http_error | not_found | network_error | malformed_response | client_unavailable",
  "detail": "string"
}
```

Some error types add extra keys:
- `auth_failed`, `http_error` → `status` (int), `detail` (whitelisted X error body)
- `rate_limited` → `reset_at_epoch` (int | null), `retry_after_seconds` (int | null)

Full per-error playbook: [TROUBLESHOOTING.md](./TROUBLESHOOTING.md).

## `search_posts` / `search_all_posts` / `get_conversation`

All three return the same success shape. `get_conversation` omits the root tweet — fetch it with `get_tweet_by_id` if you need it.

```json
{
  "posts": [
    {
      "id": "string",
      "text": "string",
      "created_at": "string (ISO8601 UTC, e.g. 2026-04-21T12:34:56.000Z)",
      "lang": "string",
      "conversation_id": "string",
      "author_id": "string",
      "edit_history_tweet_ids": ["string"],
      "public_metrics": {
        "retweet_count": "integer",
        "reply_count": "integer",
        "like_count": "integer",
        "quote_count": "integer",
        "bookmark_count": "integer",
        "impression_count": "integer"
      },
      "author": {
        "id": "string",
        "username": "string",
        "name": "string",
        "verified": "boolean"
      }
    }
  ],
  "next_token": "string | null",
  "result_count": "integer"
}
```

**`author` can take three shapes:**
- `{id, username, name, verified}` — resolved user (normal case)
- `{id, unresolved: true}` — author_id was returned but user expansion didn't resolve (suspended/deleted)
- `null` — the tweet has no `author_id` at all (rare; mostly data anomalies)

Always branch on `author is None` / `author.get("unresolved")` before reading `username`.

## `get_tweet_by_id`

```json
{
  "post": {
    "id": "string",
    "text": "string",
    "created_at": "string (ISO8601 UTC)",
    "lang": "string",
    "conversation_id": "string",
    "author_id": "string",
    "edit_history_tweet_ids": ["string"],
    "public_metrics": { /* same 6 keys as above */ },
    "author": { /* same 3-shape union as above */ }
  }
}
```

Deleted or private tweets return `{"error": "not_found", "detail": "Tweet '…' not found"}`.

## `get_user_by_username`

```json
{
  "user": {
    "id": "string",
    "username": "string",
    "name": "string",
    "verified": "boolean",
    "description": "string",
    "created_at": "string (ISO8601 UTC)",
    "public_metrics": {
      "followers_count": "integer",
      "following_count": "integer",
      "tweet_count": "integer",
      "listed_count": "integer",
      "like_count": "integer",
      "media_count": "integer"
    }
  }
}
```

Suspended, deactivated, or nonexistent handles return `{"error": "not_found", "detail": "User '…' not found"}`.

## Input constraints (validated client-side before the API call)

| Tool | Field | Rule |
|---|---|---|
| `search_posts` | `query` | length ≤ 512 chars |
| `search_posts` | `max_results` | integer in [10, 100] |
| `search_all_posts` | `query` | length ≤ 1024 chars |
| `search_all_posts` | `max_results` | integer in [10, 500] |
| `get_conversation` | `conversation_id` | matches `^\d{1,25}$` |
| `get_conversation` | `max_results` | integer in [10, 100] |
| `get_tweet_by_id` | `tweet_id` | matches `^\d{1,25}$` |
| `get_user_by_username` | `username` | matches `^[A-Za-z0-9_]{1,15}$` (no leading `@`) |

Violations return `{"error": "invalid_argument", "detail": "<reason>"}` without hitting the network.

## Quick notes for downstream code

- `public_metrics` keys on posts are whatever X returns — the 6 above cover normal tweets, but a deleted or limited-reach tweet may omit some. Use `.get("like_count", 0)` rather than direct indexing.
- `edit_history_tweet_ids` includes the current tweet's id. Single-element list means unedited.
- `next_token` is an opaque cursor. Don't cache across sessions — cursors expire.
- `created_at` is always UTC. Parse with `datetime.fromisoformat(s.rstrip("Z") + "+00:00")` in Python 3.11+ or drop the `Z` and parse as UTC.
