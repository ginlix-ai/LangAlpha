# X API Troubleshooting

Read this when:

- A tool returned `{"error": "missing_token", ...}` and you need to tell the user how to fix it.
- A tool returned `auth_failed`, `rate_limited`, `http_error`, `not_found`, `network_error`, `malformed_response`, or `invalid_argument` and you're not sure what to do.
- The user asks "how do I set up X" or "where do I get the token."

## First-time setup — get a Bearer Token and store it

The X MCP tools are read-only and use a Bearer Token (app-only auth). The user supplies one per workspace via the vault. You (the agent) never see the raw token — you just call `get("X_BEARER_TOKEN")` inside sandbox code.

Walk the user through these exact steps when the vault is empty:

### Step 1 — Create an X developer app (one-time)

1. Go to **https://console.x.com/** (requires an X account).
2. Accept the developer terms if prompted. The Free tier is enough for low-volume read.
3. In the Developer Portal, create a **Project** and an **App** inside it (any names are fine).

### Step 2 — Copy the Bearer Token

1. Open the App → **Keys and tokens** tab.
2. Under **Bearer Token**, click **Generate** (or **Regenerate** if one already exists).
3. Copy the token string immediately — X only shows it once. It looks like a long opaque string (often starts with `AAAA…`).

> If the user regenerates, any previously-stored value in the vault becomes invalid and will start returning `auth_failed`. They'll need to paste the new one.

### Step 3 — Store it in the workspace vault

1. In the LangAlpha UI, open the **Workspace Files** panel on the right side of the chat.
2. Click the **settings icon** in the top of the Workspace Files panel header — this opens **Workspace Settings**.
3. Switch to the **Vault** tab (second tab, between **Overview** and **Storage**).
4. Click **Add Secret** (or the equivalent "+" / new-entry button on that tab).
5. Set **Name** to exactly `X_BEARER_TOKEN` (case-sensitive, no whitespace, no `Bearer ` prefix — just the token itself).
6. Paste the token into **Value** and save.

The value is encrypted at rest and scoped to that one workspace — other workspaces do not see it.

### Step 4 — Verify from sandbox code

Run this in an `execute_code` block to confirm the key is visible:

```python
from vault import get
token = get("X_BEARER_TOKEN")
print("present:", bool(token), "length:", len(token) if token else 0)
```

Expect `present: True` and a non-trivial length. If `present: False`, the secret wasn't saved under the exact name `X_BEARER_TOKEN` — ask the user to re-open **Workspace Settings → Vault** and double-check the secret name.

**Never `print(token)` in full, paste it into a chat reply, or write it to a file.**

## Error playbook

Each tool returns `{"error": "...", ...}` on failure — never raises. Match the `error` value to the row below.

### `missing_token`

```json
{"error": "missing_token", "detail": "X bearer token required. Pass bearer_token from the workspace vault ..."}
```

**Meaning:** `bearer_token` argument was empty/None AND the `X_BEARER_TOKEN` env var is unset inside the MCP subprocess.

**What to do:**

1. If you forgot to pass `bearer_token=token` to the tool — fix your call.
2. Otherwise, the vault doesn't have `X_BEARER_TOKEN`. Stop trying. Tell the user:
   > "I don't have an X API token in this workspace's vault. To add one: open the **Workspace Files** panel on the right → click the settings icon at the top → switch to the **Vault** tab in **Workspace Settings** → add a secret named `X_BEARER_TOKEN`. Get the token from https://console.x.com (App → Keys and tokens → Bearer Token)."
3. Do not attempt a workaround (scraping, googling the data) unless the user says the token setup is blocked.

### `auth_failed` (status 401 or 403)

```json
{"error": "auth_failed", "status": 401, "detail": {"title": "Unauthorized", ...}}
```

**Meaning:** The token exists but X rejected it.

**First — is it a tier issue?** `search_all_posts` hits X's full-archive endpoint, which needs a pay-per-use or Enterprise X API plan. A free-tier token will 403 there even when it works fine for `search_posts`. If **only** `search_all_posts` fails, don't ask the user to regenerate anything — tell them full-archive search requires a paid X API plan and fall back to `search_posts` for the recent window.

**Otherwise**, the token is bad. Common causes: regenerated in console.x.com and vault still has the old one; copied with whitespace or a `Bearer ` prefix; wrong token type (OAuth 1.0a user token instead of app-only Bearer Token).

**What to do:**

1. Surface the exact `status` to the user.
2. Ask them to refresh: console.x.com → App → Keys and tokens → **Regenerate** Bearer Token → paste the new value into the vault (replacing `X_BEARER_TOKEN`).
3. Do not retry more than once on the same token.

### `rate_limited` (status 429)

```json
{"error": "rate_limited", "reset_at_epoch": 1777777760, "retry_after_seconds": 47}
```

**Meaning:** Rate limit hit. `search_all_posts` has a much tighter ceiling than the others (~1 req/sec), so back-to-back calls to it are the usual culprit.

**What to do:**

- `retry_after_seconds` ≤ ~60: `time.sleep(retry_after_seconds)` then retry once.
- `retry_after_seconds` > 60 or `None`: stop. Tell the user the app is rate-limited and will recover at `reset_at_epoch` (convert to local time for the message). Don't loop.
- Narrow future queries (`start_time`/`end_time`, higher `max_results` per page) to cut request volume.

### `http_error` (400 / 404 / 5xx)

```json
{"error": "http_error", "status": 400, "detail": {"errors": [{"title": "Invalid query", "detail": "..."}]}}
```

**Meaning:** X returned a non-2xx that isn't 401/403/429.

**What to do:**

- **400 Bad Request**: look at `detail.errors[].title` and `.detail`. Usually your query used an unsupported operator, exceeded 512 chars, or had bad ISO8601 times. Fix and retry.
- **404 Not Found**: the resource genuinely doesn't exist. Move on.
- **5xx**: X side. Retry once after a 5–10s wait. If it persists, tell the user the X API is unhealthy and stop.

### `not_found`

```json
{"error": "not_found", "detail": "User 'foo' not found"}
```

**Meaning:** User handle or tweet id doesn't resolve. Could be deleted, suspended, private, or never existed.

**What to do:** Move on. If the user specifically asked about that account/tweet, tell them it's unavailable.

### `network_error`

```json
{"error": "network_error", "detail": "ConnectError"}
```

**Meaning:** TCP/DNS/TLS failure before X could respond. `detail` is only the exception class name (deliberately — raw error strings can leak URLs/tokens).

**What to do:** Retry once. If it fails again, stop and tell the user the sandbox can't reach `api.x.com`.

### `malformed_response`

**Meaning:** X returned 2xx with a non-JSON body (rare, usually a CDN edge hiccup).

**What to do:** Retry once. If it recurs, treat as a transient X-side issue and stop.

### `client_unavailable`

**Meaning:** The MCP subprocess's HTTP client isn't initialized. Should never happen in normal operation — the lifespan context manager sets it up on startup and tears it down on shutdown.

**What to do:** Retry the tool call once. If it persists, the MCP server subprocess is in a bad state — tell the user to restart their workspace.

### `invalid_argument`

```json
{"error": "invalid_argument", "detail": "max_results must be 10-100"}
```

**Meaning:** You passed a value our own validator rejected before hitting X.

Common triggers and fixes:

| `detail` | Cause | Fix |
|---|---|---|
| `max_results must be 10-100` (search_posts / get_conversation) | Passed `<10` or `>100` | Use `max_results=10` minimum. Need fewer? Slice the result list. |
| `max_results must be 10-500` (search_all_posts) | Passed outside `[10, 500]` | Full-archive allows up to 500/page. Use 10 minimum. |
| `query must be <= 512 chars (got N)` (search_posts) | Long `OR`-chained query | Split calls, narrow with `lang:`, `-is:retweet`, `from:`, or switch to `search_all_posts` (1024-char limit). |
| `query must be <= 1024 chars (got N)` (search_all_posts) | Extremely long query | Split into multiple calls; there's no higher tier than 1024 without Enterprise. |
| `username must match ^[A-Za-z0-9_]{1,15}$` | Passed with `@`, spaces, or wrong length | Strip `@`, verify the handle with the user |
| `tweet_id must be numeric` / `conversation_id must be numeric` | Passed a URL or non-digit id | Extract the trailing digits from `https://x.com/user/status/1234...` |

## What not to do

- Don't regenerate the user's token on their behalf — it's their console.
- Don't hardcode or log the token. Not in code, not in comments, not in sandbox `print()`.
- Don't scrape x.com via `scrapling` / `web_fetch` as a workaround when the token is the actual blocker — ask the user to add the key first.
