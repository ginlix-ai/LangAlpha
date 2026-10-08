"""UserDataBackend: the user's portfolio, watchlists, preferences and account as JSON files.

Mounted at `.agents/user/profile/` (``portfolio.json``, ``watchlist.json``,
``preference.json``, ``user.json``). Reads serialize the live DB rows on
demand; a write is parsed, validated and diffed against the rows, then applied
in one transaction.
Concurrent writes are serialized by the profile's advisory lock, and races with
the dashboard are caught by the version (a hash of the agent-visible content).
The file plumbing (read cache, save flow, Edit, Glob, Grep) is ``DbJsonRoute``'s;
each file's rows are ``services.profile_files``'.
"""

from __future__ import annotations

from ptc_agent.agent.backends.db_json_route import README_FILE, DbJsonRoute
from ptc_agent.core.paths import SandboxLayout
from src.server.services.profile_files import (
    PORTFOLIO_FILE,
    PREFERENCE_FILE,
    PROFILE_FILES,
    USER_FILE,
    WATCHLIST_FILE,
)

__all__ = [
    "PORTFOLIO_FILE",
    "PREFERENCE_FILE",
    "README_FILE",
    "USER_FILE",
    "WATCHLIST_FILE",
    "UserDataBackend",
]

_README_CONTENT = """\
# User Profile Data

Virtual JSON files backed by the live database. Reads return fresh content;
writes are validated and applied in a single transaction.

**Read a file before you Write it.** The server tracks the version it served
you to detect concurrent edits, and refuses a write over a change you haven't
seen. Read again after a write that saves or is refused because the file
changed; after any other refusal, fix and retry.

Code sees these files at the same paths when the sandbox has the file mount.
A program saves a file whole, and a refused save is explained in the
command's result. Write in place: `sed -i` and write-then-rename helpers need
a new file in this folder, which fails with Permission denied.

**Editing one field on one row: include the whole object in `old_string`.**
Every holding has a `quantity`, every watchlist item has a `notes`, every
ticker shares the same key set — matching on a field name alone (or even
`"quantity": "100"`) will collide with siblings. To change AAPL's quantity,
your `old_string` must be the entire `{ "symbol": "AAPL", ... }` object so it
matches exactly once; your `new_string` is the same object with the field
swapped. Same rule for watchlist item edits and watchlist-level renames.
Use `Write` (whole-file replace) if you want to make many edits at once.

## user.json

```json
{
  "name": "Alex",
  "timezone": "America/New_York",
  "locale": "en-US",
  "onboarding_completed": false
}
```

The user's own account. `locale` is the language answers default to, and
`timezone` is the zone their turns and new automations run in.

| Field | Required | Type | Max | Notes |
|-------|----------|------|-----|-------|
| name                 | no | string \\| null | 255 | What to call the user. |
| timezone             | no | string \\| null | 100 | IANA zone, e.g. `Asia/Shanghai`. |
| locale               | no | string \\| null | 20  | e.g. `en-US`, `zh-CN`. |
| onboarding_completed | no | boolean         | —   | Set `true` when onboarding is done. |

## portfolio.json

```json
{
  "holdings": [
    {
      "symbol": "AAPL",
      "instrument_type": "stock",
      "exchange": "NASDAQ",
      "name": "Apple Inc.",
      "quantity": "100",
      "average_cost": "150.25",
      "currency": "USD",
      "account_name": "Main",
      "notes": "Long-term hold",
      "first_purchased_at": "2024-01-15"
    }
  ]
}
```

Rows are matched by `(symbol, instrument_type, account_name)`. To update a
position, edit the row in place. To add one, append a new object. To remove,
delete the object from the array.

| Field | Required | Type | Max | Notes |
|-------|----------|------|-----|-------|
| symbol             | yes | string  | 50  | Ticker, no whitespace. |
| instrument_type    | yes | string  | 30  | e.g. `stock`, `etf`, `crypto`, `bond`. |
| quantity           | yes | decimal | —   | Non-negative. |
| average_cost       | no  | decimal | —   | Non-negative cost basis per unit. |
| exchange           | no  | string  | 50  | e.g. `NASDAQ`. |
| name               | no  | string  | 255 | Display name. |
| currency           | no  | string  | 10  | ISO code (`USD`, `CNY`). A new row defaults to its listing's; a held row keeps its own. |
| account_name       | no  | string \\| null | 100 | Lets the same symbol exist in multiple accounts. |
| notes              | no  | string  | —   | Free-form. |
| first_purchased_at | no  | date    | —   | `YYYY-MM-DD`. |

Decimal fields (`quantity`, `average_cost`) accept either a JSON number or a
JSON string. Strings are emitted on read and preferred on write for
high-precision values across `DECIMAL(18,8)` storage. The row also carries a
server-managed `metadata` object that is not exposed here; treat the JSON
you see as the agent-editable portion of the row.

## watchlist.json

```json
{
  "watchlists": [
    {
      "name": "Tech",
      "description": "Large-cap tech",
      "is_default": true,
      "items": [
        {
          "symbol": "AAPL",
          "instrument_type": "stock",
          "exchange": "NASDAQ",
          "name": "Apple Inc.",
          "notes": "",
          "alert_settings": {}
        },
        {
          "symbol": "MSFT",
          "instrument_type": "stock",
          "exchange": "NASDAQ",
          "name": "Microsoft"
        }
      ]
    }
  ]
}
```

Watchlists are matched by `name`. Items inside each watchlist are matched by
`(symbol, instrument_type)`.

**Renaming a watchlist is a delete + insert.** If you change a watchlist's
`name`, the server treats it as deletion of the old list and creation of a new
one — keep the items array intact in the same write or you will lose them.

**At most one watchlist may have `is_default: true`.**

Watchlist fields:

| Field | Required | Type | Max | Notes |
|-------|----------|------|-----|-------|
| name        | yes | string  | 100 | Unique per user. |
| description | no  | string  | —   | Free-form. |
| is_default  | no  | boolean | —   | At most one true across all watchlists. |
| items       | yes | array   | —   | May be empty. |

Item fields (inside `items`):

| Field | Required | Type | Max | Notes |
|-------|----------|------|-----|-------|
| symbol          | yes | string | 50  | Ticker. |
| instrument_type | yes | string | 30  | e.g. `stock`, `etf`. |
| exchange        | no  | string | 50  | e.g. `NASDAQ`. |
| name            | no  | string | 255 | Display name. |
| notes           | no  | string | —   | Free-form. |
| alert_settings  | no  | object | —   | Reserved for alerts; empty object is fine. |

## preference.json

```json
{
  "risk_preference": {
    "tolerance": "moderate",
    "max_position_pct": 0.15
  },
  "investment_preference": {
    "horizon": "long_term",
    "sectors": ["technology", "healthcare"]
  },
  "agent_preference": {
    "tone": "concise",
    "include_charts": true
  }
}
```

Three free-form JSON objects. Pick keys that read naturally back to you on
future turns (e.g. `risk_preference.tolerance`, `investment_preference.horizon`).

**Write only these three top-level keys.** Empty values must be `{}`, not
`null`. The server manages a fourth `other_preference` field (onboarding
state, internal flags) that you cannot see or edit.
"""


class UserDataBackend(DbJsonRoute):
    """Filesystem surface backed by `user_portfolios` / `watchlists` / `user_preferences` / `users` tables."""

    directory = SandboxLayout.USER_PROFILE_DIR
    files = PROFILE_FILES
    readme_content = _README_CONTENT

    source = "user_data_backend"
    read_failure = "Failed to read user profile data"
    read_only = (
        "User-profile JSON files are read-only through the file panel. "
        "Edit via the dashboard widget or ask the agent to update it."
    )
    undeletable = (
        "User-profile JSON files cannot be deleted through the file panel. "
        "Manage entries via the dashboard widgets."
    )
