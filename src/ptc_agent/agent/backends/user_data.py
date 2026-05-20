"""UserDataBackend — virtual JSON files for portfolio / watchlist / preferences.

Mounted on the CompositeFilesystemBackend at `.agents/user/profile/`. Three known files:
  - portfolio.json
  - watchlist.json
  - preference.json

Reads serialize the live DB rows on demand. Writes parse JSON, validate, version-check,
diff, and apply in a single transaction. Concurrent writes within one process are
serialized by a per-user `asyncio.Lock`; cross-process and UI-vs-agent races are caught
by an optimistic `__version__` check (sha256 hash of the agent-visible content).

Interface mirrors `StoreBackend` so the composite router needs no changes — same
`root_prefix` / `aread_text` / `awrite_text` / `aedit_text` / `aread_range` /
`aglob_paths` / `agrep_rich` surface.
"""

from __future__ import annotations

import fnmatch
import re
from typing import Any

import structlog

from ptc_agent.agent.backends.langgraph_store import lock_for_namespace
from ptc_agent.agent.backends.sandbox import SandboxBackend
from src.server.services import user_data_io as io

logger = structlog.get_logger(__name__)

PORTFOLIO_FILE = "portfolio.json"
WATCHLIST_FILE = "watchlist.json"
PREFERENCE_FILE = "preference.json"
README_FILE = "README.md"

_DATA_FILES = frozenset({PORTFOLIO_FILE, WATCHLIST_FILE, PREFERENCE_FILE})
_KNOWN_FILES = _DATA_FILES | frozenset({README_FILE})

_README_CONTENT = """\
# User Profile Data

Virtual JSON files backed by the live database. Reads return fresh content;
writes are validated and applied in a single transaction.

**Read each file before each Write/Edit.** The server tracks the version it
served you to detect concurrent edits. Any failed write also invalidates that
cached read — re-Read before retrying, even within the same turn.

**Editing one field on one row: include the whole object in `old_string`.**
Every holding has a `quantity`, every watchlist item has a `notes`, every
ticker shares the same key set — matching on a field name alone (or even
`"quantity": "100"`) will collide with siblings. To change AAPL's quantity,
your `old_string` must be the entire `{ "symbol": "AAPL", ... }` object so it
matches exactly once; your `new_string` is the same object with the field
swapped. Same rule for watchlist item edits and watchlist-level renames.
Use `Write` (whole-file replace) if you want to make many edits at once.

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
| currency           | no  | string  | 10  | ISO code (`USD`, `EUR`). Defaults to `USD`. |
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


class UserDataBackend:
    """Filesystem surface backed by `user_portfolios` / `watchlists` / `user_preferences` tables."""

    def __init__(
        self,
        *,
        user_id: str,
        sandbox_backend: SandboxBackend,
        root_prefix: str,
    ) -> None:
        if not root_prefix.endswith("/"):
            root_prefix = root_prefix + "/"
        self._user_id = user_id
        self._sandbox = sandbox_backend
        self._root_prefix = root_prefix
        # Filename → (agent-visible content, content-hash). Request-scoped
        # via agent.py — never reused across users.
        self._read_cache: dict[str, tuple[str, str]] = {}

    # --- composite-compatible surface ---

    @property
    def root_prefix(self) -> str:
        return self._root_prefix

    def normalize_path(self, path: str) -> str:
        return self._sandbox.normalize_path(path)

    def virtualize_path(self, path: str) -> str:
        return self._sandbox.virtualize_path(path)

    def validate_path(self, path: str) -> bool:
        return self._sandbox.validate_path(path)

    @property
    def filesystem_config(self) -> Any:
        return self._sandbox.filesystem_config

    # --- helpers ---

    def _filename(self, normalized_path: str) -> str | None:
        """Return the known basename if `path` is one of the three files; else None."""
        if not normalized_path.startswith(self._root_prefix):
            if normalized_path.rstrip("/") == self._root_prefix.rstrip("/"):
                return None  # the directory itself
            return None
        suffix = normalized_path[len(self._root_prefix):]
        if "/" in suffix:
            return None
        if suffix in _KNOWN_FILES:
            return suffix
        return None

    def _namespace(self) -> tuple[str, ...]:
        return (self._user_id, "profile")

    def _absolute(self, filename: str) -> str:
        return f"{self._root_prefix}{filename}"

    async def _read_serialized(self, filename: str) -> str:
        """Fetch + serialize one of the data files. Cached per-instance.

        The agent sees the business content only — the `__version__` content
        hash is stripped from the JSON and stashed in `_read_cache` instead.
        Writes/edits look up the cached version to detect concurrent changes
        without leaking the hash to the agent.
        """
        if filename == README_FILE:
            return _README_CONTENT
        cached = self._read_cache.get(filename)
        if cached is not None:
            return cached[0]
        if filename == PORTFOLIO_FILE:
            rows = await io.fetch_portfolio_for_user(self._user_id)
            payload = io.serialize_portfolio(rows)
        elif filename == WATCHLIST_FILE:
            watchlists, items = await io.fetch_watchlist_for_user(self._user_id)
            payload = io.serialize_watchlist(watchlists, items)
        elif filename == PREFERENCE_FILE:
            prefs = await io.fetch_preferences_for_user(self._user_id)
            payload = io.serialize_preferences(prefs)
        else:
            raise ValueError(f"Unknown user-data file: {filename}")
        version = payload["__version__"]
        visible = {k: v for k, v in payload.items() if k != "__version__"}
        content = io.serialize_json(visible)
        self._read_cache[filename] = (content, version)
        return content

    def _cached_version(self, filename: str) -> str | None:
        cached = self._read_cache.get(filename)
        return cached[1] if cached is not None else None

    def _invalidate(self, filename: str) -> None:
        self._read_cache.pop(filename, None)

    # --- read ---

    async def aread_text(self, file_path: str) -> str | None:
        filename = self._filename(file_path)
        if filename is None:
            # Anything under the prefix that isn't one of the three known files
            # is not ours — return None so the composite reports file-not-found.
            return None
        try:
            return await self._read_serialized(filename)
        except Exception:
            logger.exception("user_data aread_text failed", path=file_path, user_id=self._user_id)
            return None

    async def aread_range(self, file_path: str, offset: int = 0, limit: int = 2000) -> str | None:
        content = await self.aread_text(file_path)
        if content is None:
            return None
        lines = content.splitlines(keepends=True)
        start = max(0, offset)
        end = start + max(0, limit)
        return "".join(lines[start:end])

    # --- write / edit ---

    async def awrite_text(self, file_path: str, content: str) -> bool:
        """Validate + apply JSON write. Raises UserDataValidationError on bad input."""
        filename = self._filename(file_path)
        if filename is None:
            # Don't claim writes for unknown paths; let composite surface
            # "not in route" by returning False so caller can decide. But
            # composite already routed to us — the only honest answer is False.
            return False
        if filename == README_FILE:
            raise io.UserDataValidationError(
                error_type="schema_error",
                file=filename,
                field_path="",
                hint=(
                    f"{file_path} is documentation, not data — it cannot be "
                    "edited. Update portfolio.json / watchlist.json / preference.json "
                    "instead."
                ),
            )

        lock = lock_for_namespace(self._namespace())
        async with lock:
            try:
                await self._apply_write(filename, content)
            except io.UserDataValidationError as exc:
                # Drop the cache on version_conflict so the next Read pulls
                # fresh data — without this, retries would see the same stale
                # content and conflict again.
                if exc.error_type == "version_conflict":
                    self._invalidate(filename)
                raise
            except Exception as exc:
                logger.exception(
                    "user_data awrite_text failed",
                    path=file_path, user_id=self._user_id,
                )
                raise io.UserDataValidationError(
                    error_type="constraint_error",
                    file=filename,
                    field_path="",
                    hint=(
                        "database write failed. Re-read the file and retry. "
                        "If the problem persists, report it to the user."
                    ),
                ) from exc
            self._invalidate(filename)
        return True

    async def aedit_text(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        *,
        replace_all: bool = False,
    ) -> dict[str, Any]:
        filename = self._filename(file_path)
        if filename is None:
            return {"success": False, "error": f"File not found: {file_path}"}
        if filename == README_FILE:
            return {
                "success": False,
                "error": (
                    f"{file_path} is documentation, not data — it cannot be "
                    "edited. Update portfolio.json / watchlist.json / preference.json "
                    "instead."
                ),
            }
        if old_string == new_string:
            return {"success": False, "error": "old_string and new_string are identical"}

        lock = lock_for_namespace(self._namespace())
        async with lock:
            try:
                content = await self._read_serialized(filename)
            except Exception as exc:
                return {"success": False, "error": f"Read failed: {exc!s}"}

            occurrences = content.count(old_string)
            if occurrences == 0:
                preview = old_string if len(old_string) <= 120 else f"{old_string[:120]}…"
                return {"success": False, "error": f"String not found: {preview!r}"}
            if occurrences > 1 and not replace_all:
                return {
                    "success": False,
                    "error": (
                        f"String appears {occurrences} times. Provide more context or "
                        "set replace_all=True."
                    ),
                }
            new_content = content.replace(
                old_string, new_string, occurrences if replace_all else 1
            )

            try:
                await self._apply_write(filename, new_content)
            except io.UserDataValidationError as exc:
                if exc.error_type == "version_conflict":
                    self._invalidate(filename)
                return {"success": False, "error": str(exc)}
            except Exception:
                logger.exception("user_data aedit_text failed", path=file_path)
                return {
                    "success": False,
                    "error": "Edit failed due to a database error. Re-read the file and retry.",
                }
            self._invalidate(filename)

        return {
            "success": True,
            "occurrences": occurrences if replace_all else 1,
            "message": (
                f"Edited {file_path} ({occurrences} occurrences replaced)"
                if replace_all
                else f"Edited {file_path}"
            ),
        }

    async def _apply_write(self, filename: str, content: str) -> None:
        """Parse, version-check, diff, apply. Caller holds the per-user lock.

        The agent's payload no longer carries `__version__`. We use the version
        captured at the most recent cached Read; comparing it against a fresh DB
        version catches any writer that touched the row(s) in between.
        """
        cached_version = self._cached_version(filename)
        if cached_version is None:
            raise io.UserDataValidationError(
                error_type="version_conflict",
                file=filename,
                field_path="",
                hint=(
                    f"no fresh read of {self._absolute(filename)} in this turn. "
                    f"Read({self._absolute(filename)}) first, then re-apply your edit."
                ),
            )

        if filename == PORTFOLIO_FILE:
            current_rows = await io.fetch_portfolio_for_user(self._user_id)
            current_version = io.serialize_portfolio(current_rows)["__version__"]
            _check_version(cached_version, current_version, filename, hint_path=self._absolute(filename))
            diff = io.parse_and_diff_portfolio(content, current_rows)
            await io.apply_portfolio_diff(diff, self._user_id, payload_version=current_version)
            return

        if filename == WATCHLIST_FILE:
            watchlists, items = await io.fetch_watchlist_for_user(self._user_id)
            current_version = io.serialize_watchlist(watchlists, items)["__version__"]
            _check_version(cached_version, current_version, filename, hint_path=self._absolute(filename))
            diff = io.parse_and_diff_watchlist(content, watchlists, items)
            await io.apply_watchlist_diff(diff, self._user_id, payload_version=current_version)
            return

        if filename == PREFERENCE_FILE:
            current = await io.fetch_preferences_for_user(self._user_id)
            current_version = io.serialize_preferences(current)["__version__"]
            _check_version(cached_version, current_version, filename, hint_path=self._absolute(filename))
            values = io.parse_preferences(content)
            # No-op short-circuit: avoid opening a DB tx when the agent rewrote
            # the file with identical content.
            if io.preferences_equal(current, values):
                return
            await io.apply_preferences(values, self._user_id, payload_version=current_version)
            return

        raise ValueError(f"Unknown user-data file: {filename}")

    # --- glob / grep ---

    async def aglob_paths(self, pattern: str, path: str = ".") -> list[str]:
        normalized_path = self.normalize_path(path)
        if not (
            normalized_path.startswith(self._root_prefix)
            or normalized_path.rstrip("/") == self._root_prefix.rstrip("/")
        ):
            return []

        out: list[str] = []
        for filename in sorted(_KNOWN_FILES):
            absolute = self._absolute(filename)
            if fnmatch.fnmatch(filename, pattern) or fnmatch.fnmatch(absolute, pattern):
                out.append(absolute)
        return out

    async def agrep_rich(
        self,
        pattern: str,
        path: str = ".",
        output_mode: str = "files_with_matches",
        glob: str | None = None,
        type: str | None = None,  # noqa: A002 — mirror sandbox.agrep_rich
        *,
        case_insensitive: bool = False,
        show_line_numbers: bool = True,
        lines_after: int | None = None,
        lines_before: int | None = None,
        lines_context: int | None = None,
        multiline: bool = False,
        head_limit: int | None = None,
        offset: int = 0,
    ) -> Any:
        # Context-window args (lines_after / lines_before / lines_context / multiline)
        # are accepted for signature parity with the sandbox grep but are not
        # honored — these files are tiny JSON documents where the agent should
        # just Read them whole instead of grep-context-paging.
        normalized_path = self.normalize_path(path)
        if not (
            normalized_path.startswith(self._root_prefix)
            or normalized_path.rstrip("/") == self._root_prefix.rstrip("/")
        ):
            return []

        flags = re.IGNORECASE if case_insensitive else 0
        try:
            compiled = re.compile(pattern, flags=flags)
        except re.error:
            return []

        files_with_matches: list[str] = []
        content_lines: list[str] = []
        counts: list[tuple[str, int]] = []

        for filename in sorted(_KNOWN_FILES):
            absolute = self._absolute(filename)
            if glob and not fnmatch.fnmatch(filename, glob) and not fnmatch.fnmatch(absolute, glob):
                continue
            try:
                content = await self._read_serialized(filename)
            except Exception:
                continue
            file_count = 0
            matches_here: list[str] = []
            for line_no, line in enumerate(content.splitlines(), start=1):
                if compiled.search(line):
                    file_count += 1
                    if show_line_numbers:
                        matches_here.append(f"{absolute}:{line_no}:{line}")
                    else:
                        matches_here.append(f"{absolute}:{line}")
            if file_count == 0:
                continue
            files_with_matches.append(absolute)
            content_lines.extend(matches_here)
            counts.append((absolute, file_count))

        def _slice(seq: list[Any]) -> list[Any]:
            start = max(0, offset)
            if head_limit is not None:
                return seq[start : start + head_limit]
            return seq[start:]

        if output_mode == "content":
            return _slice(content_lines)
        if output_mode == "count":
            return _slice(counts)
        return _slice(files_with_matches)


def _check_version(
    payload_version: str,
    current_version: str,
    file: str,
    *,
    hint_path: str,
) -> None:
    if payload_version != current_version:
        raise io.UserDataValidationError(
            error_type="version_conflict",
            file=file,
            field_path="",
            hint=(
                f"{hint_path} was modified by another writer (dashboard UI or another "
                f"agent turn) since your last Read. Re-read with Read({hint_path}) "
                "and reapply your change on the new content."
            ),
        )
