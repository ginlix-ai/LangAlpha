"""Request URLs that hold user content: a file in the query, a subagent in the path.

The file mount's daemon names the file it reads or saves in ``path=`` and
``to=``, and the file panel, a share link and the memo routes name files the
same way. An automation's file is named after the automation and a
subagent's after the subagent, which operator records leave out
(``ptc_agent.core.paths.logged_path``). The two records that would otherwise
keep the whole URL, uvicorn's access line and the server span, keep its path
alone for every mount request and for any request whose query names a file in
the automations or subagents folder. The Subagents pane's routes name the
subagent in the path itself, which both records keep as the route spells it,
``/subagents/{name}``.
"""

from __future__ import annotations

import functools
import logging
import posixpath
import re
from typing import Any
from urllib.parse import unquote_plus


@functools.cache
def _markers() -> tuple[str, tuple[str, ...]]:
    # On first use: these modules sit in the agent package, whose import
    # pulls in the agent, and this one loads with the logging setup.
    from ptc_agent.core.paths import SandboxLayout
    from ptc_agent.core.sandbox.livefs_runtime.protocol import PREFIX

    folders = (SandboxLayout.AUTOMATIONS_DIR, SandboxLayout.USER_AGENTS_DIR)
    return f"{PREFIX}/", tuple(f"/{posixpath.basename(folder)}/" for folder in folders)


def _as_a_path(query: str) -> str:
    """The query as the file routes read a path in it, or more: a ``file:``
    URL is percent-decoded a second time there, and backslashes are slashes."""
    for _ in range(4):
        decoded = unquote_plus(query)
        if decoded == query:
            break
        query = decoded
    return query.replace("\\", "/")


def is_private(path: str, query: str) -> bool:
    mount, folders = _markers()
    # Anywhere in the path, so a server mounted under a root path matches
    # too; anywhere in the decoded query, so any spelling of the folder does.
    # A false match only loses a query string from a log line.
    decoded = _as_a_path(query)
    return mount in path or any(folder in decoded for folder in folders)


# To the end of the path: the routes take ``{name:path}``, so a name may hold a slash.
_NAMED_PATH = re.compile(r"(/api/v1/subagents/)[^?#]+")


def _fold_names(target: str) -> str:
    return _NAMED_PATH.sub(r"\1{name}", target)


def _logged_target(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    path, marker, query = value.partition("?")
    return _fold_names(path if marker and is_private(path, query) else value)


class AccessLogQueryFilter(logging.Filter):
    """Drops the query from uvicorn's access line for those paths, and a
    subagent's name from its path. uvicorn logs the request target among the
    record's args and formats them only when a handler emits it. Every string
    arg is checked, not a position, so a change in uvicorn's arguments leaves
    them out still."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.args, tuple):
            record.args = tuple(_logged_target(arg) for arg in record.args)
        return True


def drop_span_query(span: Any, scope: dict[str, Any]) -> None:
    """A ``server_request_hook``: the server span of such a request keeps
    the URL without its query, and a subagent's name leaves its path. The
    span itself stays, with its latency.

    The instrumentation has set the URL attributes by now, under whichever
    semantic conventions it emits, and a span attribute cannot be removed,
    only overwritten."""
    path = scope.get("path") or ""
    query = (scope.get("query_string") or b"").decode("latin-1")
    private = is_private(path, query)
    if not (private or _fold_names(path) != path) or not span.is_recording():
        return
    attributes = getattr(span, "attributes", None) or {}
    for key in ("http.url", "url.full", "http.target", "url.path"):
        value = attributes.get(key)
        if isinstance(value, str):
            span.set_attribute(key, _fold_names(value.partition("?")[0] if private else value))
    if private and "url.query" in attributes:
        span.set_attribute("url.query", "")
