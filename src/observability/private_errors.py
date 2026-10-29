"""What an operator log keeps of a failure.

An error the database server raised quotes the row it refused in its DETAIL
and CONTEXT, which is the user's content: a holding, an automation's prompt,
a slice of a file. A failure that is one, or was raised from or during one,
or holds one, is logged by its type and sqlstate. Any other keeps its
traceback, which is what debugging it takes; that includes a connection
failure, which the server never answered and so quotes nothing.
"""

from __future__ import annotations

import logging
from typing import Any

#: The database driver's package, whose errors these are.
DATABASE_MODULE = "psycopg"


def _is_database_error(exc: BaseException) -> bool:
    module = type(exc).__module__
    return module == DATABASE_MODULE or module.startswith(f"{DATABASE_MODULE}.")


def server_error(exc: BaseException) -> BaseException | None:
    """The database server's error in ``exc``'s chain or group, if any."""
    seen: set[int] = set()
    stack: list[BaseException | None] = [exc]
    while stack:
        current = stack.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        if _is_database_error(current) and getattr(current, "sqlstate", None):
            return current
        stack += [current.__cause__, current.__context__]
        if isinstance(current, BaseExceptionGroup):
            stack += current.exceptions
    return None


def failure(exc: BaseException) -> tuple[dict[str, Any], BaseException | None]:
    """``exc`` as a log keeps it: fields to log, and what to pass as
    ``exc_info`` (None where the traceback would quote a row)."""
    fields: dict[str, Any] = {"error_type": type(exc).__name__}
    refused = server_error(exc)
    if refused is None:
        return fields, exc
    fields["sqlstate"] = refused.sqlstate
    return fields, None


class DatabaseErrorLogFilter(logging.Filter):
    """The same rule for a logger this code does not call: uvicorn logs an
    exception no route caught with its traceback, which for a database
    server's error quotes the row. The line names the error instead."""

    def filter(self, record: logging.LogRecord) -> bool:
        exc = record.exc_info[1] if record.exc_info else None
        if exc is not None:
            fields, trace = failure(exc)
            if trace is None:
                record.exc_info = None
                record.exc_text = None
                record.msg = f"{str(record.msg).rstrip()} ({fields['error_type']}, sqlstate {fields['sqlstate']})"
        return True
