"""What an operator log keeps of a failure: a database server's error quotes
the row it refused, so it is logged by type and sqlstate; anything else keeps
its traceback."""

from __future__ import annotations

import logging

import psycopg
import pytest

from src.observability.private_errors import DatabaseErrorLogFilter, failure

ROW = "Failing row contains (Sell all TSLA)."


def _refused() -> psycopg.Error:
    return psycopg.errors.CheckViolation(f"new row violates check constraint\nDETAIL:  {ROW}")


def _raised_from(cause: BaseException) -> RuntimeError:
    try:
        raise cause
    except BaseException:
        try:
            raise RuntimeError("the save failed") from cause
        except RuntimeError as error:
            return error


def test_a_server_error_is_logged_by_type_and_sqlstate_without_its_traceback():
    assert failure(_refused()) == ({"error_type": "CheckViolation", "sqlstate": "23514"}, None)


@pytest.mark.parametrize(
    "error",
    [
        _raised_from(_refused()),
        ExceptionGroup("several saves", [ValueError("fine"), _refused()]),
    ],
    ids=["raised-from", "in-a-group"],
)
def test_an_error_that_carries_one_is_logged_by_the_server_errors_sqlstate(error):
    fields, trace = failure(error)

    assert fields == {"error_type": type(error).__name__, "sqlstate": "23514"}
    assert trace is None


@pytest.mark.parametrize(
    "error",
    [psycopg.OperationalError("connection refused"), ValueError("a plain mistake")],
    ids=["connection", "plain"],
)
def test_an_error_the_server_never_answered_keeps_its_traceback(error):
    # A connection failure has no sqlstate: nothing it says came from a row.
    assert failure(error) == ({"error_type": type(error).__name__}, error)


def test_the_log_filter_keeps_a_traceback_the_server_never_wrote():
    error = ValueError("a plain mistake")
    record = logging.LogRecord("uvicorn.error", logging.ERROR, __file__, 0, "boom", (), (ValueError, error, None))

    assert DatabaseErrorLogFilter().filter(record)
    assert record.exc_info[1] is error and record.getMessage() == "boom"
