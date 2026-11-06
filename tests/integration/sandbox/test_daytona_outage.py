"""Which failures of a Daytona test read as a Daytona outage (tests/integration/conftest.py)."""

import pytest
from daytona import (
    DaytonaA11yUnavailableError,
    DaytonaBadGatewayError,
    DaytonaConnectionError,
    DaytonaConnectionTimeoutError,
    DaytonaError,
    DaytonaServiceUnavailableError,
    DaytonaTimeoutError,
)

from ptc_agent.core.sandbox.runtime import SandboxHostUnavailableError
from tests.integration.conftest import daytona_outage

pytestmark = pytest.mark.integration

_503 = DaytonaServiceUnavailableError("Failed to start sandbox: the sandbox's host recovers", status_code=503)


def _raised(exc: BaseException, cause: BaseException | None = None, context: BaseException | None = None):
    exc.__cause__ = cause
    exc.__context__ = context
    return exc


@pytest.mark.parametrize(
    "exc",
    [
        _503,
        DaytonaBadGatewayError("Failed to stop sandbox: <html>\n</html>", status_code=502),
        DaytonaConnectionError("Failed to create sandbox: connection refused"),
        SandboxHostUnavailableError("sb-1", "host recovers"),
        _raised(RuntimeError("wrapped"), cause=_503),
        ExceptionGroup("all down", [_503, DaytonaBadGatewayError("x", status_code=502)]),
        # The SDK raising its own error while handling the HTTP client's.
        _raised(DaytonaServiceUnavailableError("down", status_code=503), context=ConnectionResetError()),
    ],
    ids=["503", "502", "connection", "host-unavailable", "wrapped-cause", "group", "sdk-while-handling"],
)
def test_an_outage(exc):
    assert daytona_outage(exc) is not None


@pytest.mark.parametrize(
    "exc",
    [
        AssertionError("wrong"),
        _raised(AssertionError("wrong"), cause=_503),
        _raised(ValueError("a bug"), context=_503),
        DaytonaA11yUnavailableError("no a11y", status_code=503),
        DaytonaTimeoutError("exec exceeded its timeout"),
        # The SDK's name for a client read timeout, a DaytonaConnectionError too.
        DaytonaConnectionTimeoutError("Failed to create sandbox: "),
        DaytonaError("bad request", status_code=400),
        ExceptionGroup("mixed", [_503, ValueError("a bug")]),
        # A cleanup's 503 after a failed assertion, bare and as the runtime wraps it.
        _raised(DaytonaServiceUnavailableError("down", status_code=503), context=AssertionError("wrong")),
        _raised(
            SandboxHostUnavailableError("sb-1", "host recovers"),
            cause=_raised(DaytonaServiceUnavailableError("down", status_code=503), context=AssertionError("wrong")),
        ),
        _raised(DaytonaBadGatewayError("x", status_code=502), context=pytest.fail.Exception("failed")),
    ],
    ids=[
        "assertion", "assertion-from-503", "bug-while-handling-503", "feature-503", "timeout", "read-timeout",
        "400", "mixed-group", "cleanup-503-after-assertion", "wrapped-cleanup-503", "cleanup-502-after-fail",
    ],
)
def test_not_an_outage(exc):
    assert daytona_outage(exc) is None
