"""Contract for the connection failure classifiers.

classify_startup_failure: a stdio child that dies before the handshake surfaces
a human diagnosis, with the era-mismatch case called out. classify_remote_failure:
a remote server's failure is named from its HTTP status, and only what a restart
looks like is worth retrying."""

import json
import ssl

import httpx2
import pytest
from mcp.shared.exceptions import MCPError
from mcp.types import CONNECTION_CLOSED

from ptc_agent.config.core import MCPServerConfig
from ptc_agent.core import mcp_registry
from ptc_agent.core.mcp_diagnostics import classify_remote_failure
from ptc_agent.core.mcp_registry import MCPServerConnector, classify_startup_failure

_IMPORT_CRASH_TAIL = (
    "Traceback (most recent call last):\n"
    '  File "cli.py", line 6, in <module>\n'
    "ModuleNotFoundError: No module named 'mcp.server.fastmcp'"
)


def _closed_group() -> BaseExceptionGroup:
    # The SDK wraps the CONNECTION_CLOSED MCPError in nested TaskGroups.
    inner = MCPError(code=CONNECTION_CLOSED, message="Connection closed")
    return BaseExceptionGroup(
        "unhandled errors in a TaskGroup",
        [ExceptionGroup("unhandled errors in a TaskGroup", [inner])],
    )


def test_import_crash_names_era_mismatch_and_isolation():
    diagnosis = classify_startup_failure(_closed_group(), _IMPORT_CRASH_TAIL)
    assert diagnosis is not None
    assert "incompatible mcp version" in diagnosis
    assert "uvx/npx" in diagnosis


def test_death_with_other_stderr_points_at_the_tail():
    diagnosis = classify_startup_failure(_closed_group(), "boom: config missing")
    assert diagnosis is not None
    assert "before completing the MCP handshake" in diagnosis
    assert "stderr_tail" in diagnosis


def test_silent_death_is_still_named():
    diagnosis = classify_startup_failure(_closed_group(), "")
    assert diagnosis is not None
    assert "no stderr output" in diagnosis


def test_connection_closed_found_through_cause_chain():
    wrapped = RuntimeError("transport failed")
    wrapped.__cause__ = MCPError(code=CONNECTION_CLOSED, message="Connection closed")
    assert classify_startup_failure(wrapped, "") is not None


def test_other_failures_are_not_classified():
    assert classify_startup_failure(ValueError("bad config"), "tail") is None
    other = ExceptionGroup("g", [MCPError(code=-32603, message="internal")])
    assert classify_startup_failure(other, "tail") is None


# --- remote (HTTP/SSE) failures: the reason, and whether a retry can help ---


def _group(*errors: BaseException) -> BaseExceptionGroup:
    return ExceptionGroup("unhandled errors in a TaskGroup", list(errors))


def _generic_http_error() -> BaseExceptionGroup:
    # What streamable HTTP raises for any non-2xx answer; only the status the
    # response hook kept tells a 401 from a 503.
    return _group(_group(MCPError(code=-32603, message="Server returned an error response")))


def test_auth_refusal_is_named_and_not_retried():
    failure = classify_remote_failure(_generic_http_error(), 401)
    assert not failure.transient
    assert "HTTP 401" in failure.reason and "credentials" in failure.reason


def test_other_client_errors_are_not_retried():
    assert not classify_remote_failure(_generic_http_error(), 404).transient
    assert not classify_remote_failure(_generic_http_error(), 400).transient


def test_server_errors_are_retried():
    failure = classify_remote_failure(_generic_http_error(), 503)
    assert failure.transient
    assert "HTTP 503" in failure.reason


def test_a_busy_server_is_retried():
    assert classify_remote_failure(_generic_http_error(), 429).transient
    assert classify_remote_failure(_generic_http_error(), 408).transient


def test_unreachable_server_is_retried():
    refused = _group(httpx2.ConnectError("All connection attempts failed"))
    assert classify_remote_failure(refused, None).transient
    assert classify_remote_failure(_group(TimeoutError()), None).transient


def test_a_successful_status_does_not_mask_a_dropped_connection():
    dropped = _group(httpx2.ReadError("connection reset"))
    assert classify_remote_failure(dropped, 200).transient


def test_an_unknown_failure_is_not_retried_and_names_the_leaf():
    failure = classify_remote_failure(_group(ValueError("URL required")), None)
    assert not failure.transient
    assert failure.reason == "ValueError: URL required"


def test_a_url_with_no_scheme_is_not_retried():
    # What an unset ${VAR} standing for the whole URL leaves behind.
    unset = _group(httpx2.UnsupportedProtocol("Request URL is missing an 'http://' protocol."))
    failure = classify_remote_failure(unset, None)
    assert not failure.transient
    assert "URL" in failure.reason


def test_a_certificate_that_fails_verification_is_not_retried():
    # As httpcore raises it: its ConnectError is raised while handling the
    # ssl error, and httpx2's wraps that one.
    verdict = ssl.SSLCertVerificationError("certificate verify failed: self-signed certificate")
    core = Exception("certificate verify failed")
    core.__context__ = verdict
    connect = httpx2.ConnectError("certificate verify failed")
    connect.__cause__ = core
    failure = classify_remote_failure(_group(connect), None)
    assert not failure.transient
    assert "certificate" in failure.reason


@pytest.mark.asyncio
async def test_a_rejected_version_probe_does_not_name_a_later_failure(monkeypatch):
    """An older server answers the client's server/discover probe with a 400,
    and the client falls back to initialize. If that one then gets no answer,
    a server restarting mid-handshake, the probe's 400 is not its status."""

    def answer(request: httpx2.Request) -> httpx2.Response:
        if json.loads(request.content)["method"] == "server/discover":
            return httpx2.Response(400, text="unknown method")
        raise httpx2.RemoteProtocolError("Server disconnected", request=request)

    monkeypatch.setattr(
        mcp_registry,
        "create_mcp_http_client",
        lambda headers=None, timeout=None, auth=None: httpx2.AsyncClient(
            transport=httpx2.MockTransport(answer), headers=headers
        ),
    )
    connector = MCPServerConnector(
        MCPServerConfig(name="market", transport="http", url="http://market.test/mcp")
    )
    with pytest.raises(Exception):
        await connector.connect(timeout=5)
    assert connector.failure_transient, connector.failure_reason
    assert "HTTP 400" not in connector.failure_reason
