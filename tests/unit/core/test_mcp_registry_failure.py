"""Contract for classify_startup_failure: a stdio child that dies before the
handshake surfaces a human diagnosis, with the era-mismatch case called out."""

from mcp.shared.exceptions import MCPError
from mcp.types import CONNECTION_CLOSED

from ptc_agent.core.mcp_registry import classify_startup_failure

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
