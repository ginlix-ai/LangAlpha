"""The 503 detail carries one bit to the UI and none of the provider's text.

Two constraints meet in this string. The file panel picks its "still starting"
card by finding that word, so the in-flight case has to keep saying it. But the
exceptions behind these 503s quote provider URLs, sandbox ids and SDK response
bodies, so the raw text must not cross to a client. A sanitizer that satisfies
only the second constraint silently downgrades every "starting" card to
"unavailable", which is why both are pinned together.
"""

import asyncio
import json

import pytest

from ptc_agent.core.sandbox.runtime import SandboxGoneError, SandboxTransientError
from src.server.app.workspaces import _workspace_action_errors
from src.server.utils.error_sanitization import (
    SANDBOX_UNREACHABLE_PREFIX,
    sandbox_unreachable_detail,
)


_LEAK = (
    "download_file failed on /a.txt: 500 from "
    "https://provider.internal.example/api/sandbox/sbx-9f3c/files: "
    "upstream said 'disk quota exceeded'"
)


def _async_return(value):
    async def _inner(*args, **kwargs):
        return value

    return _inner


def test_prefix_is_stable_for_every_exception_shape():
    """Every route builds the same prefix or the panel renders a different card."""
    for exc in (
        SandboxTransientError("Sandbox is still starting: init timed out"),
        SandboxGoneError("sbx-1", "deleted"),
        RuntimeError("anything at all"),
    ):
        assert sandbox_unreachable_detail(exc).startswith(SANDBOX_UNREACHABLE_PREFIX)


def test_the_workspace_action_routes_do_not_answer_400_with_raw_text():
    """The real producer, not just the sanitizer in isolation.

    ``SandboxGoneError``/``SandboxTransientError`` subclass ``RuntimeError`` so
    that call sites map them to 503. This context manager also has a
    ``RuntimeError`` arm, mapping to 400 with ``str(e)`` — so the same
    inheritance that buys the 503 elsewhere bought a wrong status and a raw
    provider string here. Asserting on the sanitizer alone never sees this.
    """
    leaky = SandboxTransientError(
        "download_file failed: 500 from https://provider.internal.example/sbx-9f3c"
    )

    async def _run():
        async with _workspace_action_errors("start", "ws-1"):
            raise leaky

    with pytest.raises(SandboxTransientError):
        asyncio.run(_run())


@pytest.mark.parametrize(
    "raised, expect_reraise",
    [
        (SandboxTransientError(_LEAK), True),
        (SandboxGoneError("sbx-9f3c", _LEAK), True),
        (RuntimeError(_LEAK), False),
    ],
)
def test_refresh_does_not_answer_with_the_provider_text(raised, expect_reraise):
    """The one route the sanitization sweep missed.

    ``refresh_workspace`` makes the same ``get_session_for_workspace`` call
    ``acquire_sandbox`` does, but kept a bare ``except Exception`` that
    interpolated the exception into the 503 body. Typed errors now re-raise for
    the app-level handler; anything else still answers 503, but sanitized.
    """
    from fastapi import HTTPException

    from src.server.app import workspaces as workspaces_module

    class _Manager:
        async def get_session_for_workspace(self, workspace_id, user_id=None):
            raise raised

    monkey = pytest.MonkeyPatch()
    monkey.setattr(
        workspaces_module.WorkspaceManager, "get_instance", staticmethod(lambda: _Manager())
    )
    monkey.setattr(
        workspaces_module, "db_get_workspace", _async_return({"user_id": "u-1"})
    )
    monkey.setattr(workspaces_module, "require_workspace_owner", lambda *a, **kw: None)
    try:
        with pytest.raises(Exception) as excinfo:
            asyncio.run(workspaces_module.refresh_workspace("ws-1", "u-1"))
    finally:
        monkey.undo()

    if expect_reraise:
        assert excinfo.value is raised
        return
    assert isinstance(excinfo.value, HTTPException)
    assert excinfo.value.status_code == 503
    for leaked in ("provider.internal.example", "sbx-9f3c", "disk quota"):
        assert leaked not in excinfo.value.detail


def test_starting_survives_the_boundary():
    detail = sandbox_unreachable_detail(
        SandboxTransientError("Sandbox is still starting: initialization timed out after 300s")
    )
    assert "starting" in detail.lower()


def test_provider_text_does_not_reach_the_client():
    """The exact leak class: a URL, a host and a sandbox id in one SDK message."""
    exc = SandboxTransientError(
        "download_file failed on /a.txt: 500 from "
        "https://provider.internal.example/api/sandbox/sbx-9f3c/files "
        "(host=runner-07.pool.internal): upstream said 'disk quota exceeded'"
    )
    detail = sandbox_unreachable_detail(exc)
    for leaked in ("provider.internal.example", "sbx-9f3c", "runner-07", "disk quota"):
        assert leaked not in detail


def test_a_gone_sandbox_does_not_claim_to_be_starting():
    """Otherwise the panel offers a retry card for a sandbox that is never coming back."""
    detail = sandbox_unreachable_detail(SandboxGoneError("sbx-1", "sandbox was deleted"))
    assert "starting" not in detail.lower()


def test_the_handler_logs_one_line_for_a_multiline_provider_body(monkeypatch):
    """The text the client never sees still lands in the log, newlines and all.

    The provider quotes response bodies verbatim, so a body the caller shaped
    can close the line and open a convincing fake one. Driving the handler
    rather than the escaper proves the escaping is actually wired into the
    message, which is the part a helper-only assertion would miss.
    """
    from src.server.app import setup

    forged = "download failed: 500 body=\n2026-08-15 [warning] sandbox verified clean"
    captured: list[str] = []
    monkeypatch.setattr(
        setup.logger, "warning", lambda message, *a, **kw: captured.append(message)
    )

    class _Request:
        url = type("U", (), {"path": "/api/v1/workspaces/ws-1/files"})()

    response = asyncio.run(
        setup._sandbox_failure_handler(_Request(), SandboxTransientError(forged))
    )

    assert response.status_code == 503
    assert len(captured) == 1
    assert "\n" not in captured[0] and "\r" not in captured[0]
    # Escaped, not dropped: the diagnostic has to survive for the operator.
    assert "sandbox verified clean" in captured[0]


def test_an_owners_ssh_machine_is_named_as_theirs_not_ours():
    """An SSH computer's machine is its owner's, so the detail names it in the
    provider's words and says it is theirs that is not answering. The prefix
    stays, so the panel renders the same card."""
    from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError
    from ptc_agent.core.sandbox.runtime import SandboxHostUnavailableError

    silent = SSHConnectError(
        "unreachable", "Could not reach 203.0.113.9 on port 22: the connection was refused."
    )
    try:
        raise SandboxHostUnavailableError("a" * 32, "host unavailable") from silent
    except SandboxHostUnavailableError as e:
        wrapped = e
    detail = sandbox_unreachable_detail(wrapped)
    assert detail.startswith(SANDBOX_UNREACHABLE_PREFIX + "your machine is not answering.")
    assert "203.0.113.9 on port 22" in detail

    refused = SSHConnectError(
        "auth_failed", "dev@203.0.113.9 did not accept our key."
    )
    assert sandbox_unreachable_detail(refused) == (
        SANDBOX_UNREACHABLE_PREFIX + "dev@203.0.113.9 did not accept our key."
    )


@pytest.mark.parametrize(
    "code, status",
    [("auth_failed", 422), ("host_key_mismatch", 422), ("unreachable", 503)],
)
def test_the_app_handler_answers_a_refusal_as_the_computer_routes_do(code, status):
    """A refusal reaches a file or thread route wrapped, as a reconnect wraps
    it; answered 503 there, the client retried what only its owner can fix."""
    from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError
    from src.server.app import setup
    from src.server.utils.error_sanitization import sandbox_failure_http

    failure = SSHConnectError(code, "The sentence for dev@203.0.113.9.")
    try:
        raise SandboxTransientError("Could not reach sandbox sbx-9f3c: refused") from failure
    except SandboxTransientError as e:
        wrapped = e

    class _Request:
        url = type("U", (), {"path": "/api/v1/workspaces/ws-1/files"})()

    for raised in (wrapped, failure):
        response = asyncio.run(setup._sandbox_failure_handler(_Request(), raised))
        expected = sandbox_failure_http(raised)
        assert response.status_code == expected.status_code == status
        assert json.loads(response.body) == {"detail": expected.detail}
        assert "sbx-9f3c" not in response.body.decode()
    assert setup.app.exception_handlers[SSHConnectError] is setup._sandbox_failure_handler
