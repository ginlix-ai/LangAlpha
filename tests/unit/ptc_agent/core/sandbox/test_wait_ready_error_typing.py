"""``_wait_ready`` must raise typed sandbox errors, never a bare ``RuntimeError``.

It runs *before* the ``try`` that normalizes each file/exec operation's failures,
so it is the one place an untyped error can escape every classifier downstream.
The HTTP layer keys its 503 on ``SandboxGoneError``/``SandboxTransientError``, and
the chat funnel keys "is this a sandbox condition" on the same pair — a bare
``RuntimeError`` gets a 500 and the generic error card instead.
"""

import asyncio

import pytest

from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox
from ptc_agent.core.sandbox.runtime import SandboxTransientError


def _sandbox(*, ready_event, runtime=None, init_error=None) -> PTCSandbox:
    """A PTCSandbox with only the fields ``_wait_ready`` reads.

    Bypasses __init__ on purpose: constructing a real one needs a provider and a
    config, none of which this code path touches.
    """
    sandbox = PTCSandbox.__new__(PTCSandbox)
    sandbox._ready_event = ready_event
    sandbox.runtime = runtime
    sandbox._init_error = init_error
    return sandbox


@pytest.mark.asyncio
async def test_no_runtime_without_lazy_init_is_transient():
    with pytest.raises(SandboxTransientError, match="not initialized"):
        await _sandbox(ready_event=None)._wait_ready()


@pytest.mark.asyncio
async def test_ready_runtime_without_lazy_init_passes():
    await _sandbox(ready_event=None, runtime=object())._wait_ready()


@pytest.mark.asyncio
async def test_init_timeout_says_starting(monkeypatch):
    """The file panel selects its "starting" card by finding that word in the
    detail, so the genuinely-in-flight case has to keep saying it."""
    never_set = asyncio.Event()
    sandbox = _sandbox(ready_event=never_set)

    async def _immediate_timeout(awaitable, timeout):
        awaitable.close()
        raise asyncio.TimeoutError

    # monkeypatch, not a bare assignment: this replaces a stdlib symbol for
    # everything running in the process, so the restore has to survive a
    # failure raised before a finally would be reached.
    monkeypatch.setattr(asyncio, "wait_for", _immediate_timeout)
    with pytest.raises(SandboxTransientError, match="still starting"):
        await sandbox._wait_ready()


@pytest.mark.asyncio
async def test_init_error_propagates_verbatim():
    """Whatever init recorded is re-raised as-is — reconnect already normalized
    it, so re-wrapping here would only bury the real cause."""
    ready = asyncio.Event()
    ready.set()
    recorded = SandboxTransientError("Sandbox init was cancelled")

    with pytest.raises(SandboxTransientError) as caught:
        await _sandbox(ready_event=ready, init_error=recorded)._wait_ready()

    assert caught.value is recorded
