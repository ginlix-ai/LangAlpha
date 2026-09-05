"""Only a positively identified absence may come out of ``reconnect`` as Gone.

``SandboxGoneError`` is not just a status code here — the manager answers it by
provisioning a replacement and abandoning the sandbox it was holding. So the
classification is an authorization decision, and ``SandboxFailureKind.UNKNOWN``
(where a rotated or under-privileged provider key lands) must not reach it:
declaring a live sandbox gone recreates it on every request and leaks the
original each time.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox
from ptc_agent.core.sandbox.runtime import (
    SandboxFailureKind,
    SandboxGoneError,
    SandboxTransientError,
)


def _sandbox(kind: SandboxFailureKind, failure: Exception) -> PTCSandbox:
    """A PTCSandbox wired so ``provider.get`` fails and classifies as *kind*.

    Bypasses __init__ on purpose: a real one needs a provider and a config, and
    reconnect touches only the caches cleared below before it reaches the call
    under test.
    """
    sandbox = PTCSandbox.__new__(PTCSandbox)
    sandbox._bg_sessions = {}
    sandbox._bg_trace_paths = {}
    sandbox._preview_sessions = {}
    sandbox._preview_link_cache = {}
    sandbox.runtime = None
    sandbox.provider = MagicMock()
    sandbox.provider.get = MagicMock()
    sandbox.provider.classify_error = MagicMock(return_value=kind)
    sandbox._runtime_call = AsyncMock(side_effect=failure)
    return sandbox


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind",
    [
        SandboxFailureKind.UNKNOWN,
        SandboxFailureKind.TRANSIENT,
        SandboxFailureKind.PATH_ABSENT,
    ],
)
async def test_only_sandbox_gone_authorizes_destructive_recovery(kind):
    sandbox = _sandbox(kind, RuntimeError("boom"))

    with pytest.raises(SandboxTransientError):
        await sandbox.reconnect("sandbox-abc")


@pytest.mark.asyncio
async def test_confirmed_absence_still_raises_gone():
    sandbox = _sandbox(SandboxFailureKind.SANDBOX_GONE, RuntimeError("404"))

    with pytest.raises(SandboxGoneError):
        await sandbox.reconnect("sandbox-abc")


@pytest.mark.asyncio
async def test_a_transient_error_is_never_upgraded_to_gone():
    """``_runtime_call`` already raises typed transients out of its retry
    envelope; those must pass through rather than be re-classified."""
    sandbox = _sandbox(
        SandboxFailureKind.UNKNOWN, SandboxTransientError("provider blip")
    )

    with pytest.raises(SandboxTransientError, match="provider blip"):
        await sandbox.reconnect("sandbox-abc")
