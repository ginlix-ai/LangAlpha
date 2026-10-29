"""A start refused by another computer's mark names that computer, as a probe does.

The owner's repair, taking the folder back, needs the id the mark holds; a
start that dropped it left only a sentence to show.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from ptc_agent.core.sandbox.providers._ssh_transport import ScriptResult
from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError, SSHFolderMarked
from ptc_agent.core.sandbox.providers.ssh_runtime import SSHRuntime

OWNER = "3cd2aabcfce04ecf83f69a2e2952866c"


def _runtime(result: ScriptResult) -> SSHRuntime:
    # The constructor needs a target and a key loader; this path reads neither.
    runtime = object.__new__(SSHRuntime)
    runtime._working_dir = "/Users/dev/langalpha"
    runtime._id = "a" * 32
    runtime._target = SimpleNamespace(host="macbook-pro", username="dev")
    runtime._script = AsyncMock(return_value=result)
    return runtime


def _link():
    return SimpleNamespace(facts=SimpleNamespace(home="/Users/dev"))


@pytest.mark.asyncio
async def test_a_start_refused_by_another_computers_mark_names_it():
    runtime = _runtime(ScriptResult(exit_code=10, stdout=f"{OWNER}\n".encode(), stderr=b""))

    with pytest.raises(SSHFolderMarked) as raised:
        await runtime._ensure_root(_link())

    assert raised.value.owner == OWNER
    assert raised.value.code == "root_dir"
    assert "/Users/dev/langalpha on macbook-pro" in str(raised.value)


@pytest.mark.asyncio
async def test_a_mark_that_is_not_an_id_is_refused_as_before():
    runtime = _runtime(ScriptResult(exit_code=10, stdout=b"../../etc\n", stderr=b""))

    with pytest.raises(SSHConnectError) as raised:
        await runtime._ensure_root(_link())

    assert not isinstance(raised.value, SSHFolderMarked)
    assert raised.value.code == "root_dir"
    assert "already another computer's folder" in str(raised.value)
