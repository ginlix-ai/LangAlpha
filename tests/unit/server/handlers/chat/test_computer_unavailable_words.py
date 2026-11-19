"""A turn's computer failure is worded for its sender only in a shared
workspace; a personal workspace's turn keeps the failure as it was."""

from fastapi import HTTPException

from ptc_agent.core.sandbox.runtime import SandboxHostUnavailableError
from src.server.handlers.chat.computer_acquire import StoppedAwaitingComputer, computer_unavailable
from src.server.services.computer_errors import ComputerUnavailableError, MachineLockTimeout

SHARED, PERSONAL = True, False


def test_a_shared_workspaces_computer_failure_is_worded():
    still = computer_unavailable(MachineLockTimeout("Timeout acquiring lock"), SHARED)
    failed = computer_unavailable(RuntimeError("Failed to create sandbox"), SHARED)
    assert isinstance(still, ComputerUnavailableError) and still.starting
    assert isinstance(failed, ComputerUnavailableError) and not failed.starting


def test_a_personal_workspace_and_worded_refusals_keep_theirs():
    assert computer_unavailable(MachineLockTimeout("Timeout acquiring lock"), PERSONAL) is None
    for already in (
        StoppedAwaitingComputer(),
        HTTPException(status_code=409, detail="busy"),
        SandboxHostUnavailableError("sb-1", "The host is recovering. Try again in a few moments."),
    ):
        assert computer_unavailable(already, SHARED) is None
