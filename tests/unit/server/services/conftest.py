"""Shared stubs for the workspace-identity fencing the manager tests exercise.

Both workspace_manager suites patch the same two symbols. Kept here so a rename
touches one copy, not two that had already drifted apart in their docstrings.
"""

from unittest.mock import AsyncMock, patch


def _patch_identity(workspace):
    """Stub the narrow identity read every cached-session return validates against.

    Returns the workspace's own status/sandbox_id, i.e. "the cache agrees with
    Postgres" — the baseline the staleness tests deviate from deliberately.
    """
    return patch(
        "src.server.services.workspace_manager.db_get_workspace_identity",
        AsyncMock(
            return_value={
                "status": workspace["status"],
                "sandbox_id": workspace["sandbox_id"],
            }
        ),
    )


def _patch_sandbox_bind(workspace):
    """Stub the identity CAS that publishes a freshly provisioned sandbox.

    Returning the row means "we won the race"; returning None means another
    provisioner bound this workspace first.
    """
    return patch(
        "src.server.services.workspace_manager.try_bind_workspace_sandbox",
        AsyncMock(return_value=workspace),
    )
