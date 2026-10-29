"""Where a turn runs: the one seam every entry that starts a turn asks.

A message, a thread create and an automation all ask ``turn_route``, so these
cases are the whole contract: a turn naming a workspace of its own runs its
analyst there, and every other turn runs the Chief of Staff in Home. Binding
the route is the caller's, once its own gates pass (``TurnRoute.bind``).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from fastapi import HTTPException

from src.server.database.home_workspace import get_home_workspace_id
from src.server.services.turn_runtime import (
    TurnRoute,
    ensure_home,
    requested_workspace,
    turn_route,
)

USER = "user-1"
HOME = get_home_workspace_id(USER)
PROJECT = {"workspace_id": "proj-ws", "user_id": USER, "status": "running"}
UNBOUND_HOME = {"workspace_id": HOME, "user_id": USER, "status": "unbound"}
# Bound, Home carries its computer's status; its id still says what it is.
BOUND_HOME = {**UNBOUND_HOME, "status": "running", "computer_id": "c-1"}
# A Home at another id, which an account merge carries onto the account.
FORMER_ROW = {"workspace_id": "former-ws", "user_id": USER, "status": "unbound"}


@pytest.fixture
def home():
    """The Home upsert and the Home bind, as ``ensure_home`` reaches them."""
    manager = MagicMock()
    manager.ensure_home_bound = AsyncMock(return_value={"computer_id": "c-1"})
    with (
        patch(
            "src.server.database.workspace.get_or_create_home_workspace",
            new=AsyncMock(return_value=UNBOUND_HOME),
        ) as upsert,
        patch(
            "src.server.services.workspace_manager.WorkspaceManager.get_instance",
            return_value=manager,
        ),
    ):
        yield manager.ensure_home_bound, upsert


def test_a_workspace_turn_runs_its_analyst_there():
    assert turn_route(USER, PROJECT) == TurnRoute("proj-ws", "analyst")


@pytest.mark.parametrize(
    "workspace",
    [None, UNBOUND_HOME, BOUND_HOME, FORMER_ROW],
    ids=["no-workspace", "unbound-home", "bound-home", "former-home"],
)
def test_every_other_turn_runs_the_chief_of_staff_in_home(workspace):
    """A former Home runs in the user's own Home, where its threads went."""
    assert turn_route(USER, workspace) == TurnRoute(HOME, "chief_of_staff")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("workspace", "binds"),
    [(PROJECT, False), (None, True), (UNBOUND_HOME, True), (FORMER_ROW, True)],
    ids=["workspace", "no-workspace", "unbound-home", "former-home"],
)
async def test_binding_a_route_puts_only_home_on_the_computer(workspace, binds):
    """An analyst's workspace is on its computer already; Home gets the row
    the caller read, which spares the upsert."""
    with patch(
        "src.server.services.turn_runtime.ensure_home", new=AsyncMock()
    ) as ensure:
        await turn_route(USER, workspace).bind(USER, workspace)
    assert ensure.await_args_list == ([call(USER, workspace)] if binds else [])


@pytest.mark.asyncio
@pytest.mark.parametrize("workspace", [None, UNBOUND_HOME, BOUND_HOME])
async def test_ensure_home_binds_and_upserts_only_without_the_row(home, workspace):
    bind, upsert = home
    assert await ensure_home(USER, workspace) == HOME
    bind.assert_awaited_once_with(USER)
    # The row the caller read is not upserted again.
    assert upsert.await_count == (0 if workspace else 1)


@pytest.mark.asyncio
async def test_ensure_home_folds_a_former_home_in_first(home):
    """The upsert is what folds a former row into Home, so it runs first."""
    bind, upsert = home
    assert await ensure_home(USER, FORMER_ROW) == HOME
    upsert.assert_awaited_once_with(USER)
    bind.assert_awaited_once_with(USER)


@pytest.mark.asyncio
async def test_a_home_id_another_account_holds_is_forbidden(home):
    """An account merge carries the merged account's Home onto the account it
    joins; a turn from the merged id is refused as the owner check refused it."""
    from src.server.database.workspace import HomeWorkspaceTaken

    bind, upsert = home
    upsert.side_effect = HomeWorkspaceTaken(HOME, USER)
    with pytest.raises(HTTPException) as refused:
        await ensure_home(USER)
    assert refused.value.status_code == 403
    bind.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_new_thread_naming_no_workspace_is_refused():
    """Clients match the detail's opening clause, so it is pinned verbatim."""
    with pytest.raises(HTTPException) as refused:
        await requested_workspace(USER, None)
    assert refused.value.status_code == 400
    assert refused.value.detail.startswith(
        "workspace_id is required to start a thread"
    )
