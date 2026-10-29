"""A move from the backup leaves its folder on the old machine, so the claim
admits it only off a machine the server cannot rebuild, and only with a backup
to lay on the target.

The whole claim runs on Postgres in ``tests/integration``; these pin the one
check a move from the backup adds to it.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.core.sandbox.providers import provider_class
from src.server.database.workspace_move import MoveRefused, _check_left_for_backup
from src.server.services import workspace_move as service

WORKSPACE = {"workspace_id": "ws-1", "computer_id": "comp-1"}


def _may_be_left(kind: str) -> bool:
    return not provider_class(kind).recreatable


def _cursor(source, held=True):
    cur = AsyncMock()
    cur.fetchone = AsyncMock(side_effect=[source, {"held": held}])
    return cur


@pytest.mark.asyncio
async def test_a_backed_up_workspace_may_leave_its_owners_machine():
    cur = _cursor({"kind": "ssh"})

    await _check_left_for_backup(cur, WORKSPACE, _may_be_left)

    assert [c.args[1] for c in cur.execute.await_args_list] == [("comp-1",), ("ws-1",)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source", "held", "code"),
    [
        ({"kind": "daytona"}, True, "not_movable"),
        (None, True, "not_movable"),
        ({"kind": "ssh"}, False, "no_backup"),
    ],
    ids=["hosted", "computer-gone", "never-backed-up"],
)
async def test_a_move_from_the_backup_is_refused_where_it_would_lose_the_folder(
    source, held, code
):
    """A hosted machine's folder is reachable whenever it runs, so a move
    takes the folder itself. A workspace never backed up would be laid from
    nothing and read as moved while its files stayed behind."""
    with pytest.raises(MoveRefused) as refused:
        await _check_left_for_backup(_cursor(source, held), WORKSPACE, _may_be_left)

    assert refused.value.code == code


@pytest.mark.asyncio
async def test_the_claim_lets_only_a_machine_the_server_cannot_rebuild_be_left():
    """The rule lives in the service, since the database knows kinds only by name."""
    claim = AsyncMock(return_value={"move_id": "m-1"})
    with patch.object(service.db, "claim_workspace_move", claim):
        await service.claim_workspace_move(
            user_id="user-1", workspace_id="ws-1", target_computer_id="comp-2", from_backup=True
        )

    kwargs = claim.await_args.kwargs
    assert kwargs["from_backup"] is True
    may_be_left = kwargs["may_be_left"]
    assert (may_be_left("ssh"), may_be_left("daytona"), may_be_left("docker")) == (
        True,
        False,
        False,
    )
