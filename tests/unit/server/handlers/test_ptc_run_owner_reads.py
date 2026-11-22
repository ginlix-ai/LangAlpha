"""A turn reads its sender for the person and its route's owner for the machine.

Every turn that runs today has one account for both, so the generator is
handed two to show which read takes which: the credit gate and the acquire
take the sender (the acquire's caller only mints the data token; the machine
is read for its own owner), and the agent's inputs take both.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.report_back.handoff.carry import PriorAttempt
from src.server.services.turn_runtime import TurnRoute

from src.server.services.shared_workspaces.turn import TurnIdentity
from src.server.handlers.chat.request_prep import PriorThread
from tests.unit.server.handlers.test_ptc_run_workspace_status import (
    PTC,
    _make_config,
    _make_request,
    _make_workspace_manager,
)

SENDER = "u-sender"
OWNER = "u-owner"


class _Stop(Exception):
    pass


@pytest.mark.asyncio
async def test_a_turn_reads_the_sender_for_the_person_and_the_owner_for_the_machine():
    from src.server.handlers.chat.ptc_run import astream_ptc_workflow

    workspace_manager = _make_workspace_manager(has_ready=True, observed_state=None)
    registry_store = MagicMock()
    registry_store.get_or_create_registry = AsyncMock(return_value=MagicMock())
    inputs = AsyncMock(side_effect=_Stop("stop at the agent inputs"))
    with (
        patch(f"{PTC}.setup") as mock_setup,
        patch(f"{PTC}.ensure_thread", new=AsyncMock(return_value=PriorThread())),
        patch(f"{PTC}._resolve_fork", return_value=("Q", None)),
        patch(f"{PTC}.begin_run", new_callable=AsyncMock) as begin_run,
        patch(f"{PTC}.wait_or_steer", new_callable=AsyncMock, return_value=(True, None)),
        patch(
            "src.server.services.shared_workspaces.turn.get_user_profile_for_prompt",
            new_callable=AsyncMock, return_value=None,
        ) as profile,
        patch(f"{PTC}.init_tracking", return_value=(MagicMock(), MagicMock())),
        patch(f"{PTC}.apply_fetch_override"),
        patch(f"{PTC}.build_run_credit_gate", return_value=None) as gate,
        patch(f"{PTC}.WorkspaceManager") as manager_cls,
        patch(f"{PTC}._fire_and_forget"),
        patch(f"{PTC}.update_workspace_activity"),
        patch(f"{PTC}.BackgroundRegistryStore") as registry_cls,
        patch(f"{PTC}.keep_named_model", new=AsyncMock()),
        patch(f"{PTC}.carry.prior_attempt", new=AsyncMock(return_value=PriorAttempt(latest=None, retried=None))),
        patch("src.server.services.thread_agent.AgentInputs.read", inputs),
    ):
        mock_setup.agent_config = MagicMock()
        manager_cls.get_instance.return_value = workspace_manager
        registry_cls.get_instance.return_value = registry_store
        begin_run.return_value = MagicMock(run_id="r-1", turn_index=1, finalized=False)
        gen = astream_ptc_workflow(
            request=_make_request(),
            thread_id="t-1",
            run_id="r-1",
            user_input="hi",
            user_id=SENDER,
            route=TurnRoute("ws-1", "analyst", OWNER),
            identity=TurnIdentity(SENDER, OWNER, SENDER),
            config=_make_config(),
        )
        try:
            async for _ in gen:
                pass
        except Exception:
            pass
        finally:
            await gen.aclose()

    profile.assert_awaited_once_with(SENDER)
    assert gate.call_args.args[0] == SENDER
    assert workspace_manager.get_session_for_workspace.await_args.kwargs["user_id"] == SENDER
    identity = inputs.await_args.kwargs["identity"]
    assert (identity.sender, identity.owner) == (SENDER, OWNER)
