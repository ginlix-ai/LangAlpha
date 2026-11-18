"""The ``own_machines`` rule: no machine the user brings is added, started or
used for their work while it stands, a session opened before it included;
nothing is read for a computer the deployment provides or when no policy is
configured."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from ptc_agent.core.sandbox.providers import OWN_MACHINE_KINDS, PROVIDER_KINDS, provider_class
from tests.unit.server.policy_standing import standing_under
from src.config.models import PolicyLayer
from src.server.services.policy import ALLOW_ALL, OWN_MACHINES, merge
from tests.unit.server.services.test_computer_manager import (
    _MACHINES,
    _Base,
    _make_binding,
    _make_computer,
    _make_manager,
    _make_session,
)

POLICY = "src.server.services.policy"
NO_OWN = merge(None, PolicyLayer.model_validate({"own_machines": False}))
MESSAGE = (
    "Your own machines cannot be used: your organization does not allow work "
    "on a machine you bring. Use another computer instead."
)



def _refused(excinfo, status=403):
    assert excinfo.value.status_code == status
    if status == 403:
        assert excinfo.value.detail["rule"] == OWN_MACHINES
        # What a turn's error frame and the agent's tool result read.
        assert str(excinfo.value) == MESSAGE


def test_own_machine_kinds_are_the_ones_their_owner_brings():
    assert OWN_MACHINE_KINDS == tuple(
        k for k in PROVIDER_KINDS if not provider_class(k).metered
    )
    assert OWN_MACHINE_KINDS == ("ssh",)


def test_the_rule_merges_strictly_and_is_reported():
    assert merge(PolicyLayer.model_validate({"own_machines": False}), None).allows(OWN_MACHINES) is False
    client = NO_OWN.for_client()
    assert client["own_machines"] is False
    assert client["set_by"] == {OWN_MACHINES: "organization"}


# --- adding one --------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("default_kind", ["daytona", "ssh"])
async def test_adding_an_own_machine_is_refused_whatever_the_default(default_kind):
    from src.server.dependencies.usage_limits import assert_provider_allowed

    with standing_under(NO_OWN), pytest.raises(HTTPException) as refused:
        await assert_provider_allowed("u", "ssh", default_kind=default_kind)
    _refused(refused)
    with standing_under(None), pytest.raises(HTTPException) as unknown:
        await assert_provider_allowed("u", "ssh", default_kind=default_kind)
    _refused(unknown, 503)


@pytest.mark.asyncio
async def test_a_provided_kind_is_not_asked():
    from src.server.dependencies.usage_limits import assert_provider_allowed

    with standing_under(None):
        await assert_provider_allowed("u", "daytona", default_kind="daytona")


@pytest.mark.asyncio
@pytest.mark.parametrize("policy", [NO_OWN, None], ids=["denied", "unknown"])
async def test_the_providers_list_reports_own_machines_not_allowed(policy):
    from src.server.dependencies.usage_limits import get_provider_allowances

    with standing_under(policy):
        allowed = await get_provider_allowances(
            "u", ("daytona", "ssh"), default_kind="daytona"
        )
    assert allowed["ssh"] is False
    assert allowed["daytona"] is True


@pytest.mark.asyncio
async def test_the_ssh_routes_are_refused():
    from src.server.app import computers_ssh

    sandbox = MagicMock()
    sandbox.creatable_providers.return_value = ("daytona", "ssh")
    sandbox.provider = "daytona"
    with (
        standing_under(NO_OWN),
        patch.object(computers_ssh.computers, "_sandbox_config", return_value=sandbox),
        pytest.raises(HTTPException) as refused,
    ):
        await computers_ssh._ssh_settings("u")
    _refused(refused)


@pytest.mark.asyncio
async def test_nothing_configured_reads_nothing():
    from src.server.dependencies.usage_limits import (
        assert_provider_allowed,
        get_provider_allowances,
    )

    cache = MagicMock()
    with (
        patch("src.server.services.account.is_feature_enabled_system", lambda key: False),
        patch("src.server.services.membership.is_feature_enabled_system", lambda key: False),
        patch("src.server.services.account.deployment_policy", return_value=None),
        patch("src.server.dependencies.usage_limits.HOST_MODE", "oss"),
        patch("src.server.services.platform_client.HOST_MODE", "oss"),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
    ):
        await assert_provider_allowed("u", "ssh", default_kind="ssh")
        allowed = await get_provider_allowances("u", ("ssh",), default_kind="daytona")
    assert allowed["ssh"] is None
    assert not cache.method_calls


# --- using one -----------------------------------------------------------------


class TestUsingAnOwnMachine(_Base):
    def _warm(self, manager, kind="ssh", user_id="user-1"):
        session = _make_session()
        manager.resolve_binding = AsyncMock(
            return_value=_make_binding(kind=kind, user_id=user_id)
        )
        manager._take_valid_cached_session = AsyncMock(return_value=session)
        return session

    @pytest.mark.asyncio
    async def test_a_turn_is_refused_even_on_a_warm_session(self):
        manager = _make_manager()
        self._warm(manager)
        with standing_under(NO_OWN) as asked, pytest.raises(HTTPException) as refused:
            await manager._acquire_session("ws-a", user_id="user-1")
        _refused(refused)
        asked.assert_awaited_once_with("user-1")
        manager._take_valid_cached_session.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_unknown_owner_is_refused(self):
        manager = _make_manager()
        self._warm(manager, user_id=None)
        with standing_under(ALLOW_ALL), pytest.raises(HTTPException) as refused:
            await manager._acquire_session("ws-a", user_id="user-1")
        _refused(refused, 503)

    @pytest.mark.asyncio
    async def test_a_provided_computer_is_not_asked(self):
        manager = _make_manager()
        self._warm(manager, kind="daytona")
        # Stops the acquire just past the gate.
        manager._take_valid_cached_session.side_effect = LookupError("past the gate")
        with standing_under(None), pytest.raises(LookupError):
            await manager._acquire_session("ws-a", user_id="user-1")

    @pytest.mark.asyncio
    @patch(f"{_MACHINES}.get_computer")
    async def test_a_start_or_reach_does_not_hand_back_a_live_session(self, get_computer):
        """``wake_computer``, ``reach_computer`` (Chief of Staff), Claude Code's
        launcher and the Start route all come through ``_start_machine``."""
        manager = _make_manager()
        get_computer.return_value = _make_computer(kind="ssh", status="running")
        live = _make_session()
        manager._machine_session_if_live = MagicMock(return_value=live)
        with standing_under(NO_OWN), pytest.raises(HTTPException) as refused:
            await manager._start_machine("comp-1", user_id="user-1")
        _refused(refused)
        manager._machine_session_if_live.assert_not_called()

        with standing_under(ALLOW_ALL):
            assert await manager._start_machine("comp-1") is live

    @pytest.mark.asyncio
    async def test_the_chief_of_staff_hears_why(self):
        from src.server.services.computer_reach import _why_not
        from src.server.services.policy import policy_denied

        error = policy_denied(OWN_MACHINES, MESSAGE, "organization", status_code=403)
        assert _why_not("Laptop", error) == (
            'Computer "Laptop" was not used: the user\'s organization does not '
            "allow work on the user's own machines, so nothing ran there. Use a "
            "computer the user did not bring, or tell the user."
        )
        error = policy_denied(OWN_MACHINES, MESSAGE, "deployment", status_code=403)
        assert "this deployment does not allow" in _why_not("Laptop", error)

    @pytest.mark.asyncio
    async def test_a_read_that_never_starts_one_is_refused_too(self):
        """``get_session_if_running`` hands back a ready handle without an
        acquire; idle compaction and attach-only reads take it."""
        manager = _make_manager()
        manager.get_session_if_ready = MagicMock(return_value=_make_session())
        row = _make_computer(kind="ssh", status="running", provider_ref="ssh-1")
        with (
            patch(
                "src.server.services.workspace_manager.get_computer_for_workspace",
                AsyncMock(return_value=row),
            ),
            standing_under(NO_OWN),
            pytest.raises(HTTPException) as refused,
        ):
            await manager.get_session_if_running("ws-a", user_id="user-1")
        _refused(refused)
        manager.get_session_if_ready.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("policy", [NO_OWN, None], ids=["denied", "unknown"])
    async def test_work_on_a_ready_session_is_declined(self, policy):
        """MCP discovery, the vault fan-out and the skill reconcile take a
        ready handle with no acquire; each would start processes on, or write
        secrets and files to, the machine the rule keeps work off."""
        from src.server.services.computer_manager._types import MachineTarget

        manager = _make_manager()
        manager.get_session_if_ready = MagicMock(return_value=_make_session())
        target = MachineTarget(kind="ssh", root_dir=None, provider_config=None)
        with standing_under(policy):
            assert await manager.get_session_if_ready_for_work(
                "ws-a", expected_sandbox_id="ssh-1", target=target, owner_id="user-1"
            ) is None
        with standing_under(ALLOW_ALL):
            assert await manager.get_session_if_ready_for_work(
                "ws-a", expected_sandbox_id="ssh-1", target=target, owner_id="user-1"
            ) is not None

    @pytest.mark.asyncio
    async def test_the_three_workers_on_a_ready_session_ask(self):
        from src.server.app.mcp_servers import _get_live_sandbox

        manager = _make_manager()
        manager.get_session_if_ready = MagicMock(return_value=_make_session())
        manager._live_session_computer = MagicMock(return_value="comp-1")
        manager._reconcile_skills = AsyncMock()
        ws = {
            "workspace_id": "ws-a", "user_id": "user-1", "status": "running",
            "sandbox_id": "ssh-1", "computer_kind": "ssh",
        }
        _MCP = "src.server.services.computer_manager._mcp"
        with (
            standing_under(NO_OWN),
            patch("src.server.app.mcp_servers.WorkspaceManager.get_instance", return_value=manager),
            patch(f"{_MCP}.db_get_workspace", AsyncMock(return_value=ws)),
            patch(f"{_MCP}.db_get_workspace_identity", AsyncMock(return_value=ws)),
        ):
            assert await _get_live_sandbox("ws-a", ws) is None
            assert await manager.push_user_vault("user-1", ["ws-a"]) == 0
            await manager.reconcile_skills_if_running("ws-a", "user-1", source="t")
        manager._reconcile_skills.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_provided_kind_on_a_ready_session_is_not_asked(self):
        from src.server.services.computer_manager._types import MachineTarget

        manager = _make_manager()
        manager.get_session_if_ready = MagicMock(return_value=_make_session())
        target = MachineTarget(kind="daytona", root_dir=None, provider_config=None)
        with standing_under(None):
            assert await manager.get_session_if_ready_for_work(
                "ws-a", expected_sandbox_id="d-1", target=target, owner_id="user-1"
            ) is not None


# --- a send -----------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("policy", [NO_OWN, None], ids=["denied", "unknown"])
async def test_a_turn_naming_a_workspace_on_an_own_machine_is_refused_at_admission(policy):
    """Both doors that start a turn (a send and a thread create) take the
    workspace from here before any stream or thread row; refused only at the
    acquire, a send once opened its stream, wrote the thread row and ended on
    an error frame offering a retry that could not help."""
    from src.server.services.turn_runtime import requested_workspace

    row = {"workspace_id": "ws-a", "user_id": "user-1", "computer_kind": "ssh"}
    with (
        standing_under(policy),
        patch("src.server.database.workspace.get_workspace", AsyncMock(return_value=row)),
        pytest.raises(HTTPException) as refused,
    ):
        await requested_workspace("user-1", "ws-a")
    _refused(refused, 403 if policy is not None else 503)


@pytest.mark.asyncio
async def test_a_turn_on_a_provided_computer_reads_no_policy_at_admission():
    from src.server.services.turn_runtime import requested_workspace

    row = {"workspace_id": "ws-a", "user_id": "user-1", "computer_kind": "daytona"}
    with (
        standing_under(None),
        patch("src.server.database.workspace.get_workspace", AsyncMock(return_value=row)),
    ):
        assert await requested_workspace("user-1", "ws-a") is row
