"""The organization's managed Claude Code settings on a real sshd, as the
status side sees them: which the status check and the Add computer probe
name, what the computer's entry in Settings says, and a turn refused at once,
since Claude Code does not run where any is installed.

The machine is ``test_ssh_launcher``'s, its managed files placed as root in
/etc/claude-code, where the CLI there reads them.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from ptc_agent.agent.claude_code.ssh_launcher import SSHLauncher
from ptc_agent.core.sandbox.providers import _ssh_transport
from ptc_agent.core.sandbox.providers import ssh as ssh_provider
from src.server.services.claude_code import checks
from src.server.services.claude_code import machine as machines
from src.server.services.llm.claude_code import CLAUDE_CODE_OFF, require_runnable
from src.server.services.ssh_computers import probe_response
from tests.integration.claude_code import test_ssh_launcher as launcher_tests
from tests.integration.claude_code.test_ssh_launcher import CID, ROOT, Machine, launcher_finds, write_machine_token
from tests.unit.ptc_agent.agent.claude_code.fake_api import TOKEN, FakeMessagesAPI
from tests.unit.server.services.claude_code.fakes import Cache, serving

pytestmark = [pytest.mark.integration, pytest.mark.provider_docker, pytest.mark.enable_socket, pytest.mark.asyncio]

# The machine, its fake API and its CLI, as the launcher's tests set them up.
machine, api, _bundled_cli = launcher_tests.machine, launcher_tests.api, launcher_tests._bundled_cli

USER = "test-user"
MANAGED = "/etc/claude-code"


def _computer() -> dict:
    return {
        "computer_id": CID, "user_id": USER, "name": "Lab box", "kind": "ssh", "status": "running",
        "provider_config": {"helper": False, "tunnel": ""}, "provider_ref": "ref", "root_dir": ROOT,
    }


def _manage(m: Machine, *files: str) -> None:
    """The organization's managed files on the machine, readable by root only: only their names are tested."""
    m.sh(f"rm -rf {MANAGED}", user="root")
    for name in files:
        m.sh(f"mkdir -p {MANAGED}/managed-settings.d && cat > {MANAGED}/{name} && chmod 600 {MANAGED}/{name}",
             user="root", stdin='{"permissions": {"deny": ["WebFetch"]}}')


async def _list(m: Machine, fake: FakeMessagesAPI, cache: Cache) -> dict:
    """The computer's entry once its check landed, as Settings reads it."""
    def launcher(_machine, user_id, token_file):
        return SSHLauncher(_ssh_transport.Machine(m.link, lambda: ROOT), identity=user_id, token_file=token_file,
                           install=False, env={**fake.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"})

    with (
        serving(cache),
        patch(f"{machines.__name__}.rows_on", new_callable=AsyncMock, return_value=True),
        patch(f"{machines.__name__}.backend_available", return_value=False),
        patch(f"{machines.__name__}.get_computer", new_callable=AsyncMock, return_value=_computer()),
        patch(f"{machines.__name__}.get_computer_roster", new_callable=AsyncMock, return_value=[_computer()]),
        patch(f"{machines.__name__}.tunnels_for", new_callable=AsyncMock, return_value={}),
        patch(f"{machines.__name__}.machine_of", new_callable=AsyncMock, return_value=_ssh_transport.Machine(m.link, lambda: ROOT)),
        patch(f"{machines.__name__}.check_launcher", new=launcher),
        patch(f"{checks.__name__}.token_check_model", return_value="claude-haiku-5-5"),
    ):
        assert (await machines.list_computers(USER)).computers[0].state == "checking"
        await asyncio.gather(*checks._checks)
        return (await machines.list_computers(USER)).computers[0].model_dump(mode="json")


async def _admit(cache: Cache) -> None:
    config = type("Config", (), {"feature_enabled": lambda self, _: True})()
    with (
        serving(cache),
        patch("src.server.services.llm.claude_code.rows_available", return_value=True),
        patch("src.server.services.llm.claude_code.backend_available", return_value=False),
        patch("src.server.database.computer.get_computer_for_workspace", new_callable=AsyncMock, return_value=_computer()),
        patch(f"{machines.__name__}.machine_of", new=AsyncMock(side_effect=AssertionError("admission dialed the machine"))),
    ):
        await require_runnable(config, "claude-haiku-5-5-code", "analyst", user_id=USER, workspace_id="ws-1")


async def _probe(m: Machine) -> ssh_provider.SSHProbe:
    return await ssh_provider.probe_ssh_target(
        host="127.0.0.1", port=m.port, username="dev", private_key=m.key, host_key=m.host_key, allow_private_addresses=True,
    )


@pytest.mark.parametrize(("files", "sources"), [
    (("managed-settings.json",), ("settings",)),
    (("managed-settings.d/10-org.json",), ("dropins",)),
    (("managed-mcp.json",), ("mcp",)),
    (("managed-settings.json", "managed-settings.d/10-org.json", "managed-mcp.json"), ("settings", "dropins", "mcp")),
], ids=["settings", "dropins", "mcp", "all"])
async def test_a_machine_with_any_organizations_policy_installed_reads_so_and_a_turn_there_is_refused_at_once(
    machine, api, files, sources,
):
    _manage(machine, *files)
    await write_machine_token(await machine.link(), TOKEN)
    try:
        assert await launcher_finds(await machine.link()) == (sources, True)
        cache = Cache()
        entry = await _list(machine, api, cache)
        assert (entry["state"], entry["source"]) == ("policy_blocked", "machine")
        assert api.credentials == [] and machine.clis() == [], "no CLI started there"
        with pytest.raises(HTTPException) as refused:
            await _admit(cache)
        assert refused.value.detail["reason"] == "policy_blocked"
        assert refused.value.detail["message"] == CLAUDE_CODE_OFF["policy_blocked"]
        probe = await _probe(machine)
        assert probe.claude_code_blocked and probe.supported
        assert probe_response(probe, suggested_root_dir=ROOT).claude_code_blocked
    finally:
        _manage(machine)
