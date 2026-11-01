"""Where a Claude Code turn's CLI runs, and on which token.

The one module here that imports the runtime at its top, so it is imported
only as a Claude Code turn is built (``AgentInputs.claude_code_launcher``):
the SDK it loads is an optional extra that no other turn and no startup pays
for.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ptc_agent.agent.claude_code.launcher import Launcher, LocalLauncher
from ptc_agent.agent.claude_code.machine_token import token_name
from ptc_agent.agent.claude_code.ssh_launcher import SSHLauncher
from ptc_agent.core.sandbox.providers._ssh_transport import Link, Machine
from ptc_agent.core.sandbox.providers.ssh import SSHProvider
from src.llms.claude_code import backend_available

from .machine import ComputerToken
from .saved import SavedToken

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox


def turn_launcher(sandbox: PTCSandbox, *, computer_id: str | None, user_id: str) -> Launcher:
    """The launcher of a turn in ``sandbox``, on the computer ``computer_id``.

    On a computer of the user's own the CLI runs on its machine, signed in
    with the computer's token kept there, else the saved one where it is
    relayed, and the launcher tells the backend which it used, whether
    Claude refused it, and whether an organization's policy is installed
    there. Anywhere else it runs
    on the backend, signed in with the saved token, which only a self-hosted
    server offers.
    """
    if not isinstance(sandbox.provider, SSHProvider):
        return LocalLauncher(saved=SavedToken(user_id))
    if computer_id is None:
        raise RuntimeError("a turn on a computer of the user's own names that computer")
    token = ComputerToken(user_id, computer_id)

    async def link() -> Link:
        await sandbox.wait_ready()
        return await sandbox.runtime.link()

    return SSHLauncher(
        Machine(link, lambda: sandbox.working_dir), identity=user_id, token_file=token_name(computer_id),
        relay=token.relay if backend_available() else None, on_announced=token.announced, on_refused=token.refused,
        on_blocked=token.blocked, on_no_perl=token.no_perl, on_unsupported=token.unsupported,
    )
