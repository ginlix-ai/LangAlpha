"""What the script's reports and the owner's retry do to a pairing.

Each answer here is for the machine's terminal, as a status and a line of
text, except :func:`connect`, which answers the owner's dialog.
"""

from __future__ import annotations

import logging
from typing import List, Optional, Sequence, Tuple

from ptc_agent.config.core import SSHConfig
from ptc_agent.core.sandbox.providers import ssh as ssh_provider
from src.server.database.computer import get_computer, get_computers_for_user
from src.server.database.ssh_tunnels import (
    MAX_TUNNELS_PER_USER,
    TooManyTunnels,
    TunnelKeyTaken,
    get_tunnel,
    renew_tunnel,
    set_tunnel_login,
    upsert_tunnel,
)
from src.server.models.computer import (
    ComputerSSHPairingError,
    ComputerSSHPairingReport,
    ComputerSSHPairingStatus,
    ComputerSSHPairingTarget,
    ComputerSSHTunnelRequest,
)
from src.server.services.computer_errors import ComputerRepairRefused
from src.server.services.ssh_computer_repair import check_repairable
from src.server.services.ssh_dial_budget import SSHDialBudgetExceeded, spend_dials
from src.server.services.ssh_computers import (
    client_key,
    is_public_key,
    probe_response,
    suggested_root_dir,
)
from src.server.services.ssh_pairing import dial, state
from src.server.services.ssh_pairing.dial import Candidate, Missed, Reached
from src.server.services.ssh_pairing.script import TUNNEL_CUE
from src.server.services.ssh_pairing.state import (
    EXPIRED,
    PRODUCT,
    SCRIPT_FAILED,
    PairingNotFound,
    PairingResult,
    PairingState,
    ReportedMachine,
)
from src.server.services.ssh_tunnels import (
    TUNNEL_PARENT,
    UNINSTALL_SCRIPT,
    deployment_tunnel_name,
    gateway_host_key,
    gateway_public_address,
    tunnels_enabled,
)

logger = logging.getLogger(__name__)

# The second line of the report's 202, after TUNNEL_CUE: what the script
# prints first.
_TUNNEL_REASON = (
    f"This machine cannot be reached from the internet, so it will connect out "
    f"to {PRODUCT} instead."
)
# The sentence a stopped script printed is capped like any field it sends.
_MAX_MESSAGE = 500
_NO_TUNNEL = "This server has no tunnel for machines it cannot reach."
_SUPERSEDED = "The command was run again since. Follow that run instead."
_TAKING_OFF = (
    "This machine's tunnel is being taken off it. Run the command again in a "
    "minute."
)
# Not which login or machine it was: whoever reads this may be the one the
# pairing must not reach.
_ANOTHER_LOGIN = (
    "This command was already run as another account or on another machine. "
    f"To add this one, copy a new command from {PRODUCT} and run it here."
)
_ANOTHER_TUNNEL_KEY = (
    "This command already set up a tunnel with another key. Run the command again."
)
_TUNNEL_TAKEN = (
    f"This machine's tunnel belongs to another {PRODUCT} account, whose "
    "computers on this machine use it. To connect this account instead, run "
    f"sh ~/{TUNNEL_PARENT}/{{name}}/{UNINSTALL_SCRIPT}, which disconnects them, "
    "then run the command again."
)
_TOO_MANY_TUNNELS = (
    f"This account already has {MAX_TUNNELS_PER_USER} machines connecting out to "
    f"{PRODUCT}. Remove the computers you no longer use, then run the command "
    "again."
)
_NO_HOST_KEY = (
    f"{PRODUCT} could not read this machine's SSH host keys, so it cannot tell "
    "the machine apart from anything else answering at its address. Check that "
    "sshd is running, then run the command again. Or enter the machine's "
    f"address in {PRODUCT}, and check that the host key it shows is this "
    "machine's before adding it."
)


async def report(
    token: str,
    form: ComputerSSHPairingReport,
    *,
    sources: Sequence[str],
    settings: SSHConfig,
) -> Tuple[int, str]:
    """Take the machine's report, dial it, and say how it went, for its terminal.

    Every report must be the login and machine the pairing is bound to
    (:func:`state.bind`); one naming no host keys is not dialed, since
    whatever answered would be trusted. A report naming one of the owner's
    tunnels is reached through it alone. A tunnel this pairing just handed
    out is waited for; an older one is tried once, and when it is not
    connected the script sets it up again. A machine nothing reached answers
    202 when it may connect out instead, as a ``tunnel_only`` one, which has
    no SSH server to dial, always does. A ``failed`` field, sent alone, is
    the script saying it stopped, and why.

    Raises:
        SSHUnavailable: As for :func:`client_key`.
    """
    pairing = await state.load(token)
    if pairing is None:
        return 404, EXPIRED
    if form.failed is not None:
        return await _script_failed(pairing, form.failed, run=form.run.strip()[:64])
    refused = await _bound_not_repairable(pairing)
    if refused is not None:
        return await _refuse(pairing, 409, refused)
    try:
        machine = dial.parse_report(form, sources)
    except ValueError as e:
        if pairing.bound is not None:
            # Not the bound login's run, which names the same account each time.
            return 422, str(e)
        return await _refuse(pairing, 422, str(e))
    binding = await state.bind(pairing, machine)
    if binding is None:
        return 404, EXPIRED
    if not binding.admits(machine):
        # Left standing: the bound run may be the owner's, and this one not.
        logger.info("SSH pairing %s: refused a report from another login", token[:6])
        return 409, _ANOTHER_LOGIN
    machine = binding.held(machine)
    if not machine.host_keys:
        return await _unpinned(pairing, machine)
    tunnels = tunnels_enabled()
    if machine.tunnel_only and not tunnels:
        return await _refuse(pairing, 422, _NO_TUNNEL)
    tunnel_id = machine.tunnel_id if tunnels else None
    if tunnel_id:
        # Paired again now, so no take-off claimed before this one runs.
        renewed = await renew_tunnel(pairing.user_id, tunnel_id)
        if renewed is False:
            return await _refuse(pairing, 409, _TAKING_OFF)
        if renewed is None:
            if await get_tunnel(tunnel_id) is not None:
                # Another account's: setting up one of this account's in its
                # place would leave that account's computers here unreachable.
                return await _refuse(pairing, 409, await _tunnel_taken())
            tunnel_id = None
    tried = dial.candidates(machine, settings=settings, tunnel_id=tunnel_id)
    # Before anything is dialed: a report is the machine's word, and anyone
    # holding the token can post any addresses. A tunnel dials our gateway.
    try:
        await spend_dials(
            pairing.user_id,
            sum(1 for c in tried if c.tunnel_id is None),
            settings=settings,
        )
    except SSHDialBudgetExceeded as e:
        return await _refuse(pairing, 429, str(e))
    fresh = tunnel_id is not None and tunnel_id == pairing.tunnel_id
    pairing = await state.check_in(pairing, machine, tunneling=fresh)
    if pairing is None:
        return 404, EXPIRED

    if tunnel_id is not None:
        logger.info(
            "SSH pairing %s: %s@%s checked in, reaching it through tunnel %s",
            token[:6],
            machine.username,
            machine.hostname,
            tunnel_id[:8],
        )
    else:
        logger.info(
            "SSH pairing %s: %s@%s checked in, trying %d address(es) on port(s) %s",
            token[:6],
            machine.username,
            machine.hostname,
            len(tried) // len(machine.ports),
            machine.ports,
        )
    if tried:
        reached, missed = await dial.first_reachable(
            await client_key(pairing.user_id),
            machine,
            tried,
            settings=settings,
            wait_s=dial.TUNNEL_WAIT_S if fresh else 0.0,
        )
    else:
        reached, missed = None, [dial.no_address()]
    # Through a tunnel, only one that was not up is set up again: any other
    # failure is the machine's answer.
    if (
        reached is None
        and dial.may_tunnel(machine, missed)
        and (
            tunnel_id is None
            or (not fresh and missed[0].failure.code == "not_connected")
        )
    ):
        if tunnel_id is None:
            logger.info(
                "SSH pairing %s: no address answered, asking it to connect out",
                token[:6],
            )
        return await _ask_to_connect_out(pairing)

    result = await _settle(pairing, machine, reached, missed, tunnel_id=tunnel_id)
    if result is None:
        return 404, EXPIRED
    if reached is None:
        return 422, result.error.message
    name = machine.hostname or reached.host
    if not reached.found.supported:
        return 422, (
            f"{PRODUCT} reached {name}, but it cannot host a computer: "
            + " ".join(reached.found.problems)
        )
    then = "reconnect" if pairing.computer_id else "add"
    return 200, f"Connected. Go back to {PRODUCT} to {then} {name}."


async def _unpinned(pairing: PairingState, machine: ReportedMachine) -> Tuple[int, str]:
    """Check in a machine that named no host keys, without dialing it.

    A dial would trust whatever answered first at its address. The owner may
    still enter an address in the dialog, whose dial shows the host key it
    found for them to check, as adding a machine by hand does.
    """
    pairing = await state.check_in(pairing, machine, tunneling=False)
    if pairing is None:
        return 404, EXPIRED
    logger.info("SSH pairing %s: checked in with no host keys", pairing.token[:6])
    result = PairingResult(
        status="failed",
        error=ComputerSSHPairingError(code="no_host_key", message=_NO_HOST_KEY),
    )
    if not await state.settle(pairing, result):
        return 404, EXPIRED
    return 422, _NO_HOST_KEY


async def _refuse(pairing: PairingState, status: int, message: str) -> Tuple[int, str]:
    """Answer a report the server will not dial for, and fail its pairing.

    The script stops on any answer but 200 or 202 without telling the server,
    so the dialog hears it here or keeps waiting until the command expires.
    """
    if not await state.stop(pairing, message):
        return 404, EXPIRED
    return status, message


async def _tunnel_taken() -> str:
    """Names the uninstall of this deployment's tunnel, the one the machine
    reported or set up: another deployment's beside it is no obstacle."""
    return _TUNNEL_TAKEN.format(name=await deployment_tunnel_name())


async def _settle(
    pairing: PairingState,
    machine: ReportedMachine,
    reached: Optional[Reached],
    missed: Sequence[Missed],
    *,
    tunnel_id: Optional[str],
    supersede_stop: bool = False,
) -> Optional[PairingResult]:
    """Write how the dial went; None when the pairing lapsed meanwhile."""
    if reached is None:
        result = PairingResult(
            status="failed",
            error=dial.diagnosis(machine, missed, tunneled=tunnel_id is not None),
        )
    else:
        found = reached.found
        root_dir = await _bound_root_dir(pairing) or suggested_root_dir(
            await get_computers_for_user(pairing.user_id),
            home=found.home,
            host_key=found.host_key,
            host=reached.host,
            port=reached.port,
            tunnel_id=reached.tunnel_id,
        )
        if reached.tunnel_id:
            # What the sweep logs in with to take the tunnel off the machine,
            # should no computer ever be made on it.
            await set_tunnel_login(
                reached.tunnel_id,
                username=machine.username,
                host_key=found.host_key or "",
                port=reached.port,
            )
        result = PairingResult(
            status="ready",
            target=ComputerSSHPairingTarget(
                host=reached.host,
                port=reached.port,
                username=machine.username,
                tunnel_id=reached.tunnel_id,
            ),
            probe=probe_response(found, suggested_root_dir=root_dir),
        )
    if not await state.settle(pairing, result, supersede_stop=supersede_stop):
        return None
    return result


async def _bound_not_repairable(pairing: PairingState) -> Optional[str]:
    """Why the computer a pairing pairs again may no longer be repaired, as a
    start begun since (in another tab, or by a turn) makes it; None when it
    may, or when the pairing adds a machine.

    Asked before the machine takes the new key and tunnel: a running
    computer's machine paired again would drop the tunnel the computer is
    reached through, and the reconnect that would name the new one is
    refused.
    """
    if pairing.computer_id is None:
        return None
    try:
        check_repairable(await get_computer(pairing.computer_id))
    except (ValueError, ComputerRepairRefused) as e:
        return str(e)
    return None


async def _bound_root_dir(pairing: PairingState) -> Optional[str]:
    """The folder of the computer a pairing pairs again, which a reconnect
    keeps; None for a pairing that adds a machine.

    Suggesting a new folder would be wrong for it: the computer's own mark is
    on that folder, which reads to a new computer as another's.
    """
    if pairing.computer_id is None:
        return None
    computer = await get_computer(pairing.computer_id)
    if computer is None or str(computer.get("user_id")) != pairing.user_id:
        return None
    return computer.get("root_dir") or None


async def _ask_to_connect_out(pairing: PairingState) -> Tuple[int, str]:
    """The 202 that sends the script off to set up a tunnel and report again.

    Not when the script said it stopped while its machine was being dialed,
    as on a Ctrl-C at the wait: nothing is left to set the tunnel up, and the
    dialog would show a tunnel on its way until the command expired.
    """
    current = await state.load(pairing.token)
    if current is None:
        return 404, EXPIRED
    if current.attempt != pairing.attempt:
        return 409, _SUPERSEDED
    standing = current.resolved()
    error = standing.error if standing else None
    if error is not None and error.code == SCRIPT_FAILED:
        return 409, error.message
    if not await state.settle(pairing, PairingResult(status="tunneling")):
        return 404, EXPIRED
    return 202, f"{TUNNEL_CUE}\n{_TUNNEL_REASON}"


async def _script_failed(
    pairing: PairingState, said: str, *, run: str
) -> Tuple[int, str]:
    """Fail the pairing with the sentence the script printed as it stopped.

    The machine sends it in place of a report once it could not go on, so the
    dialog shows why rather than a pairing still checking, or tunneling,
    until it expires. Only the run the pairing follows, the last to check in,
    can fail it, or any run before one did.
    """
    message = dial.printable(" ".join(said.split()), _MAX_MESSAGE) or (
        "The command stopped on the machine before it finished. Run it again."
    )
    standing = pairing.resolved()
    if standing is not None and standing.status == "ready":
        # Stopped after the server reached it, as by a Ctrl-C while the last
        # answer printed: the machine is still there to add.
        return 409, "This machine is already connected."
    following = pairing.machine.run if pairing.machine else ""
    if run and following and run != following:
        # A run another checked in over, as one stopped by Ctrl-C after the
        # command was run again, or one stopped before checking in while
        # another goes on: that other run is the one the dialog shows.
        logger.info("SSH pairing %s: a run it no longer follows stopped", pairing.token[:6])
        return 200, message
    if not await state.stop(pairing, message):
        return 404, EXPIRED
    logger.info("SSH pairing %s: the script stopped: %s", pairing.token[:6], message)
    return 200, message


async def issue_tunnel(token: str, form: ComputerSSHTunnelRequest) -> Tuple[int, str]:
    """A tunnel for the machine the report just asked to connect out.

    Keyed by the public half of the key the script made, so a later run on the
    same machine, which keeps that key, gets the same tunnel back. One key per
    attempt (:func:`state.claim_tunnel`). Answers the ``key=value`` lines the
    script sets the tunnel up from; on a refusal the script tells the server
    it stopped, so the dialog shows why.
    """
    pairing = await state.load(token)
    if pairing is None:
        return 404, EXPIRED
    if not tunnels_enabled():
        return 503, _NO_TUNNEL
    standing = pairing.resolved()
    if standing is None or standing.status != "tunneling":
        return 409, (
            "This command is not setting up a tunnel right now. Run it again "
            "from the start."
        )
    parts = form.public_key.split()
    public_key = " ".join(parts[:2])
    if len(parts) < 2 or not is_public_key(public_key):
        return 422, "The tunnel's key is not an SSH public key."
    gateway = gateway_public_address()
    if gateway is None:
        return 503, _NO_TUNNEL
    fingerprint = ssh_provider.public_key_fingerprint(public_key)
    claimed = await state.claim_tunnel(pairing, fingerprint)
    if claimed is None:
        return 404, EXPIRED
    if claimed != fingerprint:
        return 409, _ANOTHER_TUNNEL_KEY
    try:
        row = await upsert_tunnel(
            pairing.user_id,
            public_key=public_key,
            fingerprint=fingerprint,
            hostname=dial.printable(form.hostname, 255),
        )
    except TunnelKeyTaken:
        return 409, await _tunnel_taken()
    except TooManyTunnels:
        return 409, _TOO_MANY_TUNNELS
    if row is None:
        return 409, _TAKING_OFF
    host_key, _ = await gateway_host_key()
    if not await state.hand_out_tunnel(pairing, row["tunnel_id"]):
        return 404, EXPIRED
    logger.info("SSH pairing %s: handed out tunnel %s", token[:6], row["tunnel_id"][:8])
    return 200, "\n".join(
        [
            f"tunnel_id={row['tunnel_id']}",
            f"gateway_host={gateway.host}",
            f"gateway_port={gateway.port}",
            f"known_hosts={gateway.display} {' '.join(host_key.split()[:2])}",
        ]
    )


async def connect(
    user_id: str,
    token: str,
    *,
    host: str,
    port: int,
    settings: SSHConfig,
) -> ComputerSSHPairingStatus:
    """Try the address the owner typed for a machine that checked in.

    The owner's own try stands over a script that stopped, as a new run of
    the command would. It logs in as the bound login and accepts only the
    bound host keys; with none bound, whatever key answers comes back for
    the owner to check, as a machine added by hand does.

    Raises:
        PairingNotFound: No such pairing for this user, or it lapsed meanwhile.
        SSHUnavailable: As for :func:`client_key`.
    """
    pairing = await state.load(token)
    if (
        pairing is None
        or pairing.user_id != user_id
        or pairing.machine is None
        or pairing.bound is None
    ):
        raise PairingNotFound(token)
    machine = pairing.bound.held(pairing.machine)
    tried: List[Candidate] = [Candidate(host, port)]
    await spend_dials(user_id, len(tried), settings=settings)
    reached, missed = await dial.first_reachable(
        await client_key(user_id), machine, tried, settings=settings
    )
    if await _settle(
        pairing, machine, reached, missed, tunnel_id=None, supersede_stop=True
    ) is None:
        raise PairingNotFound(token)
    # Read again: a run of the command that checked in meanwhile stands over
    # this try.
    current = await state.load(token)
    if current is None:
        raise PairingNotFound(token)
    return current.view()
