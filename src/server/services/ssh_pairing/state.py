"""A pairing as Redis holds it: one hash, each writer with a field of its own.

The mint, the script's reports and the dialog's polls may each land on a
different worker, and a report dials the machine for up to a minute, while
the script may say it stopped. A writer that saved the whole pairing back
would undo whatever was written meanwhile, so each writes only its own field:
``pairing`` at the mint, ``machine`` and ``attempt`` at a check-in, the
attempt's own ``result:<attempt>`` when a dial settles and
``tunnel_key:<attempt>`` when it asks for a tunnel, ``tunnel_id`` when one is
handed out and ``stopped`` when the script says it stopped.
:meth:`PairingState.resolved` decides between them.

A run of the command that checks in starts a new attempt, so the dial of an
earlier run, which may settle a minute later, writes a result no one reads
rather than one over the newer run's. The script names its run in every
report and stop, kept with the ``machine`` it reported, so an earlier run
that stops after a newer one checked in does not stop that one.

The token is a bearer credential others may see, in another account's process
list or on a shared screen, so the first check-in binds the pairing to its
login (``bound_user``) and the first naming host keys to that machine
(``bound_keys:<user>``): every later run must be the same login there.
"""

from __future__ import annotations

import ipaddress
import json
import secrets
import shlex
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional, Sequence
from urllib.parse import urlsplit

from pydantic import BaseModel, Field

from src.server.models.computer import (
    ComputerSSHPairingError,
    ComputerSSHPairingMachine,
    ComputerSSHPairingResponse,
    ComputerSSHPairingStatus,
    ComputerSSHPairingTarget,
    ComputerSSHProbeResponse,
)
from src.server.services.egress.reachability import effective_relay_base_url
from src.server.services.ssh_computers import (
    SSH_KIND,
    SSHUnavailable,
    ensure_client_key,
)
from src.utils.cache.redis_cache import get_cache_client

PAIRING_TTL_S = 15 * 60
# Not the prefix a pairing stored as one JSON string had, so a worker still
# reading those during a deploy finds no pairing rather than the wrong type.
_KEY_PREFIX = "ssh:pairing-v2:"
PRODUCT = "LangAlpha"
EXPIRED = f"This command has expired. Copy a new one from {PRODUCT} and run it again."
# The error code of a pairing whose script stopped on the machine.
SCRIPT_FAILED = "script_failed"

# Names Docker and Podman give the host inside a container. A base on one
# serves the local Docker sandboxes and resolves nowhere else.
_CONTAINER_HOSTS = ("host.docker.internal", "host.containers.internal")


class PairingNotFound(LookupError):
    """No such pairing for this user: never made, someone else's, or expired."""


class ReportedMachine(BaseModel):
    """The machine as its script reported it, cleaned and capped."""

    username: str
    hostname: str = ""
    os: str = ""
    addresses: List[str] = Field(default_factory=list)
    host_keys: List[str] = Field(default_factory=list)
    ports: List[int] = Field(default_factory=lambda: [22])
    # Whether the script could connect the machine out, and the tunnel an
    # earlier run left there, by its own account: checked before any use.
    can_tunnel: bool = False
    # No SSH server answers on the machine, so no address is worth a dial.
    tunnel_only: bool = False
    tunnel_id: Optional[str] = None
    # The run of the command that reported it; empty from a script that does
    # not name its run.
    run: str = ""

    @property
    def pinned_host_key(self) -> Optional[str]:
        """Every host key it reported, one per line, for a dial to accept any of."""
        return "\n".join(self.host_keys) or None


class Binding(BaseModel):
    """The login and machine a pairing is bound to: no later run may be another."""

    username: str
    # Empty until a check-in names host keys, as one on a machine whose sshd
    # was not running yet.
    host_keys: List[str] = Field(default_factory=list)

    def admits(self, machine: ReportedMachine) -> bool:
        """The same login, and once host keys are bound, one of them."""
        if machine.username != self.username:
            return False
        return not self.host_keys or bool(set(self.host_keys) & set(machine.host_keys))

    def held(self, machine: ReportedMachine) -> ReportedMachine:
        """*machine* as it may be dialed: with only the host keys bound.

        A report that adds a key of its own beside one bound, as a machine
        elsewhere might, still has the dial accept only those bound.
        """
        return machine.model_copy(
            update={
                "username": self.username,
                "host_keys": [k for k in machine.host_keys if k in self.host_keys],
            }
        )


class PairingResult(BaseModel):
    """Where dialing the machine stands, as a report or a connect last left it."""

    status: Literal["checking", "tunneling", "ready", "failed"]
    target: Optional[ComputerSSHPairingTarget] = None
    probe: Optional[ComputerSSHProbeResponse] = None
    error: Optional[ComputerSSHPairingError] = None


class PairingState(BaseModel):
    """Every field of one pairing, as read in one go."""

    token: str
    user_id: str
    base: str
    expires_at: datetime
    # The owner's ssh computer whose machine this pairs again, if any.
    computer_id: Optional[str] = None
    machine: Optional[ReportedMachine] = None
    bound: Optional[Binding] = None
    # The latest run's check-in; empty before any.
    attempt: str = ""
    result: Optional[PairingResult] = None
    tunnel_id: Optional[str] = None
    stopped: Optional[str] = None

    def resolved(self) -> Optional[PairingResult]:
        """The result that stands, None before the machine checked in.

        A machine reached stands even when the script stopped after, as by a
        Ctrl-C while the last answer printed: it is still there to add. Else
        a script that stopped stands over a dial that was still under way.
        """
        if self.result is not None and self.result.status == "ready":
            return self.result
        if self.stopped is not None:
            return PairingResult(
                status="failed",
                error=ComputerSSHPairingError(code=SCRIPT_FAILED, message=self.stopped),
            )
        return self.result

    def view(self) -> ComputerSSHPairingStatus:
        """What the owner's dialog reads: never the user id or the reported keys."""
        result = self.resolved()
        machine = self.machine
        return ComputerSSHPairingStatus(
            status=result.status if result else "waiting",
            expires_at=self.expires_at,
            machine=(
                ComputerSSHPairingMachine(
                    username=machine.username,
                    hostname=machine.hostname,
                    os=machine.os,
                    addresses=list(machine.addresses),
                )
                if machine
                else None
            ),
            target=result.target if result else None,
            probe=result.probe if result else None,
            error=result.error if result else None,
        )


def _key(token: str) -> str:
    return f"{_KEY_PREFIX}{token}"


def _result_field(attempt: str) -> str:
    return f"result:{attempt}"


def _tunnel_key_field(attempt: str) -> str:
    return f"tunnel_key:{attempt}"


def _bound_keys_field(username: str) -> str:
    return f"bound_keys:{username}"


def _redis() -> Any:
    client = get_cache_client().client
    if client is None:
        raise SSHUnavailable(
            "Adding a machine with a command needs Redis, which is off."
        )
    return client


async def load(token: str) -> Optional[PairingState]:
    if not token or len(token) > 64:
        return None
    fields = await _redis().hgetall(_key(token))
    pairing = fields.get(b"pairing")
    if not pairing:
        return None
    attempt = (fields.get(b"attempt") or b"").decode()
    machine = fields.get(b"machine")
    result = fields.get(_result_field(attempt).encode())
    tunnel_id, stopped = fields.get(b"tunnel_id"), fields.get(b"stopped")
    bound_user = fields.get(b"bound_user")
    return PairingState(
        token=token,
        **json.loads(pairing),
        machine=ReportedMachine.model_validate_json(machine) if machine else None,
        bound=(
            _binding(
                bound_user,
                fields.get(_bound_keys_field(bound_user.decode()).encode()),
            )
            if bound_user is not None
            else None
        ),
        attempt=attempt,
        result=PairingResult.model_validate_json(result) if result else None,
        tunnel_id=tunnel_id.decode() if tunnel_id else None,
        stopped=stopped.decode() if stopped is not None else None,
    )


async def _write(
    state: PairingState,
    fields: Dict[str, str],
    *,
    clear: Sequence[str] = (),
) -> bool:
    """Write *fields* and clear *clear*, nothing else; False once the pairing lapsed.

    The expiry is set again in the same transaction, so a write that finds
    the key gone, and so makes it anew, leaves a key that expired already
    and goes at once.
    """
    key = _key(state.token)
    async with _redis().pipeline(transaction=True) as pipe:
        pipe.exists(key)
        if fields:
            pipe.hset(key, mapping=fields)
        if clear:
            pipe.hdel(key, *clear)
        pipe.pexpireat(key, int(state.expires_at.timestamp() * 1000))
        existed = (await pipe.execute())[0]
    return bool(existed)


def _binding(username: bytes, host_keys: Optional[bytes]) -> Binding:
    return Binding(
        username=username.decode(), host_keys=json.loads(host_keys) if host_keys else []
    )


async def _first(
    state: PairingState, values: Dict[str, str], read: Sequence[str]
) -> Optional[List[Optional[bytes]]]:
    """Set each of *values* unless already set, then read *read*, in one go.

    Of two requests at once, the first sets its values and both read them.
    None once the pairing lapsed.
    """
    key = _key(state.token)
    async with _redis().pipeline(transaction=True) as pipe:
        pipe.exists(key)
        for field, value in values.items():
            pipe.hsetnx(key, field, value)
        pipe.hmget(key, list(read))
        pipe.pexpireat(key, int(state.expires_at.timestamp() * 1000))
        results = await pipe.execute()
    if not results[0]:
        return None
    return list(results[-2])


async def bind(state: PairingState, machine: ReportedMachine) -> Optional[Binding]:
    """What the pairing is bound to, *machine* if it checks in first.

    Its host keys bind once a check-in of the bound login names any: a field
    of that login's own, so a report from another login, refused, never
    binds the keys the bound one's next run must name. None once the pairing
    lapsed.
    """
    if state.bound is not None and (
        state.bound.host_keys or state.bound.username != machine.username
    ):
        return state.bound
    keys = _bound_keys_field(machine.username)
    values = {"bound_user": machine.username}
    if machine.host_keys:
        values[keys] = json.dumps(machine.host_keys)
    read = await _first(state, values, ("bound_user", keys))
    if read is None:
        return None
    username, host_keys = read
    assert username is not None
    if username.decode() != machine.username:
        return Binding(username=username.decode())
    return _binding(username, host_keys)


async def claim_tunnel(state: PairingState, fingerprint: str) -> Optional[str]:
    """The key this attempt is handed a tunnel for: *fingerprint*, if it asks first.

    One per attempt, so a token someone else saw cannot mint a tunnel for each
    key they make; the same key again, as a retried request, finds it. None
    once the pairing lapsed.
    """
    field = _tunnel_key_field(state.attempt)
    read = await _first(state, {field: fingerprint}, (field,))
    if read is None:
        return None
    claimed = read[0]
    assert claimed is not None
    return claimed.decode()


async def check_in(
    state: PairingState, machine: ReportedMachine, *, tunneling: bool
) -> Optional[PairingState]:
    """A run of the command reported: it supersedes whatever an earlier run said.

    Answers the pairing as this run's attempt, for its dial to settle; None
    once the pairing lapsed.
    """
    attempt = secrets.token_hex(8)
    result = PairingResult(status="tunneling" if tunneling else "checking")
    if not await _write(
        state,
        {
            "machine": machine.model_dump_json(),
            "attempt": attempt,
            _result_field(attempt): result.model_dump_json(),
        },
        clear=(
            "stopped",
            _result_field(state.attempt),
            _tunnel_key_field(state.attempt),
        ),
    ):
        return None
    return state.model_copy(
        update={"machine": machine, "attempt": attempt, "result": result, "stopped": None}
    )


async def settle(
    state: PairingState, result: PairingResult, *, supersede_stop: bool = False
) -> bool:
    """Where dialing the machine stands now, for the attempt *state* was read at.

    ``supersede_stop`` is for the owner's own retry, which stands over a
    script that stopped; a report's dial, under way as the script stopped,
    does not.
    """
    return await _write(
        state,
        {_result_field(state.attempt): result.model_dump_json()},
        clear=("stopped",) if supersede_stop else (),
    )


async def stop(state: PairingState, message: str) -> bool:
    return await _write(state, {"stopped": message})


async def hand_out_tunnel(state: PairingState, tunnel_id: str) -> bool:
    return await _write(state, {"tunnel_id": tunnel_id})


def pairing_base(request_base: str) -> str:
    """Where the machine fetches the script and reports back.

    The script runs where the computer's code will, so it reaches this server
    at the address a remote computer does. A base with no host (an empty
    SERVER_BASE_URL) or one only containers resolve gives way to the address
    the dialog's browser reached this server at, which at least the machine
    running that browser can dial.
    """
    base = effective_relay_base_url(SSH_KIND)
    host = (urlsplit(base).hostname or "").lower()
    if not host or host in _CONTAINER_HOSTS or host.endswith(".docker.internal"):
        base = request_base
    return base.rstrip("/")


def script_url(token: str, base: str) -> str:
    return f"{base}/api/v1/computers/ssh/pairings/{token}/script"


def curl_url_line(url: str) -> str:
    """*url* as a line of curl config, for ``curl -K -`` to read on stdin.

    A URL that carries the pairing's token stays off curl's arguments that
    way, where another login on the machine could read it. Quoted, with a
    backslash, a quote or a line break escaped as curl reads them there, so
    the URL stays one line and never reads as an option.
    """
    for raw, escaped in (("\\", "\\\\"), ('"', '\\"'), ("\n", "\\n"), ("\r", "\\r")):
        url = url.replace(raw, escaped)
    return f'url = "{url}"'


def base_is_local(base: str) -> bool:
    """Whether the command reaches this server only from the machine it runs on.

    A loopback base, the self-hosted default, names whichever machine runs
    the command, so the command works on the one running this server and
    nowhere else, and the dialog says so. A private address may serve a
    machine on the same network, so it counts as reachable.
    """
    host = (urlsplit(base).hostname or "").lower()
    try:
        return ipaddress.ip_address(host).is_loopback or host == "0.0.0.0"
    except ValueError:
        return host == "localhost" or host.endswith(".localhost")


async def create_pairing(
    user_id: str, *, request_base: str, computer_id: Optional[str] = None
) -> ComputerSSHPairingResponse:
    """A pairing for the dialog to show, with the command that completes it.

    The base is kept with it, so the script reports back the way it came, and
    *computer_id*, the computer whose machine it pairs again, which the
    caller checked is theirs.

    Raises:
        SSHUnavailable: Keys cannot be stored here, or Redis is off.
    """
    await ensure_client_key(user_id)
    token = secrets.token_hex(16)
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=PAIRING_TTL_S)
    base = pairing_base(request_base)
    pairing = {"user_id": user_id, "base": base, "expires_at": expires_at.isoformat()}
    if computer_id is not None:
        pairing["computer_id"] = computer_id
    async with _redis().pipeline(transaction=True) as pipe:
        pipe.hset(_key(token), "pairing", json.dumps(pairing))
        pipe.pexpireat(_key(token), int(expires_at.timestamp() * 1000))
        await pipe.execute()
    url = script_url(token, base)
    return ComputerSSHPairingResponse(
        token=token,
        # echo is the shell's own, so no process holds the token in its
        # arguments.
        command=f"echo {shlex.quote(curl_url_line(url))} | curl -fsSL -K - | sh",
        script_url=url,
        expires_at=expires_at,
        local_only=base_is_local(base),
    )


async def pairing_status(user_id: str, token: str) -> ComputerSSHPairingStatus:
    state = await load(token)
    if state is None or state.user_id != user_id:
        raise PairingNotFound(token)
    return state.view()
