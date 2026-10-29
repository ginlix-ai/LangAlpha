"""Computers on machines their owners bring, reached over SSH.

Each user has one client key, minted on first ask and kept encrypted at rest;
every machine they add trusts it, and no other user's computer can log in with
it. The provider reads the private half through a loader registered here in
every worker, so the key never rides ``provider_config``, which is hashed and
handed to clients.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import binascii
import hashlib
import logging
import posixpath
import re
from dataclasses import asdict, dataclass, field
from typing import Any, AsyncIterator, Dict, Iterable, Iterator, List, Optional

from ptc_agent.config.core import SSHConfig
from ptc_agent.core.sandbox.providers import ssh as ssh_provider
from ptc_agent.core.sandbox.providers._ssh_authorized_key import authorized_key_line
from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError, SSHFolderMarked
from src.server.database.computer import (
    count_live_computers_of_kind,
    get_computers_for_user,
)
from src.server.database.encryption import encryption_configured
from src.server.database.ssh_keys import (
    get_user_ssh_private_key,
    get_user_ssh_public_key,
    insert_user_ssh_key,
)
from src.server.database.ssh_tunnels import get_tunnels, get_user_tunnel, renew_tunnel
from src.server.models.computer import (
    ComputerSSHInfo,
    ComputerSSHProbeResponse,
    ComputerSSHSpec,
)
from src.server.services.ssh_dial_budget import spend_dials
from src.server.services.ssh_tunnels import tunnels_enabled

logger = logging.getLogger(__name__)

SSH_KIND = "ssh"
# A probe connects, authenticates and runs a few commands; past this the user
# is better told the machine is slow to answer than kept waiting.
PROBE_TIMEOUT_S = 20.0
_KEY_COMMENT = "langalpha"
# A key line's characters: the key's base64, its type and comment, and the
# options' address list, whose double quotes the command escapes.
_QUOTABLE_LINE = re.compile(r'[A-Za-z0-9+/=@._ ,:"-]+')


class SSHUnavailable(RuntimeError):
    """The deployment cannot hold SSH keys or reach SSH machines right now."""


class SSHKeyMissing(RuntimeError):
    """The user has no client key yet, so no machine can trust it."""


class SSHFolderTaken(RuntimeError):
    """Another of the owner's computers on the machine works in the folder, above or below it."""


class SSHComputerLimit(RuntimeError):
    """The user has as many ssh computers as the deployment allows one user."""

    code = "ssh_computer_limit"

    def __init__(self, limit: int) -> None:
        self.limit = limit
        super().__init__(
            "You can have only 1 SSH computer. Remove it to add another."
            if limit == 1
            else f"You can have up to {limit} SSH computers. Remove one to add another."
        )

    def detail(self) -> Dict[str, Any]:
        """The refusal as an API detail, whose ``code`` the client words."""
        return {"code": self.code, "message": str(self), "limit": self.limit}


def _require_encryption() -> None:
    if not encryption_configured():
        raise SSHUnavailable(
            "SSH computers need encryption at rest, which this server has not "
            "configured. Set BYOK_ENCRYPTION_KEY to enable them."
        )


def authorized_keys_command(line: str) -> str:
    """Adds the key's line to the login account once, however often it is run.

    A file whose last line has no newline gets one first, or the line would
    be appended to that one and both keys stop working. It runs under
    ``sh -c`` so a fish or csh login shell runs it as well, which is why the
    line, ours, must hold nothing either shell would read as quoting but the
    double quotes escaped here, which neither reads inside single quotes. An
    older line for the key is left; the computer's next start replaces it.
    """
    if not _QUOTABLE_LINE.fullmatch(line):
        raise ValueError("The key line holds characters the command cannot quote.")
    quoted = line.replace('"', '\\"')
    return (
        f'sh -c \'k="{quoted}"; f=~/.ssh/authorized_keys; '
        "mkdir -p ~/.ssh && chmod 700 ~/.ssh && "
        '{ grep -qxF "$k" "$f" 2>/dev/null || '
        '{ [ -s "$f" ] && [ -n "$(tail -c 1 "$f")" ] && echo >> "$f"; '
        'echo "$k" >> "$f"; }; } && chmod 600 "$f"\''
    )


def key_install(key: Dict[str, str], settings: SSHConfig) -> Dict[str, str]:
    """``key``, from :func:`ensure_client_key`, with the command that installs it."""
    line = authorized_key_line(key["public_key"], settings.login_from)
    return {**key, "authorized_keys_command": authorized_keys_command(line)}


async def ensure_client_key(user_id: str) -> Dict[str, str]:
    """The user's public key, minted on first ask.

    Raises:
        SSHUnavailable: Encryption at rest is not configured.
    """
    _require_encryption()
    row = await get_user_ssh_public_key(user_id)
    if row is None:
        key = ssh_provider.generate_client_key(_KEY_COMMENT)
        row = await insert_user_ssh_key(
            user_id,
            public_key=key.public_key,
            fingerprint=key.fingerprint,
            private_key=key.private_key,
        )
    return {"public_key": row["public_key"], "fingerprint": row["fingerprint"]}


def is_public_key(line: str) -> bool:
    """Whether ``line`` parses as an OpenSSH public key."""
    try:
        ssh_provider.public_key_fingerprint(line)
    except ValueError:
        return False
    return True


async def has_client_key(user_id: str) -> bool:
    return await get_user_ssh_public_key(user_id) is not None


async def load_client_key(key_owner: str) -> str:
    """The provider's loader: the private key a computer's owner authenticates with."""
    if not key_owner:
        raise SSHKeyMissing("This computer names no key owner.")
    private_key = await get_user_ssh_private_key(key_owner)
    if private_key is None:
        raise SSHKeyMissing("No SSH key has been created for this account.")
    return private_key


async def client_key(user_id: str) -> str:
    """The private key the user's probes log in with, minted on first ask.

    Read once per request and handed to each :func:`probe`: a pairing's report
    dials every address the machine names, and again each second while its
    tunnel comes up.

    Raises:
        SSHUnavailable: As for :func:`ensure_client_key`.
    """
    await ensure_client_key(user_id)
    return await load_client_key(user_id)


def install_client_key_loader() -> None:
    """Give the provider its key loader in this worker.

    Every worker runs this from the lifespan, since a computer's start can land
    on any of them.
    """
    ssh_provider.set_client_key_loader(load_client_key)


async def probe(
    private_key: str,
    *,
    settings: SSHConfig,
    host: str,
    port: int,
    username: str,
    host_key: Optional[str] = None,
    root_dir: Optional[str] = None,
    tunnel_id: Optional[str] = None,
    reclaim: Iterable[str] = (),
    taking_over: Optional[str] = None,
    known_as: Optional[str] = None,
) -> ssh_provider.SSHProbe:
    """Connect to the machine with the user's key and report what is there.

    ``host_key`` None is first contact, whose key comes back for the user to
    confirm; given, the machine must present it, or one of its lines when it
    holds the several keys a machine reported of itself. ``root_dir`` given,
    the folder a computer would work in is checked as well, without being made,
    and a mark there left by one of ``reclaim``, the user's removed computers,
    cleared; one left by ``taking_over`` passes, left for the create to clear. ``tunnel_id`` reaches the machine through its tunnel, where ``host`` is
    only its name and ``port`` its SSH port on the machine itself.
    ``known_as`` asks whether the machine holds that computer
    (``knows_computer``).

    Raises:
        SSHConnectError: The machine refused, could not be reached, presented
            a host key other than ``host_key``, is at an address *settings*
            do not allow, did not answer in time, or cannot give the login
            user ``root_dir`` (code ``root_dir``).
    """
    try:
        async with asyncio.timeout(PROBE_TIMEOUT_S):
            return await ssh_provider.probe_ssh_target(
                host=host,
                port=port,
                username=username,
                private_key=private_key,
                host_key=host_key,
                connect_timeout=settings.connect_timeout,
                allow_private_addresses=settings.allow_private_addresses,
                root_dir=root_dir,
                tunnel=tunnel_id,
                reclaim=frozenset(reclaim),
                taking_over=taking_over,
                known_as=known_as,
            )
    except TimeoutError:
        raise SSHConnectError(
            "timeout",
            f"{host} did not finish answering within {int(PROBE_TIMEOUT_S)} seconds.",
        ) from None


def probe_response(
    found: ssh_provider.SSHProbe, *, suggested_root_dir: str
) -> ComputerSSHProbeResponse:
    """What a probe found, as POST /ssh/probe and a ready pairing answer it."""
    return ComputerSSHProbeResponse(
        **asdict(found), suggested_root_dir=suggested_root_dir
    )


def provider_config_for(
    user_id: str,
    *,
    host: str,
    port: int,
    username: str,
    host_key: str,
    tunnel_id: Optional[str] = None,
    helper: bool = False,
    os: str = "",
) -> Dict[str, Any]:
    """What a new ssh computer is built from. The key owner is always the caller.

    Every target field is written, the tunnel too when there is none, so the
    deployment's settings it is laid over never fill one in. The system,
    which the deployment may not set either, is left out when the probe could
    not name it, as on a computer added before it was kept.
    """
    config = {
        "host": host,
        "port": port,
        "username": username,
        "host_key": host_key,
        "key_owner": user_id,
        "tunnel": tunnel_id or "",
        "helper": helper,
    }
    if os:
        config["os"] = os
    return config


async def check_ssh_capacity(user_id: str, settings: SSHConfig) -> None:
    """Refuse another ssh computer once the user has ``max_computers_per_user``.

    Raises:
        SSHComputerLimit: The user is at the limit.
    """
    limit = settings.max_computers_per_user
    if limit and await count_live_computers_of_kind(user_id, SSH_KIND) >= limit:
        raise SSHComputerLimit(limit)


def computer_sharing_folder(
    computers: Iterable[Dict[str, Any]],
    *,
    host: str,
    port: int,
    host_key: str,
    root_dir: str,
    tunnel_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """The ssh computer already working in, above or below ``root_dir`` there.

    Two computers on one folder tree would each lay out, sync and prune the
    same skills, token file and workspace folders. A machine counts as the
    same by its pinned host key, its tunnel, or its address: a clone sharing
    the key only costs its owner another folder, where a miss costs both
    computers their files. The address counts only between machines that are
    dialed, since a tunneled machine's host is just its name, which two
    machines may share.
    """
    for computer in computers:
        if computer.get("kind") != SSH_KIND:
            continue
        if not _same_machine(
            computer.get("provider_config") or {},
            host=host,
            port=port,
            host_key=host_key,
            tunnel_id=tunnel_id,
        ):
            continue
        if _nested(computer.get("root_dir") or "", root_dir):
            return computer
    return None


def other_ssh_computers(
    computers: Iterable[Dict[str, Any]], computer: Dict[str, Any]
) -> Iterator[Dict[str, Any]]:
    """The live ssh computers in ``computers`` other than ``computer``."""
    for other in computers:
        if (
            other.get("kind") == SSH_KIND
            and other.get("status") != "deleted"
            and str(other.get("computer_id")) != str(computer.get("computer_id"))
        ):
            yield other


def key_line_logins(
    computers: Iterable[Dict[str, Any]], computer: Dict[str, Any]
) -> Iterator[Dict[str, Any]]:
    """The other live ssh computers that log in through a line in ``~/.ssh``.

    Not one LangAlpha's helper answers for: the helper checks the key itself,
    so the line can go from under it.
    """
    for other in other_ssh_computers(computers, computer):
        if not (other.get("provider_config") or {}).get("helper"):
            yield other


def computer_sharing_login(
    computers: Iterable[Dict[str, Any]], computer: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """Another live ssh computer logging in to ``computer``'s machine as its user.

    A user's computers all log in with the user's one key, so the key's line
    there is that one's too, and taking it away would lock it out. A machine
    counts as the same as for :func:`computer_sharing_folder`, where a clone
    taken for it only leaves the key in place.
    """
    config = computer.get("provider_config") or {}
    for other in key_line_logins(computers, computer):
        theirs = other.get("provider_config") or {}
        if theirs.get("username") == config.get("username") and _same_machine(
            theirs,
            host=config.get("host") or "",
            port=config.get("port"),
            host_key=config.get("host_key") or "",
            tunnel_id=config.get("tunnel") or None,
        ):
            return other
    return None


def _same_machine(
    config: Dict[str, Any],
    *,
    host: str,
    port: Any,
    host_key: str,
    tunnel_id: Optional[str],
) -> bool:
    return config.get("host_key") == host_key or _same_address(
        config, host=host, port=port, tunnel_id=tunnel_id
    )


def _same_address(
    config: Dict[str, Any], *, host: str, port: int, tunnel_id: Optional[str]
) -> bool:
    theirs = config.get("tunnel") or None
    if theirs or tunnel_id:
        return theirs == tunnel_id
    return config.get("host") == host and config.get("port") == port


def suggested_root_dir(
    computers: Iterable[Dict[str, Any]],
    *,
    home: str,
    host_key: str,
    host: str,
    port: int,
    tunnel_id: Optional[str] = None,
) -> str:
    """The folder a new computer on a probed machine works in unless told otherwise.

    ``~/langalpha``, in the login's home rather than beside it: every account can
    make it without sudo, it reads as ours to whoever owns the machine, and it
    goes wherever their home does. A folder another of the caller's computers
    already works in, above or below, would only be refused at create, so the
    next numbered one is proposed instead.
    """
    base = posixpath.join(home or "/", "langalpha")
    computers = list(computers)
    candidate = base
    for n in range(2, 100):
        if (
            computer_sharing_folder(
                computers,
                host=host,
                port=port,
                host_key=host_key,
                root_dir=candidate,
                tunnel_id=tunnel_id,
            )
            is None
        ):
            break
        candidate = f"{base}-{n}"
    return candidate


def _nested(a: str, b: str) -> bool:
    a, b = posixpath.normpath(a), posixpath.normpath(b)
    return a == b or a.startswith(f"{b}/") or b.startswith(f"{a}/")


def machine_config(model: str, machine: Dict[str, Any]) -> Dict[str, Any]:
    """The display facts a computer's ``config`` keeps of its machine."""
    return ({"model": model} if model else {}) | ({"machine": machine} if machine else {})


@dataclass(frozen=True)
class SSHCreatePlan:
    """A new ssh computer's create arguments, its machine already logged in to."""

    spec: ComputerSSHSpec
    provider_config: Dict[str, Any]
    root_dir: str
    default_name: str
    settings: SSHConfig
    # A Mac's model name, and the system and hardware the details show.
    # Display data, so they go in the row's ``config``: ``provider_config``
    # holds only what the provider is built from, and refuses any other key.
    model: str = ""
    machine: Dict[str, Any] = field(default_factory=dict)

    async def recheck(self, user_id: str) -> None:
        """The limit and the folder checked again, under the lock the insert
        holds, the tunnel's pairing renewed, and a folder being taken over
        cleared.

        Two creates both pass the checks made before the probe, which is too
        slow to hold the lock. A tunnel left unused since its pairing may
        meanwhile be claimed for taking off its machine; renewed here, before
        the row that names it exists, it is not. The computer a take-over
        names is stopped and cleared from the folder last, once nothing else
        can refuse the create, so a refused one leaves it working.

        Raises:
            SSHComputerLimit: Another create took the last one since.
            SSHFolderTaken: Another of the caller's computers took it since.
            SSHConnectError: The tunnel is gone, or being taken off its
                machine, or the folder taken over could not be cleared.
            SSHFolderMarked: Another computer marked the folder since.
        """
        await check_ssh_capacity(user_id, self.settings)
        await _unshared_root_dir(user_id, self.spec, self.root_dir)
        if self.spec.tunnel_id is not None:
            renewed = await renew_tunnel(user_id, self.spec.tunnel_id)
            if renewed is not True:
                raise SSHConnectError(
                    "not_connected",
                    "That machine's tunnel no longer exists. Run the command on "
                    "the machine again."
                    if renewed is None
                    else "That machine's tunnel is being taken off it. Run the "
                    "command on the machine again in a minute.",
                )
        if self.spec.take_over is not None:
            if self.spec.tunnel_id is None:
                await spend_dials(user_id, 1, settings=self.settings)
            async with _create_dial_errors(user_id, self.spec):
                try:
                    async with asyncio.timeout(PROBE_TIMEOUT_S):
                        await ssh_provider.take_over_root(
                            host=self.spec.host,
                            port=self.spec.port,
                            username=self.spec.username,
                            private_key=await client_key(user_id),
                            host_key=self.spec.host_key,
                            root_dir=self.root_dir,
                            owner=self.spec.take_over,
                            connect_timeout=self.settings.connect_timeout,
                            allow_private_addresses=self.settings.allow_private_addresses,
                            tunnel=self.spec.tunnel_id,
                        )
                except TimeoutError:
                    raise SSHConnectError(
                        "timeout",
                        f"{self.spec.host} did not finish answering within "
                        f"{int(PROBE_TIMEOUT_S)} seconds.",
                    ) from None

    def create_kwargs(self) -> Dict[str, Any]:
        return {
            "provider_config": self.provider_config,
            "config": machine_config(self.model, self.machine) or None,
            "root_dir": self.root_dir,
            "default_name": self.default_name,
        }


async def plan_create(
    user_id: str, spec: ComputerSSHSpec, *, settings: SSHConfig
) -> SSHCreatePlan:
    """Log in to the machine a new ssh computer names, and compose its target.

    The key owner is the caller and nothing else: a key owner taken from the
    request would let anyone log in to a machine with another user's key.
    The machine is logged in to with the pinned host key before the row
    exists, so a target the build could only fail on is refused here, in a
    sentence the client shows as is, rather than as a computer that never
    starts.

    Raises:
        SSHComputerLimit: The caller has as many as the deployment allows.
        SSHKeyMissing: The caller has no client key, so the machine cannot
            trust it yet.
        SSHUnavailable: As for :func:`client_key`.
        SSHConnectError: The machine refused or could not be reached, cannot
            host a computer (``unsupported``), or names a tunnel the caller
            cannot use.
        SSHFolderTaken: Another of the caller's computers there works in the
            folder.
        SSHFolderMarked: The folder is the root of a computer the caller does
            not have, which ``spec.take_over`` naming it would clear.
    """
    await check_ssh_capacity(user_id, settings)
    if not await has_client_key(user_id):
        raise SSHKeyMissing(
            "Add your SSH key to the machine before adding it. "
            "GET /api/v1/computers/ssh/key returns it."
        )
    if spec.tunnel_id is not None:
        if not tunnels_enabled():
            raise SSHConnectError("unsupported", "This server does not offer tunnels.")
        # Only the caller's own: another user's tunnel names a machine that
        # is theirs.
        if await get_user_tunnel(user_id, spec.tunnel_id) is None:
            raise SSHConnectError(
                "not_connected",
                "That machine's tunnel is no longer yours or no longer exists. "
                "Run the command on the machine again.",
            )
    # The model checks only the line's shape. A blob that does not parse
    # would fail the dial with a ValueError, which create answers as 404.
    if not is_public_key(spec.host_key):
        raise SSHConnectError(
            "host_key_invalid",
            "The host key given is not a valid SSH public key. Test the "
            "connection again and confirm the key it shows.",
        )
    # A tunnel dials our own gateway. Otherwise a default folder takes a
    # first dial to find the home, then the one that holds the folder.
    if spec.tunnel_id is None:
        await spend_dials(
            user_id, 1 if spec.root_dir is not None else 2, settings=settings
        )
    private_key = await client_key(user_id)
    reclaim = await _removed_ssh_computers(user_id)
    await _refuse_taking_over_own(user_id, spec.take_over)

    async def dial(root_dir: Optional[str] = None) -> ssh_provider.SSHProbe:
        return await probe(
            private_key,
            settings=settings,
            host=spec.host,
            port=spec.port,
            username=spec.username,
            host_key=spec.host_key,
            root_dir=root_dir,
            tunnel_id=spec.tunnel_id,
            reclaim=reclaim if root_dir else (),
            taking_over=spec.take_over if root_dir else None,
        )

    async with _create_dial_errors(user_id, spec):
        root_dir = spec.root_dir
        if root_dir is None:
            # The default folder is in the login's home, which only the
            # machine knows.
            found = await dial()
            root_dir = suggested_root_dir(
                await get_computers_for_user(user_id),
                home=found.home,
                host_key=found.host_key,
                host=spec.host,
                port=spec.port,
                tunnel_id=spec.tunnel_id,
            )
        await _unshared_root_dir(user_id, spec, root_dir)
        found = await dial(root_dir)
    if not found.supported:
        raise SSHConnectError(
            "unsupported",
            " ".join(found.problems) or f"{spec.host} cannot host a computer.",
        )
    return SSHCreatePlan(
        spec=spec,
        provider_config=provider_config_for(
            user_id,
            host=spec.host,
            port=spec.port,
            username=spec.username,
            host_key=spec.host_key,
            tunnel_id=spec.tunnel_id,
            helper=found.helper,
            os=found.os,
        ),
        root_dir=root_dir,
        default_name=found.hostname or spec.host,
        settings=settings,
        model=found.model,
        machine=found.machine(),
    )


@contextlib.asynccontextmanager
async def _create_dial_errors(user_id: str, spec: ComputerSSHSpec) -> AsyncIterator[None]:
    """A create's dials refused in its own words: a folder marked by one of
    the caller's own computers, which the folder check did not see as on
    this machine, is theirs and never offered to take over, and the host
    key the provider words for a computer that already exists."""
    try:
        yield
    except SSHFolderMarked as e:
        mine = _ssh_computer_by_machine_id(await get_computers_for_user(user_id), e.owner)
        if mine is not None:
            raise _folder_taken(mine) from None
        raise
    except SSHConnectError as e:
        if e.code != "host_key_mismatch":
            raise
        raise SSHConnectError(
            "host_key_mismatch",
            f"{spec.host} presented a different host key from the one given. "
            "Test the connection again and confirm the key it shows.",
        ) from None


async def _removed_ssh_computers(user_id: str) -> List[str]:
    """The machine-side ids of the caller's removed ssh computers.

    A removal that could not reach its machine leaves the computer's mark on
    its folder, which would refuse that folder to every new computer; one of
    these ids there is the caller's own, so the probe clears it.
    """
    return [
        str(computer["provider_ref"])
        for computer in await get_computers_for_user(user_id, include_deleted=True)
        if computer.get("status") == "deleted"
        and computer.get("kind") == SSH_KIND
        and computer.get("provider_ref")
    ]


async def _unshared_root_dir(
    user_id: str, spec: ComputerSSHSpec, root_dir: str
) -> None:
    sibling = computer_sharing_folder(
        await get_computers_for_user(user_id),
        host=spec.host,
        port=spec.port,
        host_key=spec.host_key,
        root_dir=root_dir,
        tunnel_id=spec.tunnel_id,
    )
    if sibling is not None:
        raise _folder_taken(sibling)


def _folder_taken(computer: Dict[str, Any]) -> SSHFolderTaken:
    return SSHFolderTaken(
        f'Your computer "{computer.get("name")}" already works in '
        f"{computer.get('root_dir')} on this machine. Choose a folder "
        "outside it."
    )


def _ssh_computer_by_machine_id(
    computers: Iterable[Dict[str, Any]], machine_id: str
) -> Optional[Dict[str, Any]]:
    return next(
        (
            c
            for c in computers
            if c.get("kind") == SSH_KIND and str(c.get("provider_ref") or "") == machine_id
        ),
        None,
    )


async def _refuse_taking_over_own(user_id: str, take_over: Optional[str]) -> None:
    """``take_over`` names a computer the caller does not have, whose mark they
    chose to take the folder from. One of their own live computers is never
    taken over: removing it is how that folder frees."""
    if take_over is None:
        return
    mine = _ssh_computer_by_machine_id(await get_computers_for_user(user_id), take_over)
    if mine is not None:
        raise _folder_taken(mine)


def tunnel_of(computer: Dict[str, Any]) -> str:
    """The id of the tunnel an ssh computer's machine connects out through, or ""."""
    if computer.get("kind") != SSH_KIND:
        return ""
    return (computer.get("provider_config") or {}).get("tunnel") or ""


async def tunnels_for(
    user_id: str, computers: Iterable[Dict[str, Any]]
) -> Optional[Dict[str, Dict[str, Any]]]:
    """The tunnels *computers* name, by id, for :func:`ssh_info` to tell which are up.

    None when none of *computers* has a tunnel, so a listing without one costs
    no query, and when they cannot be read: whether a tunnel is up is not worth
    failing a listing over.
    """
    tunnel_ids = {tunnel_of(computer) for computer in computers} - {""}
    if not tunnel_ids:
        return None
    try:
        return await get_tunnels(tunnel_ids)
    except Exception:  # noqa: BLE001 - shown as not known instead
        logger.warning("Could not read the tunnels of user %s", user_id, exc_info=True)
        return None


def host_key_fingerprint(host_key: str) -> Optional[str]:
    """The SHA256 fingerprint OpenSSH shows for a key line; None if it does not decode.

    Worked out here rather than by the provider's key parser: a listing
    fingerprints every pinned key, each parsed when it was pinned.
    """
    parts = host_key.split()
    try:
        blob = base64.b64decode(parts[1], validate=True)
    except (IndexError, binascii.Error):
        return None
    digest = base64.b64encode(hashlib.sha256(blob).digest()).decode()
    return f"SHA256:{digest.rstrip('=')}"


def ssh_info(
    computer: Dict[str, Any], tunnels: Optional[Dict[str, Dict[str, Any]]] = None
) -> Optional[ComputerSSHInfo]:
    """The part of an ssh computer's target its owner may read back.

    Never the rest of ``provider_config``: the key owner, and whatever a later
    field holds, stay on the server. ``tunnels`` is :func:`tunnels_for`'s
    answer; without it, whether the machine's tunnel is up is not known.
    """
    if computer.get("kind") != SSH_KIND:
        return None
    config = computer.get("provider_config") or {}
    host_key = config.get("host_key") or ""
    fingerprint = host_key_fingerprint(host_key) if host_key else None
    if host_key and fingerprint is None:
        # A listing never fails on one bad pin.
        logger.warning(
            "Could not fingerprint the host key of computer %s",
            computer.get("computer_id"),
        )
    shown = computer.get("config") or {}
    machine = shown.get("machine") if isinstance(shown.get("machine"), dict) else {}
    tunnel_id = tunnel_of(computer)
    connected = None
    if tunnel_id and tunnels is not None:
        # A tunnel no longer there is one the machine cannot connect through.
        connected = bool((tunnels.get(tunnel_id) or {}).get("live"))
    return ComputerSSHInfo(
        host=config.get("host") or "",
        port=int(config.get("port") or 22),
        username=config.get("username") or "",
        fingerprint=fingerprint,
        tunnel=bool(tunnel_id),
        tunnel_connected=connected,
        helper=bool(config.get("helper")),
        os=config.get("os") or None,
        model=shown.get("model") or None,
        **{k: machine.get(k) for k in ("distro", "arch", "cpu", "cores", "memory_bytes")},
    )
