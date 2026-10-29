"""Taking tunnels off machines no computer uses: at a removal, and in a sweep.

A pairing that reaches a machine through a tunnel leaves a service there
keeping the tunnel open, whether or not a computer is then made on it. The
last computer's removal takes the service away (:func:`release_tunnel`) if
its machine answers then. The sweep finds the tunnels no computer names, a
day after their last pairing so that one still being set up is left alone,
and while the machine is connected runs its uninstall through the tunnel,
logged in as the server last was there. One with no login that still works,
or whose machine has been away for ``GIVE_UP_AFTER``, is forgotten without
it: the gateway refuses it from then on, and the machine's loop asks again
only every ten minutes. Every worker runs the sweep; claiming a tunnel in
Postgres has one of them try each.
"""

from __future__ import annotations

import asyncio
import logging
import random
import shlex
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any, Dict, Optional

from ptc_agent.config.core import SSHConfig
from ptc_agent.core.sandbox.providers import ssh as ssh_provider
from ptc_agent.core.sandbox.providers._ssh_transport import Link
from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError
from src.server.database import ssh_tunnels as db
from src.server.services.ssh_computers import SSH_KIND, load_client_key, tunnel_of
from src.server.services.ssh_tunnels import (
    TUNNEL_PARENT,
    UNINSTALL_SCRIPT,
    tunnels_enabled,
)
from src.utils.concurrency import cancel_and_join

logger = logging.getLogger(__name__)

SWEEP_INTERVAL_S = 3600.0
SWEEP_BATCH_LIMIT = 25
# A pass claims pages until none is due, up to this many: a backlog drains in
# one pass, and a pass still ends.
SWEEP_MAX_PAGES = 10
# Machines a page tries at once: each away one costs the uninstall's whole
# timeout, and a page of them one at a time would outlast the claim.
SWEEP_CONCURRENCY = 4
# Longer than any pairing takes from the command's run to the computer's add.
UNUSED_FOR = timedelta(days=1)
# A tunnel tried is not tried again for this long, by any worker, so a machine
# that is away costs a look per interval, not one per worker.
RETRY_AFTER = timedelta(seconds=SWEEP_INTERVAL_S)
GIVE_UP_AFTER = timedelta(days=30)
# The login the server holds no longer gets in, and never will again.
_REFUSED_CODES = frozenset({"auth_failed", "host_key_mismatch"})
STOP_GRACE = 30.0
# Taking a tunnel off its machine is one short command; a machine silent past
# this is taken to be away, and left to the sweep.
_RELEASE_TIMEOUT_S = 15.0
# A release claimed this recently is another one, or the sweep, still at it.
# Twice the timeout above, as the stamp before an uninstall lasts.
_RELEASE_CLAIM = db.TAKE_OFF_WINDOW


def _uninstall_sh(tunnel_id: str) -> str:
    """The machine's command that runs the uninstall of tunnel *tunnel_id*.

    The folder is found by the id it holds, which no other deployment hands
    out, so a tunnel another deployment keeps on the same login is left alone
    whatever its folder is called. Detached, so the command answers before the
    tunnel it rides on goes; a machine that already took it away is no error,
    and neither is one LangAlpha's helper answers for, which installs no
    folder: once the tunnel is released the gateway refuses it, and the app on
    that Mac turns the helper off.
    """
    return (
        f"id={shlex.quote(tunnel_id)}\n"
        f'for f in "$HOME"/{TUNNEL_PARENT}/tunnel*/tunnel_id; do\n'
        '  [ "$(cat "$f" 2>/dev/null)" = "$id" ] || continue\n'
        f'  u="${{f%/*}}/{UNINSTALL_SCRIPT}"\n'
        '  [ -f "$u" ] && { nohup sh "$u" >/dev/null 2>&1 & }\n'
        "done\n"
        "exit 0\n"
    )


@dataclass
class Ahead:
    """A removal's command that runs first in the uninstall's own.

    The key line a removal takes away is the login the sweep would need if
    the uninstall did not run, so the two go out as one command: a channel
    the machine refuses leaves both. ``sent`` once that command went out,
    ``said`` what this part printed once it answered.
    """

    script: str
    sent: bool = False
    said: Optional[str] = None


class TakeOff(StrEnum):
    """How an attempt to take a tunnel off its machine ended.

    ``GONE`` is a tunnel released by someone else meanwhile, which closed it
    under the attempt. ``PAIRED_AGAIN`` is a tunnel paired again, or given a
    computer, since the claim: left on its machine for that.
    """

    REMOVED = "removed"
    GONE = "gone"
    REFUSED = "refused"
    AWAY = "away"
    PAIRED_AGAIN = "paired_again"


@dataclass(frozen=True)
class TunnelLogin:
    """How the server last logged in to a tunneled machine: its name, its
    sshd's port on the machine, the account and the pinned host key."""

    host: str
    port: int
    username: str
    host_key: str

    @classmethod
    def of(cls, config: Dict[str, Any]) -> "TunnelLogin":
        """The login in an ssh computer's ``provider_config``."""
        return cls(
            host=config.get("host") or "",
            port=int(config.get("port") or 22),
            username=config.get("username") or "",
            host_key=config.get("host_key") or "",
        )


async def take_tunnel_off(
    tunnel_id: str,
    *,
    key_owner: str,
    login: TunnelLogin,
    paired_at: datetime,
    settings: SSHConfig,
    link: Optional[Link] = None,
    ahead: Optional[Ahead] = None,
) -> TakeOff:
    """Run the machine's own uninstall through its tunnel, then forget the tunnel.

    ``paired_at`` is the caller's claim on the row: a tunnel paired again since
    is left alone. A refused login leaves the row too: whether to forget it
    then is the caller's call. ``link`` and ``ahead`` as for
    :func:`uninstall_tunnel`.
    """
    if not await db.begin_take_off(tunnel_id, paired_at=paired_at):
        return TakeOff.PAIRED_AGAIN
    try:
        await uninstall_tunnel(
            tunnel_id,
            key_owner=key_owner,
            login=login,
            settings=settings,
            link=link,
            ahead=ahead,
        )
    except Exception as e:  # noqa: BLE001 - the outcome says what to do next
        if await db.get_tunnel(tunnel_id) is None:
            return TakeOff.GONE
        logger.info("Tunnel %s not taken off its machine: %r", tunnel_id, e)
        if isinstance(e, SSHConnectError) and e.code in _REFUSED_CODES:
            return TakeOff.REFUSED
        return TakeOff.AWAY
    if not await db.delete_unused_tunnel(tunnel_id, paired_at=paired_at):
        # Only past TAKE_OFF_WINDOW, as when the database stalled, can a
        # pairing renew it meanwhile; that run may have lost its service.
        logger.warning(
            "Took tunnel %s off its machine as it was paired again; the machine "
            "needs the command run again",
            tunnel_id,
        )
        return TakeOff.PAIRED_AGAIN
    return TakeOff.REMOVED


async def uninstall_tunnel(
    tunnel_id: str,
    *,
    key_owner: str,
    login: TunnelLogin,
    settings: SSHConfig,
    link: Optional[Link] = None,
    ahead: Optional[Ahead] = None,
) -> None:
    """Run the machine's own uninstall through its tunnel, logged in as given.

    ``link`` is a connection already logged in there, which a removal passes,
    with the command it runs ahead of the uninstall in ``ahead``.

    Raises:
        Exception: The machine did not answer within ``_RELEASE_TIMEOUT_S``,
            refused the login, or the command failed.
    """
    if link is not None:
        command = _uninstall_sh(tunnel_id)
        if ahead is not None:
            # A subshell, so the uninstall runs however it exits.
            command = f"(\n{ahead.script})\n{command}"
            ahead.sent = True
        result = await link.run(command, timeout=_RELEASE_TIMEOUT_S)
        if ahead is not None:
            ahead.said = result.stdout.decode(errors="replace")
        if result.exit_code != 0:
            raise RuntimeError(f"the uninstall exited {result.exit_code}")
        return
    target = settings.model_copy(
        update={
            "host": login.host,
            "port": login.port,
            "username": login.username,
            "host_key": login.host_key,
            "key_owner": key_owner,
            "tunnel": tunnel_id,
        }
    )
    async with asyncio.timeout(_RELEASE_TIMEOUT_S):
        status = await ssh_provider.run_script(
            target,
            await load_client_key(key_owner),
            _uninstall_sh(tunnel_id),
            timeout=_RELEASE_TIMEOUT_S,
        )
    if status != 0:
        raise RuntimeError(f"the uninstall exited {status}")


async def release_tunnel(
    user_id: str,
    computer: Dict[str, Any],
    *,
    settings: SSHConfig,
    link: Optional[Link] = None,
    ahead: Optional[Ahead] = None,
) -> bool:
    """Take away the tunnel of a computer its owner removed, unless another uses it.

    The tunnel is the machine's, shared by every computer on it, so only the
    last one's removal takes it, and claiming it first has one release, or a
    release or the sweep, run the uninstall. A machine that does not answer
    keeps its row, with this computer's login on it, for the sweep to try
    again once it is back. ``link`` and ``ahead`` as for
    :func:`uninstall_tunnel`; ``ahead`` is never sent when no uninstall is
    due. Never raises: False when it failed before deciding, which leaves the
    tunnel to a release that tries again, and when another release or the
    sweep is taking it off.
    """
    tunnel_id = tunnel_of(computer)
    if not tunnel_id:
        return True
    computer_id = str(computer.get("computer_id"))
    try:
        tunnel = await db.claim_released_tunnel(
            tunnel_id,
            retry_after=_RELEASE_CLAIM,
            made_at=computer.get("created_at"),
            paired_within=UNUSED_FOR,
        )
        if tunnel is None:
            # Another computer uses it, or it was paired again for a computer
            # still to be made. If another release or the sweep is taking it
            # off, that one may still log in with the key this removal takes.
            return not await db.taken_off_elsewhere(tunnel_id, within=_RELEASE_CLAIM)
        config = computer.get("provider_config") or {}
        login = TunnelLogin.of(config)
        outcome = await take_tunnel_off(
            tunnel_id,
            key_owner=config.get("key_owner") or user_id,
            login=login,
            paired_at=tunnel["paired_at"],
            settings=settings,
            link=link,
            ahead=ahead,
        )
        if outcome in (TakeOff.AWAY, TakeOff.REFUSED):
            logger.warning(
                "Tunnel %s of removed computer %s stays until its machine answers",
                tunnel_id,
                computer_id,
            )
            await db.set_tunnel_login(
                tunnel_id,
                username=login.username,
                host_key=login.host_key,
                port=login.port,
            )
        elif outcome is TakeOff.REMOVED:
            logger.info("Released tunnel %s with computer %s", tunnel_id, computer_id)
        elif outcome is TakeOff.PAIRED_AGAIN:
            logger.info(
                "Tunnel %s of removed computer %s was paired again; kept",
                tunnel_id,
                computer_id,
            )
    except Exception:  # noqa: BLE001 - the remove it follows already succeeded
        logger.exception(
            "Could not release tunnel %s of removed computer %s", tunnel_id, computer_id
        )
        return False
    return True


def _ssh_settings() -> SSHConfig:
    """The deployment's ssh settings, the ones the manager builds computers from."""
    from src.server.services.computer_manager import ComputerManager

    return ComputerManager.get_instance().config.sandbox.settings_for(SSH_KIND)


class TunnelSweeper:
    _instance: Optional["TunnelSweeper"] = None

    def __init__(self, *, interval: float = SWEEP_INTERVAL_S) -> None:
        self._interval = interval
        self._loop_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    @classmethod
    def get_instance(cls) -> "TunnelSweeper":
        if cls._instance is None:
            cls._instance = TunnelSweeper()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        cls._instance = None

    # ------------------------------------------------------------ lifecycle

    def start(self) -> None:
        """Start the sweep loop; inert where tunnels are off."""
        if not tunnels_enabled():
            return
        if self._loop_task is not None and not self._loop_task.done():
            return
        self._stop_event = asyncio.Event()
        self._loop_task = asyncio.create_task(self._loop(), name="tunnel-sweeper")
        logger.info(f"[TunnelSweeper] started (interval={self._interval:.0f}s)")

    async def stop(self) -> None:
        if self._loop_task is None:
            return
        self._stop_event.set()
        try:
            await asyncio.wait_for(self._loop_task, timeout=STOP_GRACE)
        except TimeoutError:
            await cancel_and_join(self._loop_task)
        except Exception:
            logger.warning(
                "[TunnelSweeper] loop ended with an error at shutdown", exc_info=True
            )
        self._loop_task = None
        self._stop_event = asyncio.Event()

    async def _loop(self) -> None:
        while not self._stop_event.is_set():
            # Jitter first, so workers started by one deploy do not all claim
            # at once.
            jitter = self._interval * (0.8 + 0.4 * random.random())
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=jitter)
                return
            except TimeoutError:
                pass
            try:
                await self.sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("[TunnelSweeper] sweep failed", exc_info=True)

    # ---------------------------------------------------------------- sweep

    async def sweep_once(self) -> int:
        """One bounded pass; returns how many tunnels were taken away."""
        settings = _ssh_settings()
        slots = asyncio.Semaphore(SWEEP_CONCURRENCY)

        async def sweep(row: dict[str, Any]) -> bool:
            async with slots:
                if self._stop_event.is_set():
                    return False
                try:
                    return await self._sweep_one(row, settings)
                except Exception as e:
                    logger.warning(
                        "[TunnelSweeper] tunnel %s not swept (%s)",
                        row["tunnel_id"],
                        type(e).__name__,
                    )
                    return False

        removed = 0
        for _ in range(SWEEP_MAX_PAGES):
            rows = await db.claim_unused_tunnels(
                paired_before=UNUSED_FOR,
                retry_after=RETRY_AFTER,
                give_up_after=GIVE_UP_AFTER,
                limit=SWEEP_BATCH_LIMIT,
            )
            removed += sum(await asyncio.gather(*(sweep(row) for row in rows)))
            if self._stop_event.is_set() or len(rows) < SWEEP_BATCH_LIMIT:
                break
        return removed

    async def _sweep_one(self, row: dict[str, Any], settings: SSHConfig) -> bool:
        tunnel_id = row["tunnel_id"]
        if not (row["username"] and row["host_key"]):
            reason = "no login to reach its machine with"
        elif row["live"]:
            outcome = await take_tunnel_off(
                tunnel_id,
                key_owner=row["user_id"],
                login=TunnelLogin(
                    host=row["hostname"],
                    port=row["port"],
                    username=row["username"],
                    host_key=row["host_key"],
                ),
                paired_at=row["paired_at"],
                settings=settings,
            )
            if outcome is TakeOff.REMOVED:
                logger.info(
                    "[TunnelSweeper] forgot unused tunnel %s: taken off its machine",
                    tunnel_id,
                )
                return True
            if outcome is not TakeOff.REFUSED:
                return False
            reason = "its machine no longer takes the login"
        else:  # away past GIVE_UP_AFTER, the claim's last case
            reason = f"its machine away since {_away_since(row):%Y-%m-%d}"
        if not await db.delete_unused_tunnel(tunnel_id, paired_at=row["paired_at"]):
            return False  # paired again, or a computer made on it, meanwhile
        logger.info("[TunnelSweeper] forgot unused tunnel %s: %s", tunnel_id, reason)
        return True


def _away_since(row: dict[str, Any]) -> datetime:
    return row["seen_at"] or row["paired_at"]
