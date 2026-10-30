"""Claude Code on the user's own computers: the setup token kept on each
machine, and what the backend knows of it.

An own computer (the SSH provider's, a Mac reached through Remote control
included) runs the CLI itself, signed in with the token LangAlpha keeps on
that machine. With none there, a self-hosted server relays the token the
user saved in Settings (``backend_available``), handed to the CLI by fd and
never written to the machine; a hosted server has nothing to relay.

A token passes through the backend once, from a sign-in or a paste: it is
staged on the machine over its SSH connection's stdin, beside the computer's
own token file, checked there, and renamed over that file only once Claude
serves it; a staged token that does not serve is removed and the token in use
never touched. It is never stored, logged or cached here. What is kept, in Redis under the computer id, is the machine's answer:
whether a token is there, whether Claude served it, and the account and
expiry a sign-in gave it. A status read never wakes or dials a computer
that is asleep or out of reach: it answers with what the machine last said.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from functools import partial
from typing import Any, Literal

from ptc_agent.core.sandbox.providers._ssh_transport import TRANSPORT_ERRORS, Machine
from ptc_agent.core.sandbox.providers.ssh_errors import connect_failure
from ptc_agent.core.sandbox.runtime import SandboxTransientError
from src.llms.claude_code import backend_available
from src.server.services.oauth_signin import OAuthRefusal, PendingSignIn
from src.server.database.computer import get_computer, get_computer_roster
from src.server.models.claude_code import ClaudeCodeComputer, ClaudeCodeComputers, ComputerState, Source
from src.server.models.computer import ComputerStatus as Lifecycle
from src.server.services.ssh_computers import is_own_computer, ssh_info, tunnel_of, tunnels_for
from src.utils.cache.redis_cache import get_cache_client

from .checks import (
    CHECK_CLEANUP_S,
    CHECK_S,
    CHECKED,
    MARGIN_S,
    SHORT_TTL_S,
    Held,
    in_background,
    keep_check,
    run_check,
    ttl,
)
from .flow import MACHINE_IO_S, busy, rows_on
from .saved import stored_token, token_check_key, token_state, usable_token

logger = logging.getLogger(__name__)

#: How long a computer's last answer is shown while it cannot be reached.
KEPT_TTL_S = 30 * 24 * 3600
#: How long removing a computer waits for a check or save of its token to end.
REMOVAL_WAIT_S = 30
#: A status read's check of a machine: dialing it, reading its token file's
#: state, then one check there.
STATUS_CHECK_S = 2 * MACHINE_IO_S + CHECK_S + CHECK_CLEANUP_S

#: What a machine's token is, as the backend last found it: a check's answer,
#: or ``none`` (no token there) or ``invalid`` (a file there the machine
#: refuses to read: a symlink, another owner, a mode others can read, no text).
FileState = Literal["works", "refused", "unknown", "none", "invalid"]
#: Which token Claude refused on a run there, as the launcher reports it.
Refused = Literal["file", "relay"]


def answer_key(computer_id: str) -> str:
    return f"claude_code:computer:{computer_id}"


def unreached_key(computer_id: str) -> str:
    """Set briefly when a check could not reach the machine, so reads do not dial it again at once."""
    return f"claude_code:computer:{computer_id}:unreached"


def change_lock(computer_id: str) -> str:
    """One change or check of a computer's token at a time, a status read's check included."""
    return f"claude_code:computer_change:{computer_id}"


@dataclass(frozen=True)
class MachineAnswer:
    """A machine's token as last found, never the token itself."""

    file: FileState
    email: str | None = None
    #: ISO 8601. For a pasted token, the latest it can expire.
    expires_at: str | None = None
    checked_at: float = 0.0

    @property
    def fresh(self) -> bool:
        return time.time() < self.checked_at + ttl(self.file)

    @property
    def meta(self) -> dict[str, str | None]:
        """What a sign-in told about the token, which the machine does not keep."""
        return {"email": self.email, "expires_at": self.expires_at}


async def read_answer(computer_id: str) -> MachineAnswer | None:
    """The computer's last answer, fresh or not."""
    raw = await get_cache_client().get(answer_key(computer_id))
    if not isinstance(raw, dict):
        return None
    try:
        return MachineAnswer(**raw)
    except TypeError:
        return None


async def keep_answer(computer_id: str, answer: MachineAnswer) -> MachineAnswer:
    """Keep ``answer`` as found now: fresh for its own TTL, shown for ``KEPT_TTL_S``."""
    answer = replace(answer, checked_at=time.time())
    await get_cache_client().set(answer_key(computer_id), asdict(answer), ttl=KEPT_TTL_S)
    return answer


async def forget_answer(computer_id: str) -> None:
    """Drop the computer's answer, so the next read asks the machine again."""
    await get_cache_client().delete(answer_key(computer_id))


class ComputerToken:
    """What a turn on an own computer signs in with, as the backend sees it.

    ``relay`` hands the CLI the user's saved token when the machine asks for
    it, unless Claude is known to refuse it, remembering only its hash;
    ``announced`` hears whether the machine had a token file of the
    computer's, and ``refused`` that Claude refused the token the run used,
    so Settings and admission say so from then on. Neither overrides an
    answer kept after the turn was built: a token placed since is newer than
    anything this turn found.
    """

    def __init__(self, user_id: str, computer_id: str) -> None:
        self.user_id = user_id
        self.computer_id = computer_id
        self.built_at = time.time()
        self._relayed: str | None = None

    async def _last(self) -> MachineAnswer | None:
        """The kept answer, unless one was kept after this turn was built."""
        last = await read_answer(self.computer_id)
        return None if last is not None and last.checked_at > self.built_at else last

    async def relay(self) -> str | None:
        token = await usable_token(self.user_id)
        self._relayed = token_check_key(token) if token else None
        return token

    async def announced(self, source: Literal["file", "relay", "none"]) -> None:
        """Forget an answer that has a token file there when the machine found
        none, so admission stops passing turns that would fail."""
        if source == "file":
            return
        last = await self._last()
        if last is not None and last.file in CHECKED:
            await forget_answer(self.computer_id)

    async def refused(self, source: Refused) -> None:
        if source == "relay":
            if self._relayed is not None:
                await keep_check(self._relayed, "refused")
            return
        last = await read_answer(self.computer_id)
        if last is not None and last.checked_at > self.built_at:
            return
        await keep_answer(self.computer_id, MachineAnswer("refused", **(last.meta if last else {})))


async def machine_of(computer: Mapping[str, Any]) -> Machine:
    """The computer's machine, dialed without starting the computer. Raises when it cannot be reached."""
    from src.server.services.workspace_manager import WorkspaceManager

    return await WorkspaceManager.get_instance().machine_link(dict(computer))


def check_launcher(machine: Machine, user_id: str, token_file: str) -> Any:
    """A launcher signed in with the token file ``token_file`` names only, the
    computer's or a staged one, as a check of that token needs, and one that
    never installs the CLI nor waits long for a CLI slot: a check must end
    inside its hold."""
    from ptc_agent.agent.claude_code.ssh_launcher import START_TIMEOUT_S, SSHLauncher

    return SSHLauncher(machine, identity=user_id, token_file=token_file, install=False, slot_wait_s=START_TIMEOUT_S)


async def check_machine(machine: Machine, user_id: str, computer_id: str, last: MachineAnswer | None) -> MachineAnswer:
    """The machine's token as it stands: whether a usable one is there, and if
    so whether Claude serves it, by one minimal turn on the machine. A
    machine with no CLI yet reads as ``unknown``: a check never installs it;
    one with no perl has no token a turn could use, so it reads ``none``.

    The account and expiry are the last answer's, since the machine keeps
    only the token. Raises when the machine cannot be reached or refuses to
    say.
    """
    from ptc_agent.agent.claude_code.machine_token import machine_token_state, token_name

    state = await machine_token_state(await machine.link(), computer_id)
    if state != "file":
        return MachineAnswer(state)
    meta = last.meta if last else {}
    try:
        works = await run_check(check_launcher(machine, user_id, token_name(computer_id)))
    except Exception as e:
        logger.info(f"[claude_code] token check could not tell on the machine: {type(e).__name__}")
        return MachineAnswer("unknown", **meta)
    return MachineAnswer("works" if works else "refused", **meta)


def _unreached(e: BaseException) -> bool:
    """Whether a failure means the machine was not reached: the dial or the
    connection failed, or it did not answer in time. A machine that answered
    with an error was reached."""
    return connect_failure(e) is not None or isinstance(e, (TimeoutError, SandboxTransientError, *TRANSPORT_ERRORS))


async def _check_then_keep(user_id: str, computer_id: str, last: MachineAnswer | None, held: Held) -> None:
    # The roster a listing reads holds no provider_ref, which dialing needs.
    computer = await get_computer(computer_id)
    if computer is None:
        return
    try:
        async with asyncio.timeout(MACHINE_IO_S):
            machine = await machine_of(computer)
        answer = await check_machine(machine, user_id, computer_id, last)
    except Exception as e:
        if _unreached(e):
            logger.info(f"[claude_code] computer_id={computer_id} not reached for its token check: {type(e).__name__}")
            await get_cache_client().set(unreached_key(computer_id), True, ttl=SHORT_TTL_S)
            return
        # By its type alone: what the machine said may hold anything.
        logger.warning(f"[claude_code] computer_id={computer_id} could not say how its token stands: {type(e).__name__}")
        answer = MachineAnswer("unknown", **(last.meta if last else {}))
    if await held():
        await keep_answer(computer_id, answer)


def _kind(computer: Mapping[str, Any]) -> Literal["ssh", "helper"]:
    """``helper`` for a Mac reached through Remote control, else ``ssh``."""
    info = ssh_info(dict(computer))
    return "helper" if info is not None and info.helper else "ssh"


def _awake(computer: Mapping[str, Any], tunnels: dict[str, dict[str, Any]] | None) -> bool:
    """Whether a check may dial the computer: it is running, and a machine
    that connects out through a tunnel has it up."""
    if computer.get("status") != Lifecycle.RUNNING:
        return False
    info = ssh_info(dict(computer), tunnels)
    return info is not None and (info.tunnel_connected is True if tunnel_of(dict(computer)) else True)


def _entry(
    computer: Mapping[str, Any], state: ComputerState, source: Source | None = None, answer: MachineAnswer | None = None,
) -> ClaudeCodeComputer:
    return ClaudeCodeComputer(
        computer_id=str(computer["computer_id"]), name=computer.get("name") or "", kind=_kind(computer),
        state=state, source=source, **(answer.meta if answer is not None else {}),
    )


async def _relayed(user_id: str) -> str | None:
    """The saved token a machine with none of its own would sign in with, where it is relayed."""
    return await stored_token(user_id) if backend_available() else None


async def _from_answer(computer: Mapping[str, Any], answer: MachineAnswer, saved: str | None) -> ClaudeCodeComputer:
    """The state an answer reads as. A machine with no usable token of its
    own signs in with the saved token where that is relayed, and reads as it does."""
    if answer.file in CHECKED:
        return _entry(computer, CHECKED[answer.file], "machine", answer)
    if saved:
        return _entry(computer, await token_state(saved), "saved")
    if answer.file == "invalid":
        return _entry(computer, "rejected", "machine")
    return _entry(computer, "no_token")


async def _status(
    user_id: str, computer: Mapping[str, Any], tunnels: dict[str, dict[str, Any]] | None, saved: str | None,
) -> ClaudeCodeComputer:
    """The computer's kept answer. On a miss with the computer awake, one
    worker checks it in the background and reads answer checking until it
    lands; one asleep or out of reach reads as it last answered."""
    computer_id = str(computer["computer_id"])
    answer = await read_answer(computer_id)
    if answer is not None and answer.fresh:
        return await _from_answer(computer, answer, saved)
    if _awake(computer, tunnels) and not await get_cache_client().get(unreached_key(computer_id)):
        started = await in_background(
            change_lock(computer_id), STATUS_CHECK_S, partial(_check_then_keep, user_id, computer_id, answer),
        )
        return _entry(computer, "unverified" if started is None else "checking")
    if answer is not None:
        return await _from_answer(computer, answer, saved)
    return _entry(computer, "offline")


async def _off(computer: Mapping[str, Any]) -> ClaudeCodeComputer:
    """A computer while Claude Code is off for the user: never dialed, and
    naming the token it last held, which Settings still offers to remove."""
    answer = await read_answer(str(computer["computer_id"]))
    return _entry(computer, "off", "machine" if answer is not None and answer.file != "none" else None)


async def list_computers(user_id: str) -> ClaudeCodeComputers:
    """Every own computer of the user's and where Claude Code stands on it."""
    computers = [c for c in await get_computer_roster(user_id) if is_own_computer(c)]
    if not await rows_on(user_id):
        return ClaudeCodeComputers(computers=list(await asyncio.gather(*(_off(c) for c in computers))))
    tunnels, saved = await asyncio.gather(tunnels_for(user_id, computers), _relayed(user_id))
    entries = await asyncio.gather(*(_status(user_id, c, tunnels, saved) for c in computers))
    return ClaudeCodeComputers(computers=list(entries))


async def remove_for_removed_computer(link: Any, computer_id: str) -> bool:
    """Take a removed computer's token, and any staged for it, off its machine;
    whether one was there.

    Under the computer's change lock, waited for up to ``REMOVAL_WAIT_S``, so
    a check or save in flight ends first. One that outlasts the wait does not
    stop the removal: a save re-reads the computer before putting its token in
    place, and a token staged since is swept by the next write there.
    """
    from ptc_agent.agent.claude_code.machine_token import remove_machine_token

    cache = get_cache_client()
    lock, holder = change_lock(computer_id), uuid.uuid4().hex
    deadline = time.monotonic() + REMOVAL_WAIT_S
    while (acquired := await cache.acquire_lock(lock, holder, (MACHINE_IO_S + MARGIN_S) * 1000)) is False:
        if time.monotonic() >= deadline:
            break
        await asyncio.sleep(1)
    try:
        async with asyncio.timeout(MACHINE_IO_S):
            return await remove_machine_token(link, computer_id)
    finally:
        await forget_answer(computer_id)
        if acquired:
            await cache.release_lock(lock, holder)


class MachineHome:
    """A computer's machine, as the flow changes its token: staged beside the
    computer's token file, checked there, and renamed over it only once
    Claude serves it."""

    #: Staging, then renaming in or removing the staged file, and both when a rename fails.
    place_s = 3 * MACHINE_IO_S

    def __init__(self, user_id: str, computer: dict[str, Any]) -> None:
        self.user_id = user_id
        self.computer = computer
        self.computer_id = str(computer["computer_id"])
        self.lock = change_lock(self.computer_id)
        self.sign_in = PendingSignIn(f"claude_code:sign_in:{user_id}:{self.computer_id}")
        self.who = f"user_id={user_id} computer_id={self.computer_id}"
        self._machine: Machine | None = None

    @classmethod
    async def of(cls, user_id: str, computer_id: str) -> MachineHome:
        computer = await get_computer(computer_id)
        if computer is None or str(computer.get("user_id")) != user_id or not is_own_computer(computer):
            raise OAuthRefusal(404, "computer_not_found", "Computer not found")
        return cls(user_id, computer)

    @property
    def machine(self) -> Machine:
        if self._machine is None:
            raise RuntimeError("ready the home before changing its token")
        return self._machine

    async def ready(self, *, install: bool = True) -> None:
        """The machine reached, and with ``install`` the CLI the check runs.

        Done before the hold is taken: a first install downloads the CLI,
        which the hold need not cover, and the installer keeps concurrent
        installs apart on the machine itself.
        """
        try:
            async with asyncio.timeout(MACHINE_IO_S):
                self._machine = await machine_of(self.computer)
        except Exception as e:
            failure = connect_failure(e)
            raise OAuthRefusal(
                503, "computer_unreachable",
                failure.for_owner if failure else "This computer could not be reached. Check that it is on, then try again.",
                retryable=True,
            ) from None
        if not install:
            return
        from ptc_agent.agent.claude_code.cli_install import ensure_cli
        from ptc_agent.agent.claude_code.machine_token import has_perl

        if not await has_perl(await self._machine.link()):
            raise OAuthRefusal(
                400, "needs_perl", "Claude Code needs perl on this computer. Install perl, then try again.", retryable=True,
            )
        try:
            await ensure_cli(await self._machine.link(), self._machine.root())
        except Exception as e:
            logger.warning(f"[claude_code] CLI install failed on computer_id={self.computer_id}: {type(e).__name__}")
            raise OAuthRefusal(
                503, "cli_unavailable", "Claude Code could not be set up on this computer. Try again.", retryable=True,
            ) from None

    @asynccontextmanager
    async def placed(self, token: str, *, held: Held, retryable: bool) -> AsyncIterator[Any]:
        """Stage the token beside the computer's and yield a launcher on the
        staged file. Once the check passes, it is renamed over the computer's
        file; on anything else the staged file goes and the token in use stays."""
        from ptc_agent.agent.claude_code.machine_token import promote_machine_token, stage_machine_token

        if not await held():
            raise busy()
        try:
            async with asyncio.timeout(MACHINE_IO_S):
                staged = await stage_machine_token(await self.machine.link(), self.computer_id, token)
        except Exception as e:
            logger.warning(f"[claude_code] token write failed on computer_id={self.computer_id}: {type(e).__name__}")
            raise self._write_failed(retryable) from None
        promoted = False
        try:
            yield check_launcher(self.machine, self.user_id, staged)
            if not await held():
                raise busy()
            if await get_computer(self.computer_id) is None:
                # Removed meanwhile: its token is not put back on the machine.
                raise OAuthRefusal(404, "computer_not_found", "Computer not found")
            try:
                async with asyncio.timeout(MACHINE_IO_S):
                    await promote_machine_token(await self.machine.link(), self.computer_id, staged)
            except Exception as e:
                logger.warning(f"[claude_code] token rename failed on computer_id={self.computer_id}: {type(e).__name__}")
                await forget_answer(self.computer_id)
                raise self._write_failed(retryable) from None
            promoted = True
        finally:
            if not promoted:
                await asyncio.shield(self._discard(staged))

    @staticmethod
    def _write_failed(retryable: bool) -> OAuthRefusal:
        return OAuthRefusal(
            502, "write_failed", "LangAlpha could not save the token on this computer. Try again.", retryable=retryable,
        )

    async def _discard(self, staged: str) -> None:
        """Remove a staged token: always safe, since nothing else uses it.
        One left behind is swept by the next write there."""
        from ptc_agent.agent.claude_code.machine_token import discard_staged_token

        try:
            async with asyncio.timeout(MACHINE_IO_S):
                await discard_staged_token(await self.machine.link(), staged)
        except Exception as e:
            logger.warning(f"[claude_code] could not remove a staged token on computer_id={self.computer_id}: {type(e).__name__}")

    async def keep(
        self, token: str, *, held: Held, email: str | None, expires_at: datetime, token_uuid: str,
    ) -> ClaudeCodeComputer:
        if not await held():
            # Another change may have run since: whatever it did, the machine says.
            await forget_answer(self.computer_id)
            raise busy()
        answer = await keep_answer(self.computer_id, MachineAnswer("works", email, expires_at.isoformat()))
        logger.info(f"[claude_code] setup token placed on computer_id={self.computer_id} for user_id={self.user_id}")
        return _entry(self.computer, "signed_in", "machine", answer)

    async def current(self) -> ClaudeCodeComputer:
        answer = await read_answer(self.computer_id)
        if answer is None:
            return _entry(self.computer, "checking")
        return await _from_answer(self.computer, answer, await _relayed(self.user_id))

    async def remove(self) -> None:
        from ptc_agent.agent.claude_code.machine_token import remove_machine_token

        try:
            async with asyncio.timeout(MACHINE_IO_S):
                removed = await remove_machine_token(await self.machine.link(), self.computer_id)
        except Exception as e:
            logger.warning(f"[claude_code] token removal failed on computer_id={self.computer_id}: {type(e).__name__}")
            await forget_answer(self.computer_id)
            raise OAuthRefusal(
                502, "remove_failed", "LangAlpha could not remove the token from this computer. Try again.",
                retryable=True,
            ) from None
        await keep_answer(self.computer_id, MachineAnswer("none"))
        logger.info(f"[claude_code] setup token removed={removed} on computer_id={self.computer_id} for user_id={self.user_id}")
