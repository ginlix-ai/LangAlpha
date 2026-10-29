"""The check a command (stdio) MCP server passes before it is saved.

A command server runs on the user's computer, so that is where it is checked:
the command starts there the way the computer's MCP supervisor starts it, with
its ``${vault:NAME}`` refs resolved by the same in-sandbox client from a vault
file, and it passes once it has answered a tool listing. Anything less (a
command that only parses, a package that only exists) would let a server be
saved that fails on the first call.

The check writes nothing the product reads: no discovery snapshot, no config
on the computer, no vault row. Every file it puts on the computer is removed
and the process it started is stopped before the answer comes back, so a
server that fails leaves no trace and one that passes is saved by the caller.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Mapping
from typing import Any

from fastapi import HTTPException

from ptc_agent.core.mcp_sanitize import (
    VAULT_REF_RE,
    iter_arg_credentials,
    looks_like_secret,
)

from src.server.database.user_vault_secrets import get_user_secrets_decrypted
from src.server.models.computer import ComputerStatus
from src.server.models.mcp_server import McpServerInput
from src.server.services.computer_manager import ComputerManager
from src.server.services.mcp_config import user_row_to_server_config
from src.server.services.mcp_validation import CheckFailure, Passed
from src.server.utils.secret_redactor import get_redactor

logger = logging.getLogger(__name__)

#: The whole check, waking a stopped computer included. A cold Daytona start
#: plus a first npx/uvx fetch fits; anything longer reads as a hung form.
CHECK_TIMEOUT_S = 90.0
# The run's own ceiling: the client's handshake ladder (two 10 s probes) and
# its 30 s listing, with room for a package fetch on a cold cache.
_RUN_BUDGET_S = 60.0
# Less than this left after the wake and the run would time out on the
# command's account when it was the computer that took the time.
_MIN_RUN_S = 15.0
# The run's budget ends this far ahead of the check's deadline, so a command
# out of time answers as its own timeout before the deadline cuts the run off.
_RUN_MARGIN_S = 10.0
_REASON_MAX = 300
# A value cut off this far in is still worth hiding; any shorter, and the end
# of an ordinary word would match the start of a key.
_PARTIAL_MIN = 8

NO_COMPUTER = (
    "a command server is checked by starting it on your computer, and there "
    "is none yet; create a workspace first, then add the server again"
)
NO_MANAGER = "computers are not available on this server"
WAKE_TIMEOUT = "your computer is still starting; try again in a minute"
WAKE_FAILED = "your computer could not be started to run the check; try again in a moment"
BROKEN_COMPUTER = "your computer is in an error state, so the command could not be checked on it"
RUN_FAILED = "the command could not be run on your computer; try again in a moment"
NO_ANSWER = "the command did not start and list its tools within {seconds} seconds"

# Strong refs so a wake the check stopped waiting for still finishes; never
# consulted as state.
_wakes: set[asyncio.Task] = set()


class _Refused(Exception):
    """The check cannot run; ``reason`` is the user's answer."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _vault_refs(server: McpServerInput) -> list[str]:
    """Every vault name the command's env and args refer to, first one first."""
    names: list[str] = []
    for value in [*server.env.values(), *server.args]:
        names.extend(n for n in VAULT_REF_RE.findall(value or "") if n not in names)
    return names


async def check_command_server(
    user_id: str,
    server: McpServerInput,
    *,
    extra_secrets: Mapping[str, str] | None = None,
    workspace_id: str | None = None,
) -> Passed | CheckFailure:
    """Start ``server`` on the user's computer and list its tools.

    ``extra_secrets`` are values the same request writes to the vault once the
    check passes; they win over stored values of the same name, as they will
    once written, and reach only this check's own vault file on the computer.
    ``workspace_id`` picks that workspace's computer; without it the account's
    most ready one is used. A ref with no value fails as ``missing_secrets``
    with nothing started; anything else that keeps the command from listing
    its tools fails as ``command_failed`` with a short reason.
    """
    extra = dict(extra_secrets or {})
    refs = _vault_refs(server)
    stored_names = [n for n in refs if n not in extra]
    stored = (
        await get_user_secrets_decrypted(user_id, stored_names) if stored_names else {}
    )
    missing = [n for n in stored_names if n not in stored]
    if missing:
        return CheckFailure(
            server.name,
            "missing_secrets",
            f"no value is set for {', '.join(missing)}",
            missing_secrets=tuple(missing),
        )
    vault = {n: extra[n] if n in extra else stored[n] for n in refs}

    def failed(reason: str) -> CheckFailure:
        return CheckFailure(
            server.name, "command_failed", reason or "the command did not start"
        )

    loop = asyncio.get_running_loop()
    deadline = loop.time() + CHECK_TIMEOUT_S
    sandbox: Any = None
    budget = _RUN_BUDGET_S
    try:
        async with asyncio.timeout_at(deadline):
            manager = ComputerManager.current()
            if manager is None:
                raise _Refused(NO_MANAGER)
            computer = await manager.pick_for_user(user_id, workspace_id=workspace_id)
            if computer is None:
                raise _Refused(NO_COMPUTER)
            sandbox = await _wake(manager, computer, user_id)
            left = deadline - loop.time() - _RUN_MARGIN_S
            if left < _MIN_RUN_S:
                raise _Refused(WAKE_TIMEOUT)
            budget = min(_RUN_BUDGET_S, left)
        # The deadline travels into the run rather than wrapping it: the run
        # stops at it on its own and then waits a bounded time for its files
        # to go, so a run that answered in time is never turned into a timeout
        # by its cleanup.
        raw = await sandbox.check_user_mcp_server(
            user_row_to_server_config(server.to_config_blob()),
            vault,
            budget_s=budget,
            deadline=deadline,
        )
    except _Refused as e:
        return failed(e.reason)
    except TimeoutError:
        if sandbox is None:
            return failed(WAKE_TIMEOUT)
        return failed(NO_ANSWER.format(seconds=int(budget)))
    except Exception:
        logger.warning(
            "[MCP_CHECK] command check for %s failed to run",
            server.name,
            exc_info=True,
        )
        return failed(WAKE_FAILED if sandbox is None else RUN_FAILED)

    status = raw.get("status")
    if status == "ok":
        return Passed(server.name)
    if status == "timeout":
        return failed(NO_ANSWER.format(seconds=int(budget)))
    return failed(command_failure_reason(str(raw.get("error") or ""), server, vault))


async def _wake(manager: Any, computer: dict[str, Any], user_id: str) -> Any:
    """This worker's sandbox on ``computer``, starting it through the normal path.

    The start runs as its own task and is waited on through a shield: a check
    that runs out of time stops waiting, but a start cut off midway would hand
    its claim back half built, so it finishes and the next check finds the
    computer up.
    """
    if computer.get("status") == ComputerStatus.ERROR:
        raise _Refused(BROKEN_COMPUTER)
    task = asyncio.ensure_future(
        manager.wake_computer(str(computer["computer_id"]), user_id=user_id)
    )
    _wakes.add(task)
    task.add_done_callback(_wakes.discard)
    task.add_done_callback(_log_late_wake_failure)
    try:
        session = await asyncio.shield(task)
    except HTTPException as e:
        # The plan's computer limit; its message is the platform's own copy.
        detail = e.detail if isinstance(e.detail, dict) else {}
        raise _Refused(str(detail.get("message") or WAKE_FAILED)) from e
    sandbox = getattr(session, "sandbox", None)
    if sandbox is None:
        raise _Refused(WAKE_FAILED)
    return sandbox


def _log_late_wake_failure(task: asyncio.Task) -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None and not isinstance(exc, HTTPException):
        logger.info("[MCP_CHECK] computer wake ended with %s: %s", type(exc).__name__, exc)


_STDERR_MARK = "\nserver stderr tail:\n"
# npm leads every stderr line with its own tag, which says nothing.
_NPM_TAG_RE = re.compile(r"^npm (?:error|ERR!|warn|WARN)(?: \d{3})?\s*")
# A line that points somewhere else instead of saying what went wrong.
_NOISE_RE = re.compile(r"complete log of this run", re.I)
# A line that names the failure, as opposed to advice or context around it.
_TELLING_RE = re.compile(
    r"error|exception|not found|no matching|denied|refused|invalid|missing"
    r"|required|cannot|can't|could not|unable|failed|not exist|unknown",
    re.I,
)
_SERVER_PREFIX_RE = re.compile(r"^MCP server \S+ ")


def _telling_line(tail: str) -> str:
    """The stderr line that says what went wrong: the last one naming a failure.

    Last, because a traceback ends on its exception; naming, because npm
    follows its error with lines of advice.
    """
    lines = []
    for raw in tail.splitlines():
        line = _NPM_TAG_RE.sub("", raw.strip()).strip()
        if line and not _NOISE_RE.search(line):
            lines.append(line)
    for line in reversed(lines):
        if _TELLING_RE.search(line):
            return line
    return lines[-1] if lines else ""


def _secrets_in_play(
    server: McpServerInput, vault: Mapping[str, str]
) -> dict[str, str]:
    """Every value the command was started with that reads as a credential.

    The vault values the check resolved, and the literals written straight
    into the server's env and args, which no vault scan knows about.
    """
    found = {
        key: value
        for key, value in server.env.items()
        if value and not VAULT_REF_RE.search(value) and looks_like_secret(key, value)
    }
    found.update(iter_arg_credentials(server.args))
    found.update(vault)
    return found


def _cut_partial_secret(line: str, secrets: Mapping[str, str]) -> str:
    """Hide the start of a secret the line was cut off inside.

    The client caps what it relays (a stderr line, a quoted stdout line)
    before the host sees it, so a value that straddled the cap arrives as a
    prefix no full-value scan matches. A quoted cut ends on its quote.
    """
    quote = line[-1] if line[-1:] in ("'", '"') else ""
    body = line[: len(line) - len(quote)]
    for name, value in secrets.items():
        for n in range(min(len(value), len(body)), _PARTIAL_MIN - 1, -1):
            if body.endswith(value[:n]):
                return f"{body[:-n]}[REDACTED:{name}]{quote}"
    return line


def command_failure_reason(
    error: str, server: McpServerInput, secrets: Mapping[str, str]
) -> str:
    """The client's failure as one short clause the user can act on.

    The text can quote the server's stderr, so every value the command was
    started with is redacted before any of it leaves the computer's side,
    including the start of one the client's own cap cut in half.
    """
    secrets = _secrets_in_play(server, secrets)
    text = get_redactor().redact(error, vault_secrets=secrets)
    head, _, tail = text.partition(_STDERR_MARK)
    head = head.strip().splitlines()[0] if head.strip() else ""
    head = _cut_partial_secret(head, secrets)
    if head.startswith("[Errno 2]"):
        reason = f"the command {server.command!r} was not found on your computer"
    elif head.startswith("[Errno 13]"):
        reason = f"the command {server.command!r} is not allowed to run on your computer"
    elif "closed connection" in head:
        reason = "the command exited before it answered"
    elif "timed out after" in head:
        reason = "the command started but did not answer in time"
    elif head.startswith("tools/list error"):
        reason = "the server refused to list its tools: " + head.partition(":")[2].strip()
    else:
        reason = _SERVER_PREFIX_RE.sub("", head) or "the command did not start"
    said = _cut_partial_secret(_telling_line(tail), secrets)
    if said:
        reason = f"{reason}: {said}"
    if len(reason) > _REASON_MAX:
        reason = reason[: _REASON_MAX - 3].rstrip() + "..."
    return reason
