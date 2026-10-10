"""The file mount as the host drives it: the daemon's own commands in a
sandbox, and each tool command run through it.

The server decides what to mount and mints the token; this only carries them
into the sandbox. The daemon's commands are idempotent, so every caller asks
for the state it wants and the daemon does whatever part of it is missing.
"""

from __future__ import annotations

import functools
import json
import secrets
import shlex
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from ptc_agent.core.paths import (
    MEMO_USER_DIR,
    MEMORY_USER_DIR,
    SandboxLayout,
    WorkspaceLayout,
)
from ptc_agent.core.sandbox.livefs_runtime import lifecycle, protocol
from ptc_agent.core.sandbox.livefs_runtime.protocol import MountError
from ptc_agent.core.sandbox.retry import RetryPolicy

if TYPE_CHECKING:
    from langchain_core.messages import AnyMessage

    from ptc_agent.agent.transcript import TranscriptTarget, Window
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

# Covers a cold start's wait for its mount, plus the probe and links.
_EXEC_TIMEOUT_S = lifecycle.START_TIMEOUT_S + 20

_ANSWERS = "livefs-mount-answers"
#: Prints ``_ANSWERS`` when something answers at the mount's path. Killed at
#: 2 s: a stat on a daemon that hangs waits until killed.
PROBE = f"timeout -s KILL 2 test -e {shlex.quote(protocol.MOUNT + '/.')} && echo {_ANSWERS}"


def answered(stdout: str | None) -> bool:
    """Whether a command that ran ``PROBE`` found the mount answering."""
    return _ANSWERS in (stdout or "")


@dataclass(frozen=True)
class CallContext:
    """Who a command runs for. The mount serves the whole computer, so a save
    through it names only the call, and a file whose meaning depends on the
    conversation (a new automation's defaults and ``"current"`` thread)
    reads the rest from here."""

    workspace_id: str | None = None
    thread_id: str | None = None
    # None reads the user's stored zone, when a file needs one.
    timezone: str | None = None


class MountHandle(Protocol):
    """What a tool asks of a serving mount around one command. The host
    implements it, since the token, the links and the save outcomes are its
    own."""

    async def ready(self, workspace_id: str | None = None) -> bool:
        """Wait out links still going in and a token that has to be replaced
        first; whether the mount then serves ``workspace_id``'s folder (None:
        the computer's root). A command run without it would write beside
        the files the store holds."""

    async def prepare(self, call_id: str, context: CallContext | None = None) -> None:
        """File who ``call_id`` runs for."""

    async def report(
        self, call_id: str, output: str, context: CallContext | None = None
    ) -> str:
        """Saves through the mount that failed during ``call_id`` (and any
        that failed after an earlier command in ``context``'s thread
        returned), as text for the tool result; empty when none did. An
        ``output`` showing the mount dead restarts it."""

    async def save_transcript(
        self, target: TranscriptTarget, messages: Sequence[AnyMessage], *, window: Window
    ) -> bool:
        """Store one agent's transcript from the messages in hand, ``window``
        having been trimmed from their head, where the mount serves it;
        whether it landed."""


async def through_mount(
    mount: MountHandle | None,
    context: CallContext | None,
    run: Callable[[str | None], Awaitable[tuple[str, dict[str, Any]]]],
) -> tuple[str, dict[str, Any]]:
    """Run one command under a fresh call id while the mount serves, and add
    to its output what its saves through the mount reported.

    The id is collected once ``run`` returns, so the later saves of a command
    it only launched reach the next command's result or a BashOutput read. A
    cancelled run reports nothing.
    """
    if mount is None:
        return await run(None)
    call_id = secrets.token_hex(8)
    await mount.prepare(call_id, context)
    text, artifact = await run(call_id)
    report = await mount.report(call_id, text, context)
    return (f"{text}\n\n{report}" if report else text), artifact


_MEMORY_MARKERS = (f"{MEMORY_USER_DIR}/", f"{WorkspaceLayout.MEMORY_DIR}/", f"{MEMO_USER_DIR}/")
_SERVED_DIRS = (
    SandboxLayout.AUTOMATIONS_DIR,
    SandboxLayout.USER_PROFILE_DIR,
    SandboxLayout.WORKFLOWS_DIR,
)


def unserved_tree(mount: MountHandle | None, text: str) -> str | None:
    """The store-backed tree a command names while no mount serves it:
    "memory" for the memory and memo tiers, else the directory. The tool
    refuses such a command: it would find no files there, and a write would
    land where nothing reads it."""
    if mount is not None:
        return None
    if any(marker in text for marker in _MEMORY_MARKERS):
        return "memory"
    return next((d for d in _SERVED_DIRS if d in text), None)


@dataclass(frozen=True)
class MountOutcome:
    ok: bool
    error: MountError | None = None
    reason: str | None = None
    started: bool = False
    failed: dict[str, str] | None = None
    #: Link paths that already held files, and where those were moved.
    set_aside: dict[str, str] | None = None


@functools.cache
def _boot() -> tuple[str, str, str, str]:
    """``boot``'s text and what it checks the shipped package against: the
    daemon's private directory, and the code and manifest this host ships."""
    from ptc_agent.core.sandbox.livefs_runtime import boot, lifecycle

    with open(boot.__file__) as f:
        script = f.read()
    manifest = json.dumps(lifecycle.code_manifest(), sort_keys=True)
    return script, lifecycle.Paths().private, lifecycle.code_version(), manifest


def code_version() -> str:
    """The daemon code this host ships, which ``start`` brings a daemon to."""
    return _boot()[2]


def _command(layout: SandboxLayout, action: str, *args: str) -> str:
    script, private, code, manifest = _boot()
    shipped = f"{layout.internal_src}/{protocol.PACKAGE_NAME}"
    argv = (script, private, shipped, code, manifest, action, "--root", layout.root, *args)
    # Root runs the host's own text, which runs the package from a checked
    # copy: the shipped one is the sandbox user's to rewrite.
    return "cd / && python3 -I -c " + " ".join(shlex.quote(a) for a in argv)


def _parse(result: Any) -> dict[str, Any] | None:
    lines = (getattr(result, "stdout", "") or "").strip().splitlines()
    try:
        return json.loads(lines[-1]) if lines else None
    except ValueError:
        return None


async def _run(
    sandbox: PTCSandbox,
    action: str,
    args: list[str],
    config: dict[str, Any] | None,
) -> MountOutcome:
    """Run one daemon command as root, carrying ``config`` in when given."""
    assert sandbox.runtime is not None
    env = None if config is None else {protocol.CONFIG_ENV: json.dumps(config)}
    result = await sandbox._runtime_call(
        sandbox.runtime.exec_as_root,
        _command(sandbox.layout, action, *args),
        _EXEC_TIMEOUT_S,
        env,
        retry_policy=RetryPolicy.SAFE,
    )
    answer = _parse(result)
    if answer is None:
        return MountOutcome(
            ok=False,
            error=MountError.UNANSWERED,
            reason=(getattr(result, "stderr", "") or "")[-300:],
        )
    error = answer.get("error")
    return MountOutcome(
        ok=bool(answer.get("ok")),
        error=MountError(error) if error else None,
        reason=answer.get("reason"),
        started=bool(answer.get("started")),
        failed=answer.get("failed"),
        set_aside=answer.get("set_aside"),
    )


async def start(
    sandbox: PTCSandbox,
    *,
    config: dict[str, Any] | None,
    base_url: str,
) -> MountOutcome:
    """Make the mount serve ``base_url``, starting the daemon if it is not
    serving current code.

    ``config`` is a new token to install; None keeps the one the sandbox
    has. It travels in the root command's environment and the daemon keeps
    it where only root can read it, which hides it from agent code only where
    that code runs as another user (see ``protocol.CONFIG_ENV``).
    """
    return await _run(sandbox, "start", ["--base-url", base_url], config)


async def answers(sandbox: PTCSandbox) -> bool | None:
    """Whether the mount answers at its path (None: the exec failed). After a
    restart no reconnect saw, the links dangle and answer ENOENT, which no
    command tells apart from a missing file."""
    assert sandbox.runtime is not None
    try:
        result = await sandbox._runtime_call(
            sandbox.runtime.exec, PROBE, retry_policy=RetryPolicy.SAFE
        )
    except Exception:  # noqa: BLE001 - unknown, so the caller changes nothing
        return None
    return answered(getattr(result, "stdout", None))


async def link(sandbox: PTCSandbox, links: Sequence[tuple[str, str]]) -> MountOutcome:
    """Link exactly ``links`` (mount path, target), removing the links an
    earlier call laid that this one leaves out."""
    args: list[str] = []
    for source, target in links:
        args += ["--link", f"{source}:{target}"]
    return await _run(sandbox, "link", args, None)
