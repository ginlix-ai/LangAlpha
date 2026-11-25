"""The livefs package's daemon commands as the host runs them in a sandbox,
for both of its mounts: ``boot`` as root, the answer it prints, and the
daemon's own files shipped when a command finds them stale.

Each mount names its own private directory, so the file mount and the Drive
keep apart the code copy, lock and state they run from.
"""

from __future__ import annotations

import functools
import json
import shlex
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import structlog

from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.sandbox.livefs_runtime import lifecycle, protocol
from ptc_agent.core.sandbox.livefs_runtime.protocol import MountError
from ptc_agent.core.sandbox.retry import RetryPolicy

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

logger = structlog.get_logger(__name__)


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
    file mount's private directory, and the code and manifest this host ships."""
    from ptc_agent.core.sandbox.livefs_runtime import boot

    with open(boot.__file__) as f:
        script = f.read()
    manifest = json.dumps(lifecycle.code_manifest(), sort_keys=True)
    return script, lifecycle.Paths().private, lifecycle.code_version(), manifest


def code_version() -> str:
    """The daemon code this host ships, which a start brings a daemon to."""
    return _boot()[2]


def _shipped_dir(layout: SandboxLayout) -> str:
    return f"{layout.internal_src}/{protocol.PACKAGE_NAME}"


def _command(layout: SandboxLayout, action: str, *args: str, private: str | None = None) -> str:
    """``private`` is the daemon's own directory: the file mount's unless
    named."""
    script, own_private, code, manifest = _boot()
    argv = (
        script,
        private or own_private,
        _shipped_dir(layout),
        code,
        manifest,
        action,
        "--root",
        layout.root,
        *args,
    )
    # Root runs the host's own text, which runs the package from a checked
    # copy: the shipped one is the sandbox user's to rewrite.
    return "cd / && python3 -I -c " + " ".join(shlex.quote(a) for a in argv)


def parse(result: Any) -> dict[str, Any] | None:
    """The JSON answer a daemon command printed last (None: none)."""
    lines = (getattr(result, "stdout", "") or "").strip().splitlines()
    try:
        answer = json.loads(lines[-1]) if lines else None
    except ValueError:
        return None
    return answer if isinstance(answer, dict) else None


async def _ship_code(sandbox: PTCSandbox) -> bool:
    """Put the daemon's files where ``boot`` checks them, saying whether they
    landed. The asset sync ships the same files with the other internal
    packages; this is for a command that ran before it got to them."""
    assert sandbox.runtime is not None
    shipped = _shipped_dir(sandbox.layout)
    try:
        await sandbox._runtime_call(
            sandbox.runtime.exec,
            f"mkdir -p {shlex.quote(shipped)}",
            retry_policy=RetryPolicy.SAFE,
        )
        await sandbox._runtime_call(
            sandbox.runtime.upload_files,
            [(data, f"{shipped}/{name}") for name, data in lifecycle.package_files()],
            retry_policy=RetryPolicy.SAFE,
        )
    except Exception as e:  # noqa: BLE001 - the caller answers stale_code as before
        logger.warning("Could not ship the file mount's code", error=str(e))
        return False
    return True


async def execute(
    sandbox: PTCSandbox,
    action: str,
    args: Sequence[str],
    *,
    timeout_s: float,
    env: dict[str, str] | None = None,
    private: str | None = None,
    retry_policy: RetryPolicy = RetryPolicy.SAFE,
) -> Any:
    """One daemon command as root, its result as the runtime gave it."""
    assert sandbox.runtime is not None
    return await sandbox._runtime_call(
        sandbox.runtime.exec_as_root,
        _command(sandbox.layout, action, *args, private=private),
        timeout_s,
        env,
        retry_policy=retry_policy,
    )


async def _ask(
    sandbox: PTCSandbox,
    action: str,
    args: Sequence[str],
    config: dict[str, Any] | None,
    *,
    private: str | None,
    timeout_s: float,
) -> MountOutcome:
    env = None if config is None else {protocol.CONFIG_ENV: json.dumps(config)}
    result = await execute(sandbox, action, args, env=env, private=private, timeout_s=timeout_s)
    answer = parse(result)
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


async def run(
    sandbox: PTCSandbox,
    action: str,
    args: Sequence[str],
    config: dict[str, Any] | None = None,
    *,
    timeout_s: float,
    private: str | None = None,
) -> MountOutcome:
    """Run one daemon command as root, carrying ``config`` in when given.

    The daemon's code reaches the machine with the asset sync, which a
    turn's mount does not wait for. A command refused for stale code ships
    the daemon's own files and asks once more, so a mount never depends on
    that sync having got there first."""
    outcome = await _ask(sandbox, action, args, config, private=private, timeout_s=timeout_s)
    if outcome.error == MountError.STALE_CODE and await _ship_code(sandbox):
        outcome = await _ask(sandbox, action, args, config, private=private, timeout_s=timeout_s)
    return outcome


async def probe(sandbox: PTCSandbox, command: str, marker: str) -> bool | None:
    """Whether ``command`` printed ``marker`` (None: the exec failed)."""
    assert sandbox.runtime is not None
    try:
        result = await sandbox._runtime_call(
            sandbox.runtime.exec, command, retry_policy=RetryPolicy.SAFE
        )
    except Exception:  # noqa: BLE001 - unknown, so the caller changes nothing
        return None
    return marker in (getattr(result, "stdout", "") or "")
