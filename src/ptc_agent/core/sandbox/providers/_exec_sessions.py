"""Background sessions built from plain exec, for runtimes with no session API.

Daytona has a session API; Docker, E2B and SSH do not, so each keeps a session
as a directory of files that small shell scripts launch, poll, list and kill
through ``exec``. A runtime subclasses this and supplies ``exec``, the shell
expression naming its current boot, and, where ``/tmp`` is shared with other
people, its own sessions root.
"""

from __future__ import annotations

import base64
import re
import uuid
from typing import ClassVar

from ptc_agent.core.sandbox.providers._proc_sh import EXEC_MARK, PROC_SH, ps_start_sh
from ptc_agent.core.sandbox.runtime import (
    SandboxRuntime,
    SessionCommandResult,
    SessionState,
)

_SESSION_ID_RE = re.compile(r"^[a-zA-Z0-9_-]+$")

# Session state sits on the sandbox's disk, which outlives a stop, while the
# processes it names do not. ``R`` is the current run's directory, named by the
# runtime's boot key, which every start that ends processes replaces, so a
# restart begins with no sessions, as on Daytona. A command's .pid holds its
# wrapper's pid and start time, which ``plive`` and ``psig`` (``_proc_sh``)
# check before the command reads as running or is signalled.
_SESSIONS_ROOT = "/tmp/.sessions"

# The code a shell reports for a SIGKILL, which is how a wrapper ends without
# writing its exit code (the OOM killer, or a kill -9).
_KILLED_EXIT = 137


def _status_sh(name: str) -> str:
    """Shell that sets ``e`` to command ``name``'s exit code, empty while it runs.

    Liveness is read before the exit code because the wrapper writes the code
    before it ends: one still missing after the wrapper was seen gone is never
    coming. A command with no .pid yet is still launching.
    """
    return (
        f"a=1; [ ! -e {name}.pid ] || plive {name}.pid || a=0; "
        f"e=$(cat {name}.exit 2>/dev/null); "
        f"[ -n \"$e\" ] || [ $a = 1 ] || e={_KILLED_EXIT}; "
    )


class ExecSessionRuntime(SandboxRuntime):
    """A runtime whose session methods are implemented over ``exec``."""

    # Shell expression that changes exactly when the sandbox's processes were
    # all ended. A container's PID 1 is replaced on every start, so its start
    # time serves; a VM overrides it.
    session_boot_key_sh: ClassVar[str] = "$(pstart 1)"

    @property
    def _sessions_root(self) -> str:
        """Shell text naming the directory every run's sessions live under.

        ``/tmp`` is the sandbox's own on Docker and E2B. A machine shared with
        other people overrides it with a directory only its own user can write.
        The scripts expand it inside double quotes, so it may name a variable,
        whose value then never splits on a space a home folder may hold.
        """
        return _SESSIONS_ROOT

    @property
    def _session_sh(self) -> str:
        """Every session script starts with this."""
        return (
            f"{PROC_SH}"
            f'R="{self._sessions_root}/run-{self.session_boot_key_sh}"; '
        )

    async def create_session(self, session_id: str) -> None:
        if not _SESSION_ID_RE.match(session_id):
            raise ValueError(f"Invalid session_id: {session_id!r}")
        # Exit 3 marks an existing session. An earlier run's directory names
        # nothing that is still running, so it goes.
        result = await self.exec(
            f'{self._session_sh}[ -e "$R/{session_id}" ] && exit 3; '
            f'mkdir -p "$R/{session_id}" || exit 1; '
            f'for d in "{self._sessions_root}"/run-*; do '
            '[ "$d" = "$R" ] || rm -rf -- "$d"; done; true',
            timeout=10,
        )
        if result.exit_code == 3:
            raise RuntimeError(f"Session already exists: {session_id}")
        if result.exit_code != 0:
            detail = (result.stderr or result.stdout).strip()
            raise RuntimeError(
                f"Could not create session {session_id} (exit {result.exit_code}): {detail}"
            )

    async def session_execute(self, session_id: str, command: str) -> str:
        if not _SESSION_ID_RE.match(session_id):
            raise ValueError(f"Invalid session_id: {session_id!r}")
        cmd_id = uuid.uuid4().hex[:8]
        encoded = base64.b64encode(command.encode()).decode("ascii")
        # Use setsid to create a new process group so delete_session
        # can kill the entire tree (not just the wrapper); where there is
        # no setsid (macOS), job control gives it a group of its own. The
        # command runs in a subshell so its own `exit` still reaches the
        # .exit write, and with no arguments, so the session directory the
        # wrapper takes as $1 stays out of its view.
        # The wrapper is bash, so field 22 of its stat is its start time;
        # with no stat to read, it is the one ``pstart`` reads from ps.
        # Its .pid is written aside and renamed in, so no reader sees half
        # of it. A stop can land before then: delete_session moves the
        # session aside before reading PIDs, so the rename fails and a
        # command the stop missed never runs.
        wrapped = (
            f'{self._session_sh}D="$R/{session_id}"; '
            "L=; if command -v setsid >/dev/null 2>&1; then L=setsid; else set -m; fi; "
            "nohup $L bash -c '"
            "__la_s=$(cut -d\" \" -f22 /proc/$$/stat 2>/dev/null || "
            f"{{ {ps_start_sh('$$')}; }}); "
            f"echo \"$$ $__la_s\" > \"$1/{cmd_id}.new\" "
            f"&& mv \"$1/{cmd_id}.new\" \"$1/{cmd_id}.pid\" || exit; "
            f"export {EXEC_MARK}=\"$$-$__la_s\"; "
            f"( set --; unset __la_s; eval \"$(echo {encoded} | base64 -d)\" ); "
            f"echo $? > \"$1/{cmd_id}.exit\""
            f"' bash \"$D\" > \"$D/{cmd_id}.stdout\" 2> \"$D/{cmd_id}.stderr\" &"
        )
        await self.exec(wrapped, timeout=10)
        return cmd_id

    async def session_command_logs(
        self, session_id: str, command_id: str
    ) -> SessionCommandResult:
        if not _SESSION_ID_RE.match(session_id):
            raise ValueError(f"Invalid session_id: {session_id!r}")
        if not _SESSION_ID_RE.match(command_id):
            raise ValueError(f"Invalid command_id: {command_id!r}")
        # Single exec round-trip: emit all three files separated by a null
        # byte. The exit code is read first, so a finished status never pairs
        # with output read before the last of it was written. Exit 3 marks a
        # missing session.
        _SEP = "\\x00"
        result = await self.exec(
            f'{self._session_sh}cd "$R/{session_id}" 2>/dev/null || exit 3; '
            f"{_status_sh(command_id)}"
            f"cat {command_id}.stdout 2>/dev/null; "
            f"printf '{_SEP}'; "
            f"cat {command_id}.stderr 2>/dev/null; "
            f"printf '{_SEP}%s' \"$e\"",
            timeout=5,
        )
        if result.exit_code == 3:
            raise FileNotFoundError(f"No session {session_id}")
        parts = result.stdout.split("\x00", 2)
        stdout = parts[0] if len(parts) > 0 else ""
        stderr = parts[1] if len(parts) > 1 else ""
        exit_str = parts[2].strip() if len(parts) > 2 else ""
        exit_code = int(exit_str) if exit_str.isdigit() else None
        return SessionCommandResult(
            cmd_id=command_id,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
        )

    async def session_logs(self, session_id: str) -> SessionCommandResult | None:
        if not _SESSION_ID_RE.match(session_id):
            raise ValueError(f"Invalid session_id: {session_id!r}")
        _SEP = "\\x00"
        latest = _status_sh('"$id"')
        # Exit 3 marks a missing session; the newest .pid names its command.
        # Its exit code is read before its output, as in session_command_logs.
        result = await self.exec(
            f'{self._session_sh}cd "$R/{session_id}" 2>/dev/null || exit 3; '
            "id=$(ls -t -- *.pid 2>/dev/null | head -1); id=${id%.pid}; "
            f"{latest}"
            f"printf '%s{_SEP}' \"$id\"; "
            f"cat \"$id.stdout\" 2>/dev/null; printf '{_SEP}'; "
            f"cat \"$id.stderr\" 2>/dev/null; printf '{_SEP}%s' \"$e\"; true",
            timeout=5,
        )
        if result.exit_code == 3:
            return None
        parts = result.stdout.split("\x00", 3) + ["", "", "", ""]
        exit_str = parts[3].strip()
        return SessionCommandResult(
            cmd_id=parts[0],
            exit_code=int(exit_str) if exit_str.isdigit() else None,
            stdout=parts[1],
            stderr=parts[2],
        )

    async def list_sessions(self) -> list[SessionState]:
        # A session runs while one of its commands has a live wrapper and no
        # .exit, or while it has no .pid at all: a launch is about to use it,
        # so eviction leaves it alone.
        result = await self.exec(
            f'{self._session_sh}cd "$R" 2>/dev/null || exit 0; '
            "for d in */; do [ -d \"$d\" ] || continue; n=0; r=0; "
            "for p in \"$d\"*.pid; do [ -e \"$p\" ] || continue; n=1; "
            "plive \"$p\" && [ ! -e \"${p%.pid}.exit\" ] && r=1; done; "
            "[ $n = 0 ] && r=1; "
            "echo \"${d%/} $r\"; done",
            timeout=10,
        )
        states = []
        for line in result.stdout.splitlines():
            name, _, running = line.rpartition(" ")
            if name:
                states.append(SessionState(session_id=name, running=running == "1"))
        return states

    async def delete_session(self, session_id: str) -> None:
        if not _SESSION_ID_RE.match(session_id):
            raise ValueError(f"Invalid session_id: {session_id!r}")
        # One rename takes the session away before its PIDs are read, so a
        # launch that has not written its .pid yet finds it gone and never
        # runs (session_execute). Hidden, so list_sessions skips it. Exit 3
        # marks a missing session.
        #
        # The whole process group goes, so what the command started goes
        # with it, but only a group proven the command's (``psig``), which a
        # killed wrapper left a zombie still proves.
        result = await self.exec(
            f"{self._session_sh}"
            f'D="$R/{session_id}"; X="$R/.{session_id}.$$"; '
            'mv "$D" "$X" || { [ -e "$D" ] || exit 3; exit 1; }; '
            'for f in "$X"/*.pid; do '
            "  { read -r pid start < \"$f\"; } 2>/dev/null; "
            "  case $pid in \"\"|*[!0-9]*) continue;; esac; "
            '  psig TERM "$pid" "$start" z; '
            'done 2>/dev/null; rm -rf -- "$X"',
            timeout=10,
        )
        if result.exit_code == 3:
            raise FileNotFoundError(f"No session {session_id}")
        # A timeout or a failed exec comes back as -1, and the command may
        # still be running, so only 0 is a stop.
        if result.exit_code != 0:
            detail = (result.stderr or result.stdout).strip()
            raise RuntimeError(
                f"Could not delete session {session_id} (exit {result.exit_code}): {detail}"
            )
