"""The wire protocol of a detached job: the shell scripts and their replies.

Every script here is plain text an exec runs, and every parser reads what one
printed; nothing here waits or talks to a computer, which the package does. A
job keeps its state in a directory of its own, which any worker finds from
its ``JobRef`` alone:

- ``at``: the launch time, in the server's epoch seconds, which the listing
  of jobs that never ended reports.
- ``files``: the log and trace paths, which a later prune removes.
- ``pid``: the wrapper's pid and start time. The wrapper leads the job's
  process group, and checking the start time keeps a reused pid from reading
  as the job.
- ``exit``: the command's exit code, written before the wrapper ends, so a
  wrapper gone with no exit written is a job that vanished.
- ``stop``: a stop reached it. A wrapper that has not started the command yet
  never will.
- ``waited``: when a call last waited on it, in the computer's own epoch
  seconds. Every launch and wait exec writes it, so a job no call has
  waited on for a few chunks has nobody waiting (``WAITER_GONE_S``).
- ``reaped``: a wait returned its output, and this holds the exit code it
  reported (``v`` for a job that vanished). The directory stays as the launch
  guard, so a retried launch reports the same job instead of running it
  again, until a later launch prunes it with its log and trace.

Only the first launch of an id creates the directory (``mkdir``), which is what
makes a retried launch safe.
"""

from __future__ import annotations

import base64
import json
import shlex
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import structlog

from ptc_agent.core.sandbox.providers._proc_sh import EXEC_MARK, PROC_SH, ps_start_sh
from ptc_agent.core.sandbox.runtime import ExecResult

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.jobs import JobRef, JobSpec

logger = structlog.get_logger(__name__)

# A finished job's output returns whole up to this; past it the reply carries
# its end and the log stays for the rest.
OUTPUT_INLINE_MAX = 8 * 1024 * 1024
# A trace this small rides the reply; a larger one costs a read of its own.
_TRACE_INLINE_MAX = 256 * 1024
# Agent code can write to the trace file, so a file far past any real trace is
# skipped rather than read into memory.
_TRACE_READ_MAX = 16 * 1024 * 1024
# The files a launch writes travel inside the script up to this much base64;
# past it they are uploaded first, since a command line has a size limit.
_FILES_INLINE_MAX = 64 * 1024
# How long a reaped or stopped job's directory guards its id before a launch
# prunes it, and how long a finished job nobody reaped keeps its state.
_GUARD_TTL_MIN = 10
_FINISHED_TTL_MIN = 24 * 60
#: The code a shell reports for a SIGKILL: a wrapper that vanished in the wait.
KILLED_EXIT = 137
# What ``reaped`` holds for a job that vanished, in place of an exit code, so
# a later read of it still says vanished rather than a command's own 137.
_VANISHED_MARK = "v"
# Seconds a stop gives the group to end on TERM before it sends KILL.
_STOP_GRACE_TENTHS = 30
# Job directories a launch leaves before it prunes the old ones.
_PRUNE_AT_DIRS = 32
#: A file in the job's state directory a code run writes the module its own
#: unhandled exception failed to import to, read back in a reply section of
#: its own.
RAISED_FILE = "raised"
# The most of it a reply carries: a module name is far shorter.
_RAISED_READ_MAX = 4 * 1024
# Free space under this, in KiB, where a launch could not write reads as a
# full disk.
_DISK_FULL_KIB = 1024

#: A job's status as one reading: ``ended`` with an exit code written,
#: ``vanished`` with its wrapper gone and none written (a kill, or a stop),
#: ``missing`` with no state directory.
Status = Literal["running", "ended", "vanished", "missing"]


# -- Records the replies decode to --------------------------------------------


@dataclass(frozen=True)
class JobState:
    """One read of a job, from anywhere."""

    found: bool
    exit_code: int | None
    vanished: bool
    tail: str
    # A stop reached it. A job a stop ended reads vanished, as one killed any
    # other way does; this tells the two apart. Set as well on one that had
    # ended before the stop came, which ``exit_code`` shows.
    stopped: bool = False

    @property
    def running(self) -> bool:
        """Running, or still launching: found, with no exit and not vanished."""
        return self.found and self.exit_code is None and not self.vanished


@dataclass(frozen=True)
class StoppedJob:
    """What a stop found."""

    was_running: bool
    # An exit was written, or a wait reaped it, before the stop came: the job
    # ended on its own, and a wait reports it as ended.
    had_ended: bool
    mcp_trace: list[dict]


@dataclass(frozen=True)
class UnendedJob:
    """A job with no exit written and not reaped (``list_unended_jobs``)."""

    command_id: str
    # The launch time, in the server's epoch seconds; None if not written.
    launched_at: int | None
    # Seconds since a call last waited on it, on the computer's own clock;
    # None for a job launched before calls stamped their waits.
    waited_ago: int | None


@dataclass(frozen=True)
class WaitReply:
    """What a launch or a wait reported."""

    status: Status | Literal["nolaunch"]
    exit_code: int | None
    log_bytes: int
    trace_bytes: int
    output: str
    trace_text: str
    raised: str | None = None
    # A launch that could not write: whether the disk it writes to is full.
    disk_full: bool = False


@dataclass(frozen=True)
class ForegroundReply:
    """What a foreground run reported: its exit code and output, and whether
    the sandbox ended it at its limit."""

    exit_code: int
    output: str
    cut: bool


class JobReplyError(RuntimeError):
    """A job script's reply was not one it writes: the exec failed around it.

    ``limit`` and ``ran_s`` are that exec's, where the caller set them, so a
    reply a provider cut off at the exec's own limit reads as the timeout it
    was. ``result`` is what the exec returned.
    """

    limit: int | None = None
    ran_s: float | None = None
    result: ExecResult | None = None


# -- Shell --------------------------------------------------------------------

# Every script starts with this: nothing on stderr may reach the reply, which
# several providers merge into stdout, and a zsh, which is what some sandbox
# images run exec with, behaves as sh (it splits ``set -- $s`` only so). The
# options a bash takes from an exported ``SHELLOPTS`` are cleared too:
# noglob empties every ``"$JD"/*/`` loop, errexit and nounset end a script
# halfway, and noclobber refuses the records it rewrites. Every name a script
# reads, it sets first.
_PREAMBLE = 'exec 2>/dev/null\n[ -z "${ZSH_VERSION-}" ] || emulate sh\nset +efuC\n'

# ``js DIR [NEW]`` sets ``s`` to the job's status (running, done, vanished,
# reaped or missing) and ``e`` to its exit code. Liveness is read before the
# exit code, because the wrapper writes the code before it ends; a zombie
# wrapper reads as gone (``plive``). With no pid yet, or an empty record the
# wrapper is still writing, a job is still launching, unless a stop reached it
# first or its directory has sat a minute without one; ``NEW``, from the
# launch that just started it, skips that check.
_STATUS_SH = (
    PROC_SH
    + 'js() { s=missing; e=; [ -d "$1" ] || return 0; '
    'if [ -f "$1/reaped" ]; then s=reaped; { read -r e < "$1/reaped"; } 2>/dev/null; return 0; fi; '
    'local a=1; [ ! -s "$1/pid" ] || plive "$1/pid" || a=0; '
    '[ ! -f "$1/exit" ] || { read -r e < "$1/exit"; } 2>/dev/null; '
    'if [ -n "$e" ]; then s=done; '
    "elif [ $a = 0 ]; then s=vanished; "
    'elif [ -s "$1/pid" ]; then s=running; '
    'elif [ -e "$1/stop" ]; then s=vanished; '
    'elif [ -z "$2" ] && [ -n "$(find "$1" -maxdepth 0 -mmin +1)" ]; then s=vanished; '
    "else s=running; fi; }\n"
)

# ``jw DIR SECONDS [NEW]`` waits while the job runs, at most SECONDS. The first
# checks come quickly, so a command of a few milliseconds is not held to a
# polling interval; after them it checks ten times a second and reads the
# clock once a second.
_WAIT_SH = (
    'jw() { js "$1" "$3"; [ "$s" = running ] && [ "$2" -gt 0 ] || return 0; '
    "local end k t; end=$(( $(date +%s) + $2 )); "
    "for t in 0.001 0.001 0.001 0.002 0.002 0.004 0.008 0.016 0.032 0.064; do sleep $t; "
    'js "$1" "$3"; [ "$s" = running ] || return 0; done; '
    "k=0; while :; do sleep 0.1; "
    'js "$1" "$3"; [ "$s" = running ] || return 0; '
    "k=$((k+1)); [ $k -lt 10 ] && continue; k=0; "
    '[ "$(date +%s)" -lt $end ] || return 0; done; }\n'
)

# ``jr DIR LOG TRACE [NEW]`` reports the job. One that ended prints its output
# and a small trace with it, and its ``raised`` record (``RAISED_FILE``) when
# it wrote one, and is reaped unless its output was cut, in which case the log
# is what the agent reads and the state stays with it. Reaping moves the log
# and trace into the job's directory: out of the agent's way at once, and
# still there for a retried call to report again.
_REPORT_SH = (
    'jr() { js "$1" "$4"; local o t z y; '
    "case $s in done|vanished|reaped) ;; "
    '*) printf "\\n%s H %s %s - -\\n" "$N" "$s" "${e:--}"; return 0;; esac; '
    'o=$2; t=$3; if [ $s = reaped ]; then o=$1/log; t=$1/trace; fi; '
    'z=$(wc -c < "$o"); z=$((z+0)); y=0; [ ! -s "$t" ] || y=$(wc -c < "$t"); y=$((y+0)); '
    'printf "\\n%s H %s %s %s %s\\n" "$N" "$s" "${e:--}" "$z" "$y"; '
    f'[ $z = 0 ] || tail -c {OUTPUT_INLINE_MAX} "$o"; '
    'printf "\\n%s T\\n" "$N"; '
    f'[ $y = 0 ] || [ $y -gt {_TRACE_INLINE_MAX} ] || cat "$t"; '
    f'[ ! -s "$1/raised" ] || {{ printf "\\n%s R\\n" "$N"; tail -c {_RAISED_READ_MAX} "$1/raised"; }}; '
    f"[ $s = reaped ] || [ $z -gt {OUTPUT_INLINE_MAX} ] || "
    f'{{ echo "${{e:-{_VANISHED_MARK}}}" > "$1/reaped"; mv -f "$o" "$1/log"; '
    f'[ $y -gt {_TRACE_INLINE_MAX} ] || [ ! -e "$t" ] || mv -f "$t" "$1/trace"; }}; }}\n'
)

# ``jn`` reports a launch that could write nothing, and ends the script. The
# free space where it writes rides along: a full disk is the usual cause, and
# the caller words its answer by it without asking the computer again.
_NOLAUNCH_SH = (
    'jn() { local d=$JD; while [ -n "$d" ] && [ ! -d "$d" ]; do d=${d%/*}; done; '
    'printf "\\n%s H nolaunch - - -\\n%s F %s\\n" "$N" "$N" '
    "\"$(df -Pk \"${d:-/}\" | awk 'NR==2 {print $4}')\"; exit 0; }\n"
)

# A launch or wait exec stamps the job as waited on, on the computer's clock.
_WAITED_SH = '[ ! -d "$J" ] || date +%s > "$J/waited"\n'

# ``jl DIR`` succeeds while an ended job's process group still has a member
# the job started, such as a server it left running with ``&``. Its record
# stays while one does: the stop of an SSH computer ends what the records name.
_LEFT_SH = (
    'jl() { local g k; { read -r g k < "$1/pid"; } 2>/dev/null || return 1; '
    'case $g in ""|*[!0-9]*) return 1;; esac; '
    'kill -0 "-$g" 2>/dev/null && ours "$g" "$g-$k"; }\n'
)

# ``jp`` prunes the jobs directory ``$JD``: the directory and trace of a job
# reaped or stopped a while ago, and of one that finished a day ago with
# nobody reaping it, unless something it started still runs (``jl``). One
# launched a day ago that wrote none of those (a job that vanished, whose
# discard then failed) goes too once it is not running. A log in a thread's
# folder is the agent's, and stays; one kept beside the state, for a job with
# no thread, goes with it.
_PRUNE_SH = _LEFT_SH + (
    'jp() { find "$JD" -mindepth 2 -maxdepth 2 '
    f"\\( \\( -name reaped -o -name stop \\) -mmin +{_GUARD_TTL_MIN} "
    f"-o \\( -name exit -o -name at \\) -mmin +{_FINISHED_TTL_MIN} \\) -print | "
    'while IFS= read -r f; do d=${f%/*}; [ -d "$d" ] || continue; '
    'case $f in */at) [ ! -e "$d/exit" ] && [ ! -e "$d/reaped" ] && [ ! -e "$d/stop" ] '
    '|| continue; js "$d"; [ "$s" != running ] || continue;; esac; '
    '! jl "$d" || continue; l=; t=; '
    '[ ! -f "$d/files" ] || { read -r l; read -r t; } < "$d/files"; '
    '[ -z "$t" ] || rm -f -- "$t"; '
    'case $l in "$JD"/*) rm -f -- "$l";; esac; rm -rf -- "$d"; done; }\n'
)

# The detached wrapper, run by bash as ``bash -c WRAPPER bash DIR LOG COMMAND``.
# It records itself, then refuses to start once a stop has reached it, so a
# stop that read no pid still holds. The command runs in a subshell, so its
# own ``exit`` still reaches the exit write, with no arguments and none of the
# wrapper's variables. Both records are written in place, one short write
# each: a reader takes an empty pid record for a job still launching, and an
# exit code still being written for one still running.
_WRAPPER_SH = (
    "__la_j=$1 __la_l=$2 __la_c=$3; "
    'if { read -r __la_s < "/proc/$$/stat"; } 2>/dev/null; then '
    "__la_s=${__la_s##*) }; set -- $__la_s; __la_s=${20}; "
    "else __la_s=$(" + ps_start_sh("$$") + "); fi; "
    'echo "$$ $__la_s" > "$__la_j/pid" || exit 1; '
    '[ ! -e "$__la_j/stop" ] || exit 1; '
    f'export {EXEC_MARK}="$$-$__la_s"; '
    '( exec >> "$__la_l" 2>&1; set --; unset __la_j __la_l __la_s; '
    'eval "unset __la_c; $__la_c" ); '
    'echo $? > "$__la_j/exit"'
)

# ``z`` is the trace's size, then its text unless it is past the read cap.
_TRACE_SH = (
    'z=$(wc -c < "$T"); z=$((z+0)); printf "\\n%s T %s\\n" "$N" "$z"\n'
    f'[ $z = 0 ] || [ $z -gt {_TRACE_READ_MAX} ] || cat "$T"'
)


def _q(value: str) -> str:
    return shlex.quote(value)


def _jobs_dir(ref: JobRef) -> str:
    return ref.state_dir.rpartition("/")[0] or "/"


def _paths_sh(ref: JobRef) -> str:
    return (
        f"JD={_q(_jobs_dir(ref))}; J={_q(ref.state_dir)}; "
        f"L={_q(ref.log_path)}; T={_q(ref.trace_path)}\n"
    )


# A foreground run's supervisor, in bash, whose job control gives the command
# a process group of its own with no terminal and no ``setsid`` (macOS has
# none). A provider's exec cut at its limit can leave what it ran running
# (Docker's ends nothing), so the limit is kept here: a watchdog TERMs the
# command's group at it and KILLs what outlives the grace. The command's end
# stands the watchdog down, and the watchdog's exit code says whether it had
# fired, so a command that ended first never reads as cut. ``C`` reports the
# command's exit code and that.
_FOREGROUND_SH = (
    PROC_SH
    + 'set +efuC; N=$1 t=$2 c=$3; set -m; bash -c "$c" 2>&1 & p=$!; set +m\n'
    "( trap 'kill $s; exit 0' TERM; sleep \"$t\" & s=$!; wait $s; trap '' TERM; "
    f'kill -TERM "-$p"; pgrace {_STOP_GRACE_TENTHS} "$p"; exit 1 ) & w=$!\n'
    'wait $p; e=$?; kill -TERM $w; wait $w; x=$?\n'
    'printf "\\n%s C %s %s\\n" "$N" "$e" "$x"\n'
)


def foreground_script(spec: JobSpec, limit: int, nonce: str) -> str:
    """Shell that runs the spec's command in the foreground as its job would
    have, for at most ``limit`` seconds: in bash, with the job's environment
    and its stderr in its output, in whatever shell the computer's exec
    starts (zsh on some images)."""
    return (
        f"{_PREAMBLE}{_exports(spec)}exec bash -c {_q(_FOREGROUND_SH)} bash "
        f"{nonce} {int(limit)} {_q(spec.command)}"
    )


def _exports(spec: JobSpec) -> str:
    """Exports made in the launching subshell, so the wrapper starts with them."""
    words = [_q(f"{k}={v}") for k, v in spec.env.items()]
    if spec.pythonpath:
        words.append(_q(f"PYTHONPATH={spec.pythonpath}") + '"${PYTHONPATH:+:$PYTHONPATH}"')
    return f"export {' '.join(words)}; " if words else ""


def _encoded_size(files: Iterable[tuple[str, bytes]]) -> int:
    return sum((len(content) + 2) // 3 * 4 for _, content in files)


def _write_files_sh(files: Iterable[tuple[str, bytes]]) -> str:
    """Write the launch's files unless its job already exists.

    A retried launch whose first reply was lost finds the job running from
    files already written; writing them again could only fail (on a disk the
    job has since filled) and report a job that runs as never launched, which
    would run its command a second time.
    """
    lines = []
    for path, content in files:
        payload = base64.b64encode(content).decode("ascii")
        tmp = f"{_q(path)}.tmp$$"
        lines.append(
            f"printf %s {_q(payload)} | base64 -d > {tmp} && mv -f {tmp} {_q(path)} "
            '|| [ -d "$J" ] || jn\n'
        )
    if not lines:
        return ""
    return 'if [ ! -d "$J" ]; then\n' + "".join(lines) + "fi\n"


def _start_sh(spec: JobSpec, detach: str) -> str:
    """Shell that starts the wrapper in the background, detached from the exec.

    Paths are literal rather than the launch's variables, since without
    ``setsid`` this runs in a bash of its own.
    """
    ref = spec.ref
    return (
        f"( {_exports(spec)}exec {detach}bash -c {_q(_WRAPPER_SH)} bash "
        f"{_q(ref.state_dir)} {_q(ref.log_path)} {_q(spec.command)} ) "
        "</dev/null >/dev/null 2>&1 &"
    )


def launch_script(
    spec: JobSpec,
    wait_s: int,
    nonce: str,
    *,
    inline_files: bool = True,
    at: int | None = None,
) -> str:
    """Start the job unless its id already ran, wait up to ``wait_s``, report.

    The wrapper is started with ``setsid`` where there is one, so it leads a
    session of its own and outlives the exec with no terminal to hang up.
    macOS has none, and there a bash with job control gives it a group of its
    own, under ``nohup``: bash, because dash turns job control off with no
    terminal. ``at`` is the server's clock, so every duration reads one clock.
    The prune waits until the jobs directory holds enough to be worth a
    ``find``. ``NEW`` says this launch started the job, so its wait never
    takes a wrapper slow to record itself for one that vanished.
    """
    ref = spec.ref
    dirs = [_q(d) for d in dict.fromkeys(spec.dirs)]
    have = "".join(f"[ -d {d} ] && " for d in dirs)
    files = _write_files_sh(spec.files) if inline_files else ""
    stamp = int(time.time()) if at is None else int(at)
    return (
        f"{_PREAMBLE}N={nonce}; NEW=\n{_STATUS_SH}{_WAIT_SH}{_REPORT_SH}{_NOLAUNCH_SH}"
        f"{_PRUNE_SH}{_paths_sh(ref)}"
        f'[ -d "$JD" ] && {have}: || mkdir -p "$JD" {" ".join(dirs)}\n'
        f"{files}"
        f'if mkdir "$J"; then echo {stamp} > "$J/at"; '
        'printf "%s\\n%s\\n" "$L" "$T" > "$J/files"; : > "$L"\n'
        "if command -v setsid >/dev/null 2>&1; then "
        f"{_start_sh(spec, 'setsid ')}\n"
        f"else bash -c {_q('set -m; ' + _start_sh(spec, 'nohup '))}; fi\n"
        f'NEW=1; set -- "$JD"/*/; [ $# -lt {_PRUNE_AT_DIRS} ] || jp\n'
        'elif [ ! -d "$J" ]; then jn\n'
        "fi\n"
        f"{_WAITED_SH}"
        f'jw "$J" {int(wait_s)} "$NEW"; jr "$J" "$L" "$T" "$NEW"\n'
    )


def wait_script(ref: JobRef, wait_s: int, nonce: str) -> str:
    """Wait up to ``wait_s`` more for a job already launched, then report it."""
    return (
        f"{_PREAMBLE}N={nonce}\n{_STATUS_SH}{_WAIT_SH}{_REPORT_SH}{_paths_sh(ref)}"
        f'{_WAITED_SH}jw "$J" {int(wait_s)}; jr "$J" "$L" "$T"\n'
    )


def states_script(refs: Sequence[JobRef], tail_bytes: int, nonce: str) -> str:
    """One record per job: its status, exit code, stop mark and log tail."""
    lines = [
        f"{_PREAMBLE}N={nonce}\n{_STATUS_SH}",
        'jo() { js "$1"; local x=-; [ ! -e "$1/stop" ] || x=stop; '
        'printf "\\n%s S %s %s %s %s\\n" "$N" "$3" "$s" "${e:--}" "$x"; '
        f'tail -c {int(tail_bytes)} "$2"; }}\n',
    ]
    for ref in refs:
        lines.append(
            f"jo {_q(ref.state_dir)} {_q(ref.log_path)} {_q(ref.command_id)}\n"
        )
    return "".join(lines)


def stop_script(
    ref: JobRef, nonce: str, *, discard: bool = False, trace: bool = False
) -> str:
    """Mark the job stopped, then end its process group; report whether it
    ran, and the exit code it had written before the stop (``r`` once reaped).

    The mark goes first, so a wrapper that has not recorded itself yet finds
    it and never starts the command, and a launch still in flight finds the
    id taken. The group is signalled only while it is proven the job's
    (``psig``). ``trace`` prints the trace once the group is ended, as
    ``trace_script`` does, and before ``discard`` removes it; without
    ``discard`` it leaves the file, so the script stays safe to run twice.
    """
    discard_sh = 'rm -f -- "$L" "$T"\n' if discard else ""
    trace_sh = f"{_TRACE_SH}\n" if trace else ""
    return (
        f"{_PREAMBLE}N={nonce}\n{PROC_SH}{_paths_sh(ref)}"
        'mkdir -p "$JD"; mkdir "$J"; : > "$J/stop"; r=0; e=\n'
        '[ ! -f "$J/exit" ] || { read -r e < "$J/exit"; }\n'
        '[ ! -f "$J/reaped" ] || e=r\n'
        'if [ -z "$e" ] && { read -r p t < "$J/pid"; }; then case $p in ""|*[!0-9]*) ;; *)\n'
        f'  if psig TERM "$p" "$t"; then r=1; pgrace {_STOP_GRACE_TENTHS} "$p"; fi;; esac; fi\n'
        'printf "\\n%s X %s %s\\n" "$N" "$r" "${e:--}"\n'
        f"{trace_sh}"
        f"{discard_sh}"
    )


def trace_script(ref: JobRef, nonce: str, *, delete: bool = True) -> str:
    """Print the job's trace, then delete it unless ``delete`` is False."""
    rm = '; rm -f -- "$T"' if delete else ""
    return f"{_PREAMBLE}N={nonce}; T={_q(ref.trace_path)}\n{_TRACE_SH}{rm}\n"


def end_jobs_script(
    jobs_dir: str, command_ids: Sequence[str] | None, nonce: str, *, mark: bool
) -> str:
    """End the named jobs under ``jobs_dir`` that still run, or every one.

    TERM goes to each group at once and KILL to what outlives the grace, so
    many jobs end in one grace, not one each. A group is signalled under the
    same proof as ``stop_script``. ``mark`` writes each job's stop mark
    first; without it a job ended here reads as having vanished, as one a
    computer's stop ended (the SSH stop does the same). A job whose wrapper
    has not recorded itself yet is marked either way, since there is no group
    to signal and the mark is what keeps its command from starting."""
    if command_ids is None:
        dirs = '"$JD"/*/'
    else:
        dirs = " ".join(_q(f"{jobs_dir}/{cid}") for cid in command_ids) or ":"
    mark_sh = ': > "$d/stop"; ' if mark else ""
    return (
        f"{_PREAMBLE}N={nonce}\n{PROC_SH}JD={_q(jobs_dir)}; P=; n=0\n"
        f"for d in {dirs}; do d=${{d%/}}; [ -d \"$d\" ] || continue; {mark_sh}"
        '[ ! -f "$d/exit" ] || continue; '
        'p=; t=; { read -r p t < "$d/pid"; } 2>/dev/null; '
        'case $p in ""|*[!0-9]*) : > "$d/stop"; continue;; esac; '
        'psig TERM "$p" "$t" || continue; P="$P $p"; n=$((n+1)); done\n'
        f"pgrace {_STOP_GRACE_TENTHS} $P\n"
        'printf "\\n%s E %s\\n" "$N" "$n"\n'
    )


def unended_jobs_script(jobs_dir: str, nonce: str) -> str:
    """One record per job under ``jobs_dir`` with no exit written and not
    reaped: its id, launch time and how many seconds ago a call last waited
    on it (``waited``, read against the computer's own clock). What may
    still be running, or was cut off by a stop, whatever its wrapper reads
    as now."""
    return (
        f"{_PREAMBLE}N={nonce}; JD={_q(jobs_dir)}; n=$(date +%s)\n"
        'for d in "$JD"/*/; do d=${d%/}; [ -d "$d" ] || continue; '
        '[ ! -f "$d/exit" ] && [ ! -f "$d/reaped" ] || continue; at=; w=; '
        '[ ! -f "$d/at" ] || { read -r at < "$d/at"; } 2>/dev/null; '
        '[ ! -f "$d/waited" ] || { read -r w < "$d/waited"; } 2>/dev/null; '
        'case $w in ""|*[!0-9]*) w=-;; *) w=$((n - w));; esac; '
        'printf "\\n%s U %s %s %s\\n" "$N" "${d##*/}" "${at:--}" "$w"; done\n'
        'printf "\\n%s D\\n" "$N"\n'
    )


def discard_script(refs: Sequence[JobRef]) -> str:
    """Remove the jobs' traces, and their state unless something they started
    still runs (``jl``), which the prune removes once it ends. Their logs
    stay: they are the agent's."""
    lines = [_PREAMBLE, PROC_SH, _LEFT_SH]
    for ref in refs:
        d = _q(ref.state_dir)
        lines.append(f"rm -f -- {_q(ref.trace_path)}; jl {d} || rm -rf -- {d}\n")
    return "".join(lines)


# -- Replies ------------------------------------------------------------------

Section = tuple[list[str], str]


def _sections(stdout: str, nonce: str) -> list[Section]:
    """The ``(fields, payload)`` of every section a script printed.

    A section starts on a line of its own, with the nonce and its fields; its
    payload runs to the next one. Each header is printed after a newline of its
    own, so a payload keeps exactly the bytes it had.
    """
    parts = stdout.split(f"\n{nonce} ")
    sections = []
    for part in parts[1:]:
        head, _, body = part.partition("\n")
        sections.append((head.split(), body))
    return sections


def _reply(
    result: ExecResult, nonce: str, kind: str, what: str, *, size: int
) -> tuple[list[str], str, list[Section]]:
    """The first ``kind`` section with at least ``size`` fields, its payload,
    and every section the script printed.

    Raises ``JobReplyError`` naming ``what`` when there is none: whatever the
    exec returned, the script did not write it.
    """
    sections = _sections(result.stdout or "", nonce)
    for fields, body in sections:
        if fields[:1] == [kind] and len(fields) >= size:
            return fields, body, sections
    detail = (result.stderr or result.stdout or "").strip()[-500:]
    raise JobReplyError(
        f"{what} did not report (exit {result.exit_code}): {detail or 'no output'}"
    )


def _int(word: str) -> int | None:
    try:
        return int(word)
    except ValueError:
        return None


def _status(word: str, exit_word: str) -> tuple[Status, int | None]:
    """``js``'s status and exit code as one reading. A reaped job reads as
    its wait reported it: its exit code, or ``v`` for one that vanished."""
    if word in ("done", "reaped"):
        code = _int(exit_word)
        return ("ended", code) if code is not None else ("vanished", None)
    if word == "vanished":
        return "vanished", None
    if word == "missing":
        return "missing", None
    return "running", None


def parse_wait_reply(result: ExecResult, nonce: str) -> WaitReply:
    fields, output, sections = _reply(result, nonce, "H", "Job script", size=5)
    _, word, exit_word, log_word, trace_word = fields[:5]
    if word == "nolaunch":
        free = next(
            (_int(f[1]) for f, _ in sections if f[:1] == ["F"] and len(f) > 1), None
        )
        return WaitReply(
            "nolaunch", None, 0, 0, "", "",
            disk_full=free is not None and free < _DISK_FULL_KIB,
        )
    status, exit_code = _status(word, exit_word)
    return WaitReply(
        status=status,
        exit_code=exit_code,
        log_bytes=_int(log_word) or 0,
        trace_bytes=_int(trace_word) or 0,
        output=output,
        trace_text=next((body for f, body in sections if f == ["T"]), ""),
        raised=next((body for f, body in sections if f == ["R"]), None),
    )


def parse_states_reply(result: ExecResult, nonce: str) -> dict[str, JobState]:
    _, _, sections = _reply(result, nonce, "S", "Job state read", size=5)
    states: dict[str, JobState] = {}
    for fields, body in sections:
        if fields[:1] != ["S"] or len(fields) < 5:
            continue
        _, command_id, word, exit_word, mark = fields[:5]
        status, exit_code = _status(word, exit_word)
        states[command_id] = JobState(
            found=status != "missing",
            exit_code=exit_code,
            vanished=status == "vanished",
            tail=body,
            stopped=mark == "stop",
        )
    return states


def parse_stop_reply(result: ExecResult, nonce: str, command_id: str) -> StoppedJob:
    fields, _, sections = _reply(result, nonce, "X", "Job stop", size=3)
    trace = next(
        (_trace(f, body, command_id) for f, body in sections if f[:1] == ["T"] and len(f) > 1),
        [],
    )
    return StoppedJob(was_running=fields[1] == "1", had_ended=fields[2] != "-", mcp_trace=trace)


def parse_trace_reply(result: ExecResult, nonce: str, command_id: str) -> list[dict]:
    fields, body, _ = _reply(result, nonce, "T", "Job trace read", size=2)
    return _trace(fields, body, command_id)


def parse_foreground_reply(result: ExecResult, nonce: str) -> ForegroundReply:
    fields, after, _ = _reply(result, nonce, "C", "Foreground run", size=3)
    # The command's output runs up to the report; what it left running may
    # still have written after it.
    before = (result.stdout or "").split(f"\n{nonce} ", 1)[0]
    code = _int(fields[1])
    return ForegroundReply(
        exit_code=-1 if code is None else code, output=before + after, cut=fields[2] == "1"
    )


def parse_end_reply(result: ExecResult, nonce: str) -> int:
    fields, _, _ = _reply(result, nonce, "E", "Job end", size=2)
    return _int(fields[1]) or 0


def parse_unended_reply(result: ExecResult, nonce: str) -> list[UnendedJob]:
    _, _, sections = _reply(result, nonce, "D", "Job listing", size=1)
    return [
        UnendedJob(fields[1], _int(fields[2]), _int(fields[3]))
        for fields, _ in sections
        if fields[:1] == ["U"] and len(fields) == 4
    ]


def _trace(fields: list[str], body: str, command_id: str) -> list[dict]:
    """The trace a ``T <size>`` section carries, which a trace past the read
    cap leaves empty."""
    size = _int(fields[1]) or 0
    if size > _TRACE_READ_MAX:
        logger.warning(
            "Job trace too large to read; its MCP calls are not recorded",
            command_id=command_id,
            trace_bytes=size,
        )
        return []
    return parse_trace(body)


def parse_trace(text: str) -> list[dict]:
    """One dict per JSON line; a partial or foreign line is skipped."""
    records: list[dict] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if isinstance(entry, dict):
            records.append(entry)
    return records
