"""The shell an SSH computer runs on its machine, as text.

Kept apart from the channels that carry it so every script can be read whole,
and run against a local bash, without a connection. Each one reaches bash on
stdin (``BOOT``) and assumes nothing beyond it: a Mac's bash 3.2 with no /proc
reads the same text as Linux, branching where the two differ.
"""

from __future__ import annotations

import re
import shlex
from collections.abc import Iterable, Mapping

from ptc_agent.core.paths import (
    EXEC_SCRIPT_PREFIX,
    LEFTOVERS_TRASH_PREFIX,
    SandboxLayout,
    WorkspaceLayout,
)
from ptc_agent.core.sandbox.providers import _ssh_bootstrap as bootstrap
from ptc_agent.core.sandbox.providers._proc_sh import EXEC_MARK, PROC_SH, ps_start_sh
from ptc_agent.core.sandbox.providers._ssh_root import CLAIM_HOLD, ROOT_MARKER
from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError

# The login shell parses only this line. Its quoted part holds no ' or \, which
# every shell takes literally, and the script it evals arrives on stdin.
BOOT = "exec bash --noprofile --norc -c 's=$(cat); exec </dev/null; eval \"$s\"'"

# What a framed script reports on its own streams around the command's output:
# where the output begins (stdout), the group it runs in, or that the computer
# is stopped (stderr).
FRAME_MARK = b"__langalpha__ "
FRAME_BEGIN = FRAME_MARK + b"begin\n"
FRAME_PGID = FRAME_MARK + b"pgid "
FRAME_STOPPED = FRAME_MARK + b"stopped"

# After a TERM, how long a group has to exit before it gets a KILL.
TERM_GRACE_S = 3

# The last field is where the machine sees the connection come from, the
# address sshd matched the key's from= against.
HELLO = (
    'printf "%s\\n" "$(id -u)" "$(id -un)" "$HOME" "$(uname -s)" "$(uname -m)" '
    '"${SSH_CONNECTION%% *}" "${XDG_CONFIG_HOME:-}"'
)
SUDO_CHECK = "LC_ALL=C sudo -n true </dev/null 2>&1 >/dev/null\n"

_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def check_env(env: Mapping[str, str] | None) -> dict[str, str]:
    env = dict(env or {})
    bad = [k for k in env if not _ENV_NAME.match(k)]
    if bad:
        raise ValueError(f"Invalid environment variable name: {bad[0]!r}")
    return env


def _proc_or_ps(linux: str, other: str) -> str:
    """Shell that defines *linux* where /proc can be read, else *other* (macOS)."""
    return f"if [ -d /proc/1 ]; then\n{linux}else\n{other}fi\n"


# -- Boot key and state ---------------------------------------------------------

# Sets ``__la_k`` to a key that changes whenever every process on the machine
# was ended: the kernel's boot id, which a reboot changes, and PID 1's start
# time, which a container's restart changes while the boot id stays (a VPS
# that is an LXC container). Without fork, since every command reads it. A
# machine with no /proc (macOS) runs no containers, so its boot session id
# serves alone, at the cost of one fork. The paren in the pattern is escaped:
# bash 3.2 (macOS) reads a bare one as the end of a $(...) around this.
BOOT_KEY_SH = (
    "__la_k=; if { read -r __la_k < /proc/sys/kernel/random/boot_id; } 2>/dev/null; then "
    "__la_s=; { read -r __la_s < /proc/1/stat; } 2>/dev/null; "
    '__la_s=${__la_s##*\\) }; set -- $__la_s; __la_k="$__la_k-${20}"; set --; '
    "else __la_k=$(sysctl -n kern.bootsessionuuid 2>/dev/null); fi; "
)


def state_sh(computer_dir_sh: str) -> str:
    """Shell that sets ``__la_up`` to 1 when the computer is marked running on
    this boot, else 0. A ``running`` written before a reboot is stale."""
    return (
        BOOT_KEY_SH
        + "__la_st=; __la_sk=; "
        + f'{{ read -r __la_st __la_sk < "{computer_dir_sh}/state"; }} 2>/dev/null; '
        + "__la_up=0; "
        + '[ "$__la_st" = running ] && [ "$__la_sk" = "$__la_k" ] && __la_up=1; '
    )


# Letters, digits and these cover the home folders systems make.
_HOME = re.compile(r"/[\w.@+/-]*")


def home_problem(home: str) -> str | None:
    """Why a computer cannot live under *home*, a sentence for its owner; None
    when it can.

    A home is joined into PATH, written into scripts and read back one per
    line, so a space, colon, quote or glob in it breaks one of them, and a
    path that splits on a space can point a removal outside the computer.
    """
    if _HOME.fullmatch(home):
        return None
    if not home.startswith("/"):
        return f"The login's home folder ({home or 'none'}) is not an absolute path."
    return (
        f"The home folder {home!r} holds whitespace or a character shells treat "
        "specially, which is not supported."
    )


_COMPUTERS = f"{bootstrap.STATE_DIR}/computers"


def computers_dir(home: str) -> str:
    """Where the machine keeps its computers' folders, in the login's home.

    Every path a computer uses there is built from this, so a home that
    :func:`home_problem` refuses stops here, whichever call reads it first.

    Raises:
        SSHConnectError: ``unsupported``, with that sentence.
    """
    problem = home_problem(home)
    if problem is not None:
        raise SSHConnectError("unsupported", problem)
    return f"{home}/{_COMPUTERS}"


def computer_dir(home: str, sandbox_id: str) -> str:
    return f"{computers_dir(home)}/{sandbox_id}"


def home_shared_script(sandbox_ids: Iterable[str]) -> str:
    """Prints ``shared <id>`` and exits when the login's home holds the folder
    of one of *sandbox_ids*; empty for none.

    A computer whose folder is there logs in through this home's
    ``authorized_keys`` as well, whether it reaches this machine or another
    that mounts the same home, so a script after the check that would take
    its key away must not run.
    """
    return "".join(
        f'[ -d "$HOME"/{shlex.quote(f"{_COMPUTERS}/{i}")} ] '
        f"&& {{ echo shared {shlex.quote(i)}; exit 0; }}\n"
        for i in sandbox_ids
    )


# A tunnel's folder in ``~/.langalpha`` holds this, which takes the folder and
# the tunnel away again; the runtime's uninstall leaves such a folder to it.
TUNNEL_UNINSTALL_SCRIPT = "uninstall.sh"

# What LangAlpha makes at the top of a root besides workspace folders, the
# internal folder aside: what it holds is the owner's in part.
_ROOT_SETUP = (
    SandboxLayout.AGENTS_DIR,
    SandboxLayout.MCP_SERVERS_DIR,
    SandboxLayout.SYSTEM_DIR,
)
# Made at the root by every start, empty unless the agent wrote there: it goes
# only empty.
_ROOT_DATA = WorkspaceLayout.DATA_DIR


def uninstall_script(root: str, sandbox_id: str) -> str:
    """Deletes what LangAlpha set up on the machine for a removed computer,
    its owner having asked: its folders in *root*, the root once nothing
    else is in it, and the login's runtime once no computer is left on it.

    Run after the computer's own folder under ``~/.langalpha`` is gone. Only a
    root marked as this computer's is touched, any other kept as it is, and in
    it only names LangAlpha makes: files the owner put there stay, and the folders the root folder
    script set aside for them (theirs that stood in a workspace folder's way,
    a deleted workspace's they kept) go back to the root's top. One that
    cannot, its name taken there, keeps the leftovers folder and so the root.
    A root kept keeps its mark too, naming a computer that is gone, which
    lets the next computer claim the folder as LangAlpha's rather than refuse
    it as the owner's. The root is read and emptied under the claims' hold,
    so a computer added there meanwhile is not emptied with it.
    The runtime stays for any other computer whose folder is on the login, of
    this deployment or another, for a root inside it, and while an install
    holds its lock, which the uninstall takes too. Everything else in ``~/.langalpha`` goes,
    including what the agent put there itself (a browser it downloaded), but a
    tunnel's folder, which stays for its own uninstall. A ``python3.X`` link
    an older install left in ``~/.local/bin`` goes with the runtime it points
    into. Both are moved out of their names, then deleted detached, so a
    runtime of a few GB does not hold the removal up. Prints ``root kept`` and
    ``runtime kept`` for what stays, and exits 1 when anything it set out to
    delete or put back did not go.
    """
    q = shlex.quote
    state = f'"$HOME"/{q(bootstrap.STATE_DIR)}'
    setup = " ".join(q(n) for n in _ROOT_SETUP)
    trash_prefix = q(LEFTOVERS_TRASH_PREFIX)
    return (
        f"r={q(root.rstrip('/'))}; me={q(sandbox_id)}\n"
        f's={state}; c="$HOME"/{q(_COMPUTERS)}\n'
        f'i="$r"/{q(SandboxLayout.INTERNAL_DIR)}; l="$r"/{q(SandboxLayout.LEFTOVERS_DIR)}\n'
        f'm="$r"/{q(ROOT_MARKER)}; f=0\n'
        # A trash beside the folder it empties, on its disk, so moving into
        # it is a rename; deleted in place where none can be made.
        "trash() {\n"
        '  t=$(mktemp -d "$1/.langalpha-removing.XXXXXX" 2>/dev/null) || t=\n'
        "}\n"
        "drop() {\n"
        '  [ -e "$1" ] || [ -L "$1" ] || return 0\n'
        '  if [ -n "$t" ]; then mv -- "$1" "$t"/ 2>/dev/null && return 0; fi\n'
        '  rm -rf -- "$1" && return 0\n'
        "  f=1; return 1\n"
        "}\n"
        "gone() {\n"
        '  [ -n "$t" ] && nohup rm -rf -- "$t" >/dev/null 2>&1 &\n'
        "}\n"
        f"{bootstrap.LOCK_SH}"
        f"{CLAIM_HOLD}"
        'o=; { read -r o < "$m"; } 2>/dev/null\n'
        'if [ -d "$r" ] && [ ! -L "$r" ] && [ -n "$o" ] && [ "$o" = "$me" ]; then\n'
        # A root that is a disk of its own empties into a trash on that disk,
        # since one beside it would make every move a copy; such a root can
        # never go itself, so it is kept. A flag says so, since the detached
        # delete may have taken its trash before the root is read. Its mount
        # point ends df's line, spaces and all.
        '  d=; case "$(df -P -- "$r" 2>/dev/null | sed -n 2p)" in\n'
        '    *" $r") d=1; trash "$r" ;;\n'
        '    *) trash "$(dirname -- "$r")" ;;\n'
        "  esac\n"
        "  k=0\n"
        '  for x in "$l"/* "$l"/.[!.]* "$l"/..?*; do\n'
        '    [ -e "$x" ] || [ -L "$x" ] || continue\n'
        '    b=${x##*/}\n'
        # A removal's trash, deleting itself, and a copy aside cut short,
        # whose folder is still where it was.
        f'    case "$b" in {trash_prefix}*) continue ;; *.copying) drop "$x"; continue ;; esac\n'
        '    if [ -e "$r/$b" ] || [ -L "$r/$b" ]; then k=1\n'
        '    elif ! mv -- "$x" "$r/$b"; then k=1; f=1; fi\n'
        "  done\n"
        f'  for n in {setup}; do drop "$r/$n"; done\n'
        '  if [ "$k" = 0 ]; then drop "$i"; else\n'
        '    for x in "$i"/* "$i"/.[!.]* "$i"/..?*; do\n'
        '      [ -e "$x" ] || [ -L "$x" ] || continue\n'
        '      [ "$x" = "$l" ] || drop "$x"\n'
        "    done\n"
        "  fi\n"
        f'  for x in "$r"/{q(EXEC_SCRIPT_PREFIX)}*.py; do drop "$x"; done\n'
        f'  rmdir -- "$r"/{q(_ROOT_DATA)} 2>/dev/null\n'
        "  gone\n"
        # Finder's view settings, left in any folder a Mac has shown, are
        # no file of the owner's.
        f'  if [ -n "$d" ] || [ -n "$(ls -A -- "$r" | grep -vxF -e {q(ROOT_MARKER)} -e .DS_Store | head -n 1)" ]; then echo root kept\n'
        '  else rm -f -- "$m" "$r"/.DS_Store; rmdir -- "$r" 2>/dev/null || echo root kept; fi\n'
        # Not this computer's (its mark gone, or another's): left as it is.
        'elif [ -e "$r" ] || [ -L "$r" ]; then echo root kept\n'
        "fi\n"
        'trap - EXIT; rm -f -- "$h"\n'
        'if [ -d "$s" ] && [ ! -L "$s" ]; then\n'
        # The install's lock, so none starts, or is part done, while the
        # runtime goes; under it, a ``.install.*`` left is a dead install's.
        f'  u=; {{ exec 9>>"$s"/{q(bootstrap.LOCK_FILE)}; }} 2>/dev/null && lock 5 || u=install\n'
        # A root in the runtime's folder is the owner's to keep.
        '  [ -n "$u" ] || case "$r/" in "$s"/*) u=root ;; esac\n'
        '  [ -n "$u" ] || u=$(ls -A -- "$c" 2>/dev/null | grep -vxF -- "$me" | head -n 1)\n'
        '  if [ -n "$u" ]; then echo runtime kept; else\n'
        '    trash "$HOME"\n'
        '    for x in "$HOME"/.local/bin/python3*; do\n'
        '      [ -L "$x" ] || continue\n'
        '      case "$(readlink -- "$x")" in "$s"/python/*) rm -f -- "$x" || f=1 ;; esac\n'
        "    done\n"
        '    for x in "$s"/* "$s"/.[!.]* "$s"/..?*; do\n'
        '      [ -e "$x" ] || [ -L "$x" ] || continue\n'
        f'      case "$x" in "$c"|"$s"/{q(bootstrap.LOCK_FILE)}) continue ;; esac\n'
        f'      [ -f "$x"/{q(TUNNEL_UNINSTALL_SCRIPT)} ] || drop "$x"\n'
        "    done\n"
        # A computer whose folder arrived since the read keeps its folder,
        # and the lock its install waits on.
        '    drop "$c/$me"; rmdir -- "$c" 2>/dev/null\n'
        f'    [ -e "$c" ] || rm -f -- "$s"/{q(bootstrap.LOCK_FILE)}\n'
        "    gone\n"
        '    rmdir -- "$s" 2>/dev/null\n'
        "  fi\n"
        "fi\n"
        '[ "$f" = 0 ]\n'
    )


def read_state_script(cdir: str) -> str:
    """Prints 1 when the computer is marked running on this boot, else 0."""
    return f"d={shlex.quote(cdir)}; {state_sh('$d')}echo $__la_up\n"


def layout_script(cdir: str) -> str:
    """Makes the computer's folder; prints ``has-env`` when its env file is
    already there."""
    d = shlex.quote(cdir)
    return (
        f"mkdir -p {d}/execs {d}/sessions && chmod 700 {d} && "
        f"{{ [ -e {d}/env.sh ] && echo has-env; true; }}\n"
    )


def _write_state(state: str, on_failure: str) -> str:
    """Shell that replaces ``$d/state`` with *state* in one rename.

    The file it renames in is this script's own: a start and a stop, or two
    workers' starts, that shared one would rename each other's away, failing
    a ``mv`` or leaving ``running`` after a stop.
    """
    return (
        f'echo {state} > "$d/state.new.$$" && mv -f "$d/state.new.$$" "$d/state" '
        f'|| {{ rm -f "$d/state.new.$$"; {on_failure}; }}\n'
    )


def mark_running_script(cdir: str) -> str:
    """Marks the computer running on this boot; prints ``rebooted`` when its
    records came from an earlier one, which it drops with their sessions.

    A state with no key (``stopped``, or none yet) is not a reboot: the root
    records a stop without sudo leaves still name live processes.
    """
    return (
        f"d={shlex.quote(cdir)}\n"
        + BOOT_KEY_SH
        + '__la_sk=; { read -r __la_st __la_sk < "$d/state"; } 2>/dev/null\n'
        + 'if [ -n "$__la_sk" ] && [ "$__la_sk" != "$__la_k" ]; then\n'
        + '  rm -f "$d"/execs/*\n'
        + '  for r in "$d"/sessions/run-*; do\n'
        + '    [ "$r" = "$d/sessions/run-$__la_k" ] || rm -rf "$r"\n'
        + "  done\n"
        + "  echo rebooted\n"
        + "fi\n"
        + 'mkdir -p "$d/sessions/run-$__la_k"\n'
        + _write_state('"running $__la_k"', "exit 1")
    )


def delete_script(cdir: str, *, root: str | None = None) -> str:
    """Removes the computer's own folder and, given its *root*, the
    credentials the server put there for its processes. Every other file in
    the root is the owner's and stays."""
    script = f"rm -rf -- {shlex.quote(cdir)}\n"
    if root is not None:
        layout = SandboxLayout(root)
        held = " ".join(shlex.quote(p) for p in layout.credential_files)
        checks = shlex.quote(layout.join(SandboxLayout.MCP_CHECK_PREFIX))
        script += f"rm -f -- {held} {checks}*\n"
    return script


# -- Env ------------------------------------------------------------------------

# Set by the runtime, never by the computer's env, so they always point at
# what the bootstrap installed and where the root keeps its files.
_RUNTIME_OWNED_ENV = frozenset(
    {
        "PATH",
        "VIRTUAL_ENV",
        "PLAYWRIGHT_BROWSERS_PATH",
        "UV_PYTHON_INSTALL_DIR",
        "UV_CACHE_DIR",
        "MPLCONFIGDIR",
        "GINLIX_TOKEN_FILE",
    }
)


def env_file(env: Mapping[str, str], home: str, root: str) -> str:
    """The computer's ``env.sh``: its own env, then what the runtime owns.

    The data token's path comes from the root, not from ``env.json``, so a
    start after that file was lost still points the data client at it.
    """
    p = bootstrap.paths(home)
    q = shlex.quote
    lines = ["# Written by langalpha; rewritten whenever this computer's env changes."]
    for key in sorted(env):
        if key not in _RUNTIME_OWNED_ENV:
            lines.append(f"export {key}={q(env[key])}")
    base_path = q(env["PATH"]) if env.get("PATH") else '"$PATH"'
    lines += [
        f"export VIRTUAL_ENV={q(p.venv)}",
        f"export PATH={q(p.venv_bin)}:{q(p.bin)}:{base_path}",
        f"export PLAYWRIGHT_BROWSERS_PATH={q(p.browsers)}",
        f"export UV_PYTHON_INSTALL_DIR={q(p.python)}",
        f"export UV_CACHE_DIR={q(p.uv_cache)}",
        # Where the install leaves the font cache a Mac needs (see bootstrap).
        f"export MPLCONFIGDIR={q(p.matplotlib)}",
        f"export GINLIX_TOKEN_FILE={q(SandboxLayout(root).mcp_tokens)}",
        ': "${LANG:=C.UTF-8}"; export LANG',
    ]
    if "NODE_PATH" in env:
        # Points into the sandbox image's global npm tree, which a machine
        # without that layout does not have.
        lines.append('[ -d "$NODE_PATH" ] || unset NODE_PATH')
    return "\n".join(lines) + "\n"


# -- The root folder (claimed and checked in ``_ssh_root``) --------------------


def knows_computer_script(home: str, computer_id: str, root: str | None) -> str:
    """Prints 1 when the machine holds *computer_id*: its own folder in the
    login's home, which every start keeps, or its mark on *root*.

    Either is only ever written by that computer on that machine, so finding
    one tells a machine paired again from a different one; a host key does
    not, since reinstalling the system makes a new one.
    """
    q = shlex.quote
    lines = [f"[ -d {q(computer_dir(home, computer_id))} ] && {{ echo 1; exit 0; }}"]
    if root:
        lines.append(
            f"o=; {{ read -r o < {q(root.rstrip('/') + '/' + ROOT_MARKER)}; }} 2>/dev/null; "
            f'[ "$o" = {q(computer_id)} ] && echo 1'
        )
    return "\n".join(lines) + "\nexit 0\n"


# -- Process groups and sessions ------------------------------------------------

# A command's group is not all it started: ``timeout``, and any job control,
# give a child a group of its own in the same session, so ending a command
# ends its session too. ``la_sid PGID`` prints the session a live group is in;
# ``la_sk SIG SID`` signals every process in it but its leader, the shell that
# started the command, which ends once the command does. It never signals
# this script's own session, nor session 0 (kernel threads), and fails when
# nothing was there to signal.
#
# macOS has no /proc, and ps reports every session as 0, so there ``la_sid``
# prints the group's members and their descendants instead, as one word, and
# ``la_sk`` signals those. A process that left the tree, by its parent ending
# before the list was taken, is out of reach there.
_SESSION_LINUX_SH = (
    "la_sid() { local g=$1 f l; for f in /proc/[0-9]*/stat; do "
    '{ read -r l < "$f"; } 2>/dev/null || continue; l=${l##*) }; set -- $l; '
    'if [ "$3" = "$g" ]; then echo "$4"; return 0; fi; done; return 1; }\n'
    "la_sk() { local sig=$1 s=$2 me f l p n=1; "
    "{ read -r l < /proc/$$/stat; } 2>/dev/null; l=${l##*) }; set -- $l; me=$4; "
    'case $s in ""|0|"$me") return 1;; esac; '
    "for f in /proc/[0-9]*/stat; do "
    '{ read -r l < "$f"; } 2>/dev/null || continue; l=${l##*) }; set -- $l; '
    '[ "$4" = "$s" ] || continue; p=${f#/proc/}; p=${p%/stat}; '
    '[ "$p" = "$s" ] && continue; kill -"$sig" "$p" 2>/dev/null && n=0; done; '
    "return $n; }\n"
)
_SESSION_PS_SH = (
    'la_sid() { local g=$1 t a=" " m=1 p q r; t=$(ps -axo pid=,ppid=,pgid=) || return 1\n'
    'while read -r p q r; do [ "$r" = "$g" ] && a="$a$p "; done <<< "$t"\n'
    "while [ $m = 1 ]; do m=0; while read -r p q r; do "
    'case $a in *" $p "*) ;; *" $q "*) a="$a$p "; m=1;; esac; done <<< "$t"; done\n'
    '[ "$a" != " " ] || return 1; set -- $a; local IFS=,; echo "d$*"; }\n'
    "la_sk() { local sig=$1 p n=1; case $2 in d?*) ;; *) return 1;; esac\n"
    'local IFS=,; for p in ${2#d}; do kill -"$sig" "$p" 2>/dev/null && n=0; done; return $n; }\n'
)
SESSION_SH = _proc_or_ps(_SESSION_LINUX_SH, _SESSION_PS_SH)


# Field 22 of /proc/<pid>/stat is the start time, counted after the command
# name, which may hold spaces. A group's leader writes its own pid and start
# time, so a stop never signals a pid that has since gone to someone else,
# and exports them as the mark that proves a group it left is its own.
# macOS's bash 3.2 has no $BASHPID, so the leader learns its pid as the parent
# of a process it execs, and with no /proc its start time is ps's, as
# ``pstart`` reads it (``_proc_sh``).
_PIDFILE_SH = (
    "__la_me=${BASHPID:-$(exec sh -c 'echo $PPID')}; "
    'if { read -r __la_s < "/proc/$__la_me/stat"; } 2>/dev/null; then '
    "__la_s=${__la_s##*) }; set -- $__la_s; __la_s=${20}; "
    f"else __la_s=$({ps_start_sh(chr(34) + '$__la_me' + chr(34))}); fi; "
    'echo "$__la_me $__la_s" > "$__la_dir/execs/$__la_pre$__la_me"; '
    f'export {EXEC_MARK}="$__la_me-$__la_s"; '
)


def framed_script(
    command: str,
    *,
    computer_dir: str,
    cwd: str | None,
    env: Mapping[str, str] | None = None,
    computer: bool = True,
    merge: bool = True,
    root: bool = False,
) -> str:
    """The script that runs *command* as its own process group on the machine.

    It refuses a computer marked stopped, or marked running before the
    machine last rebooted, sources the computer's env file,
    exports the per-call env, and records the group under ``execs/`` until
    nothing in it is left. On stderr it reports the group's id before the
    command writes anything, which is how a timeout knows what to kill.
    ``computer`` False skips the state and the env file, for the install,
    which runs before either exists. ``merge`` sends the command's stderr to
    stdout, in the order written.
    """
    lines = [
        f"__la_dir={shlex.quote(computer_dir)}",
        # Workspace folders, their .agents/tools and the runtime the install
        # lays out come from commands, and on a server whose accounts share a
        # group, one it could write in would let another account plant code
        # the agent imports.
        "umask go-w",
    ]
    if computer:
        lines.append(
            state_sh("$__la_dir")
            + f'[ $__la_up = 1 ] || {{ echo "{FRAME_STOPPED.decode()}" >&2; exit 0; }}'
        )
        lines.append('[ ! -r "$__la_dir/env.sh" ] || . "$__la_dir/env.sh"')
    for key, value in check_env(env).items():
        lines.append(f"export {key}={shlex.quote(value)}")
    lines += [
        f"__la_c={shlex.quote(command)}",
        f"__la_w={shlex.quote(cwd or '')}",
        f"__la_pre={'r' if root else ''}",
        # Job control has the shell print a notice such as "[1]+ Done" when
        # the group ends quickly, so the shell's own stderr goes nowhere and
        # only the group's reaches the output.
        "exec 3>&2 2>/dev/null",
        f"echo {FRAME_BEGIN.decode().strip()}",
        "set -m",
        "( exec 2>&3; "
        + _PIDFILE_SH
        + f'echo "{FRAME_PGID.decode()}$__la_me" >&3; '
        + ("exec 2>&1 3>&-; " if merge else "exec 3>&-; ")
        + 'if [ -n "$__la_w" ]; then cd -- "$__la_w" || exit 1; fi; '
        "set --; __c=$__la_c; "
        "unset __la_c __la_w __la_s __la_st __la_sk __la_k __la_up __la_pre __la_me; "
        'eval "$__c" ) &',
        "__la_p=$!",
        "set +m",
        'wait "$__la_p"; __la_rc=$?',
        'kill -0 -- "-$__la_p" 2>/dev/null || rm -f "$__la_dir/execs/$__la_pre$__la_p"',
        'exit "$__la_rc"',
    ]
    return "\n".join(lines) + "\n"


def kill_group_script(computer_dir: str, pgid: int, *, root: bool) -> str:
    """TERM the group and its session, KILL whatever outlives the grace, forget
    its record."""
    pre = "r" if root else ""
    return SESSION_SH + (
        f"p={pgid}; f={shlex.quote(computer_dir)}/execs/{pre}{pgid}; "
        's=$(la_sid "$p"); '
        'if kill -TERM -- "-$p" 2>/dev/null; then la_sk TERM "$s"; i=0; '
        'while { kill -0 -- "-$p" 2>/dev/null || la_sk 0 "$s"; } '
        f"&& [ $i -lt {TERM_GRACE_S * 10} ]; "
        "do sleep 0.1; i=$((i+1)); done; "
        'kill -KILL -- "-$p" 2>/dev/null; la_sk KILL "$s"; fi; rm -f "$f"; exit 0\n'
    )


# -- Stopping -------------------------------------------------------------------

# ``la_sup ROOT`` prints the login's processes running the MCP supervisor of
# the computer rooted at ROOT, found by the arguments every launch gives it
# (``python3 -m supervisor ROOT``). The generated client starts one in a
# session of its own when none answers, so no record names it. Linux reads
# argv from /proc, exactly; macOS has no /proc, and there it is the tail of
# ps's argument column.
_SUPERVISOR_LINUX_SH = r"""la_sup() {
  local f p x a w
  w=$'\n'-m$'\n'supervisor$'\n'"$1"$'\n'
  for f in /proc/[0-9]*/cmdline; do
    p=${f%/cmdline}; [ -O "$p" ] || continue
    a=$'\n'; while IFS= read -r -d '' x; do a="$a$x"$'\n'; done < "$f"
    case $a in *"$w") echo "${p#/proc/}";; esac
  done 2>/dev/null
}
"""
_SUPERVISOR_PS_SH = r"""la_sup() {
  local u p pu a
  u=$(id -u)
  ps -axww -o pid=,uid=,args= 2>/dev/null | while read -r p pu a; do
    [ "$pu" = "$u" ] || continue
    case $a in *" -m supervisor $1") echo "$p";; esac
  done
}
"""
_SUPERVISOR_SH = _proc_or_ps(_SUPERVISOR_LINUX_SH, _SUPERVISOR_PS_SH)


def stop_script(cdir: str, *, root: bool, computer_root: str = "") -> str:
    """Mark the computer stopped and end every process group it left.

    A group is signalled only while it is proven the record's (``psig``):
    its leader is the process recorded, a zombie one included, or a member
    carries the command's mark. Records from before a reboot name nothing alive, and their ids may
    be anyone's now, so they are dropped unsignalled. ``root`` ends only the
    records ``exec_as_root`` left, and prints nothing; otherwise ``has-root``
    says such records remain. Given ``computer_root``, the computer's MCP
    supervisor and the servers in its session end too, however it was
    started, and so do its detached jobs (``sandbox/jobs``), which leave the
    session of the exec that started them. A job's record stays, so it reads
    as having vanished.
    """
    supervisor = ""
    if root:
        records = '"$d"/execs/r[0-9]*'
        cleanup = 'rm -f "$d"/execs/r[0-9]*'
        mark = ""
    else:
        records = '"$d"/sessions/run-*/*/*.pid "$d"/execs/[0-9]*'
        cleanup = 'rm -rf "$d"/sessions/run-*; rm -f "$d"/execs/[0-9]*'
        if computer_root:
            records += f" {shlex.quote(computer_root)}/{SandboxLayout.SYSTEM_JOBS_DIR}/*/pid"
            supervisor = (
                f"  for pid in $(la_sup {shlex.quote(computer_root)}); do\n"
                '    S="$S $(la_sid "$pid")"; kill -"$1" -- "-$pid"; kill -"$1" "$pid"; n=1\n'
                "  done 2>/dev/null\n"
            )
        # A state with no key was not marked on any boot, so its records are
        # not stale and are signalled as usual.
        mark = (
            BOOT_KEY_SH
            + '__la_sk=; { read -r __la_st __la_sk < "$d/state"; } 2>/dev/null\n'
            + _write_state("stopped", "true")
            + 'if [ -n "$__la_sk" ] && [ "$__la_sk" != "$__la_k" ]; then\n'
            + '  rm -rf "$d"/sessions/run-*; rm -f "$d"/execs/*; exit 0\n'
            + "fi\n"
        )
    return f"""d={shlex.quote(cdir)}
[ -d "$d" ] || exit 0
{mark}{PROC_SH}{SESSION_SH}{_SUPERVISOR_SH if supervisor else ""}S= P=
sig() {{
  n=0
  for f in {records}; do
    [ -e "$f" ] || continue
    {{ read -r pid start < "$f"; }} 2>/dev/null
    case $pid in ""|*[!0-9]*) continue;; esac
    # Proven on the TERM round and trusted on the KILL round a second later,
    # while its pid names no process, not even a zombie: what outlived the
    # TERM may carry no mark of its own. A group's session is read before the
    # signal, while the group still has it.
    if case " $P " in *" $pid "*) [ -z "$(pstart "$pid" z)" ];; *) false;; esac; then
      kill -"$1" "-$pid" && n=1
    else
      s=$(la_sid "$pid"); if psig "$1" "$pid" "$start" z; then P="$P $pid"; S="$S $s"; n=1; fi
    fi
  done 2>/dev/null
{supervisor}  for s in $S; do la_sk "$1" "$s" && n=1; done
  [ $n = 1 ]
}}
if sig TERM; then sleep 1; sig KILL; fi
{cleanup}
for f in "$d"/execs/r[0-9]*; do [ -e "$f" ] && {{ echo has-root; break; }}; done
exit 0
"""
