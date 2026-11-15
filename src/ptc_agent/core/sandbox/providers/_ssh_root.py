"""A computer's root folder on its machine: checking a folder as one, taking
it, and the words for a folder refused.

Kept apart from the rest of the machine's shell because each check is one
script whose exits the server words (``root_dir_error``), and the two have to
be read together.
"""

from __future__ import annotations

import shlex
from collections.abc import Iterable

from ptc_agent.core.paths import SandboxLayout, name_fold
from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError

# A file, not ``.langalpha``: that is the runtime's folder in the home, and a
# home holding it must not read as some computer's root.
ROOT_MARKER = ".langalpha-root"


# One claim of a root at a time on the login, from the look at the folder to
# the mark: two starts that both found it free would each mark it, and an
# uninstall taking a root's folders out holds it too, so no claim lands
# between its look at the mark and what it deletes. Held in ``$c``, the
# computers folder. A claim takes moments, so a hold older than that was cut
# off; one still held after the wait is exit 15, inside the command's timeout.
# The hold is a file the shell creates exclusively (noclobber), since a uutils
# mkdir reports success for a folder another run made at the same moment; an
# older script's hold is a folder.
CLAIM_HOLD = (
    'h="$c/.claiming"; n=0; mkdir -p -- "$c" || exit 9\n'
    'until (set -C && : > "$h") 2>/dev/null; do\n'
    '  n=$((n + 1)); [ "$n" -le 20 ] || exit 15\n'
    '  if [ -n "$(find "$h" -prune -mmin +2 2>/dev/null)" ]; then rm -f -- "$h" 2>/dev/null || rmdir -- "$h" 2>/dev/null; else sleep 1; fi\n'
    "done\n"
    "trap 'rm -f -- \"$h\"' EXIT; trap 'exit 1' HUP INT TERM\n"
)


def claim_root_script(
    path: str,
    computer_id: str | None,
    *,
    computers: str,
    cleared: str,
    forget: Iterable[str] = (),
) -> str:
    """Mark *path* as *computer_id*'s root, once *cleared*, the computer that
    marked it, was cleared from it.

    The cleared computer's mark would otherwise stay until a start marks the
    folder, and that computer's own uninstall empties a folder its mark names.
    *forget* are paths under each folder in the root that only vouch for what
    the cleared computer laid there, such as a restore's marker, removed so
    they cannot stand for this computer's files.

    Under the claim hold starts take in *computers*, and only over the cleared
    computer's mark (or none, or the taker's own): a start that took the
    folder since it was cleared is refused with exit 10 and its id printed,
    not overwritten. Any other exit is one ``root_dir_error`` words.
    *computer_id* None, a computer with no id on the machine yet, forgets
    without marking: its first start marks the folder, which the cleared
    computer's mark no longer keeps from it.
    """
    q = shlex.quote
    lines = [
        f"r={q(path.rstrip('/'))}; c={q(computers)}",
        '[ -d "$r" ] && [ ! -L "$r" ] || exit 0',
        CLAIM_HOLD.rstrip("\n"),
        f'm="$r"/{q(ROOT_MARKER)}; o=; {{ read -r o < "$m"; }} 2>/dev/null',
        f'[ -z "$o" ] || [ "$o" = {q(cleared)} ] || [ "$o" = {q(computer_id or "")} ] '
        '|| { printf \'%s\\n\' "$o"; exit 10; }',
    ]
    names = [n for n in forget if n]
    if names:
        targets = " ".join(f'"$d"/{q(n)}' for n in names)
        lines.append(
            'for d in "$r"/*; do [ -d "$d" ] && [ ! -L "$d" ] || continue; '
            f"rm -f -- {targets}; done"
        )
    if computer_id:
        lines.append(f'printf %s {q(computer_id)} > "$m" || exit 9')
    return "\n".join(lines) + "\n"


def root_dir_script(path: str, *, computers: str, computer_id: str | None) -> str:
    """Exit 0 when *path* is, or can become, this computer's own root folder.

    A computer deletes and moves folders by name under its root, so it takes
    only a root no one else's files are in: a missing folder, which it makes
    (with passwordless sudo where the user cannot), an empty one, or one a
    computer marked as its root, and never one every account may write in,
    such as ``/tmp``, where anyone can plant entries the computer would then
    treat as its own, or one named through a symbolic link, whose files the
    server would read as outside it. A name in other letter case or Unicode
    normalization, which a Mac's disk takes for the same folder, exits 12 as
    well, and the caller checks the disk's spelling instead (``adopted_root``).
    The folder must be the login's own, and taking it leaves only the login
    able to write in it and in the workspace folders at its top: another
    account that could would swap in a folder of its own, whose
    ``.agents/tools`` the agent imports. Nothing deeper is touched, since
    the folders below are the owner's. ``computer_id`` None checks, as the
    probe does; given, the folder is made and marked, and ``_internal``,
    which holds the computer's credentials, kept to the user. A start also
    keeps a root without the mark when the computer's own folder under
    *computers* exists, so a lost mark does not lock a computer out of its
    files. A root marked by another computer that still exists is refused
    with exit 10, and the id the mark holds printed; exit 12 prints the
    folder it checked and the real path it has (``adopted_root``). Claims on
    one login run one at a time and make the claiming computer's folder under
    *computers*, so two starts at once cannot both take a folder; exit 15
    is one that waited too long for its turn. Exit 16 is a folder macOS's
    privacy controls keep the login from listing, which would otherwise read
    as empty. A check refuses a new folder under Desktop, Documents or
    Downloads with exit 16 even when the login may list it, since iCloud can
    sync Desktop and Documents with no sign in the path, and one in iCloud
    Drive with exit 17: iCloud would upload the credentials the computer
    keeps there. A computer already working in one keeps starting. Any other
    exit is one ``root_dir_error`` words.
    """
    i = SandboxLayout.INTERNAL_DIR
    if computer_id is None:
        make, sudo, claim, hold = "exit 0", "sudo -n true", "exit 0", ""
        # Compared in lower case: a Mac's disk takes any case for these, and a
        # folder not made yet has no spelling of its own to compare.
        synced = (
            '[ "$(uname -s)" = Darwin ] || return 0; '
            "f=$(printf '%s/' \"$1\" | LC_ALL=C tr A-Z a-z); "
            "h=$(printf '%s/' \"$hd\" | LC_ALL=C tr A-Z a-z); "
            'case "$f" in "${h}library/mobile documents/"*) exit 17 ;; '
            '"${h}desktop/"* | "${h}documents/"* | "${h}downloads/"*) exit 16 ;; esac'
        )
    else:
        make = 'mkdir -p -- "$w" 2>/dev/null && claim'
        sudo = 'sudo -n install -d -o "$(id -un)" -g "$(id -gn)" -- "$w"'
        claim = "claim"
        hold = CLAIM_HOLD
        synced = ":"
    return (
        f"w={shlex.quote(path)}; c={shlex.quote(computers)}; "
        f'me={shlex.quote(computer_id or "")}; m="$w/{ROOT_MARKER}"; o=\n'
        # What it makes, the folder's parents included, only the login writes.
        "umask go-w\n"
        f"{hold}"
        "claim() {\n"
        '  [ -n "$o" ] && [ "$o" = "$me" ] || printf \'%s\\n\' "$me" > "$m" || exit 9\n'
        '  chmod go-w "$w" 2>/dev/null\n'
        '  [ -z "$(find "$w" -prune \\( -perm -0020 -o -perm -0002 \\) 2>/dev/null)" ] || exit 14\n'
        '  for x in "$w"/* "$w"/.[!.]* "$w"/..?*; do\n'
        '    [ -d "$x" ] && [ ! -L "$x" ] || continue\n'
        '    if [ -O "$x" ]; then chmod go-w "$x" 2>/dev/null\n'
        '    elif [ -z "$(find "$x" -prune -user 0 2>/dev/null)" ]; then exit 13; fi\n'
        "  done\n"
        f'  mkdir -p -- "$w/{i}" && chmod 700 "$w/{i}" || exit 9\n'
        # The mark keeps another computer out only once this one's folder
        # exists, so it is made before the next claim may look, and last, so
        # a claim refused above leaves none to lock the retry out.
        '  mkdir -p -- "$c/$me" && chmod 700 "$c/$me" || exit 9\n'
        "  exit 0\n"
        "}\n"
        'hd=$(cd -P -- "$HOME" 2>/dev/null && env pwd -P)\n'
        'real() { r=$(cd -P -- "$1" 2>/dev/null && env pwd -P); '
        '[ "$r" = "$1" ] && return; '
        "printf '%s\\n%s\\n' \"$1\" \"$r\"; exit 12; }\n"
        # Only a check refuses a folder macOS keeps private or iCloud syncs: a
        # computer already working in one keeps starting.
        f"synced() {{ {synced} ; }}\n"
        # The privacy controls deny with EPERM, where file modes deny with
        # EACCES, and may deny a folder whose modes let the login in.
        'tcc() { case $(LC_ALL=C ls -A -- "$1" 2>&1 >/dev/null) in '
        '*"not permitted"*) exit 16 ;; esac; }\n'
        # Where the folder is, asked of it or of the parent it would be made in.
        'place() { real "$1"; synced "$1"; tcc "$1"; }\n'
        'if [ -d "$w" ]; then\n'
        '  place "$w"\n'
        '  [ -r "$w" ] && [ -w "$w" ] && [ -x "$w" ] || exit 4\n'
        '  [ -z "$(find "$w" -prune -perm -0002 2>/dev/null)" ] || exit 11\n'
        '  [ -O "$w" ] || exit 13\n'
        '  if [ -e "$m" ]; then\n'
        '    { read -r o < "$m"; } 2>/dev/null\n'
        '    if [ -n "$o" ] && [ "$o" != "$me" ] && [ -d "$c/$o" ]; then\n'
        "      printf '%s\\n' \"$o\"; exit 10\n"
        "    fi\n"
        # A listing that fails reads as one with files, never as empty.
        '  elif [ -n "$({ ls -A -- "$w" || echo /; } 2>/dev/null | head -n 1)" ]; then\n'
        '    [ -n "$me" ] && [ -d "$c/$me" ] || exit 8\n'
        "  fi\n"
        f"  {claim}\n"
        "fi\n"
        '[ -e "$w" ] && exit 6\n'
        'p=$w; while [ ! -e "$p" ]; do p=$(dirname -- "$p"); done\n'
        '[ -d "$p" ] || exit 7\n'
        'place "$p"\n'
        # Asked of the folder itself too: a missing Documents or iCloud folder
        # leaves its nearest parent, which is outside it.
        'synced "$w"\n'
        f'if [ -w "$p" ] && [ -x "$p" ]; then {make}; fi\n'
        f"e=$(LC_ALL=C {sudo} </dev/null 2>&1 >/dev/null) && {claim}\n"
        'case "$e" in *tty*) exit 5 ;; esac\n'
        "exit 3\n"
    )


def adopted_root(path: str, stdout: str) -> str | None:
    """*path* spelled as the disk has it, from ``root_dir_script``'s exit 12,
    when the two differ only in letter case or Unicode normalization, else
    None, which leaves the exit refusing it as a symbolic link.

    A case-insensitive disk (a Mac's, by default) takes either spelling for
    the same folder, but the server compares paths as strings, and what
    commands there print is the disk's. A link whose target differs from its
    name only so is adopted as its target, which is then checked like any
    other folder.
    """
    lines = stdout.splitlines()
    if len(lines) < 2:
        return None
    checked, real = lines[-2], lines[-1]
    if (
        not real
        or real == checked
        or "\ufffd" in real
        or not path.startswith(checked)
        or name_fold(real) != name_fold(checked)
    ):
        return None
    return real + path[len(checked) :]


# What each exit of ``root_dir_script`` (and ``claim_root_script``) says
# about the folder, then what to choose instead, filled in with the path, the
# login user and the host.
_OWNED = "Choose a folder {user} owns."
_NEW = "Choose a new or empty folder."
_ROOT_DIR_EXITS: dict[int | None, tuple[str, str]] = {
    4: ("{path} exists on {host}, but {user} cannot read and write in it.", _OWNED),
    5: (
        "Could not create {path} on {host}: {user} cannot write to its parent, "
        "and sudo for {user} requires a terminal (requiretty in sudoers).",
        _OWNED,
    ),
    6: ("{path} on {host} is a file, not a folder.", _OWNED),
    7: ("Could not create {path} on {host}: part of that path is a file.", _OWNED),
    8: ("{path} on {host} already holds files.", _NEW),
    9: ("Could not set up {path} on {host} for {user}.", _OWNED),
    10: ("{path} on {host} is already another computer's folder.", _NEW),
    11: ("{path} on {host} is open to every account on the machine.", _OWNED),
    12: (
        "{path} on {host} is reached through a symbolic link.",
        "Enter the folder by its real path.",
    ),
    13: (
        "{path} on {host}, or a folder at its top, belongs to another account.",
        _OWNED,
    ),
    14: (
        "Could not make {path} on {host} writable by {user} alone.",
        "Choose a folder on a disk that keeps file permissions.",
    ),
    16: (
        "{path} on {host} is in a folder macOS keeps private.",
        "Choose a folder outside Desktop, Documents, Downloads and iCloud Drive.",
    ),
    17: (
        "{path} on {host} is in iCloud Drive, which would upload the credentials "
        "the computer keeps in its folder.",
        "Choose a folder outside iCloud Drive.",
    ),
}
# Every other exit: the parent cannot be written, by the login or by sudo.
_NO_SUDO = (
    "Could not create {path} on {host}: {user} cannot write to its parent and "
    "has no passwordless sudo.",
    _OWNED,
)


def root_dir_error(
    exit_code: int | None, *, path: str, user: str, host: str
) -> SSHConnectError | None:
    """What ``root_dir_script``'s exit says about the folder, None when it will do."""
    if exit_code == 0:
        return None
    message, choose = _ROOT_DIR_EXITS.get(exit_code, _NO_SUDO)
    words = {"path": path, "user": user, "host": host}
    return SSHConnectError(
        "root_dir", f"{message.format(**words)} {choose.format(**words)}"
    )


def folder_marked_message(path: str, host: str) -> str:
    """The refusal of a folder another computer marked. The client offers the
    take-over itself, so a build without the offer reads it as whole."""
    return (
        f"{path} on {host} is the folder of a LangAlpha computer this account "
        "does not have, perhaps one from another account or an earlier "
        "install. Choose a new or empty folder."
    )
