"""The setup tokens LangAlpha keeps on a user's own computer, as the backend reaches them.

Each computer has its own token file on the machine, named by its computer id
in LangAlpha's folder of the login's config home, so two computers on one
login never share one. The backend stages a token there in a file of its own
(from a sign-in or a paste), checks it there, and only then renames it over
the computer's file, so a token Claude refuses never touches the one in use;
it removes the file, and asks whether a usable one is there, but never reads
it back. Every script runs on the computer's pooled SSH connection, the token
travelling on the exec channel's stdin only. What makes a token file usable is
written once, in perl (``token_file.pl``), and both the status check
(``machine_state``) and the shim a turn starts the CLI through
(``token_shim.pl``, run by ``SSHLauncher``) read the file by it.
"""

from __future__ import annotations

import asyncio
import functools
import re
import shlex
import uuid
from pathlib import Path
from typing import Any, Literal, NamedTuple, cast, get_args

from ptc_agent.core.claude_code_managed import claude_code_managed_sh, parse_managed
from ptc_agent.core.paths import CLAUDE_CODE_TOKEN_DIR, LANGALPHA_CONFIG_DIR
from ptc_agent.core.sandbox.providers._ssh_transport import Link
from ptc_agent.core.sandbox.providers.ssh import IS_MUSL_SH
from src.llms.claude_code import is_token_text

#: Run by the login shell: reads one line of stdin as a script and runs it,
#: leaving the rest of stdin to what it execs (bash reads a pipe a byte at a
#: time). No backslash in it, so fish and csh parse it as sh does.
STREAM_BOOT = "exec bash --noprofile --norc -c 'IFS= read -r s; eval \"$s\"'"
#: Sets ``$c`` to the login's config home, ``$XDG_CONFIG_HOME`` only when it is
#: absolute (the XDG spec ignores a relative one), and ``$d`` to the folder of
#: the tokens in it, as ``paths`` places them. Every script starts with it.
TOKEN_DIR_SH = (
    'c=${XDG_CONFIG_HOME:-}; case "$c" in /*) ;; *) c="$HOME/.config" ;; esac; '
    f'd="$c/{CLAUDE_CODE_TOKEN_DIR}"; '
)
#: Prints the tokens' folder ``$d`` on a line of its own, ``~`` for the login's home.
FOLDER_SH = 's=$d; h=${HOME%/}; [ -n "$h" ] && case "$d" in "$h"/*) s="~/${d#"$h"/}" ;; esac; printf "%s\\n" "$s"; '
NO_PERL = "Claude Code on this computer needs perl, which was not found on its PATH"
#: Prints 1 if anything is at the token's path ``$f``, a file the shim
#: ignores included, else 0. Needs no perl.
TOKEN_THERE_SH = 'if [ -e "$f" ] || [ -L "$f" ]; then echo 1; else echo 0; fi; '
#: Finds perl as ``$p``, or says it is missing and exits 127.
FIND_PERL = f"p=$(command -v perl) || {{ echo {shlex.quote(NO_PERL)} >&2; exit 127; }}; "
#: A staged token left this long belongs to a check that died with its connection.
STAGED_STALE_MINUTES = 15
_SCRIPT_S = 30
#: What a token script may print on each stream: a name, a state, a line of
#: error. A login whose shell prints more is not one these scripts can read.
OUTPUT_CAP = 4096
_STAGED = re.compile(r"\.staged\.[0-9a-f-]{36}\.[A-Za-z0-9]{6,}")

TokenFile = Literal["file", "invalid", "none", "policy", "no_perl", "unsupported"]


class MachineState(NamedTuple):
    """What the status check finds on the machine: the token file, as the shim
    would read it, unless Claude Code cannot run there at all. ``present``
    says whether anything is at the token's path, which ``policy``,
    ``no_perl`` and ``unsupported`` cannot tell; ``folder`` is the
    tokens' folder, with ``~`` for the login's home."""

    file: TokenFile
    present: bool = False
    folder: str | None = None


def token_name(computer_id: str) -> str:
    """The computer's token file's name in the tokens' folder: its id, which must be a UUID."""
    return str(uuid.UUID(str(computer_id)))


def checked_name(name: str) -> str:
    """``name`` if it names a token file in the folder, a computer's or a staged one; raises otherwise."""
    if _STAGED.fullmatch(name):
        return name
    if token_name(name) != name:
        raise ValueError(f"{name!r} is not a token file's name")
    return name


@functools.cache
def _perl(name: str) -> str:
    return Path(__file__).with_name(name).read_text()


def shim() -> str:
    """The perl that starts the CLI with the turn's token (``token_shim.pl``)."""
    return _perl("token_file.pl") + _perl("token_shim.pl")


def state_script() -> str:
    """The perl that prints what ``token_file`` makes of the token file it is given, never its content."""
    return _perl("token_file.pl") + "my ($how) = token_file(shift); print $how;\n"


def word(s: str) -> str:
    """``s`` as one bash word on one line: a script must fit the line ``STREAM_BOOT`` reads."""
    if "\n" not in s and "\r" not in s:
        return shlex.quote(s)
    escaped = s.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "\\r")
    return f"$'{escaped}'"


async def _capped(link: Link, stream: Any) -> bytes:
    """``stream`` to its end, failing once it passes ``OUTPUT_CAP``."""
    out = bytearray()
    while chunk := await stream.read(OUTPUT_CAP):
        out += chunk
        if len(out) > OUTPUT_CAP:
            raise RuntimeError(f"{link.target.host} printed more than {OUTPUT_CAP} bytes in reply to a token script")
    return bytes(out)


async def _run_stream(link: Link, script: str, data: bytes = b"") -> tuple[int | None, str, str]:
    """Run a one-line script with ``data`` after it on stdin, on the control channel."""
    async with link.in_use(), asyncio.timeout(_SCRIPT_S):
        process = await link.open_process(STREAM_BOOT, slot=link.control_slot)
        reads = [asyncio.ensure_future(_capped(link, s)) for s in (process.stdout, process.stderr)]
        try:
            process.stdin.write(script.encode() + b"\n" + data)
            process.stdin.write_eof()
            out, err = await asyncio.gather(*reads)
            await process.wait_closed()
        finally:
            for read in reads:
                read.cancel()
            process.close()
    status = process.exit_status if process.exit_signal is None else None
    return status, out.decode(errors="replace"), err.decode(errors="replace").strip()


def _check(ok: bool, link: Link, doing: str, status: int | None, err: str) -> None:
    """Raise unless ``ok``, naming what could not be done there in the machine's own words."""
    if not ok:
        raise RuntimeError(f"Could not {doing} on {link.target.host}: {err or f'exit {status}'}")


async def stage_machine_token(link: Link, computer_id: str, token: str) -> str:
    """Write ``token`` to a staged file of its own beside the computer's, for a
    check to sign in with; the staged file's name.

    The token travels on the exec channel's stdin only, into a file made by
    mktemp under umask 077. A config folder that is not the login's own is
    refused. Staged files a lost connection left behind are swept on the way.

    Raises:
        ValueError: ``token`` is not one line of text, or ``computer_id`` not a UUID.
        RuntimeError: The machine refused or failed the write; the text holds no token.
    """
    if not is_token_text(token):
        raise ValueError("a setup token is one line of text with no spaces")
    script = (
        f'umask 077; {TOKEN_DIR_SH}mkdir -p "$d" || exit 1; '
        f'for x in "$c/{LANGALPHA_CONFIG_DIR}" "$d"; do '
        'if [ -L "$x" ] || [ ! -d "$x" ] || [ ! -O "$x" ]; then echo "$x is not a folder you own" >&2; exit 3; fi; '
        'chmod 700 "$x" || exit 1; done; '
        f"find \"$d\" -maxdepth 1 -type f -name '.staged.*' -mmin +{STAGED_STALE_MINUTES} -exec rm -f {{}} + 2>/dev/null; "
        f't=$(mktemp "$d/.staged.{token_name(computer_id)}.XXXXXX") || exit 1; '
        'if cat > "$t" && [ -s "$t" ] && chmod 600 "$t"; then printf "%s\\n" "${t##*/}"; exit 0; fi; rm -f "$t"; exit 1'
    )
    status, out, err = await _run_stream(link, script, token.encode() + b"\n")
    name = out.strip()
    _check(status == 0 and _STAGED.fullmatch(name) is not None, link, "save the token", status, err)
    return name


async def promote_machine_token(link: Link, computer_id: str, staged: str) -> str:
    """Rename the staged token over the computer's, once its check passed; the
    tokens' folder, as ``MachineState.folder`` spells it. A token path that is
    a symlink or not a file is refused, as a removal refuses it; a file of
    another owner's is replaced, as a removal would take it."""
    script = (
        f"{TOKEN_DIR_SH}f=\"$d/{token_name(computer_id)}\"; s=\"$d/{checked_name(staged)}\"; "
        'if [ -L "$s" ] || [ ! -f "$s" ] || [ ! -O "$s" ]; then echo "the token staged in $d is gone" >&2; exit 3; fi; '
        'if [ -L "$f" ] || { [ -e "$f" ] && [ ! -f "$f" ]; }; then '
        'echo "$f is a symbolic link or not a file, so it was left as it is" >&2; exit 3; fi; '
        f'mv -f -- "$s" "$f" || exit 1; {FOLDER_SH}'
    )
    status, out, err = await _run_stream(link, script)
    _check(status == 0, link, "save the token", status, err)
    return out.strip()


async def discard_staged_token(link: Link, staged: str) -> None:
    """Remove a staged token: the computer's own is never touched."""
    status, _, err = await _run_stream(link, f'{TOKEN_DIR_SH}rm -f -- "$d/{checked_name(staged)}"')
    _check(status == 0, link, "remove a staged token", status, err)


async def remove_machine_token(link: Link, computer_id: str) -> bool:
    """Delete the computer's token and any staged for it; whether a token was
    there. A symlink in the token's place is refused, neither followed nor removed."""
    name = token_name(computer_id)
    script = (
        f'{TOKEN_DIR_SH}f="$d/{name}"; rm -f -- "$d/.staged.{name}."* 2>/dev/null; '
        'if [ -L "$f" ]; then echo "$f is a symbolic link, so it was left as it is" >&2; exit 3; fi; '
        '[ -e "$f" ] || { echo 0; exit 0; }; '
        '[ -f "$f" ] || { echo "$f is not a file, so it was left as it is" >&2; exit 3; }; '
        'rm -f -- "$f" && echo 1'
    )
    status, out, err = await _run_stream(link, script)
    _check(status == 0, link, "remove the token", status, err)
    return out.strip() == "1"


async def machine_state(link: Link, computer_id: str) -> MachineState:
    """Whether the machine holds a token for the computer that a turn would
    use, by the rules the shim reads it by; its content never leaves the machine.

    ``invalid`` is a file the shim ignores: a symlink, not a regular file, not
    the login's own, open to group or others, or holding nothing but space.
    ``policy`` is a machine an organization's managed Claude Code settings
    are on (``claude_code_managed_sh``), whatever else holds there; else
    ``unsupported`` one on musl, where no build of the CLI runs, told before
    ``no_perl``, one with no perl, where the shim cannot start the CLI with
    any token, its own or a relayed one. ``present`` still says whether a
    token file is there, as found without perl.
    The computer's staged tokens a lost connection left behind are swept on the
    way, so call it only under the computer's change lock.
    """
    name = token_name(computer_id)
    script = (
        f"{TOKEN_DIR_SH}find \"$d\" -maxdepth 1 -type f -name '.staged.{name}.*' -mmin +{STAGED_STALE_MINUTES} "
        "-exec rm -f {} + 2>/dev/null; "
        f'{claude_code_managed_sh()}printf "%s\\n" "${{cm# }}"; '
        f'f="$d/{name}"; {TOKEN_THERE_SH}'
        f'{FOLDER_SH}{IS_MUSL_SH.strip()}; if [ "$(uname -s)" = Linux ] && is_musl; then echo unsupported; exit 0; fi; '
        'p=$(command -v perl) || { echo no_perl; exit 0; }; '
        f'exec "$p" -e {word(state_script())} "$f"'
    )
    status, out, err = await _run_stream(link, script)
    managed, _, rest = out.partition("\n")
    present, _, rest = rest.partition("\n")
    folder, _, found = rest.rstrip("\n").rpartition("\n")
    ok = status == 0 and present in ("0", "1") and found in get_args(TokenFile)
    _check(ok, link, "read the token's state", status, err)
    return MachineState("policy" if parse_managed(managed) else cast(TokenFile, found), present == "1", folder)
