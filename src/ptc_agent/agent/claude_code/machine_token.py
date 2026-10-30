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
(``machine_token_state``) and the shim a turn starts the CLI through
(``token_shim.pl``, run by ``SSHLauncher``) read the file by it.
"""

from __future__ import annotations

import asyncio
import functools
import re
import shlex
import uuid
from pathlib import Path
from typing import Literal

from ptc_agent.core.paths import CLAUDE_CODE_TOKEN_DIR, LANGALPHA_CONFIG_DIR
from ptc_agent.core.sandbox.providers._ssh_transport import Link
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
NO_PERL = "Claude Code on this computer needs perl, which was not found on its PATH"
#: Finds perl as ``$p``, or says it is missing and exits 127.
FIND_PERL = f"p=$(command -v perl) || {{ echo {shlex.quote(NO_PERL)} >&2; exit 127; }}; "
#: A staged token left this long belongs to a check that died with its connection.
STAGED_STALE_MINUTES = 15
_SCRIPT_S = 30
_STAGED = re.compile(r"\.staged\.[0-9a-f-]{36}\.[A-Za-z0-9]{6,}")

TokenFile = Literal["file", "invalid", "none"]


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


async def _run_stream(link: Link, script: str, data: bytes = b"") -> tuple[int | None, str, str]:
    """Run a one-line script with ``data`` after it on stdin, on the control channel."""
    async with link.in_use(), asyncio.timeout(_SCRIPT_S):
        process = await link.open_process(STREAM_BOOT, slot=link.control_slot)
        try:
            process.stdin.write(script.encode() + b"\n" + data)
            process.stdin.write_eof()
            out, err = await asyncio.gather(process.stdout.read(), process.stderr.read())
            await process.wait_closed()
        finally:
            process.close()
    status = process.exit_status if process.exit_signal is None else None
    return status, out.decode(errors="replace"), err.decode(errors="replace").strip()


async def has_perl(link: Link) -> bool:
    """Whether the machine has the perl the shim and the status check run in."""
    status, _, _ = await _run_stream(link, f"{FIND_PERL}exit 0")
    return status == 0


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
    if status != 0 or not _STAGED.fullmatch(name):
        raise RuntimeError(f"Could not save the token on {link.target.host}: {err or f'exit {status}'}")
    return name


async def promote_machine_token(link: Link, computer_id: str, staged: str) -> None:
    """Rename the staged token over the computer's, once its check passed. A
    token path that is a symlink, not a file, or not the login's own is refused."""
    script = (
        f"{TOKEN_DIR_SH}f=\"$d/{token_name(computer_id)}\"; s=\"$d/{checked_name(staged)}\"; "
        'if [ -L "$s" ] || [ ! -f "$s" ] || [ ! -O "$s" ]; then echo "the token staged in $d is gone" >&2; exit 3; fi; '
        'if [ -L "$f" ] || { [ -e "$f" ] && { [ ! -f "$f" ] || [ ! -O "$f" ]; }; }; then '
        'echo "$f is a symbolic link, not a file, or not yours, so it was left as it is" >&2; exit 3; fi; '
        'mv -f -- "$s" "$f"'
    )
    status, _, err = await _run_stream(link, script)
    if status != 0:
        raise RuntimeError(f"Could not save the token on {link.target.host}: {err or f'exit {status}'}")


async def discard_staged_token(link: Link, staged: str) -> None:
    """Remove a staged token: the computer's own is never touched."""
    status, _, err = await _run_stream(link, f'{TOKEN_DIR_SH}rm -f -- "$d/{checked_name(staged)}"')
    if status != 0:
        raise RuntimeError(f"Could not remove a staged token on {link.target.host}: {err or f'exit {status}'}")


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
    if status != 0:
        raise RuntimeError(f"Could not remove the token on {link.target.host}: {err or f'exit {status}'}")
    return out.strip() == "1"


async def machine_token_state(link: Link, computer_id: str) -> TokenFile:
    """Whether the machine holds a token for the computer that a turn would
    use, by the rules the shim reads it by; the content never leaves the
    machine.

    ``invalid`` is a file the shim ignores: a symlink, not a regular file, not
    the login's own, open to group or others, or holding nothing but space. A
    machine with no perl has no token a turn could use, so it reads ``none``.
    The computer's staged tokens a lost connection left behind are swept on the
    way, so call it only under the computer's change lock.
    """
    name = token_name(computer_id)
    script = (
        f"{TOKEN_DIR_SH}find \"$d\" -maxdepth 1 -type f -name '.staged.{name}.*' -mmin +{STAGED_STALE_MINUTES} "
        "-exec rm -f {} + 2>/dev/null; "
        f'p=$(command -v perl) || {{ echo none; exit 0; }}; '
        f'exec "$p" -e {word(state_script())} "$d/{name}"'
    )
    status, out, err = await _run_stream(link, script)
    state = out.strip()
    if status != 0 or state not in ("file", "invalid", "none"):
        raise RuntimeError(f"Could not read the token's state on {link.target.host}: {err or f'exit {status}'}")
    return state  # type: ignore[return-value]
