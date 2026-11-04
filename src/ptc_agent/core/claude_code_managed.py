"""An organization's managed Claude Code settings on a computer, by name.

LangAlpha does not run Claude Code where an organization manages it: the CLI
would follow that organization's settings, not LangAlpha's. So the backend
reads which managed sources are present (never their content) for Settings,
the pairing probe, admission and a turn's start, on the user's computers and
on the backend's own machine alike, and refuses Claude Code wherever any is.
"""

from __future__ import annotations

import asyncio
import shlex

#: Where an organization's managed Claude Code settings sit, as Claude Code
#: reads them: a folder by system, and on a Mac a configuration profile too.
CLAUDE_CODE_MANAGED_DIR_LINUX: str = "/etc/claude-code"
CLAUDE_CODE_MANAGED_DIR_MAC: str = "/Library/Application Support/ClaudeCode"
MAC_MANAGED_PREFERENCES_DIR: str = "/Library/Managed Preferences"
MAC_PROFILE: str = "com.anthropic.claudecode.plist"
#: The managed sources by the names a machine reports them: managed-settings.json,
#: any drop-in in managed-settings.d, managed-mcp.json, and a profile.
CLAUDE_CODE_MANAGED: tuple[str, ...] = ("settings", "dropins", "mcp", "profile")


def parse_managed(text: str) -> tuple[str, ...]:
    """The managed sources a machine named, in ``CLAUDE_CODE_MANAGED``'s order.
    A name not known here is dropped, so a newer probe naming a new source
    never fails a check."""
    names = text.split()
    return tuple(n for n in CLAUDE_CODE_MANAGED if n in names)


def claude_code_managed_sh() -> str:
    """One line of bash setting ``$cm`` to the managed sources on the
    machine, each name after a space. Names only: nothing is read, and only
    ``[ -e ]`` and a glob run, which a Mac's bash 3.2 and BusyBox share."""
    linux, mac, prefs = (
        shlex.quote(p) for p in (CLAUDE_CODE_MANAGED_DIR_LINUX, CLAUDE_CODE_MANAGED_DIR_MAC, MAC_MANAGED_PREFERENCES_DIR)
    )
    return (
        f'cs=$(uname -s); ce={linux}; [ "$cs" = Darwin ] && ce={mac}; cm=""; '
        '[ -e "$ce/managed-settings.json" ] && cm="$cm settings"; '
        'for cf in "$ce"/managed-settings.d/*.json; do { [ -f "$cf" ] || [ -L "$cf" ]; } && { cm="$cm dropins"; break; }; done; '
        '[ -e "$ce/managed-mcp.json" ] && cm="$cm mcp"; '
        f'if [ "$cs" = Darwin ]; then for cf in {prefs}/{MAC_PROFILE} '
        f'{prefs}/"$(id -un)"/{MAC_PROFILE}; do '
        '[ -e "$cf" ] && { cm="$cm profile"; break; }; done; fi; '
    )


async def local_managed() -> tuple[str, ...]:
    """The managed sources on this machine, found by ``claude_code_managed_sh``
    as on any other: for the CLI the backend runs itself."""
    process = await asyncio.create_subprocess_exec(
        "/bin/sh", "-c", claude_code_managed_sh() + 'printf %s "$cm"',
        stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
    )
    out, _ = await process.communicate()
    return parse_managed(out.decode(errors="replace"))
