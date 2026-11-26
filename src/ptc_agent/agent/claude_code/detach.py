"""The protocol of the supervisor that keeps a Claude Code turn's CLI
running apart from the channel that started it (``detach.pl``), as
``SSHTransport`` speaks it, and the dial a lost connection retries.

The supervisor holds the CLI's stdout and stderr in files in the turn's
config dir and feeds its stdin from whichever channel is attached. A channel
attached prints ``langalpha-attach N|done|gone`` first on stdout, then the
CLI's stdout from the byte it was asked for; on stderr ``__langalpha__
attach``, then the CLI's stderr from the line it was asked for, with
``__langalpha__ in N`` lines between as the supervisor takes input, and,
once the CLI has ended, ``__langalpha__ exit x N|s N|g`` last. A channel
that closes without that line lost its connection.
"""

from __future__ import annotations

import asyncio
import functools
import re
from pathlib import Path

from claude_agent_sdk import CLIConnectionError

from ptc_agent.agent.claude_code.machine_token import FIND_PERL, word
from ptc_agent.agent.claude_code.shell_link import ShellLink
from ptc_agent.core.sandbox.providers._ssh_transport import TRANSPORT_ERRORS, Machine
from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError
from ptc_agent.core.sandbox.runtime import SandboxTransientError

#: The first line a channel attached prints on stdout.
ATTACHED = re.compile(rb"langalpha-attach (\d+|done|gone)")
#: The supervisor's own lines on a channel's stderr, which are not the CLI's.
MARK = "__langalpha__ "
ATTACH_MARK = MARK + "attach"
TAKEN_MARK = MARK + "in "
EXIT_MARK = MARK + "exit "
#: How the CLI ended, past ``EXIT_MARK``, when its supervisor ended it for
#: having no channel attached.
GRACE_KILLED = "g"
#: The input line that ends the CLI's input.
EOF_LINE = "langalpha-eof\n"
#: How long a dial that finds the machine not answering waits before the next.
RETRY_S = 1


@functools.cache
def script() -> str:
    return Path(__file__).with_name("detach.pl").read_text()


def start_command(config_dir: str, grace_s: int, cli: str) -> str:
    """Start the supervisor running ``cli`` (a shell command line), from a
    script that has set ``$p`` to perl, and attach to it from its start."""
    return f'exec "$p" -e {word(script())} start {word(config_dir)} {grace_s} {cli}'


def attach_command(config_dir: str, out_at: int, err_lines: int) -> str:
    """Attach to the turn's CLI, its stdout from byte ``out_at`` and its stderr from line ``err_lines`` on."""
    return f'{FIND_PERL}exec "$p" -e {word(script())} attach {word(config_dir)} {out_at} {err_lines}'


def exit_code(said: str) -> int:
    """The exit code of a CLI whose channel said ``EXIT_MARK + said``: -1 for a signal or an end not told."""
    kind, _, n = said.partition(" ")
    return int(n) if kind == "x" and n.isdigit() else -1


class InputLedger:
    """The input sent to the CLI that its supervisor has not said it took
    (``unsent``), which starts at byte ``start`` of all the input sent: what
    a channel attached after a lost one sends again."""

    def __init__(self) -> None:
        self.unsent = bytearray()
        self.start = 0

    def add(self, data: bytes) -> None:
        self.unsent += data

    def takes(self, n: int) -> bool:
        """Whether ``n`` bytes taken is within the input sent."""
        return self.start <= n <= self.start + len(self.unsent)

    def ack(self, n: int) -> None:
        """The supervisor has taken the first ``n`` bytes: none of them is sent again."""
        if self.takes(n):
            del self.unsent[: n - self.start]
            self.start = n


class Gone(CLIConnectionError):
    """The CLI is not on the machine any more: nothing to attach to."""


class Broken(CLIConnectionError):
    """The machine said what the protocol rules out: a fault no reattach mends."""


class Dropped(CLIConnectionError):
    """A channel attaching to the CLI ended before it said so, or failed as
    it was written: the next dial may attach."""


def unanswered(e: BaseException) -> bool:
    """Whether ``e``, or what it was raised from, is a machine or its tunnel
    not answering, which may answer again in a moment: not one that answered
    and refused, nor one the computer's state rules out."""
    cause: BaseException | None = e
    for _ in range(32):
        if cause is None:
            return False
        if isinstance(cause, SSHConnectError):
            return not cause.answered
        if isinstance(cause, (*TRANSPORT_ERRORS, SandboxTransientError, OSError, TimeoutError)):
            return True
        cause = cause.__cause__
    return False


async def link_within(machine: Machine[ShellLink], wait_s: float) -> ShellLink:
    """The machine's connection, dialed again while it does not answer, for up to ``wait_s``."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + wait_s
    while True:
        try:
            return await machine.link()
        except Exception as e:
            if not unanswered(e) or loop.time() + RETRY_S > deadline:
                raise
        await asyncio.sleep(RETRY_S)
