"""Adding an SSH machine by running one command on it.

The dialog mints a pairing and shows ``curl ... | sh``. The script, run on the
machine as the account the agent will use, authorizes the owner's key and
reports how to reach the machine: the account, the machine's own host keys,
its sshd ports and addresses. The server dials it at once and the dialog,
polling, finds it ready to add. A pairing only replaces the probe; the create
that follows pins the key and checks the folder as it always does.

The host keys come from inside the machine over HTTPS, so the first dial
trusts those and nothing else, which no fingerprint a user is asked to
compare could do as well.

A machine the server cannot dial, behind a home router or a firewall, is told
so, and where this server runs a tunnel gateway the script connects it out
instead: it asks for a tunnel, installs a service that keeps one open, and
reports again with its id. The server then reaches the machine's own sshd
through the gateway, with the same keys as a direct dial.

A machine running LangAlpha's helper, which answers SSH itself and has no
sshd, pairs through the same routes without the script: it takes the key
from ``/key``, keeps it in its own folder, and reports ``tunnel_only``, so
it is sent straight to a tunnel rather than dialed.

State lives in Redis: the mint, the script's reports and the dialog's polls
may each land on a different worker.
"""

from src.server.services.ssh_pairing.dial import source_addresses
from src.server.services.ssh_pairing.flow import connect, issue_tunnel, report
from src.server.services.ssh_pairing.script import pairing_key, render_script
from src.server.services.ssh_pairing.state import (
    EXPIRED,
    PairingNotFound,
    create_pairing,
    pairing_status,
)

__all__ = [
    "EXPIRED",
    "PairingNotFound",
    "connect",
    "create_pairing",
    "issue_tunnel",
    "pairing_key",
    "pairing_status",
    "render_script",
    "report",
    "source_addresses",
]
