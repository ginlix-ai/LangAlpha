"""The script ``curl ... | sh`` runs on the machine.

Rendered per pairing from the parts in ``scripts/``, filled in by :func:`_fill`.
The tunnel part rides along only where this server runs a gateway, so a
script never reads or offers what its server could not use.
"""

from __future__ import annotations

import re
import shlex
from importlib.resources import files
from typing import Dict, Optional

from ptc_agent.config.core import SSHConfig
from ptc_agent.core.sandbox.providers._ssh_authorized_key import authorized_key_line
from src.server.services.ssh_computers import ensure_client_key
from src.server.services.ssh_pairing import state
from src.server.services.ssh_tunnels import (
    TUNNEL_PARENT,
    UNINSTALL_SCRIPT,
    deployment_tunnel_name,
    tunnels_enabled,
)

# The first line of the report's 202, which the script takes as its cue to
# connect out.
TUNNEL_CUE = "tunnel"

_SLOTS = re.compile(
    r"\b(PRODUCT|KEY|REPORT|TUNNEL_URL|TUNNEL_DIR|TUNNEL_NAME|UNINSTALL_SCRIPT"
    r"|TUNNEL_CUE)\b"
)


def _part(name: str) -> str:
    return (files(__package__) / "scripts" / name).read_bytes().decode()


_HEAD, _BODY, _END = _part("head.sh"), _part("body.sh"), _part("end.sh")
_DIRECT = _HEAD + _part("head_direct.sh") + _BODY + _part("report.sh") + _END
_TUNNELED = _HEAD + _part("head_tunnel.sh") + _BODY + _part("tunnel.sh") + _END

_EXPIRED_SCRIPT = f"""#!/bin/sh
echo "{state.EXPIRED}" >&2
exit 1
"""


def _fill(template: str, values: Dict[str, str]) -> str:
    """One pass, so a value is never itself searched for another slot."""
    return _SLOTS.sub(lambda m: values[m.group(1)], template)


async def pairing_key(token: str, settings: SSHConfig) -> Optional[str]:
    """The line the script would authorize, for a machine that pairs without it.

    LangAlpha's helper keeps the line in its own folder rather than
    ``~/.ssh/authorized_keys``, so it asks for the line alone. None once the
    pairing has expired.

    Raises:
        SSHUnavailable: As for :func:`ensure_client_key`.
    """
    pairing = await state.load(token)
    return None if pairing is None else await _key_line(pairing.user_id, settings)


async def _key_line(user_id: str, settings: SSHConfig) -> str:
    return authorized_key_line(
        (await ensure_client_key(user_id))["public_key"], settings.login_from
    )


async def render_script(token: str, settings: SSHConfig) -> str:
    """The script for a live pairing, or one that says the command expired.

    Raises:
        SSHUnavailable: As for :func:`ensure_client_key`.
    """
    pairing = await state.load(token)
    if pairing is None:
        return _EXPIRED_SCRIPT
    key = await _key_line(pairing.user_id, settings)
    pairing_url = state.script_url(token, pairing.base).removesuffix("/script")
    values = {
        "PRODUCT": state.PRODUCT,
        "KEY": shlex.quote(key),
        "REPORT": shlex.quote(state.curl_url_line(f"{pairing_url}/report")),
    }
    if not tunnels_enabled():
        return _fill(_DIRECT, values)
    name = await deployment_tunnel_name()
    values |= {
        "TUNNEL_URL": shlex.quote(state.curl_url_line(f"{pairing_url}/tunnel")),
        "TUNNEL_DIR": f"{TUNNEL_PARENT}/{name}",
        "TUNNEL_NAME": name,
        "UNINSTALL_SCRIPT": UNINSTALL_SCRIPT,
        "TUNNEL_CUE": TUNNEL_CUE,
    }
    return _fill(_TUNNELED, values)
