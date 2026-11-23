"""Which requests an open-source server takes for its owner.

With no sign-in, an open-source server takes a request that presents nothing
for its one user. The desktop app's server also runs code on its own machine,
where any container reaches it as ``host.docker.internal`` and can name any
Host, and Docker Desktop shows that connection as one from 127.0.0.1, so
nothing about a request tells a container from the app. So the app gives the
server a secret at each launch (``LANGALPHA_OWNER_SECRET``) and adds it to its
own windows' requests, and with one set only a request carrying it is the
owner. The server's calls to itself carry its service token and a computer's
calls its own token, as on a hosted server, which has no owner fallback.
"""

from __future__ import annotations

import hmac
import os
from collections.abc import Mapping
from typing import Any

from fastapi import HTTPException

OWNER_SECRET_ENV = "LANGALPHA_OWNER_SECRET"
OWNER_HEADER = "x-langalpha-owner"
REFUSAL = "This server takes a request with no token only from the LangAlpha app."


def is_owner(scope: Mapping[str, Any]) -> bool:
    """Whether the owner fallback may answer *scope*: any request where no
    secret is set, as on a server the app did not start. A server the app
    started without one answers no one, rather than everyone."""
    secret = os.environ.get(OWNER_SECRET_ENV, "")
    if not secret:
        return os.environ.get("LANGALPHA_DESKTOP_APP") != "1"
    for name, value in scope.get("headers") or ():
        if name == OWNER_HEADER.encode():
            return hmac.compare_digest(value, secret.encode())
    return False


def refuse_unless_owner(scope: Mapping[str, Any]) -> None:
    """Raise 401 where the open-source owner fallback would answer anyone else."""
    if not is_owner(scope):
        raise HTTPException(status_code=401, detail=REFUSAL)
