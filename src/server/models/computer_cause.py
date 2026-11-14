"""What a computer start or stop was for, as the computer events and the run
ledger record it.

Here rather than beside the context that carries a cause
(``computer_usage/causes.py``) so the database layer can take one without
reaching up into services.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional

#: What can ask for a start. A turn records its origin (``turn_kinds``), and
#: a Chief of Staff hand-off is ``delegated``. ``unknown`` is a start nothing
#: named, which is how a caller still missing a cause shows up in the data.
StartCause = Literal[
    "turn",
    "automation",
    "wakeup",
    "market_target",
    "report",
    "delegated",
    "warm",
    "button",
    "create",
    "spec_change",
    "repair",
    "always_on",
    "reach",
    "claude_code",
    "mcp_check",
    "files",
    "upload",
    "preview",
    "packages",
    "refresh",
    "restore",
    "compact",
    "move",
    "duplicate",
    "rebuild",
    "platform_secret",
    "unknown",
]

#: Why a stop happens, beside the reasons its writer names directly (``idle``).
StopCause = Literal["user", "archive", "spec_change", "unknown"]

#: The app a request came from (``utils/request_client.py``); None for one the
#: server started itself.
Client = Literal["browser", "desktop", "mobile", "cli", "channel"]


@dataclass(frozen=True)
class Cause:
    name: StartCause | StopCause
    client: Optional[Client]
    #: Free text kept short, such as a warm's signal.
    detail: Optional[str]
    #: When it was asked for, so a start that waited behind a lock or another
    #: worker's claim is timed from the ask.
    at: datetime
