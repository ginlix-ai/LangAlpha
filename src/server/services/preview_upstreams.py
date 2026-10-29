"""Where this worker dials the port a preview capability opens.

Every dial of a capability's port goes through ``upstream_for``: the proxy's
requests and sockets, and the health probe of a URL the server handed out. A
record's stored upstream is dialed as it is. A record with none is a port its
provider forwards from whichever process asks, so the serving worker asks for
itself: it checks the computer row, then asks the computer's runtime for the
port.

Across workers, the capability (Redis) and the computer row (Postgres) are the
truth, and both are read on every dial. Nothing here is kept between dials:
the forward rides the provider's pooled connection to the machine, which stays
open while a channel through it is and closes once idle, so a page's next
asset reuses it without this module holding anything.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from urllib.parse import urlsplit

from src.server.database.computer import get_computer_for_workspace
from src.server.models.computer import ComputerStatus
from src.server.services.preview_base import preview_base
from src.server.services.preview_capabilities import (
    Capability,
    PreviewProxyUnavailable,
    Upstream,
    preview_origin,
    resolve_host,
)

logger = logging.getLogger(__name__)

__all__ = ["probe_target", "upstream_for"]

# Opening a runtime may connect to the machine, and asking it for a port may
# open a forward.
_OPEN_TIMEOUT_S = 15


async def upstream_for(capability: Capability) -> Upstream | None:
    """Where this worker dials *capability*'s port.

    None when nothing here opens it: the computer stopped or moved to another
    sandbox, or its machine could not be reached. Raises
    ``PreviewProxyUnavailable`` when Postgres cannot say whether the computer
    runs the sandbox.
    """
    if capability.upstream is not None:
        return capability.upstream
    computer = await _running_computer(capability)
    if computer is None:
        return None
    from src.server.services.workspace_manager import WorkspaceManager

    manager = WorkspaceManager.get_instance()
    try:
        async with (
            asyncio.timeout(_OPEN_TIMEOUT_S),
            manager.computer_runtime(computer, capability.sandbox_id) as runtime,
        ):
            info = await runtime.get_preview_url(capability.port)
        return Upstream(preview_origin(info.url), dict(info.auth_headers))
    except Exception as e:
        logger.debug(
            "Preview port %s of sandbox %s did not open here: %s",
            capability.port,
            capability.sandbox_id,
            type(e).__name__,
        )
        return None


async def probe_target(url: str) -> tuple[str, dict[str, str]] | None:
    """Where the server checks a URL it handed a browser, and with which headers.

    A capability URL is checked at its sandbox, as this worker dials it, never
    through the server's own preview host. None means the capability no
    longer opens anything.
    """
    base = preview_base()
    parts = urlsplit(url)
    if base is None or not base.claims(parts.netloc):
        return url, {}
    try:
        capability = await resolve_host(parts.netloc, base=base)
        upstream = None if capability is None else await upstream_for(capability)
    except PreviewProxyUnavailable:
        return None
    if upstream is None:
        return None
    target = upstream.origin + (parts.path or "/")
    if parts.query:
        target = f"{target}?{parts.query}"
    return target, dict(upstream.headers)


async def _running_computer(capability: Capability) -> dict[str, Any] | None:
    """The capability's computer row, while it runs the capability's sandbox."""
    try:
        computer = await get_computer_for_workspace(capability.workspace_id)
    except Exception as e:
        raise PreviewProxyUnavailable("could not read the computer") from e
    if (
        computer is None
        or computer.get("status") != ComputerStatus.RUNNING
        or str(computer.get("provider_ref") or "") != capability.sandbox_id
    ):
        return None
    return computer
