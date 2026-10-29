"""User-level MCP OAuth — durable two-phase connect + host-only token lifecycle.

The SDK's in-process ``OAuthClientProvider`` generates state + PKCE inside one
awaiting coroutine, which cannot survive ``--workers N``. This package runs the
same spec chain (401 → RFC 9728 → RFC 8414 → DCR → PKCE → token) as two
independent phases bridged by a single-use Redis state record, using the SDK's
piecemeal ``mcp.client.auth.utils`` helpers so no protocol logic is hand-rolled.

Tokens are host-only: the refresh token is a host singleton and the access
token is attached by the egress relay per request — neither ever reaches a
sandbox in any form.
"""

from src.server.database.mcp_oauth import (  # noqa: F401
    SERVABLE,
    ConnectionStatus,
)
from src.server.services.mcp_oauth.lifecycle import (  # noqa: F401
    AccessToken,
    TokenUnavailable,
    current_access_token,
    disconnect_server,
    ensure_fresh_access_token,
    mark_connection_needs_reauth,
)

# The connect flow is imported from ``mcp_oauth.connect``, not re-exported
# here: its callback finishes the saves in ``services.mcp_saves``, which reach
# the catalog and plugin services, and those import this package's lower
# modules. Loaded with the package, it would close that loop.
