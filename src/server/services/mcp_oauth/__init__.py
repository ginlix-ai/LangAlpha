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
from src.server.services.mcp_oauth.connect import (  # noqa: F401
    McpOAuthError,
    McpServerMoved,
    McpServerNotFound,
    StartedConnect,
    complete_callback,
    start_connect,
)
from src.server.services.mcp_oauth.lifecycle import (  # noqa: F401
    AccessToken,
    TokenUnavailable,
    current_access_token,
    disconnect_server,
    ensure_fresh_access_token,
    mark_connection_needs_reauth,
)
