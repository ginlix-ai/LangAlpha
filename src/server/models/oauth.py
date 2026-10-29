"""The status a client reads for a subscription connection."""

from datetime import datetime
from typing import Any, Mapping, Optional

from pydantic import BaseModel

from src.server.services.oauth_health import ConnectionState, connection_state


def _iso(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value else None


class OAuthStatus(BaseModel):
    # ``connected`` stays "a connection is stored", as older clients read it;
    # ``state`` says whether it still works.
    connected: bool
    state: ConnectionState
    account_id: Optional[str] = None
    email: Optional[str] = None
    plan_type: Optional[str] = None
    expires_at: Optional[str] = None
    needs_reauth_at: Optional[str] = None
    last_error_code: Optional[str] = None

    @classmethod
    def from_row(cls, row: Optional[Mapping[str, Any]]) -> "OAuthStatus":
        """From a ``user_oauth_tokens`` row, or None when nothing is stored."""
        state = connection_state(row)
        if not row:
            return cls(connected=False, state=state)
        flagged = state == "needs_reauth"
        return cls(
            connected=True,
            state=state,
            account_id=row.get("account_id"),
            email=row.get("email"),
            plan_type=row.get("plan_type"),
            expires_at=_iso(row.get("expires_at")),
            needs_reauth_at=_iso(row.get("needs_reauth_at")),
            # Only meaningful while flagged.
            last_error_code=row.get("last_error_code") if flagged else None,
        )
