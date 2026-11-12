"""The status a client reads for a subscription connection."""

from datetime import date, datetime, timezone
from typing import Any, Literal, Mapping, Optional

from pydantic import BaseModel

from src.server.services.oauth_health import ConnectionState, connection_state


def _iso(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value else None


#: Where signing in with a provider stands on this deployment: ``active``
#: where it set no end day, ``ending`` until that day passes, ``legacy``
#: after it.
SignInLifecycle = Literal["active", "ending", "legacy"]


def sign_in_lifecycle(ends_on: Optional[date]) -> SignInLifecycle:
    if ends_on is None:
        return "active"
    return "ending" if ends_on >= datetime.now(timezone.utc).date() else "legacy"


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
    # Whether the user may start a sign-in, where signing in stands on this
    # deployment, and while it is ``ending`` the day it stops working.
    can_sign_in: bool = True
    ends_on: Optional[str] = None
    lifecycle: SignInLifecycle = "active"

    @classmethod
    def from_row(
        cls, row: Optional[Mapping[str, Any]], *, can_sign_in: bool = True, ends_on: Optional[date] = None,
    ) -> "OAuthStatus":
        """From a ``user_oauth_tokens`` row, or None when nothing is stored."""
        state = connection_state(row)
        lifecycle = sign_in_lifecycle(ends_on)
        day = ends_on.isoformat() if ends_on and lifecycle == "ending" else None
        if not row:
            return cls(connected=False, state=state, can_sign_in=can_sign_in, ends_on=day, lifecycle=lifecycle)
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
            can_sign_in=can_sign_in,
            ends_on=day,
            lifecycle=lifecycle,
        )
