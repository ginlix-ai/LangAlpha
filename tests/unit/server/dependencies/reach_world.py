"""Fixed ids and a stand-in for the ``turn_targets`` lookups, for the
principal tests."""

from __future__ import annotations

from unittest.mock import AsyncMock

from fastapi import Request

from src.server.database import turn_targets
from src.server.dependencies.principal import _CLI_CALL, CliCall, Principal, get_principal
from src.server.services.cli_tokens import SCOPES

USER = "user-reach-1"
COMPUTER = "c0ffee00-0000-4000-8000-000000000001"
OTHER_COMPUTER = "c0ffee00-0000-4000-8000-000000000002"
WORKSPACE = "a1100000-0000-4000-8000-000000000001"
OTHER_WORKSPACE = "a1100000-0000-4000-8000-000000000002"
THREAD = "b2200000-0000-4000-8000-000000000001"
OTHER_THREAD = "b2200000-0000-4000-8000-000000000002"
AUTOMATION = "d4400000-0000-4000-8000-000000000001"
OTHER_AUTOMATION = "d4400000-0000-4000-8000-000000000002"
UNKNOWN = "e5500000-0000-4000-8000-000000000009"


class ReachWorld:
    """Stands in for the ``turn_targets`` lookup the principal tests reach:
    ``turn_sender`` answers from ``senders``, ``None`` for an id it does not
    list, as the database does for one the user lacks."""

    def __init__(self, monkeypatch) -> None:
        # The thread that sent each thread its latest turn.
        self.senders: dict[str, str] = {}
        self.sender_of = AsyncMock(
            side_effect=lambda user_id, thread_id: self.senders.get(thread_id)
        )
        monkeypatch.setattr(turn_targets, "turn_sender", self.sender_of)


def act_as_computer(app, *, call_thread: str | None = None, computer: str = COMPUTER):
    """Every request ``app`` serves comes from ``computer``'s token, holding
    every scope a computer's token holds, its call filed as run in
    ``call_thread``. Returns the principal."""
    principal = Principal(USER, "computer", scopes=SCOPES, computer_id=computer)

    async def as_computer(request: Request) -> Principal:
        call = CliCall(principal, "call-0123456789" if call_thread else None, call_thread)
        request.scope.setdefault("state", {})[_CLI_CALL] = call
        return principal

    app.dependency_overrides[get_principal] = as_computer
    return principal
