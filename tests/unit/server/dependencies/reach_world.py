"""A fixed map of which computer each resource sits on, in place of the
``principal_reach`` lookups, for the principal tests."""

from __future__ import annotations

from unittest.mock import AsyncMock

from fastapi import Request

from src.server.database import principal_reach as reach
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
    """``computer_of`` answers from ``computers`` and ``reach.MISSING`` for an
    id it does not list, as the database does for one the user lacks;
    ``automation_thread`` from ``automation_threads``."""

    def __init__(self, monkeypatch) -> None:
        self.computers: dict[str, reach.Reach] = {
            WORKSPACE: COMPUTER,
            OTHER_WORKSPACE: OTHER_COMPUTER,
            THREAD: COMPUTER,
            OTHER_THREAD: OTHER_COMPUTER,
            AUTOMATION: COMPUTER,
            OTHER_AUTOMATION: OTHER_COMPUTER,
        }

        async def of(kind: str, user_id: str, resource_id: str):
            return self.computers.get(resource_id, reach.MISSING)

        self.of = AsyncMock(side_effect=of)
        monkeypatch.setattr(reach, "computer_of", self.of)
        self.automation_threads: dict[str, str] = {}

        async def thread_of(user_id: str, automation_id: str):
            return self.automation_threads.get(automation_id)

        self.thread_of = AsyncMock(side_effect=thread_of)
        monkeypatch.setattr(reach, "automation_thread", self.thread_of)

    def asked(self, kind: str) -> list[tuple[str, str]]:
        """The (user, id) of each lookup of ``kind``, in order."""
        return [call.args[1:] for call in self.of.await_args_list if call.args[0] == kind]

    @property
    def lookups(self) -> int:
        return self.of.await_count


def act_as_computer(app, *, call_thread: str | None = None):
    """Every request ``app`` serves comes from ``COMPUTER``'s token, holding
    every scope a computer's token holds, its call filed as run in
    ``call_thread``. Returns the principal."""
    principal = Principal(USER, "computer", scopes=SCOPES, computer_id=COMPUTER)

    async def as_computer(request: Request) -> Principal:
        call = CliCall(principal, "call-0123456789" if call_thread else None, call_thread)
        request.scope.setdefault("state", {})[_CLI_CALL] = call
        return principal

    app.dependency_overrides[get_principal] = as_computer
    return principal
