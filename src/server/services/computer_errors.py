"""Refusals a computer action raises that the client can act on.

Each refusal subclasses RuntimeError so every existing caller keeps treating
them as a refused action (the workspace aliases still answer 400 with the
message); the computer routes map them to a 409 with a code the UI switches on.
Kept out of the ComputerManager package so the router can import them without
loading the provider stack.
"""

from __future__ import annotations

from enum import StrEnum


class Retirement(StrEnum):
    """How a decision on retiring a computer ended.

    The kept outcomes are the remove route's refusal codes. ``BUSY`` is a
    transition another owner holds, or one that moved the row under the
    claim; ``DEFERRED`` is the decision key not taken, which leaves a
    delete's marker for the cleanup sweep; ``WITHDRAWN`` is a marker the
    sweep found taken off by the time it held the key.
    """

    RETIRED = "retired"
    GONE = "gone"
    PRIMARY = "primary"
    OWNERS_MACHINE = "owners_machine"
    HAS_WORKSPACES = "has_workspaces"
    BUSY = "busy"
    DEFERRED = "deferred"
    WITHDRAWN = "withdrawn"


_REMOVAL_REFUSALS: dict[Retirement, str] = {
    Retirement.PRIMARY: "Your primary computer can't be removed.",
    Retirement.HAS_WORKSPACES: "Delete the workspaces on this computer first.",
    Retirement.BUSY: "This computer is busy. Try again in a moment.",
}


class MachineBusyError(RuntimeError):
    """An agent turn is running on the machine, so it cannot be replaced now."""


class ComputerBusyError(RuntimeError):
    """Another operation holds the machine or moved it underneath this one; retry later."""


class DiskTooSmallError(RuntimeError):
    """The machine's backed-up files would not fit the smaller disk a downgrade asks for."""


class SpecNotChangeable(RuntimeError):
    """The machine's size is its owner's to set, so no tier change applies to it."""


class SpecChangeLostError(RuntimeError):
    """Another request took the change over; this runner stops before it touches the machine."""


class ComputerPreparingError(RuntimeError):
    """The computer's first sandbox is still being built, so it takes no workspace yet.

    A workspace placed on it would pay for that build on its first request,
    which is what preparing at creation exists to spare.
    """


class ComputerRemovalRefused(RuntimeError):
    """The computer stays, for a reason its owner can act on.

    The client words its own copy from ``code``; the message is the server's
    sentence for a client that shows it as it is. ``deleted`` names the
    workspaces a removal had already deleted when something that landed
    meanwhile refused it, for the client to forget.
    """

    def __init__(self, code: Retirement, *, deleted: list[str] | None = None):
        self.code = code
        self.deleted = deleted or []
        super().__init__(_REMOVAL_REFUSALS[code])


class ComputerNotFoundError(ValueError):
    """The caller has no live computer by that id.

    Raised as well for a computer another user owns, so a guessed id learns
    nothing about it. A ValueError, which the computer routes answer with 404.
    """


class ProviderNotOffered(ValueError):
    """The deployment creates no computers on that sandbox provider.

    A ValueError, which the computer routes answer with 400 rather than the
    404 a plain one gets there.
    """


class UnknownResourceTier(ValueError):
    """The provider declares no resource tier by that name.

    Each provider declares its own presets, so a tier one offers may not exist
    on another. A ValueError answered with 400, like ``ProviderNotOffered``.
    """


class ComputerRepairRefused(RuntimeError):
    """A reinstall or reconnect refused before it changed anything.

    ``code`` is what the client words its copy from and ``status`` the HTTP
    status the route answers; ``extra`` rides along in the detail, as the
    ``owner`` a folder's mark names.
    """

    def __init__(
        self, code: str, message: str, *, status: int = 409, **extra: str
    ) -> None:
        self.code = code
        self.status = status
        self.extra = extra
        super().__init__(message)

    def detail(self) -> dict[str, str]:
        return {"code": self.code, "message": str(self), **self.extra}
