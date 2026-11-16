"""Refusals a computer action raises that the client can act on.

Most subclass RuntimeError so every existing caller keeps treating them as a
refused action (the workspace aliases still answer 400 with the message); the
computer routes map them to a 409 with a code the UI switches on.
Kept out of the ComputerManager package so the router can import them without
loading the provider stack.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Mapping, Optional


class Retirement(StrEnum):
    """How a decision on retiring a computer ended.

    The kept outcomes are the remove route's refusal codes. ``BUSY`` is a
    transition another owner holds, or one that moved the row under the
    claim; ``DEFERRED`` is the decision key not taken, which leaves a
    delete's marker for the cleanup sweep; ``WITHDRAWN`` is a marker the
    sweep found taken off by the time it held the key. ``HANDBACK_LEFT``
    keeps a computer only from a retirement nobody asked for, never from its
    owner's remove, so it is no refusal code.
    """

    RETIRED = "retired"
    GONE = "gone"
    PRIMARY = "primary"
    ONLY_COMPUTER = "only_computer"
    OWNERS_MACHINE = "owners_machine"
    HAS_WORKSPACES = "has_workspaces"
    HANDBACK_OWED = "handback_owed"
    HANDBACK_LEFT = "handback_left"
    BUSY = "busy"
    DEFERRED = "deferred"
    WITHDRAWN = "withdrawn"

    @property
    def keeps_machine(self) -> bool:
        """The computer stays for a reason of its own, not a race: a delete
        that asked still removes its workspace's folder from it."""
        return self in _KEEPS_MACHINE

    @property
    def stays_pending(self) -> bool:
        """The decision is owed again later, so a delete's marker stays for
        the sweep: a lost key or claim, or files Home is owed, which the
        sweep retires the computer after once they are in Home."""
        return self in _STAYS_PENDING


_KEEPS_MACHINE = frozenset(
    {
        Retirement.PRIMARY,
        Retirement.ONLY_COMPUTER,
        Retirement.OWNERS_MACHINE,
        Retirement.HAS_WORKSPACES,
        Retirement.HANDBACK_OWED,
        Retirement.HANDBACK_LEFT,
    }
)
_STAYS_PENDING = frozenset(
    {Retirement.BUSY, Retirement.DEFERRED, Retirement.HANDBACK_OWED}
)


_REMOVAL_REFUSALS: dict[Retirement, str] = {
    Retirement.PRIMARY: (
        "Your primary computer can't be removed. Make another computer "
        "primary first, and Home moves there."
    ),
    Retirement.ONLY_COMPUTER: (
        "This is your only computer, so it can't be removed. Add another "
        "computer first."
    ),
    Retirement.HAS_WORKSPACES: "Delete the workspaces on this computer first.",
    Retirement.HANDBACK_OWED: (
        "What Home's Chief of Staff wrote here while your own computer was away "
        "isn't in Home yet. It is copied there once that computer is back; "
        "remove this one after that."
    ),
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


class NeedsComputer(Exception):
    """The user has no computer Home can live on, and none is built for them.

    Either they chose to work on their own machine, or the deployment builds
    no computers, so nothing runs until they connect one or turn on the
    backup of the one they have; whatever needs a computer stops here instead
    of making a cloud computer they did not ask for. Not a RuntimeError, so
    no route's catch-all words it as a failed action: the app answers it
    once, 409 with ``detail``.
    """

    def __init__(
        self, code: str, message: str, computer: Optional[Mapping[str, Any]] = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.computer = computer

    @classmethod
    def no_computer(cls) -> "NeedsComputer":
        return cls(
            "needs_computer",
            "You have no computer to work on yet. Add one in Computers first.",
        )

    @classmethod
    def backup_off(cls, computer: Mapping[str, Any]) -> "NeedsComputer":
        """Not "you have no computer": they have one, and turning its backup
        on lets it be primary, where Home lives and is restored from."""
        return cls(
            "needs_backup",
            f"File backup is off on {computer.get('name') or ''}. Turn it on in "
            "Computers to make it your primary computer, where Home lives.",
            computer,
        )

    @property
    def detail(self) -> dict[str, str]:
        """The 409 body, in the shape of the other computer refusals."""
        detail = {"code": self.code, "message": str(self)}
        if self.computer is not None:
            detail["computer_id"] = str(self.computer["computer_id"])
            detail["computer_name"] = self.computer.get("name") or ""
        return detail


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
