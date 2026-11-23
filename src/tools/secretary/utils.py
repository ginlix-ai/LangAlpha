"""Where the user's computers and workspaces are, as an agent is told."""

import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from ptc_agent.core.sandbox.providers import MACHINE_KINDS


def spoken_computer_name(name: str | None) -> str:
    """A computer's name as an agent reads it, and so as it passes the name
    back as ``computer``: on one line, and never cut short."""
    return " ".join((name or "").split()) or "Untitled"


def computer_label(name: str | None, computers: int) -> str | None:
    """What an agent is told a computer is called, out of ``computers`` live ones.

    Which computer a workspace is on tells an agent something only once the
    user has another it could be on, so a user with one sees no computer named.
    """
    if name is None or computers < 2:
        return None
    return spoken_computer_name(name)


def computer_state(
    kind: str | None, status: str | None
) -> Literal["", "stopped", "failed"]:
    """What a call on a computer meets before it runs, as the agent is told.

    "stopped" for a hosted computer that is down or going down, which the call
    starts first. A machine of the user's own reads as nothing there: its
    status is only whether our processes run on it, and reaching it connects.
    "failed" for one in error, which no call starts.
    """
    if status == "error":
        return "failed"
    if kind not in MACHINE_KINDS and status in ("stopped", "stopping"):
        return "stopped"
    return ""


def computer_machine(
    kind: str | None, provider_config: Mapping[str, Any] | None
) -> str:
    """Whose machine a computer is, as a token the agent's context words.

    "" for a computer this deployment hosts. Otherwise the machine is the
    user's ("own"), and the one they work on ("personal") when the desktop
    app's helper paired it, or when the server itself runs on it; "_mac"
    follows when it runs macOS. A computer added before its system was kept
    reads as not a Mac.
    """
    if kind == "local":
        return "personal_mac" if sys.platform == "darwin" else "personal"
    if kind != "ssh":
        return ""
    config = provider_config or {}
    owner = "personal" if config.get("helper") else "own"
    return f"{owner}_mac" if config.get("os") == "Darwin" else owner


@dataclass(frozen=True, slots=True)
class PlacedComputer:
    """One of the user's computers, as the Chief of Staff is told of it."""

    name: str
    #: ``computer_machine``'s token: "" for a hosted computer.
    machine: str
    #: Where its workspace folders sit, and where a call on it starts.
    root: str
    is_home: bool
    #: ``computer_state``'s word for it: "" when a call there just runs.
    state: Literal["", "stopped", "failed"] = ""


@dataclass(frozen=True, slots=True)
class ComputerPlacement:
    """Which computer each of the user's workspaces is on, as the Chief of Staff is told.

    Every workspace names its folder, which sits at the root of its computer:
    the Chief of Staff's tools reach any of the user's computers. With more
    than one, each workspace names its computer too, and ``computers`` holds
    them by id in the roster's order, primary first; with one it is empty.
    """

    computers: Mapping[str, PlacedComputer] = field(default_factory=dict)

    @classmethod
    def from_rows(cls, rows: Sequence[Mapping[str, Any]]) -> "ComputerPlacement":
        return cls(
            {
                str(row["computer_id"]): PlacedComputer(
                    name=name,
                    machine=computer_machine(row.get("kind"), row.get("provider_config")),
                    root=row["root_dir"],
                    is_home=bool(row["is_home"]),
                    # Home's computer is being started for the turn that reads this.
                    state=""
                    if row["is_home"]
                    else computer_state(row.get("kind"), row.get("status")),
                )
                for row in rows
                if (name := computer_label(row["name"], len(rows))) is not None
            }
        )

    def _placed(self, workspace: Mapping[str, Any]) -> PlacedComputer | None:
        return self.computers.get(str(workspace.get("computer_id")))

    def path(self, workspace: Mapping[str, Any]) -> str | None:
        """The folder's whole path when the workspace is on another computer
        than Home's: a call there takes that machine's own paths, and its
        root is not one the agent can guess."""
        placed = self._placed(workspace)
        folder = workspace.get("dir_name")
        if placed is None or placed.is_home or not folder:
            return None
        return f"{placed.root.rstrip('/')}/{folder}"

    def computer(self, workspace: Mapping[str, Any]) -> str | None:
        placed = self._placed(workspace)
        return placed.name if placed else None

    def machine(self, workspace: Mapping[str, Any]) -> str | None:
        """What sets the workspace's computer apart when it is the user's own.

        Said only beside the computer's name: with one computer, Home's own
        block says whose it is.
        """
        placed = self._placed(workspace)
        if placed is None or not placed.machine:
            return None
        if placed.machine.endswith("_mac"):
            return "the user's own Mac"
        return "the user's own machine"


async def read_computer_placement(user_id: str) -> ComputerPlacement:
    # Local import: this module loads at agent import time.
    from src.server.database.computer import get_computer_roster

    return ComputerPlacement.from_rows(await get_computer_roster(user_id))
