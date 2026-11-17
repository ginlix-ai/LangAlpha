"""Response models for where the user's Home is and what its stand-in handed back.

Home's stand-in is the hidden folder on a cloud computer where Home's turns
run while the machine Home lives on can't be reached; a hand-back copies what
those turns wrote into Home once it is back (``services.home_handback``).
"""

from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from src.server.models.workspace_move import WorkspaceMoveOut


class HomeStandInComputer(BaseModel):
    """The cloud computer Home's turns run on while Home's machine is away."""

    computer_id: str
    name: str
    status: str


class HomeHandbackNotice(BaseModel):
    """The last hand-back, until the user dismisses it."""

    handback_id: str
    folder: Optional[str] = Field(
        None, description="Where in Home the files went; null when none did"
    )
    files: int = Field(description="How many files were copied")
    left_on_cloud: List[str] = Field(
        default_factory=list,
        description="Paths too large to copy, still only on the cloud computer",
    )
    unreadable: List[str] = Field(
        default_factory=list,
        description="Paths that couldn't be read there, still only on the cloud computer",
    )
    computer_name: str
    cloud_name: str
    created_at: str
    failed: bool = Field(
        False,
        description="The copy kept failing and was given up: every file is still on the cloud",
    )


class HomeLeftOnStandIn(BaseModel):
    """What the last hand-back left on the stand-in's computer and Home lacks;
    removing that computer deletes it. ``failed`` when it gave up, so which
    files is not known."""

    failed: bool
    paths: List[str]
    #: The computer holding them, the one whose removal deletes them.
    computer_id: str


class HomePlacementResponse(BaseModel):
    computer_id: Optional[str] = Field(None, description="The computer Home is on")
    computer_name: Optional[str] = None
    computer_kind: Optional[str] = Field(
        None, description="``ssh`` for a machine the user brought"
    )
    unreachable: bool = Field(
        description="Home's computer, a machine of the user's own, is not answering"
    )
    stand_in: Optional[HomeStandInComputer] = Field(
        None,
        description="Where Home's turns run while it is away; null: they wait for it",
    )
    handback_owed: bool = Field(
        description="What Home's turns wrote in the stand-in is not in Home yet"
    )
    handback: Optional[HomeHandbackNotice] = None
    left_on_stand_in: Optional[HomeLeftOnStandIn] = None
    move: Optional[WorkspaceMoveOut] = Field(None, description="Home's latest move")


class HomeHandbackOut(BaseModel):
    through: datetime = Field(description="The latest cloud turn this hand-back carried")
    folder: Optional[str] = Field(
        None, description="The folder in Home it filled; null when it copied nothing"
    )
    files: List[str] = Field(
        description="Paths, relative to the stand-in's folder, it carried there, sorted"
    )
    kept: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Paths it found as an earlier hand-back carried them, each with the"
            " folder in Home the latest of those filled"
        ),
    )


class HomeHandbacksResponse(BaseModel):
    stand_in_workspace_id: str
    handbacks: List[HomeHandbackOut] = Field(description="Oldest first")


class HomeHandbackSeenRequest(BaseModel):
    handback_id: Optional[str] = Field(
        None, description="Dismiss only this notice; any when omitted"
    )
