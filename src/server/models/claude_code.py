"""Claude Code's setup tokens, as Settings reads and changes them."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, SecretStr

#: Where the user's Claude Code models stand on the backend. ``unverified``: a
#: token is saved but the check could not reach Anthropic; its turns run, as
#: admission lets them. ``policy_blocked``: an organization's managed Claude
#: Code settings are installed on the backend's machine, so no turn runs there.
State = Literal["off", "no_token", "checking", "unverified", "signed_in", "rejected", "policy_blocked"]
#: Where Claude Code stands on a computer of the user's own. ``offline``: the
#: computer is asleep or cannot be reached, and nothing is known of it yet.
#: ``policy_blocked``: an organization's managed Claude Code settings are
#: installed there, so no turn runs there whatever its token.
#: ``needs_perl``: the computer has no perl, which a turn starts the CLI with,
#: so no token, relayed or its own, can be used there. ``computer_unsupported``:
#: the computer runs musl, which no build of the CLI runs on.
ComputerState = Literal[
    "off", "offline", "no_token", "checking", "unverified", "signed_in", "rejected", "policy_blocked",
    "needs_perl", "computer_unsupported",
]
#: Which token a turn on a computer signs in with: the machine's own, or, on a
#: self-hosted server, the one the user saved in Settings.
Source = Literal["machine", "saved"]


class ClaudeCodeStatus(BaseModel):
    state: State
    #: The Claude account a token made through sign-in belongs to.
    email: str | None = None
    #: When the token stops working; for a pasted one, the latest it can.
    expires_at: datetime | None = None
    #: Whether a token is saved, whatever the state: one saved before Claude
    #: Code was turned off can still be removed.
    saved: bool = False
    #: Whether the user's Claude Code models are on, wherever they run. With
    #: ``state`` off they still run on the user's own computers, each with
    #: the token it holds, where this server does not run them on a saved one.
    enabled: bool = False


class ClaudeCodeComputer(BaseModel):
    computer_id: str
    name: str
    kind: Literal["ssh", "helper"]
    state: ComputerState
    source: Source | None = None
    #: Whether the computer's own token file is on the machine, one it refuses
    #: included: with ``source`` saved, Settings still offers to remove it.
    own_file: bool = False
    #: The Claude account a token made through sign-in belongs to.
    email: str | None = None
    #: When the token stops working; for a pasted one, the latest it can.
    expires_at: datetime | None = None


class ClaudeCodeComputers(BaseModel):
    computers: list[ClaudeCodeComputer]


#: The credentials a user may choose for their turns on a cloud computer.
HostedMode = Literal["api_key", "stored"]


class ClaudeCodeCustody(BaseModel):
    """How the user's Claude Code turns on a cloud computer run: ``local``
    runs none there (only on their own computers), ``api_key`` runs them on
    their Anthropic API key, the one saved under Model access, which
    ``api_key_saved`` says is there, and ``stored`` on the setup token they
    let the server store. ``offered`` is what the server lets them choose
    now: a mode chosen before its switch went off is kept, and paused.
    ``blocked``: an operator turned storing a token off for this user.
    ``runs_on`` and ``refusal`` are the verdict admission gives a turn there:
    the credential it runs on, else the ``CLAUDE_CODE_OFF`` reason it is
    refused for."""

    mode: Literal["local", "api_key", "stored"]
    api_key_saved: bool
    offered: list[HostedMode]
    blocked: bool = False
    runs_on: Literal["api_key", "stored_token"] | None = None
    refusal: str | None = None
    #: The version of the consent text storing a token needs accepted now.
    disclosure: str | None = None


class CustodyBody(BaseModel):
    mode: Literal["local", "api_key", "stored"]
    #: The version of the consent text accepted, which storing a token needs.
    disclosure: str | None = Field(default=None, max_length=32)


class TokenBody(BaseModel):
    token: SecretStr


class CodeBody(BaseModel):
    code: SecretStr
