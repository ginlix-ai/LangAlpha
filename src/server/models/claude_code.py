"""Claude Code's setup tokens, as Settings reads and changes them."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, SecretStr, StrictInt

from ptc_agent.core.claude_code_clis import CLIS_MAX

from src.llms.claude_code import PlanWindow

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


class PlanUse(BaseModel):
    #: The share of the window used, as a fraction.
    used: float
    resets_at: datetime


class PlanHeld(BaseModel):
    #: None where Claude named no window, which holds every model.
    window: PlanWindow | None
    until: datetime
    #: The model family the limit holds (``PLAN_LIMITS``), None for every model.
    family: Literal["opus", "sonnet", "fable"] | None = None
    #: False where Claude said no reset: ``until`` only bounds the hold, and is never shown.
    reset_known: bool = True


class ClaudeCodePlan(BaseModel):
    """The Claude plan behind a token, as of the last Claude Code request on
    it (``observed_at``): a window whose reset passed is none, and so is a
    limit. ``limits``: every limit used up whose reset is still ahead, each
    naming the model family it holds. ``credits``: whether the account's
    usage credits pay past them, unknown until a request reports it."""

    five_hour: PlanUse | None = None
    seven_day: PlanUse | None = None
    limits: list[PlanHeld] = []
    credits: Literal["on", "off"] | None = None
    observed_at: datetime


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
    #: The Claude plan behind the token, once a request on it reported one.
    plan: ClaudeCodePlan | None = None
    #: Seconds until Check now may read that plan again, while a check made
    #: in the last 5 minutes holds it; None when it may run now.
    check_in: int | None = None
    #: Whether a check would be billed to usage credits the user chose not to
    #: spend (``check_would_bill``), so Check now is not offered.
    check_bills: bool = False


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
    #: The Claude plan behind the token a turn there signs in with: the
    #: computer's own, or with ``source`` saved the saved token's.
    plan: ClaudeCodePlan | None = None
    #: Seconds until Check now may read that plan again (``ClaudeCodeStatus``).
    check_in: int | None = None
    #: Whether a check would be billed (``ClaudeCodeStatus``).
    check_bills: bool = False
    #: How many Claude Code CLIs it runs at once, turns' and subagents'
    #: together (``hosted.own_clis``).
    clis: int


#: Why a cloud computer refuses a turn whatever its credential
#: (``hosted.tier_refusal``): it is smaller than Medium.
ComputerRefusal = Literal["needs_bigger_computer"]


#: Why no Claude Code turn runs in a workspace (``hosted.Refused``), each a
#: ``CLAUDE_CODE_OFF`` reason.
PlacementRefusal = Literal[
    "needs_own_computer", "no_own_computer", "cli_computer_offline", "policy_blocked", "signed_out",
    "token_refused", "needs_bigger_computer",
]
#: Where a turn's CLI runs: on the workspace's own computer (``own``), on an
#: own computer the server picked for a workspace on a cloud computer that
#: runs none (``picked``), inside Home's cloud computer's sandbox
#: (``home_cloud``), or in this server's workers (``server``).
CliKind = Literal["own", "picked", "home_cloud", "server"]


class ClaudeCodeRunsOn(BaseModel):
    """Where the CLI of some of the user's Claude Code turns runs (``kind``),
    on the computer ``computer_id`` (named ``name``) where one does, or
    refused for ``refusal``, naming the computer it would run on where one
    is offline."""

    kind: CliKind
    computer_id: str | None = None
    name: str | None = None
    refusal: PlacementRefusal | None = None


class ClaudeCodeChiefOfStaff(ClaudeCodeRunsOn):
    """Where the Chief of Staff's CLI runs, and whether it runs a Claude Code
    Default the user saved (``default_runs``, ``chief_of_staff_default_runs``
    for Home), the one answer the composer starts Home's turns by."""

    default_runs: bool = False


class ClaudeCodeComputers(BaseModel):
    computers: list[ClaudeCodeComputer]
    #: Where the CLI of turns in workspaces on cloud computers, Home's
    #: included, runs where those computers run none for the user
    #: (``hosted.picks_own``): on one of their own computers (``picked``),
    #: or refused (``no_own_computer``, ``cli_computer_offline``). None where
    #: cloud computers run it themselves.
    cli_computer: ClaudeCodeRunsOn | None = None
    #: Each of the user's cloud computers that refuses a turn whatever the
    #: credential, by computer id (``hosted.cloud_refusals``). A turn there is
    #: told it only once its credential would run.
    cloud_refusals: dict[str, ComputerRefusal] = Field(default_factory=dict)
    #: The most CLIs a computer of the user's own may be set to run at once.
    clis_max: int = CLIS_MAX


class ClaudeCodeCloudComputer(BaseModel):
    computer_id: str
    name: str


#: Where the user's Claude Code stands as a whole, the first that holds: a
#: token works somewhere (``connected``), one is being checked
#: (``checking``), one was refused or has expired (``refused``), somewhere
#: runs it once a token is added (``not_connected``), an own computer is out
#: of reach (``offline``), every own computer has something that stops it
#: (``unsupported``), or nothing the user has can run it (``no_computer``).
SetupState = Literal["connected", "checking", "refused", "not_connected", "offline", "unsupported", "no_computer"]


class ClaudeCodeSetup(BaseModel):
    """Claude Code's entry under Connect a subscription, read whether the user
    turned it on or not. ``on`` is their opt-in, which adding a first token
    makes; until then an own computer reads ``off`` where it is reachable
    and ``offline`` where it is not. ``saved_token_runs``: this server runs
    turns on a setup token the user adds in Settings, so one can be added
    there whatever their computers. ``cloud``: users choose how Claude Code
    runs for workspaces on this server's cloud computers, so a computer of
    their own is not the only place it runs. A self-hosted server's own
    machine is the user's, so it never sets this. ``cli_in_sandbox``: a turn on
    a cloud computer runs Claude Code inside that computer's sandbox, not on
    this server (``hosted.in_sandbox``). ``too_small``: their cloud
    computers too small to run it inside (``hosted.tier_refusals``),
    whatever they chose for cloud computers, so the entry names what to
    resize, and warns of it before they choose to run it there.
    ``chief_of_staff``: where the Chief of Staff's Claude Code runs."""

    state: SetupState
    on: bool
    computers: list[ClaudeCodeComputer]
    saved_token_runs: bool = False
    cloud: bool = False
    cli_in_sandbox: bool = False
    too_small: list[ClaudeCodeCloudComputer] = Field(default_factory=list)
    #: Where the Chief of Staff's Claude Code runs (``hosted.cli_placement``
    #: for Home), or why it can't, and whether it runs a saved Claude Code
    #: Default; None until the user turns Claude Code on.
    chief_of_staff: ClaudeCodeChiefOfStaff | None = None


#: The credentials a user may choose for their turns on a cloud computer.
HostedMode = Literal["stored"]


class ClaudeCodeCustody(BaseModel):
    """How the user's Claude Code turns on a cloud computer run: ``local``
    runs none there (only on their own computers), and ``stored`` runs them
    on the setup token they let the server store. ``offered`` is what the
    server lets them choose now, none where ``blocked``: an operator turned
    storing a token off for this user.
    ``runs_on`` and ``refusal`` are the verdict admission gives a turn there:
    the credential it runs on, else the ``CLAUDE_CODE_OFF`` reason it is
    refused for."""

    mode: Literal["local", "stored"]
    offered: list[HostedMode]
    blocked: bool = False
    runs_on: Literal["stored_token"] | None = None
    refusal: str | None = None


class CustodyBody(BaseModel):
    mode: Literal["local", "stored"]
    #: The version of the consent text the client showed, which storing a
    #: token needs to be the current one (``hosted.DISCLOSURE``). The view never
    #: names it, so a tab showing older copy cannot echo it back.
    disclosure: str | None = Field(default=None, max_length=32)


class TokenBody(BaseModel):
    token: SecretStr


class ClisBody(BaseModel):
    #: How many Claude Code CLIs the computer runs at once, 1 to ``clis_max``.
    clis: StrictInt = Field(ge=1, le=CLIS_MAX)


class CodeBody(BaseModel):
    code: SecretStr
