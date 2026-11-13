"""Claude Code models: catalog rows whose turns the Claude Code runtime runs.

langalpha never calls a ``claude-code`` row over an API. The factory builds a
``ClaudeCodeTarget`` for it instead: a chat model in shape, so the stamps,
profile and copies every built client goes through still apply, that refuses
to be called. A turn reads what to run off it (the model id, the effort, the
billing stamps) and hands the turn to the runtime in
``ptc_agent.agent.claude_code``, which this module never imports: the runtime's
SDK is an optional extra that only a Claude Code turn loads.
"""

from __future__ import annotations

import importlib.util
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar, Literal

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatResult

SDK = "claude-code"
FEATURE = "claude_code_runtime"
#: Which Claude message an assistant message is, stamped on every reply a
#: Claude Code turn commits, so it also tells Claude's replies from another model's.
MESSAGE_ID_KEY = "claude_message_id"
#: The row the token check runs on, the least a request costs on the plan.
TOKEN_CHECK_ROW = "claude-haiku-5-5-code"


class ClaudeTurnFailed(RuntimeError):
    """The CLI ended the turn without doing it.

    The CLI reports a refused or failed request as an ordinary result
    (``subtype="success"``, ``terminal_reason="api_error"``) with its error
    text as the assistant's reply, so a logged-out machine or a spent plan
    would otherwise commit an empty turn. ``kind`` is what the user is told:
    ``auth_failed``, ``plan_exhausted`` (a refusal no plan window is known
    for; ``PlanStopped`` names one), ``interrupted``, ``api_error``,
    ``no_result``, ``nothing_to_resume``, ``policy_blocked``, ``needs_perl``,
    ``computer_unsupported``, ``token_refused`` (Claude refused the saved
    setup token, which its store reports in place of ``auth_failed``), one of
    the reasons a cloud computer's run is refused
    as it starts (``hosted.hosted``, ``hosted.tier_refusal``), or one of
    ``TEMPLATES``, which words it.
    """

    #: The failures the machine's Claude Code ends a turn with, in words a
    #: user reads; the detail takes the ``{}``.
    TEMPLATES: ClassVar[dict[str, str]] = {
        "model_unavailable": "Your organization's Claude Code policy does not allow {}. Pick another model.",
        "startup_failed": "Claude Code could not start: {}",
    }

    def __init__(self, kind: str, detail: str, **fields: Any) -> None:
        template = self.TEMPLATES.get(kind)
        super().__init__(template.format(detail) if template else f"{kind}: {detail}")
        self.kind = kind
        self.detail = detail
        #: What the user's words for ``kind`` name (``CLAUDE_CODE_OFF``'s fields).
        self.fields = fields


#: The Claude plan's limits, as the CLI names them, each with the model
#: family it binds; None binds every model. ``seven_day_overage_included`` is
#: the weekly limit the CLI calls the Fable limit.
PlanWindow = Literal["five_hour", "seven_day", "seven_day_opus", "seven_day_sonnet", "seven_day_overage_included"]
PLAN_LIMITS: dict[PlanWindow, str | None] = {
    "five_hour": None, "seven_day": None, "seven_day_opus": "opus", "seven_day_sonnet": "sonnet",
    "seven_day_overage_included": "fable",
}
#: What a used-up plan does to the user's Claude Code turns: stop until it
#: resets, or go on on the account's usage credits.
PlanOut = Literal["stop", "credits"]
#: How long a limit Claude named no reset for holds the plan: the shortest
#: plan window, so a missing reset bills at most one request a window, not
#: one a turn. No one is told this time.
UNKNOWN_RESET_HOLD = timedelta(hours=5)
#: The windows whose use is kept: the two every plan reports.
METERED = ("five_hour", "seven_day")
#: Why usage credits cannot pay, among the reasons the CLI reports, that
#: says nothing of the account: the CLI could not read it.
_UNKNOWN_CREDITS = ("fetch_error", "unknown")
#: The ``response_metadata`` key of a reply whose last request was billed to
#: the account's usage credits under Stop: the notice under it, as the
#: ``claude_code_plan`` event that showed it live carried it.
PLAN_NOTICE_KEY = "claude_plan_notice"


@dataclass(frozen=True)
class PlanLimit:
    """A used-up Claude plan window, and when it resets. Where Claude said no
    reset (``known`` False), ``until`` is ``UNKNOWN_RESET_HOLD`` after
    the report and no one is told it, and ``window`` is None where Claude
    named no window either, which binds every model."""

    window: PlanWindow | None
    until: datetime
    known: bool = True

    def binds(self, model: str) -> bool:
        """Whether the limit stops a run on ``model``: a weekly family limit
        stops only that family's models."""
        family = PLAN_LIMITS.get(self.window)
        return family is None or family in model

    def current(self, now: datetime | None = None) -> bool:
        return self.until > (now or datetime.now(UTC))


def binding_limit(limits: Iterable[PlanLimit], model: str, now: datetime | None = None) -> PlanLimit | None:
    """The limit that holds ``model`` now: of those still ahead that bind its
    family, the one that lasts longest, since the model runs again only once
    every one of them has reset."""
    held = [limit for limit in limits if limit.binds(model) and limit.current(now)]
    return max(held, key=lambda limit: limit.until, default=None)


@dataclass(frozen=True)
class PlanReading:
    """One ``rate_limit_event``, on a request for ``model``, as a
    credential's plan state keeps it.

    ``windows`` holds only the windows the event carried, each as (fraction
    used, reset). ``limit`` is the window the response was refused for, or
    billed to usage credits past, with its reset: where the event names no
    plan window with one, the window it shows used up, and else a reset
    unknown. Its use and whether usage credits paid come from the event's
    raw dict, which carries what the typed fields leave out.
    """

    seen_at: datetime
    model: str
    windows: Mapping[str, tuple[float, datetime]]
    limit: PlanLimit | None
    #: Claude refused the request for the plan.
    refused: bool
    #: The response was billed to the account's usage credits.
    billed: bool
    credits: Literal["on", "off"] | None

    @property
    def allowed(self) -> bool:
        """Whether the request ran on the plan, which says every limit that
        holds its model has reset."""
        return not (self.refused or self.billed)

    @classmethod
    def of(cls, raw: Mapping[str, Any], seen_at: datetime, model: str) -> PlanReading:
        windows: dict[str, tuple[float, datetime]] = {}
        for name, window in (raw.get("unifiedWindows") or {}).items():
            used, resets = (window or {}).get("utilization"), (window or {}).get("resetsAt")
            if name in METERED and isinstance(used, int | float) and isinstance(resets, int | float):
                windows[name] = (float(used), _at(resets))
        refused = raw.get("status") == "rejected"
        billed = raw.get("isUsingOverage") is True or raw.get("overageInUse") is True
        kind, resets = raw.get("rateLimitType"), raw.get("resetsAt")
        limit = None
        if refused or billed:
            window = kind if kind in PLAN_LIMITS else None
            full = [(at, name) for name, (used, at) in windows.items() if used >= 1]
            if window is not None and isinstance(resets, int | float):
                limit = PlanLimit(window, _at(resets))
            elif full:
                at, name = max(full)
                limit = PlanLimit(name, at)
            else:
                limit = PlanLimit(window, seen_at + UNKNOWN_RESET_HOLD, known=False)
        overage, disabled = raw.get("overageStatus"), raw.get("overageDisabledReason")
        credits: Literal["on", "off"] | None = None
        if overage in ("allowed", "allowed_warning"):
            credits = "on"
        elif overage == "rejected" and disabled and disabled not in _UNKNOWN_CREDITS:
            credits = "off"
        return cls(seen_at, model, windows, limit, refused, billed, credits)


def _at(epoch: float) -> datetime:
    return datetime.fromtimestamp(epoch, UTC)


class PlanStopped(ClaudeTurnFailed):
    """The Claude plan's ``window`` limit is used up until ``until``, so the
    turn stopped (``plan_stopped``): Claude refused its request, or the
    plan gate stopped it before the next one. Both are None where Claude
    said no reset. ``credits_billed``: requests were billed to the account's
    usage credits first. The detail names no reset as the CLI words it, nor
    an epoch: the app tells the user the time in their own zone."""

    def __init__(
        self, window: PlanWindow | None = None, until: datetime | None = None, *, credits_billed: bool = False,
    ) -> None:
        super().__init__("plan_stopped", f"the Claude plan's {window} limit is used up" if window else
                         "the Claude plan's limit is used up")
        self.window = window
        self.until = until
        self.credits_billed = credits_billed


class ClaudeCodeTarget(BaseChatModel):
    """The runtime target a ``claude-code`` row resolves to.

    Calling it raises, so a path that would send the turn's model an API
    request fails at once instead of reaching another model in its place.
    """

    model: str
    effort: str | None = None

    @property
    def _llm_type(self) -> str:
        return SDK

    def _generate(self, *args: Any, **kwargs: Any) -> ChatResult:
        raise RuntimeError(f"{self.model} runs through the Claude Code runtime, not as a chat model call")


def last_reply_index(messages: Sequence[BaseMessage]) -> int | None:
    """Where the thread's last reply is. A task resume the steering made for
    the turn (``lc_source`` ``steering``) is no model's reply."""
    return next(
        (i for i in range(len(messages) - 1, -1, -1)
         if isinstance(m := messages[i], AIMessage) and (m.additional_kwargs or {}).get("lc_source") != "steering"),
        None,
    )


def _last_reply(messages: Sequence[BaseMessage]) -> AIMessage | None:
    at = last_reply_index(messages)
    return messages[at] if at is not None else None


def another_model_replied_last(messages: Sequence[BaseMessage]) -> bool:
    """Whether the thread's last reply is another model's, which Claude's
    session does not hold: it would carry on without that reply and
    everything before it that the other model saw."""
    last = _last_reply(messages)
    return last is not None and MESSAGE_ID_KEY not in last.response_metadata


def claude_replied_last(messages: Sequence[BaseMessage]) -> bool:
    """Whether the thread's last reply is Claude Code's, whatever model the
    thread names: one following the account default holds none."""
    last = _last_reply(messages)
    return last is not None and MESSAGE_ID_KEY in last.response_metadata


def is_claude_code_model(name: object) -> bool:
    """Whether ``name`` is a catalog row the Claude Code runtime runs."""
    from src.llms.llm import LLM

    mc = LLM.get_model_config()
    entry = mc.get_model_config(name) if isinstance(name, str) and name else None
    return bool(entry) and mc.get_provider_info(entry.get("provider", "")).get("sdk") == SDK


def rows_available() -> bool:
    """Whether this deployment offers Claude Code rows at all: the operator
    turned the feature on, the SDK extra is installed, and the at-rest key is
    set, since it keys the device Claude Code reports on every turn and
    without it no computer can run one: the backend's own CLI needs it for
    the saved token, and an own computer for the SSH key it is reached with.

    Where a turn on one may run is the turn's computer's question: a user's
    own computer runs the CLI itself (``backend_available`` aside), a hosted
    sandbox only where the backend does.
    """
    from src.config.features import is_feature_enabled_system
    from src.server.database.encryption import encryption_configured

    return (
        is_feature_enabled_system(FEATURE)
        and importlib.util.find_spec("claude_agent_sdk") is not None
        and encryption_configured()
    )


def backend_available() -> bool:
    """Whether the CLI may run on the backend's own machine, on the setup
    token each user saves in Settings: only on a self-hosted server, whose
    backend is the user's own.

    The same holds for relaying that token to a user's own computer that has
    none of its own.
    """
    from src.config.settings import HOST_MODE

    return rows_available() and HOST_MODE == "oss"


def is_token_text(text: str) -> bool:
    """Whether ``text`` can be a setup token: one line of printable text with
    no spaces, the one rule a paste, a machine's write and a relay share."""
    return bool(text) and text.isprintable() and not any(c.isspace() for c in text)


def token_check_model() -> str:
    """The model id the token check asks for, as a turn on ``TOKEN_CHECK_ROW`` would."""
    from src.llms.llm import LLM

    return LLM.get_model_config().get_model_config(TOKEN_CHECK_ROW)["model_id"]
