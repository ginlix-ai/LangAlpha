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
from collections.abc import Sequence
from typing import Any, ClassVar

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
    ``auth_failed``, ``plan_exhausted``, ``interrupted``, ``api_error``,
    ``no_result``, ``nothing_to_resume``, ``policy_blocked``, ``needs_perl``,
    ``computer_unsupported``, or one of
    ``TEMPLATES``, which words it.
    """

    #: The failures the machine's Claude Code ends a turn with, in words a
    #: user reads; the detail takes the ``{}``.
    TEMPLATES: ClassVar[dict[str, str]] = {
        "model_unavailable": "Your organization's Claude Code policy does not allow {}. Pick another model.",
        "startup_failed": "Claude Code could not start: {}",
    }

    def __init__(self, kind: str, detail: str) -> None:
        template = self.TEMPLATES.get(kind)
        super().__init__(template.format(detail) if template else f"{kind}: {detail}")
        self.kind = kind
        self.detail = detail


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


def _last_reply(messages: Sequence[BaseMessage]) -> AIMessage | None:
    return next((m for m in reversed(messages) if isinstance(m, AIMessage)), None)


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
