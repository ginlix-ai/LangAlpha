"""A `schedule_wakeup` call's arguments, read as the model sent them.

Models send a list serialized into a string as often as the list, and a bare
id as often as a list of one, so each is read as what it was meant to be.
``asks_change`` is the web's ``wakeupArgListed`` too, which folds a quiet turn
by the calls it sees, so the two must read an argument the same way.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, Optional

from src.tools.wakeups.wording import BUILD_TOOL_RESULTS, CANCEL_ARG_REFUSAL

TOP_ARGS = ("triggers", "cancel", "quiet")


def _json(text: str) -> Any:
    try:
        return json.loads(text)
    except ValueError:
        return text


def _listed(value: Any) -> Any:
    """A list sent as JSON text, decoded; anything else as it came."""
    if isinstance(value, str) and value.strip().startswith("["):
        return _json(value)
    return value


def parse_cancel(value: Any) -> list[str] | Literal["all"] | None:
    """One id, a list, "all", or a JSON-string list; None when it is none of those."""
    value = _listed(value)
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("["):  # list text that does not decode
            return None
        if text.lower() == "all":
            return "all"
        return [text] if text else None
    if not isinstance(value, list):
        return None
    ids: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            return None
        if item.strip().lower() == "all":
            return "all"
        if item.strip() not in ids:
            ids.append(item.strip())
    return ids


@dataclass
class Call:
    triggers: Optional[list[Any]] = None
    cancel: list[str] | Literal["all"] | None = None
    quiet: Optional[str] = None
    #: Argument problems, which refuse the whole call before anything runs.
    problems: list[str] = field(default_factory=list)


def parse_call(args: Mapping[str, Any]) -> Call:
    call = Call()
    call.problems = [
        BUILD_TOOL_RESULTS["unknown_arg"].format(k=k) for k in args if k not in TOP_ARGS
    ]
    triggers = args.get("triggers")
    # Any JSON text, not only list text, so ``"null"`` still reads as none.
    if isinstance(triggers, str):
        triggers = _json(triggers)
    if triggers is not None and not isinstance(triggers, list):
        call.problems.append(BUILD_TOOL_RESULTS["triggers_not_list"])
    else:
        call.triggers = triggers
    if args.get("cancel") is not None:
        cancel = parse_cancel(args.get("cancel"))
        if cancel is None:
            call.problems.append(CANCEL_ARG_REFUSAL)
        elif cancel != []:
            call.cancel = cancel
    quiet = args.get("quiet")
    if quiet is not None and (not isinstance(quiet, str) or not quiet.strip()):
        call.problems.append(BUILD_TOOL_RESULTS["quiet_not_text"])
    else:
        call.quiet = quiet
    return call


def asks_change(args: Any) -> bool:
    """Whether a call asks to set or cancel a wakeup, read from the call alone."""
    if not isinstance(args, Mapping):
        return False
    return any(_listed(args.get(key)) for key in ("triggers", "cancel"))
