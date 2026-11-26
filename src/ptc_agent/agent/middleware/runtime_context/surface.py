"""The surface grammar of ``ChatRequest.platform``, and the surfaces we name.

One module owns what a platform string means, because the turn row's
run-context pointer and ``envelope/surface_rules.md.j2`` both read it: the
pointer names the surface this turn arrived on, and the rules paragraph decides
whether langalpha has a line of its own for it. It has one only for the
surfaces it renders itself; any other surface states its rules on the request
(``ChatRequest.surface_rules``), written by the client that draws it.

It also owns which surfaces langalpha draws and which are chat apps, since
every reader that branches on that (the rules paragraph, steering, a
report-back's reminder, ShowWidget, an automation's delivery) has to agree.

The known set is documentation, not admission control. The caller sending a turn
owns its own surface vocabulary and ships on its own schedule, so a name this
build has never heard of still renders in the pointer. It is logged once, because
the failure it hides is a typo, not a new surface.
"""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    # Defined on the request model, which the agent package already imports
    # elsewhere: defining it here instead would make the request model import
    # the middleware package, whose import reaches back into that model.
    from src.server.models.chat import Device

logger = logging.getLogger(__name__)

# The surface names this build recognizes. Membership decides only whether an
# unrecognized name is logged; it is not what gives a surface its delivery
# rules, which come either from a built-in line or from the posting client.
KNOWN_SURFACES: frozenset[str] = frozenset(
    {
        "web",
        "market_view",
        "telegram",
        "slack",
        "discord",
        "feishu",
        "imessage",
    }
)

#: The surfaces langalpha draws itself, which ``surface_rules.md.j2`` carries a
#: line for. Every other named surface is a chat app, drawn by its gateway.
BUILT_IN_SURFACES: frozenset[str] = frozenset({"web", "market_view"})

#: The surfaces whose built-in line reads the device the turn was sent from.
#: The market_view chat is a narrow panel on any device, so only web's does.
DEVICE_SURFACES: frozenset[str] = frozenset({"web"})

#: Chat apps as the user knows them. A name not listed is still a chat app.
CHAT_APP_NAMES: dict[str, str] = {
    "slack": "Slack",
    "discord": "Discord",
    "telegram": "Telegram",
    "imessage": "iMessage",
    "feishu": "Feishu",
}


class Surface(NamedTuple):
    """A parsed platform string: the surface, the symbol it is scoped to, and
    the device it was sent from where its line reads one.

    Fields rather than one string because the contract is keyed on ``name``
    alone. The symbol travels as its own value so the pointer hands the model a
    ticker instead of a compound key it has to split.
    """

    name: str | None
    symbol: str | None
    device: Device | None = None

    def sent_from(self, device: Device | None) -> Surface:
        """This surface with the device the turn was sent from, kept only on a
        surface whose line reads it, so neither the rules key nor the line has
        to ask again which surfaces those are."""
        return self._replace(device=device if self.name in DEVICE_SURFACES else None)


def parse_surface(platform: str | None) -> Surface:
    """``Surface(name, symbol)`` from a platform string like ``market_view:AAPL``.

    The grammar is ``^[a-z_]+(:[A-Z0-9][A-Z0-9.-]*)?$`` (``ChatRequest.platform``):
    a surface name optionally parameterized by one ticker. Nothing is rejected
    here. The request model already validated the shape, and an unknown surface
    is a version skew rather than a bad turn.
    """
    if not isinstance(platform, str) or not platform.strip():
        return Surface(None, None)
    name, _, symbol = platform.strip().partition(":")
    name = name or None
    if name is not None and not is_known_surface(name):
        _note_unknown_surface(name)
    return Surface(name, symbol or None)


def is_known_surface(name: str | None) -> bool:
    """Whether this build recognizes the surface name."""
    return isinstance(name, str) and name in KNOWN_SURFACES


def split_surface(platform: str | None) -> tuple[str | None, str | None]:
    """The older name for :func:`parse_surface`, kept for callers outside this package."""
    name, symbol, _ = parse_surface(platform)
    return name, symbol


def is_chat_app(platform: str | None) -> bool:
    """Whether the turn came through a chat app's gateway rather than a
    surface langalpha draws (``web``, ``market_view:<SYMBOL>``). No surface (an
    automation, the phone app) is not one."""
    name = parse_surface(platform).name
    return name is not None and name not in BUILT_IN_SURFACES


def chat_app_name(platform: str | None) -> str | None:
    """The chat app the turn came from, as the user knows it, or None when it
    came from none (see :func:`is_chat_app`). An app this build does not list
    is named as its surface is."""
    if not is_chat_app(platform):
        return None
    name = parse_surface(platform).name
    return CHAT_APP_NAMES.get(name, name)


@lru_cache(maxsize=256)
def _note_unknown_surface(name: str) -> None:
    """Log an unrecognized surface once per process, per name.

    Debug rather than warning: a surface the caller added first is the expected
    order of events, so the line is for whoever goes looking after a reply came
    back shaped wrong. Bounded rather than unbounded, because the key arrives
    from a client and re-logging after an eviction is cheaper than a latch that
    grows without a ceiling.
    """
    logger.debug(
        "[Envelope] surface %r is not one this build names; the pointer still "
        "renders. Known surfaces: %s",
        name,
        ", ".join(sorted(KNOWN_SURFACES)),
    )
