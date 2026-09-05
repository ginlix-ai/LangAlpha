"""The brokerage connectors LangAlpha ships, and what makes each one unusual.

A brokerage is an ordinary user-tier MCP server with one thing held back from
the user: its endpoint. The address a broker's MCP server answers on is where
an OAuth token ends up, so it is named here in source rather than typed into a
form or seeded by a client — a stale build or a tampered page must not get to
answer "which host is Robinhood".

Everything past that first write is the generic catalog. Enabling one creates
the user's own row, which they can then edit, scope per workspace, disconnect,
or delete like any server they added themselves. Nothing here runs, and no row
exists, until someone turns one on.

The per-provider quirks are flags rather than sentences. The copy that explains
them is translated, so the wire carries the fact and the client owns the words.
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Brokerage:
    """One shipped brokerage connector.

    ``name`` is the catalog row name, so it is also the identity every other
    tier keys on: the OAuth connection, the egress grant, and the row the user
    ends up owning. It must satisfy the catalog's ``NAME_RE``.
    """

    name: str
    label: str
    url: str
    # The broker's website, which is where their logo is and is almost never
    # the endpoint's host: an MCP endpoint sits on an API subdomain that has no
    # page (agent.robinhood.com) or one with no icons (api.ibkr.com). Named
    # rather than derived from ``url``, because no amount of trimming labels
    # off api.ibkr.com produces interactivebrokers.com.
    site: str
    # Stored on the row, so it reaches the agent's prompt — describe the tools,
    # not the company.
    description: str
    # The authorization server allowlists only the RFC 8252 native-app profile
    # and refuses a hosted redirect_uri at the authorize step. The refusal is
    # silent by construction (the vendor renders its own error and never
    # redirects), so only the desktop shell's loopback listener can finish one.
    native_callback_only: bool = False
    # The provider permits one connected AI platform per account and drops the
    # previous one on a new grant, which makes connecting here destructive to a
    # connection the user may still be relying on somewhere else.
    exclusive_connection: bool = False


BROKERAGES: tuple[Brokerage, ...] = (
    Brokerage(
        name="robinhood",
        label="Robinhood",
        url="https://agent.robinhood.com/mcp/trading",
        site="robinhood.com",
        description=(
            "Robinhood brokerage account: balances, positions, order history, "
            "instrument lookup, and order placement."
        ),
        native_callback_only=True,
        exclusive_connection=True,
    ),
    Brokerage(
        name="ibkr",
        label="Interactive Brokers",
        url="https://api.ibkr.com/v1/api/mcp-public",
        site="interactivebrokers.com",
        description=(
            "Interactive Brokers account: portfolio, positions, account "
            "performance, market data, and draft orders the user confirms in "
            "IBKR before anything is placed."
        ),
        exclusive_connection=True,
    ),
    Brokerage(
        name="webull",
        label="Webull",
        url="https://api.webull.com/mcp",
        site="webull.com",
        description=(
            "Webull brokerage account: balances, positions and buying power, "
            "order history, watchlists, real-time quotes, and instrument "
            "reference data. Reads only -- nothing here places, previews or "
            "stages an order. Covers equities, options, futures, event "
            "contracts and crypto."
        ),
    ),
    Brokerage(
        name="moomoo",
        label="moomoo",
        url="https://mcp.moomoo.com/mcp",
        site="moomoo.com",
        description=(
            "moomoo brokerage account: balances, positions, and order history, "
            "real-time quotes and order book, option chains and volatility, "
            "company fundamentals and analyst research, stock and IPO "
            "screeners, and order placement. Covers US, Greater China, Japan "
            "and Southeast Asia."
        ),
    ),
)

_BY_NAME: dict[str, Brokerage] = {b.name: b for b in BROKERAGES}


def brokerage_by_name(name: str) -> Brokerage | None:
    """The shipped definition for a catalog name, or None if it is not ours."""
    return _BY_NAME.get(name)


def _host_of(url: str | None) -> str | None:
    if not url:
        return None
    try:
        return urlsplit(url).hostname
    except ValueError:
        return None


_BY_HOST: dict[str, Brokerage] = {
    host: b for b in BROKERAGES if (host := _host_of(b.url))
}


def brokerage_for_url(url: str | None) -> Brokerage | None:
    """The brokerage a server address belongs to, matched on host.

    Host and not the whole URL, the same join the client makes: the row is the
    user's to edit once it exists, and a sibling path on the vendor's host is
    still that vendor. The two answers have to agree, because the page draws a
    row wearing whatever this says it is and the connect enforces the same
    vendor's rules on it.

    Name is the wrong question here even though it is the identity everywhere
    else. A user can own two rows at one vendor -- the shipped row plus their
    own, or one repointed onto the other's host -- and the constraint that
    matters is the vendor's, not the row's.
    """
    host = _host_of(url)
    return _BY_HOST.get(host) if host else None


def brokerage_names() -> set[str]:
    """Names nothing else may take.

    A row under one of these is joined to the shipped definition by name and
    shown wearing it: the vendor's label, its tile, its description and its
    warnings. Whoever owns the row owns where Connect sends the user, so the
    name has to be reserved the way a builtin's is, or a plugin can install
    a server called ``robinhood`` pointing anywhere and be presented as
    Robinhood by a page that only ever asked its name.

    Reserving a name reaches writes, never rows that are already there. Adding a
    brokerage therefore takes a companion migration to move whatever holds the
    name aside, the way ``030_free_brokerage_names`` did for these two -- without
    one the reservation locks an existing row out of the only rule that could
    have repaired it.
    """
    return set(_BY_NAME)

