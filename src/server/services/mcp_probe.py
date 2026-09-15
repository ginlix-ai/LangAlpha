"""Credential-agnostic host-side probe of a remote MCP server.

One network routine for everything that asks a remote server what it offers
from this process: the OAuth refresh (a bearer from the token store), the
catalog discovery for header-auth and open servers (the row's own headers,
vault refs resolved), and the add form's check of an address nothing has
saved yet.

The SDK folds a 4xx on the handshake into a generic JSON-RPC error, so the
status that says *why* a server refused is read by a preflight ``initialize``
POST before the SDK session runs. That preflight is also the only place a
server's auth requirement is observable: a 401 with OAuth metadata behind it
is OAuth; a 401/403 without is some credential we were not given; a listing
that succeeds without credentials is open. A server that checks its key only
inside tool results looks open to every probe, and nothing here pretends
otherwise.

The single word every surface renders is computed here too, by
:func:`probe_verdict`. It needs the status, the challenge behind it and
whether a credential was sent, and this is the only place holding all three.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any, Literal, Mapping
from urllib.parse import urlsplit

import httpx2
from mcp.client import Client
from mcp.client.streamable_http import streamable_http_client

from src.server.models.mcp_server import (
    ProbeResult,
    ProbeTool,
    ProbeVerdict,
)
from src.server.services.mcp_identity import bounded_identity
from src.server.services.mcp_oauth.http import (
    OAuthHopBlocked,
    oauth_http_client,
    pinned_discovery_client,
    pinned_request,
)
from src.server.services.mcp_oauth.tokens import PROTOCOL_VERSION
from src.server.utils.egress_guard import (
    EgressBlockedError,
    pin_public_url,
    strip_configured_headers,
)

logger = logging.getLogger(__name__)

AuthRequirement = Literal["none", "credential", "oauth", "unknown"]

# The add form waits on this one; background discovery has the larger budget
# because nobody is watching it and a slow vendor is still worth caching.
PROBE_TIMEOUT_S = 12
DISCOVERY_TIMEOUT_S = 30

# Every probe this process makes shares one ceiling: the add form's single
# check, a plugin install's fan-out over an mcp.json that puts no bound on its
# own entry count, and background catalog discovery, which is one probe per row
# but one task per row of an import or a secret rotation. Concurrency alone
# bounds the sockets, so a caller with many endpoints bounds the count too.
#
# Background work takes the smaller lane first, so it can never hold the whole
# ceiling: a 100-row import drains at the lane's width while the add form,
# which somebody is watching, always finds a slot within one probe's duration.
# The lane is a share of the ceiling, not a second ceiling, so the socket
# count per worker is still MAX_CONCURRENT_PROBES whatever the mix.
MAX_CONCURRENT_PROBES = 8
MAX_BACKGROUND_PROBES = 6
_gate = asyncio.Semaphore(MAX_CONCURRENT_PROBES)
_background_lane = asyncio.Semaphore(MAX_BACKGROUND_PROBES)

_PREFLIGHT_HEADERS = {
    "Accept": "application/json, text/event-stream",
    "Content-Type": "application/json",
    "MCP-Protocol-Version": PROTOCOL_VERSION,
}
# The session id a server hands back is echoed straight into the DELETE that
# closes it, so it has to look like a header value first: visible ASCII only,
# per the MCP spec's own rule for the field.
_SESSION_ID_RE = re.compile(r"^[\x21-\x7e]{1,256}$")

_PREFLIGHT_BODY = (
    '{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"'
    + PROTOCOL_VERSION
    + '","capabilities":{},"clientInfo":{"name":"langalpha-probe","version":"1"}}}'
).encode()


@dataclass(frozen=True)
class ProbeOutcome:
    """What one probe learned. ``tools`` is already sanitized for caching.

    ``sent_credential`` is not decoration: a 401 means "connect this server"
    when we sent nothing and "your key is wrong" when we did, and the wire
    looks identical either way, so the verdict can only be read here.
    """

    ok: bool
    auth: AuthRequirement
    tools: list[dict[str, Any]] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)
    identity: Any | None = None
    error: str = ""
    http_status: int | None = None
    sent_credential: bool = False


def probe_verdict(
    outcome: ProbeOutcome, *, missing_secrets: Sequence[str] = ()
) -> ProbeVerdict:
    """Fold one probe into the single word every surface renders."""
    if missing_secrets:
        # Nothing was dialled: the headers name a vault entry with no value,
        # and sending the literal ref would come back as a rejected key.
        return "missing_secrets"
    if outcome.ok:
        return "ok_authed" if outcome.sent_credential else "ok"
    if outcome.auth == "oauth":
        return "oauth"
    if outcome.auth == "credential":
        return "credential_rejected" if outcome.sent_credential else "needs_credential"
    return "unreachable"


def missing_secrets_result(missing: Sequence[str]) -> "ProbeResult":
    """The verdict for headers that name a vault secret nobody has set.

    One spelling for every path that notices it, so the add form, discovery
    and the relay's log agree on what the user is being told to do.
    """
    return probe_result(
        ProbeOutcome(
            ok=False, auth="credential",
            error="missing vault secret(s): " + ", ".join(missing),
        ),
        missing_secrets=missing,
    )


# One wording for a header value that cannot be framed, whoever catches it:
# ``_describe`` below, when httpx refuses the value it was handed, and
# ``resolve_header_refs``, which refuses a resolved vault value before it gets
# that far. The vault stores what a PEM needs, so the rule is the sink's.
INVALID_HEADER_VALUE = (
    "a configured header value is not valid HTTP (control characters "
    "such as a trailing newline are not allowed)"
)


def rejected_header_result() -> "ProbeResult":
    """The verdict for a header this process refused to put on the wire."""
    return probe_result(
        ProbeOutcome(ok=False, auth="unknown", error=INVALID_HEADER_VALUE)
    )


def probe_result(
    outcome: ProbeOutcome,
    *,
    missing_secrets: Sequence[str] = (),
    include_tools: bool = False,
) -> ProbeResult:
    """The wire/storage shape of one probe, verdict computed here and nowhere else.

    ``include_tools`` is the ad-hoc route's preview. A stored verdict leaves
    them out: the row's tools are its cached snapshot's, and they outlive a
    probe that starts failing.
    """
    return ProbeResult(
        verdict=probe_verdict(outcome, missing_secrets=missing_secrets),
        tools=(
            [
                ProbeTool(
                    name=t.get("name", ""), description=t.get("description", "")
                )
                for t in outcome.tools
            ]
            if include_tools
            else []
        ),
        server_info=bounded_identity(outcome.identity),
        error=outcome.error,
        http_status=outcome.http_status,
        missing_secrets=list(missing_secrets),
        probed_at=datetime.now(UTC),
    )


async def effective_secrets_for_probe(
    user_id: str, workspace_id: str | None, names: Sequence[str]
) -> dict[str, str]:
    """The vault the form's refs resolve against: the user's, plus a workspace's
    the user owns when the form is that workspace's.

    Only ``names`` are decrypted: each row is a full S2K derivation, and the
    header-free probe a URL edit fires refers to none of them.
    """
    from fastapi import HTTPException

    from src.server.database.user_vault_secrets import get_user_secrets_decrypted
    from src.server.database.vault_secrets import get_effective_secrets
    from src.server.database.workspace import get_workspace

    if workspace_id:
        workspace = await get_workspace(workspace_id)
        if workspace is None or workspace.get("user_id") != user_id:
            raise HTTPException(status_code=404, detail="Workspace not found")
    if not names:
        return {}
    if workspace_id:
        return await get_effective_secrets(workspace_id, user_id, names)
    return await get_user_secrets_decrypted(user_id, names)


def _host(url: str) -> str:
    try:
        return urlsplit(url).hostname or url
    except ValueError:
        return url


# What a remote endpoint writes into an error is persisted on the row, logged,
# and served by the catalog API, so its length and shape are decided here rather
# than by the endpoint: one line, capped, with the values we sent it removed.
PROBE_ERROR_MAX_CHARS = 240
_TRUNCATION_MARKER = " [truncated]"
_REDACTED = "[redacted]"
_CONTROL_RUN_RE = re.compile(r"[\s\x00-\x1f\x7f]+")


def _bounded(text: str, limit: int = PROBE_ERROR_MAX_CHARS) -> str:
    """One capped line out of text whose length a remote endpoint chose."""
    # Sliced before the collapse: a multi-megabyte message should not be
    # rewritten in full only to be cut to a line.
    head = text[: limit * 4]
    flat = _CONTROL_RUN_RE.sub(" ", head).strip()
    if len(flat) <= limit and len(head) == len(text):
        return flat
    return flat[: limit - len(_TRUNCATION_MARKER)].rstrip() + _TRUNCATION_MARKER


def _scrub_sent_values(error: str, sent: Mapping[str, str]) -> str:
    """Blank the credentials this probe sent out of an error quoting them back.

    A server is free to echo the header it rejected, and what it answers with
    lands on the row; the scheme word of a ``Bearer``/``Basic`` value is dropped
    so the token half is caught on its own too.
    """
    secrets: set[str] = set()
    for value in sent.values():
        v = value.strip()
        if not v:
            continue
        secrets.add(v)
        scheme, _, rest = v.partition(" ")
        if scheme.lower() in ("bearer", "basic") and rest.strip():
            secrets.add(rest.strip())
    # Longest first: a value that contains another must not be half-replaced.
    for secret in sorted(secrets, key=len, reverse=True):
        error = error.replace(secret, _REDACTED)
    return error


def _describe(e: BaseException) -> str:
    """A one-line reason a user can act on, from whatever the stack raised."""
    if isinstance(e, BaseExceptionGroup):
        leaves = e.exceptions
        return _describe(leaves[0]) if leaves else "connection failed"
    text = str(e).strip()
    if isinstance(e, httpx2.ConnectTimeout):
        return "connection timed out"
    if isinstance(e, httpx2.ConnectError):
        return f"could not connect: {_bounded(text) or 'connection refused'}"
    if isinstance(e, httpx2.LocalProtocolError):
        # The message quotes the header it choked on, which on this path is the
        # resolved credential. This text is logged and stored on the row.
        return INVALID_HEADER_VALUE
    if isinstance(e, httpx2.ProtocolError):
        return "the server's answer was not valid HTTP"
    if isinstance(e, httpx2.HTTPError) and not text:
        return type(e).__name__
    return _bounded(text) or type(e).__name__


async def _oauth_metadata_behind(url: str) -> bool:
    """Whether RFC 9728/8414 discovery finds an authorization server for ``url``.

    The connect flow's own discovery, run for its verdict alone: it raises when
    no AS metadata resolves, which is the difference between a server that
    wants OAuth and one that wants a header we do not hold.
    """
    from src.server.services.mcp_oauth.connect import McpOAuthError, _discover

    try:
        async with oauth_http_client() as client:
            await _discover(client, url)
    except (McpOAuthError, OAuthHopBlocked):
        return False
    except Exception as e:  # noqa: BLE001, a hop that blew up is not metadata
        # Host only: a row's URL may carry a credential in its query.
        logger.info("[mcp_probe] OAuth discovery for %s failed: %s", _host(url), e)
        return False
    return True


async def _preflight(
    url: str, headers: Mapping[str, str]
) -> tuple[ProbeOutcome | None, AuthRequirement]:
    """POST one ``initialize`` and read the status the SDK would hide.

    Returns a terminal outcome when the handshake cannot proceed, else None and
    the auth requirement the answer implied.
    """
    send = {**_PREFLIGHT_HEADERS, **headers}
    async with oauth_http_client() as client:
        response = await pinned_request(
            client, "POST", url, headers=send, content=_PREFLIGHT_BODY
        )
        # A stateful server opened a session for that initialize, and the SDK
        # handshake that follows opens its own; left alone, every probe would
        # leave one behind until the server expires it.
        session = response.headers.get("mcp-session-id")
        if session and _SESSION_ID_RE.match(session):
            try:
                await pinned_request(
                    client, "DELETE", url, headers={**send, "mcp-session-id": session}
                )
            except Exception as e:  # noqa: BLE001, best effort by definition
                logger.debug(
                    "[mcp_probe] closing preflight session failed: %s", _describe(e)
                )
    status = response.status_code
    if status in (401, 403):
        auth: AuthRequirement = (
            "oauth" if await _oauth_metadata_behind(url) else "credential"
        )
        if auth == "oauth":
            what = "it wants an OAuth connection"
        elif headers:
            what = "it rejected the configured credentials"
        else:
            what = "it needs a credential"
        # The realm, when the server names one, is the only part of the
        # challenge a user can act on; the rest is for the OAuth client.
        realm = re.search(r'realm="([^"]{1,60})"', response.headers.get("www-authenticate", ""))
        detail = f' (realm "{realm.group(1)}")' if realm and auth != "oauth" else ""
        return (
            ProbeOutcome(
                ok=False, auth=auth, http_status=status,
                error=f"server answered HTTP {status}: {what}{detail}",
            ),
            auth,
        )
    if status >= 400:
        return (
            ProbeOutcome(
                ok=False, auth="unknown", http_status=status,
                error=f"server answered HTTP {status} to the MCP handshake",
            ),
            "unknown",
        )
    content_type = (response.headers.get("content-type") or "").lower()
    if content_type.startswith("text/html"):
        return (
            ProbeOutcome(
                ok=False, auth="unknown", http_status=status,
                error="not an MCP endpoint: the address answered an HTML page",
            ),
            "unknown",
        )
    return None, ("credential" if headers else "none")


async def probe_remote_server(
    url: str,
    headers: Mapping[str, str] | None = None,
    *,
    timeout_s: float = DISCOVERY_TIMEOUT_S,
) -> ProbeOutcome:
    """List a remote server's tools with exactly the headers given.

    Never raises: every failure is an outcome with an ``error`` a user can
    read and an ``auth`` verdict as far as the wire allowed one.
    """
    # Stripped once here, so the preflight, its DELETE and the SDK session all
    # see the same map: a row spelling ``host`` would otherwise put a second
    # one on the wire beside the pin, and a row spelling
    # ``MCP-Protocol-Version`` would win the merge below against the version
    # this probe's own handshake body announces.
    sent = strip_configured_headers(headers)
    outcome = await _probe(url, sent, timeout_s=timeout_s)
    return replace(
        outcome,
        error=_scrub_sent_values(outcome.error, sent),
        sent_credential=bool(sent),
    )


async def bounded_probe(
    url: str,
    headers: Mapping[str, str] | None = None,
    *,
    timeout_s: float = PROBE_TIMEOUT_S,
    background: bool = False,
) -> ProbeOutcome:
    """A probe under the process-wide concurrency ceiling: the one way onto the
    network from here.

    Background discovery comes through too, with the larger
    ``DISCOVERY_TIMEOUT_S`` and ``background=True``, which routes it through
    the reserved lane. It is one probe per row, but a bulk import or a rotated
    secret schedules one task per row, and a ceiling the fan-out does not share
    bounds nothing.

    The wait for the gate is inside the budget, not before it: a fan-out that
    fills the ceiling is exactly when a caller's deadline matters, and a queue
    outside it made ``timeout_s`` a promise about the network alone while the
    request hung for as long as the queue took.
    """
    try:
        async with asyncio.timeout(timeout_s):
            async with _background_lane if background else contextlib.nullcontext():
                async with _gate:
                    return await probe_remote_server(url, headers, timeout_s=timeout_s)
    except TimeoutError:
        # The same outcome a probe that ran and timed out returns: nothing was
        # learned about the server either way.
        return ProbeOutcome(
            ok=False, auth="unknown",
            error=f"{_host(url)} did not answer within {timeout_s:.0f}s",
        )


async def _probe(
    url: str, headers: dict[str, str], *, timeout_s: float
) -> ProbeOutcome:
    from ptc_agent.core.mcp_schema import client_identity
    from src.server.services.mcp_discovery import sanitize_discovered_tools

    try:
        async with asyncio.timeout(timeout_s):
            # Pinned inside the deadline, not before it: pinning resolves DNS
            # through an unbounded getaddrinfo, and a stalling resolver is the
            # one way a probe outruns the budget its caller was promised.
            #
            # One pin for the whole session. The SDK dials hostnames itself, so
            # a validation that merely precedes the connect leaves the
            # rebinding TOCTOU open; the pin has to travel with the requests,
            # which is what the transport inside pinned_discovery_client does.
            try:
                target = await pin_public_url(url)
            except EgressBlockedError as e:
                return ProbeOutcome(
                    ok=False, auth="unknown", error=f"blocked url: {e}"
                )
            except Exception as e:  # noqa: BLE001
                return ProbeOutcome(
                    ok=False, auth="unknown", error=f"invalid url: {e}"
                )
            terminal, auth = await _preflight(url, headers)
            if terminal is not None:
                return terminal
            async with pinned_discovery_client(target, headers=headers) as http_client:
                # The streams context manager IS the SDK's Transport protocol.
                transport = streamable_http_client(url, http_client=http_client)
                async with Client(transport) as client:
                    result = await client.list_tools(cache_mode="refresh")
                    # The handshake already asked who this is; read it here
                    # rather than reconnect later. Never raises.
                    identity = client_identity(client)
    except TimeoutError:
        return ProbeOutcome(
            ok=False, auth="unknown",
            error=f"{_host(url)} did not answer within {timeout_s:.0f}s",
        )
    except OAuthHopBlocked as e:
        return ProbeOutcome(ok=False, auth="unknown", error=_bounded(str(e)))
    except Exception as e:  # noqa: BLE001, the reason is the product here
        # Scrubbed here as well as on the outcome: a server that echoes the
        # header it rejected would otherwise put the credential in the log.
        reason = _scrub_sent_values(_describe(e), headers)
        logger.info("[mcp_probe] %s failed: %s", _host(url), reason)
        return ProbeOutcome(
            ok=False, auth="unknown", error=f"discovery failed: {reason}"
        )

    raw = [
        {
            "name": t.name,
            "description": t.description or "",
            "input_schema": t.input_schema or {},
        }
        for t in result.tools
    ]
    kept, skipped = sanitize_discovered_tools(raw)
    return ProbeOutcome(
        ok=True, auth=auth, tools=kept, skipped=skipped, identity=identity,
        http_status=200,
    )
