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
import json
import re
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any, Iterable, Literal, Mapping
from urllib.parse import unquote_plus, urlsplit

import httpx2
from mcp.client import Client
from mcp.client.streamable_http import streamable_http_client

from ptc_agent.core.mcp_sanitize import VAULT_REF_RE, looks_like_secret
from src.server.database.user_vault_secrets import get_user_secrets_decrypted
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

# A dial that never opened a connection sent nothing, so dialling again repeats
# nothing the server saw. A save waits on the probe's answer, and a route that
# drops a share of its connects would otherwise refuse a server a second try
# reaches; one more try, after this pause and inside the same budget.
CONNECT_RETRY_PAUSE_S = 0.5


def _never_connected(e: BaseException) -> bool:
    """Whether a failure happened before any request reached the server."""
    while isinstance(e, BaseExceptionGroup) and e.exceptions:
        e = e.exceptions[0]
    return isinstance(e, (httpx2.ConnectError, httpx2.ConnectTimeout))


@dataclass(frozen=True)
class ProbeOutcome:
    """What one probe learned. ``tools`` is already sanitized for caching.

    ``sent_credential`` is not decoration: a 401 means "connect this server"
    when we sent nothing and "your key is wrong" when we did, and the wire
    looks identical either way, so the verdict can only be read here.
    ``missing_secrets`` marks a probe that never dialled, because a header
    names a vault entry with no value.
    """

    ok: bool
    auth: AuthRequirement
    tools: list[dict[str, Any]] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)
    identity: Any | None = None
    error: str = ""
    http_status: int | None = None
    sent_credential: bool = False
    missing_secrets: tuple[str, ...] = ()


def probe_verdict(outcome: ProbeOutcome) -> ProbeVerdict:
    """Fold one probe into the single word every surface renders.

    A sign-in challenge in answer to a credential is that credential turned
    down: pointing the user at a sign-in would hide that the key they gave
    did not work, so a save and its preview both say it was rejected.
    """
    if outcome.missing_secrets:
        # Nothing was dialled: the headers name a vault entry with no value,
        # and sending the literal ref would come back as a rejected key.
        return "missing_secrets"
    if outcome.ok:
        return "ok_authed" if outcome.sent_credential else "ok"
    if outcome.auth == "oauth":
        return "credential_rejected" if outcome.sent_credential else "oauth"
    if outcome.auth == "credential":
        return "credential_rejected" if outcome.sent_credential else "needs_credential"
    return "unreachable"


# One wording for a header value that cannot be framed, whoever catches it:
# ``_describe`` below, when httpx refuses the value it was handed, and
# ``resolve_header_refs``, which refuses a resolved vault value before it gets
# that far. The vault stores what a PEM needs, so the rule is the sink's.
INVALID_HEADER_VALUE = (
    "a configured header value is not valid HTTP (control characters "
    "such as a trailing newline are not allowed)"
)


def vault_ref_names(headers: Mapping[str, str] | None) -> list[str]:
    """Every ``${vault:NAME}`` a header map refers to, first occurrence first."""
    names: list[str] = []
    for value in (headers or {}).values():
        names.extend(n for n in VAULT_REF_RE.findall(value or "") if n not in names)
    return names


class RejectedHeaderValue(ValueError):
    """A resolved header value this process will not put on the wire.

    Raised rather than dropped, for the reason a missing ref is: a configured
    header that silently goes missing comes back as a 401 and reads as the
    wrong key. Each caller turns this into the refusal its own surface renders.
    """

    def __init__(self, header: str) -> None:
        super().__init__(INVALID_HEADER_VALUE)
        self.header = header


def resolve_header_refs(
    headers: Mapping[str, str] | None, secrets: Mapping[str, str]
) -> tuple[dict[str, str], list[str]]:
    """Expand ``${vault:NAME}`` in header values; also the names that had no value.

    A header with a missing ref is dropped rather than sent with the literal
    ref string: a server told ``Bearer ${vault:X}`` answers 401 and the row
    would then read as "rejected the credentials" when the truth is that no
    credential was configured yet.
    """
    resolved: dict[str, str] = {}
    missing: list[str] = []
    for key, value in (headers or {}).items():
        names = VAULT_REF_RE.findall(value or "")
        unknown = [n for n in names if n not in secrets]
        if unknown:
            missing.extend(n for n in unknown if n not in missing)
            continue
        expanded = VAULT_REF_RE.sub(lambda m: secrets[m.group(1)], value or "")
        # This sink's own control-character rule, which storage no longer
        # applies for it: a trailing newline came with the paste rather than
        # with the key, while a break inside the value is a second header, and
        # there is no reading of that worth guessing at.
        expanded = expanded.rstrip()
        if "\r" in expanded or "\n" in expanded:
            raise RejectedHeaderValue(key)
        resolved[key] = expanded
    return resolved, missing


# Header names that carry a credential beside the ones ``looks_like_secret``
# reads as one: "key" anywhere in the name (its test wants the bare word), and
# the cookie or session header a session rides on.
_CREDENTIAL_HEADER_RE = re.compile(r"(?i)key|cookie|session")


def sends_credential(headers: Mapping[str, str] | None) -> bool:
    """Whether these headers carry a credential rather than only plain config.

    The one reading of it: the probe's own verdict and every surface built
    on it take it from here. Read off the configured values where there are
    any, since a vault ref still marks a secret there, and only off the
    headers that actually go out. ``X-Region: us`` is config, so a 401 in
    answer to it means nothing was offered, not that a key was turned down.
    """
    return any(
        VAULT_REF_RE.search(value or "")
        or looks_like_secret(name, value or "")
        or _CREDENTIAL_HEADER_RE.search(name)
        for name, value in strip_configured_headers(headers).items()
    )


def probe_result(
    outcome: ProbeOutcome, *, include_tools: bool = False
) -> ProbeResult:
    """The wire/storage shape of one probe, verdict computed here and nowhere else.

    ``include_tools`` is the ad-hoc route's preview. A stored verdict leaves
    them out: the row's tools are its cached snapshot's, and they outlive a
    probe that starts failing.
    """
    return ProbeResult(
        verdict=probe_verdict(outcome),
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
        missing_secrets=list(outcome.missing_secrets),
        probed_at=datetime.now(UTC),
    )


def _host(url: str) -> str:
    try:
        return urlsplit(url).hostname or url
    except ValueError:
        return url


def _scrub_url(text: str, url: str) -> str:
    """Cut the address we dialled back to its host inside a message quoting it.

    A row's URL may carry a credential in its path or query, and a message
    that quotes it is stored on the row, logged and served by the catalog API.
    """
    if not url:
        return text
    return text.replace(url, _host(url))


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


# Shorter than this, a value is as likely to be a word of the message as a
# credential, and blanking it would shred the reason. The vault redaction the
# command check runs draws the same line.
_MIN_SECRET_CHARS = 8


def _sent_secrets(
    url: str, headers: Mapping[str, str], vault_values: Iterable[str] = ()
) -> tuple[str, ...]:
    """Every value this dial puts on the wire that a server could quote back.

    A server echoes the part it rejected, not the field it sat in: a vault
    value spliced into a longer header (``acct=42;key=${vault:K}``), the half
    after any scheme word (``Token``, ``DeepL-Auth-Key``), one query value, or
    a path segment that reads as a token. Longest first, so a value inside
    another is never half-replaced.
    """
    candidates = [*headers.values(), *vault_values]
    try:
        parts = urlsplit(url)
    except ValueError:
        parts = None
    if parts is not None:
        for pair in parts.query.split("&"):
            value = pair.partition("=")[2]
            candidates += (value, unquote_plus(value))
        candidates += (s for s in parts.path.split("/") if looks_like_secret("", s))
    found: set[str] = set()
    for value in candidates:
        value = value.strip()
        rest = value.partition(" ")[2].strip()
        found.update(v for v in (value, rest) if len(v) >= _MIN_SECRET_CHARS)
    return tuple(sorted(found, key=len, reverse=True))


def _scrub_sent_values(text: str, url: str, secrets: Iterable[str]) -> str:
    """Cut the address we dialled to its host and blank every value we sent.

    The address goes first: a query value blanked inside it would leave the
    rest of the URL behind, where ``_scrub_url`` no longer matches it.
    """
    text = _scrub_url(text, url)
    for secret in secrets:
        text = text.replace(secret, _REDACTED)
    return text


def _describe(e: BaseException, url: str = "") -> str:
    """A reason a user can act on, from whatever the stack raised.

    ``url`` is the address dialled, cut back to its host wherever the message
    quotes it. Uncapped: the message may also quote a header value we sent,
    and a cap applied before that scrub could cut the value in half and leave
    a prefix no scrub matches, so callers cap with ``_bounded`` after it.
    """
    if isinstance(e, BaseExceptionGroup):
        leaves = e.exceptions
        return _describe(leaves[0], url) if leaves else "connection failed"
    text = _scrub_url(str(e).strip(), url)
    if isinstance(e, httpx2.ConnectTimeout):
        return "connection timed out"
    if isinstance(e, httpx2.ConnectError):
        return f"could not connect: {text or 'connection refused'}"
    if isinstance(e, httpx2.LocalProtocolError):
        # The message quotes the header it choked on, which on this path is the
        # resolved credential. This text is logged and stored on the row.
        return INVALID_HEADER_VALUE
    if isinstance(e, httpx2.ProtocolError):
        return "the server's answer was not valid HTTP"
    if isinstance(e, httpx2.HTTPError) and not text:
        return type(e).__name__
    return text or type(e).__name__


async def _oauth_metadata_behind(url: str, secrets: Iterable[str]) -> bool:
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
        logger.info(
            "[mcp_probe] OAuth discovery for %s failed: %s",
            _host(url), _scrub_sent_values(str(e), url, secrets),
        )
        return False
    return True


async def _preflight(
    url: str, headers: Mapping[str, str], credential: bool, secrets: Iterable[str]
) -> tuple[ProbeOutcome | None, AuthRequirement]:
    """POST one ``initialize`` and read the status the SDK would hide.

    Returns a terminal outcome when the handshake cannot proceed, else None and
    the auth requirement the answer implied. ``credential`` is whether the
    headers carry one (``sends_credential``), which is what a refusal means;
    ``secrets`` is the dial's ``_sent_secrets``, for what this logs.
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
                    "[mcp_probe] closing preflight session failed: %s",
                    _bounded(_scrub_sent_values(_describe(e, url), url, secrets)),
                )
    status = response.status_code
    if status in (401, 403):
        auth: AuthRequirement = (
            "oauth" if await _oauth_metadata_behind(url, secrets) else "credential"
        )
        if auth == "oauth":
            what = "it wants an OAuth connection"
        elif credential:
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
    if status == 429:
        return (
            ProbeOutcome(
                ok=False, auth="unknown", http_status=status,
                error="server answered HTTP 429: it is limiting requests, "
                "try again in a minute",
            ),
            "unknown",
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
    if (refusal := _handshake_refusal(response.content)) is not None:
        # Some servers check the key at the handshake and answer 200 with an
        # error in place of the handshake's result. The SDK then fails to
        # parse it and reports schema noise; the server's own words are what
        # the user can act on.
        auth = "credential" if credential else "unknown"
        return (
            ProbeOutcome(
                ok=False, auth=auth, http_status=status,
                error=f"the server refused the handshake: {refusal}",
            ),
            auth,
        )
    return None, ("credential" if credential else "none")


def _handshake_refusal(body: bytes) -> str | None:
    """The message of an error a server sent in answer to ``initialize``.

    Covers a JSON-RPC ``error`` and a ``result`` flagged ``isError`` (a tool
    result shape some servers reuse for a rejected key). None for a real
    handshake reply or anything unreadable. Uncapped, since the message may
    quote the key it refused: ``probe_remote_server`` caps after the scrub.
    """
    text = body[:8192].decode("utf-8", errors="replace")
    candidates = [text]
    # An SSE answer carries the message on its data lines.
    candidates += [
        line[5:].strip() for line in text.splitlines() if line.startswith("data:")
    ]
    for candidate in candidates:
        try:
            message = json.loads(candidate)
        except ValueError:
            continue
        if not isinstance(message, dict):
            continue
        error = message.get("error")
        if isinstance(error, dict):
            return str(error.get("message") or "no reason given")
        result = message.get("result")
        if isinstance(result, dict) and result.get("isError") is True:
            return _error_text(result.get("content"))
    return None


def _error_text(content: Any) -> str:
    """The readable part of an ``isError`` result's content."""
    texts = [
        block.get("text", "")
        for block in content if isinstance(block, dict)
    ] if isinstance(content, list) else []
    joined = " ".join(t for t in texts if isinstance(t, str)).strip()
    try:
        # Often a JSON document itself: {"error": {"message": "..."}}.
        inner = json.loads(joined)
    except ValueError:
        return joined or "no reason given"
    if isinstance(inner, dict):
        error = inner.get("error", inner)
        if isinstance(error, dict) and isinstance(error.get("message"), str):
            return error["message"].strip() or "no reason given"
        if isinstance(error, str):
            return error.strip() or "no reason given"
    return joined or "no reason given"


async def probe_remote_server(
    url: str,
    headers: Mapping[str, str] | None = None,
    *,
    timeout_s: float = DISCOVERY_TIMEOUT_S,
    credential: bool | None = None,
    vault_values: Iterable[str] = (),
) -> ProbeOutcome:
    """List a remote server's tools with exactly the headers given.

    Never raises: every failure is an outcome with an ``error`` a user can
    read and an ``auth`` verdict as far as the wire allowed one.
    ``credential`` is ``sends_credential`` of the headers as configured, for a
    caller that resolved vault refs out of them; None reads the headers given.
    ``vault_values`` are what those refs resolved to, which the headers alone
    cannot tell apart from the text around them.
    """
    # Stripped once here, so the preflight, its DELETE and the SDK session all
    # see the same map: a row spelling ``host`` would otherwise put a second
    # one on the wire beside the pin, and a row spelling
    # ``MCP-Protocol-Version`` would win the merge below against the version
    # this probe's own handshake body announces.
    sent = strip_configured_headers(headers)
    if credential is None:
        credential = sends_credential(sent)
    secrets = _sent_secrets(url, sent, vault_values)
    outcome = await _probe(
        url, sent, credential=credential, timeout_s=timeout_s, secrets=secrets
    )
    return replace(
        outcome,
        # The one cap every error takes, after the scrub: capped first, a
        # value straddling the cut would leave a prefix nothing matches.
        error=_bounded(_scrub_sent_values(outcome.error, url, secrets)),
        sent_credential=credential,
    )


async def bounded_probe(
    url: str,
    headers: Mapping[str, str] | None = None,
    *,
    timeout_s: float = PROBE_TIMEOUT_S,
    background: bool = False,
    credential: bool | None = None,
    vault_values: Iterable[str] = (),
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
                    return await probe_remote_server(
                        url,
                        headers,
                        timeout_s=timeout_s,
                        credential=credential,
                        vault_values=vault_values,
                    )
    except TimeoutError:
        # The same outcome a probe that ran and timed out returns: nothing was
        # learned about the server either way.
        return ProbeOutcome(
            ok=False, auth="unknown",
            error=f"{_host(url)} did not answer within {timeout_s:.0f}s",
        )


async def probe_configured(
    user_id: str,
    url: str,
    headers: Mapping[str, str] | None,
    *,
    extra_secrets: Mapping[str, str] | None = None,
    bearer: str | None = None,
    timeout_s: float = PROBE_TIMEOUT_S,
    background: bool = False,
) -> ProbeOutcome:
    """Probe ``url`` with a server's configured headers, vault refs resolved.

    The one sequence behind the add form's preview, the check a save waits
    on and a header row's discovery, so the three cannot read the same
    server differently. ``extra_secrets`` are values the same request is
    about to write, seen ahead of the stored ones and never stored here. A ref
    with no value, or a value that cannot go on the wire, answers without
    dialling.

    ``bearer`` is a sign-in's token, sent in place of the headers: once a
    server is signed in to, discovery and the relay send the token and
    nothing else, so a check that sent the headers too could pass on a key
    nothing will send, or refuse a row over a ref nothing reads.
    """
    if bearer is not None:
        return await bounded_probe(
            url,
            {"Authorization": f"Bearer {bearer}"},
            timeout_s=timeout_s,
            background=background,
            credential=True,
        )
    extra = dict(extra_secrets or {})
    refs = vault_ref_names(headers)
    names = [n for n in refs if n not in extra]
    stored = await get_user_secrets_decrypted(user_id, names) if names else {}
    values = {**stored, **extra}
    try:
        resolved, missing = resolve_header_refs(headers, values)
    except RejectedHeaderValue:
        return ProbeOutcome(ok=False, auth="unknown", error=INVALID_HEADER_VALUE)
    if missing:
        return ProbeOutcome(
            ok=False,
            auth="credential",
            error="missing vault secret(s): " + ", ".join(missing),
            missing_secrets=tuple(missing),
        )
    return await bounded_probe(
        url,
        resolved,
        timeout_s=timeout_s,
        background=background,
        credential=sends_credential(headers),
        vault_values=[values[n] for n in refs],
    )


async def _probe(
    url: str,
    headers: dict[str, str],
    *,
    credential: bool,
    timeout_s: float,
    secrets: tuple[str, ...],
) -> ProbeOutcome:
    from ptc_agent.core.mcp_schema import client_identity
    from src.server.services.mcp_discovery import sanitize_discovered_tools

    # The first attempt's failure, when it never connected and a second one
    # is under way: the reason to report if that one runs out of budget.
    unconnected: BaseException | None = None
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
                    ok=False, auth="unknown",
                    error=f"blocked url: {_scrub_url(str(e), url)}",
                )
            except Exception as e:  # noqa: BLE001
                return ProbeOutcome(
                    ok=False, auth="unknown",
                    error=f"invalid url: {_scrub_url(str(e), url)}",
                )
            while True:
                try:
                    terminal, auth = await _preflight(
                        url, headers, credential, secrets
                    )
                    if terminal is not None:
                        return terminal
                    async with pinned_discovery_client(
                        target, headers=headers
                    ) as http_client:
                        # The streams context manager IS the SDK's Transport
                        # protocol.
                        transport = streamable_http_client(url, http_client=http_client)
                        async with Client(transport) as client:
                            result = await client.list_tools(cache_mode="refresh")
                            # The handshake already asked who this is; read it
                            # here rather than reconnect later. Never raises.
                            identity = client_identity(client)
                    break
                except Exception as e:
                    if unconnected is not None or not _never_connected(e):
                        raise
                    unconnected = e
                    logger.info(
                        "[mcp_probe] %s did not connect (%s); dialling once more",
                        _host(url),
                        _bounded(_scrub_sent_values(_describe(e, url), url, secrets)),
                    )
                    await asyncio.sleep(CONNECT_RETRY_PAUSE_S)
    except TimeoutError:
        if unconnected is not None:
            # The second try ran out of budget, which says less than how the
            # first one failed.
            reason = _scrub_sent_values(_describe(unconnected, url), url, secrets)
            return ProbeOutcome(
                ok=False, auth="unknown", error=f"discovery failed: {reason}"
            )
        return ProbeOutcome(
            ok=False, auth="unknown",
            error=f"{_host(url)} did not answer within {timeout_s:.0f}s",
        )
    except OAuthHopBlocked as e:
        # The URL leads these messages. Capped on the way out, after every
        # scrub, since a half-URL is something nothing can match.
        return ProbeOutcome(ok=False, auth="unknown", error=_scrub_url(str(e), url))
    except Exception as e:  # noqa: BLE001, the reason is the product here
        # Scrubbed here as well as on the outcome: a server that echoes the
        # header it rejected would otherwise put the credential in the log.
        reason = _scrub_sent_values(_describe(e, url), url, secrets)
        logger.info("[mcp_probe] %s failed: %s", _host(url), _bounded(reason))
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
