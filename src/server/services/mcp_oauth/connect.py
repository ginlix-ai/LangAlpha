"""Durable two-phase OAuth connect for user-level MCP servers.

Phase 1 (:func:`start_connect`, any worker): discovery + DCR via SDK helpers,
generate state + PKCE, persist the bridge record in Redis, return the
authorize URL. Phase 2 (:func:`complete_callback`, any worker): atomic
single-use claim of the state record, token exchange, encrypted bundle into
``user_mcp_oauth_connections``, best-effort host-side schema discovery.

Callback identity comes exclusively from ``state``; the post-connect redirect
is an allowlisted relative path.
"""

from __future__ import annotations

import json
import logging
import secrets
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, NamedTuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import httpx2
from pydantic import BaseModel, ValidationError

from mcp.client.auth import OAuthFlowError, PKCEParameters
from mcp.client.auth.utils import (
    build_oauth_authorization_server_metadata_discovery_urls,
    build_protected_resource_metadata_discovery_urls,
    create_client_info_from_metadata_url,
    create_client_registration_request,
    extract_resource_metadata_from_www_auth,
    extract_scope_from_www_auth,
    get_client_metadata_scopes,
    handle_auth_metadata_response,
    handle_protected_resource_response,
    handle_registration_response,
    is_valid_client_metadata_url,
    should_use_client_metadata_url,
    validate_authorization_response_iss,
    validate_metadata_issuer,
)
from mcp.shared.auth import (
    OAuthClientInformationFull,
    OAuthClientMetadata,
    OAuthMetadata,
    ProtectedResourceMetadata,
)

from src.config.env import MCP_CLIENT_METADATA_URL
from src.server.database.egress_grants import apply_consent_to_active_grants
from src.server.database.mcp_oauth import Secrets, get_connection, upsert_connection
from src.server.database.mcp_servers import (
    bump_user_workspaces_mcp_version,
    get_catalog_server,
    list_catalog_servers,
)
from src.server.database.pool import get_db_connection
from src.server.database.workspace import get_running_workspace_ids_for_user
from src.server.services.brokerage_capabilities import group_keys_for, vendor_for_url
from src.server.services.brokerages import Brokerage, brokerage_for_url
from src.server.services.mcp_config import same_consented_url
from src.server.services.mcp_oauth.http import (
    OAuthHopBlocked,
    oauth_http_client,
    pinned_request,
    pinned_send,
)
from src.server.services.mcp_oauth.redirects import (
    DEFAULT_RETURN_TO,
    CallbackError,
    callback_is_loopback,
    callback_uri,
    redirect_to,
    sanitize_loopback_redirect,
    sanitize_return_to,
    sanitize_web_origin,
)
from src.server.services.mcp_oauth.tokens import (
    PROTOCOL_VERSION,
    TokenExchangeError,
    TokenFailure,
    build_context,
    exchange_token,
)

logger = logging.getLogger(__name__)

STATE_TTL_SECONDS = 600
_STATE_KEY_PREFIX = "mcp:oauth:state:"
_INFLIGHT_KEY_PREFIX = "mcp:oauth:inflight:"

CLIENT_NAME = "LangAlpha"

# The MCP endpoint probe advertises a protocol version so servers answer with
# era-appropriate WWW-Authenticate hints.
_PROBE_HEADERS = {
    "Accept": "application/json, text/event-stream",
    "MCP-Protocol-Version": PROTOCOL_VERSION,
}


class McpOAuthError(Exception):
    """A connect-flow step failed in a way the caller should surface."""


class McpServerNotFound(McpOAuthError):
    """No such server in this user's catalog — the router's 404."""


class McpServerMoved(McpOAuthError):
    """The row is no longer at the address the caller answered for.

    A connect can carry questions that were asked of a particular server --
    above all the one a vendor allowing a single connected AI platform per
    account forces, since agreeing to it costs the user a connection elsewhere.
    The page asks them against the address it drew the row from, and that row is
    the user's to edit from any tab. Move it in between and the connect the user
    agreed to and the one about to start are for different servers, with the
    warning belonging to neither. Refusing is what makes the question a gate
    rather than a decoration.
    """


@dataclass(frozen=True, slots=True)
class StartedConnect:
    """Phase 1's result.

    ``browser_nonce`` is cookie-only: it must never reach a JSON body, which
    would defeat HttpOnly and hand it to any XSS on the page. Keeping the
    result a record rather than a dict makes that a projection the router
    performs explicitly instead of a convention it remembers.
    """

    authorize_url: str
    state: str
    browser_nonce: str
    # The callback this flow was actually minted against, which is not always
    # the one the caller asked for: a redirect_uri that is not loopback, or that
    # this build does not understand, degrades to the hosted callback silently
    # and by design. A desktop shell has already armed a listener by the time it
    # finds out, so the effective value has to come back or it cannot tell a
    # flow that will reach it from one that never can.
    redirect_uri: str


class ConnectState(BaseModel):
    """The bridge record phase 1 parks in Redis and phase 2 claims.

    It crosses worker — and, across a deploy, build — boundaries as JSON, so it
    is validated on the way back in: a truncated or older-shaped record must
    fail like an expired one, not KeyError partway through the token exchange.
    Field names are the wire format; do not rename them.

    Only the presentation fields carry defaults. Everything the exchange
    depends on (identity, the PKCE verifier, the endpoints) is required,
    because a record missing any of it cannot produce a correct token request.
    """

    user_id: str
    server_name: str
    server_url: str
    code_verifier: str
    redirect_uri: str
    token_endpoint: str
    issuer: str
    resource: str | None = None
    scope: str | None = None
    client_info: dict[str, Any]
    as_metadata: dict[str, Any]
    resource_metadata: dict[str, Any] | None = None
    return_to: str = DEFAULT_RETURN_TO
    web_origin: str = ""
    # DCR confidential-client secret, carried out-of-band from client_info so it
    # never lands in the plaintext client_info JSONB column at persist. Empty
    # for public clients. Phase 2 re-attaches it for the token exchange and
    # stores it in its own encrypted column.
    client_secret: str = ""
    # High-entropy value mirrored into an HttpOnly cookie on the initiating
    # browser; the callback must present it back. Binds the callback to the
    # browser that started the flow so a stolen (state, code) pair replayed in
    # a victim's browser has no matching cookie and is refused. Defaulted so an
    # older-shaped record still validates (its callback simply skips the check).
    browser_nonce: str = ""
    # Capability groups consented to in phase 1, for a vendor that has them.
    # Carried across the browser round trip rather than re-read at the callback,
    # so the grant that gets written is the one the user actually agreed to and
    # not whatever a concurrent edit left on the row meanwhile. Defaulted so an
    # older-shaped record still validates.
    granted_capabilities: list[str] | None = None


def _cache_client():
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not (cache.enabled and cache.client):
        raise McpOAuthError("Redis is required for the OAuth connect flow")
    return cache.client


async def _try_hop(client, url: str) -> httpx2.Response | None:
    """One discovery GET; None when that URL is simply absent or unusable.

    A blocked hop is not a miss — it aborts discovery rather than falling
    through to the next candidate.
    """
    try:
        return await pinned_request(client, "GET", url, headers=_PROBE_HEADERS)
    except OAuthHopBlocked:
        raise
    except Exception as e:
        logger.info("[mcp_oauth] discovery hop %s failed: %s", url, e)
        return None


# So an issuer that names its default port compares equal to one that omits it.
_DEFAULT_PORTS = {"http": 80, "https": 443}


def _same_origin(a: str, b: str) -> bool:
    def origin(url: str) -> tuple[str | None, str | None, int | None]:
        parts = urlsplit(url)
        return parts.scheme, parts.hostname, parts.port or _DEFAULT_PORTS.get(
            parts.scheme
        )

    try:
        return origin(a) == origin(b)
    except ValueError:
        # A malformed port. Unequal is the answer that refuses the correction.
        return False


def _require_issuer_match(meta: OAuthMetadata, expected: str) -> None:
    """RFC 8414 §3.3, raised as something the route can report.

    The SDK's check raises ``OAuthFlowError``, a bare ``Exception`` that
    reaches the handler as a 500 with no detail — and an issuer mismatch is
    precisely the line whoever is wiring the server up needs to read.
    """
    try:
        validate_metadata_issuer(meta, expected)
    except OAuthFlowError as e:
        raise McpOAuthError(str(e)) from e


async def _fetch_as_metadata(
    client, identifier: str | None, server_url: str
) -> OAuthMetadata | None:
    """RFC 8414 discovery against one issuer identifier."""
    for url in build_oauth_authorization_server_metadata_discovery_urls(
        identifier, server_url
    ):
        resp = await _try_hop(client, url)
        if resp is None:
            continue
        keep_trying, meta = await handle_auth_metadata_response(resp)
        if meta is not None:
            return meta
        if not keep_trying:
            break
    return None


async def _resolve_as_metadata(
    client, advertised: str | None, server_url: str
) -> tuple[str | None, OAuthMetadata | None]:
    """AS metadata, plus the identifier its ``issuer`` is actually bound to.

    RFC 8414 §3.3 wants the two identical, and that binding is the control that
    stops a resource server from pointing at metadata some other authorization
    server published. A deployment can still name itself one way in its resource
    metadata and another in the AS document — the binding is intact, just held at
    the identifier the AS itself claims.

    So the mismatch is re-checked rather than waived: discovery runs again at the
    claimed identifier and the result must be self-consistent *there*, which is
    the same proof the first pass wanted. The origin gate is what keeps this from
    becoming a redirect — a document may correct its own name, never hand the
    flow to another host — and the returned identifier is the corrected one,
    since registration and the stored client are keyed on it.
    """
    meta = await _fetch_as_metadata(client, advertised, server_url)
    if meta is None or not advertised:
        return advertised, meta

    claimed = str(meta.issuer)
    if claimed == advertised or not _same_origin(claimed, advertised):
        _require_issuer_match(meta, advertised)  # raises on anything else
        return advertised, meta

    corrected = await _fetch_as_metadata(client, claimed, server_url)
    if corrected is None:
        # Nothing published there, so there is no second opinion to hold it
        # to and the original mismatch stands — named here rather than by the
        # generic issuer check, because which of the two hops came up empty is
        # the part that tells the operator where to look.
        raise McpOAuthError(
            f"{server_url} advertises {advertised}, whose metadata is issued by "
            f"{claimed}, which publishes no metadata of its own"
        )
    _require_issuer_match(corrected, claimed)
    logger.info(
        "[mcp_oauth] %s advertises %s but its metadata is issued by %s; "
        "using the issuer, which discovery confirms",
        server_url,
        advertised,
        claimed,
    )
    return claimed, corrected


async def _discover(client, server_url: str) -> tuple[
    ProtectedResourceMetadata | None, OAuthMetadata, str | None, str | None
]:
    """Run 401-probe → PRM → AS-metadata discovery. Returns
    (prm, as_metadata, auth_server_url, www_scope)."""
    www_auth_url: str | None = None
    www_scope: str | None = None
    # The probe is a hint source only — discovery can proceed without it.
    probe = await _try_hop(client, server_url)
    if probe is not None and probe.status_code == 401:
        www_auth_url = extract_resource_metadata_from_www_auth(probe)
        www_scope = extract_scope_from_www_auth(probe)

    prm: ProtectedResourceMetadata | None = None
    for url in build_protected_resource_metadata_discovery_urls(
        www_auth_url, server_url
    ):
        resp = await _try_hop(client, url)
        if resp is None:
            continue
        prm = await handle_protected_resource_response(resp)
        if prm is not None:
            break

    auth_server_url = (
        str(prm.authorization_servers[0]) if prm and prm.authorization_servers
        else None
    )

    auth_server_url, as_metadata = await _resolve_as_metadata(
        client, auth_server_url, server_url
    )

    if as_metadata is None:
        raise McpOAuthError(
            "No OAuth authorization server metadata found for this server "
            "(RFC 8414 discovery failed) — it may not support OAuth."
        )
    return prm, as_metadata, auth_server_url, www_scope


def _client_metadata_url() -> str | None:
    """This deployment's CIMD URL, or None when it is unset or unusable.

    An unusable value warns rather than raises: a typo here should cost the
    servers that would have read the document, not the ones that register
    dynamically and never look at it.
    """
    url = MCP_CLIENT_METADATA_URL.strip()
    if not url:
        return None
    if not is_valid_client_metadata_url(url):
        logger.warning(
            "[mcp_oauth] ignoring MCP_CLIENT_METADATA_URL: a client metadata "
            "document must be HTTPS on a non-root path"
        )
        return None
    return url


def _minimal_registration_request(
    as_metadata: OAuthMetadata,
    client_metadata: OAuthClientMetadata,
    auth_base_url: str,
) -> httpx2.Request:
    """The same registration, cut back to the members RFC 7591 defines.

    An AS is entitled to reject metadata it does not support, and it answers
    with one opaque ``invalid_client_metadata`` for the whole body rather than
    naming the member it disliked. There is nothing to negotiate against, so the
    only sound recovery is to stop sending everything optional at once.

    Two members are the likeliest offenders.

    ``application_type`` ships on every registration whether or not we want it.
    It is SEP-837, typed as a plain ``Literal`` with a ``"native"`` default
    rather than an optional, so ``exclude_none`` never drops it and there is no
    value meaning "unset" — an AS that does not know the member cannot be told
    to ignore it.

    ``scope`` is read by RFC 7591 as the set the client asks to be *allowed*,
    which is not the question the resource server's ``WWW-Authenticate``
    answers, and the MCP scope ladder feeds the second into the first. Dropping
    it widens nothing: the authorize request still carries the scope, so what
    the user is shown and approves still bounds the token.

    No shipped brokerage needs this path today. It is interop hardening against
    a refusal that cannot be diagnosed from the wire, not a fix for a known
    server.
    """
    body: dict[str, Any] = {
        "redirect_uris": [str(u) for u in (client_metadata.redirect_uris or [])],
        "grant_types": list(client_metadata.grant_types),
        "response_types": list(client_metadata.response_types),
    }
    # Kept because it is what the consent screen names as the thing being
    # granted access; a registration without it asks the user to trust a blank.
    if client_metadata.client_name:
        body["client_name"] = client_metadata.client_name
    if client_metadata.token_endpoint_auth_method:
        body["token_endpoint_auth_method"] = client_metadata.token_endpoint_auth_method
    url = (
        str(as_metadata.registration_endpoint)
        if as_metadata.registration_endpoint
        else urljoin(auth_base_url, "/register")
    )
    return httpx2.Request(
        "POST", url, json=body, headers={"Content-Type": "application/json"}
    )


class _Registration(NamedTuple):
    """A client identity, plus the scope to authorize with if it is not its own.

    Two values because they answer different questions and only one is stored.
    ``info`` is the registration and is persisted as ``client_info``, so its
    ``scope`` has to keep saying what the client is registered for -- that is
    what the next connect's coverage check reads. ``authorize_scope`` is what
    this authorize request should ask for, which on a reused registration is
    narrower. Folding them into one field wrote the narrowed value back over
    the registration's, and a later widening then failed a coverage check the
    original client would have passed, costing a redundant registration at the
    provider.
    """

    info: OAuthClientInformationFull
    authorize_scope: str | None = None


async def _register_client(
    client,
    *,
    user_id: str,
    server_name: str,
    as_metadata: OAuthMetadata,
    client_metadata: OAuthClientMetadata,
    auth_base_url: str,
) -> _Registration:
    """Identify this client to the AS: by hosted document where it takes one, else DCR.

    CIMD comes first and consults nothing stored. A URL-based client_id is one
    value for every user and every authorization server, so there is no
    registration to reuse, and a client_id left over from DCR against a server
    that has since advertised CIMD is precisely what must not be reused.

    The DCR path reuses a stored registration when it still fits: same issuer,
    and it already covers the redirect_uris we are about to send. That second
    half matters after a SERVER_BASE_URL change, where the stored registration
    carries the old callback and the AS rejects every authorize request with no
    path back in-product.
    """
    metadata_url = _client_metadata_url()
    if should_use_client_metadata_url(as_metadata, metadata_url):
        logger.info(
            "[mcp_oauth] %s reads a client metadata document; skipping DCR",
            server_name,
        )
        return _Registration(
            create_client_info_from_metadata_url(
                metadata_url,  # type: ignore[arg-type]
                redirect_uris=client_metadata.redirect_uris,
            )
        )
    existing = await get_connection(user_id, server_name, secrets=Secrets.FULL)
    if existing and existing.client_info:
        try:
            stored = OAuthClientInformationFull.model_validate(existing.client_info)
            if _is_metadata_document_id(stored.client_id):
                logger.info(
                    "[mcp_oauth] re-registering %s: the stored client id is a "
                    "metadata document and this server is asking for DCR",
                    server_name,
                )
            elif stored.client_id and existing.as_metadata.get("issuer") == str(
                as_metadata.issuer
            ):
                wanted = {str(u) for u in (client_metadata.redirect_uris or [])}
                registered = {str(u) for u in (stored.redirect_uris or [])}
                # A stored registration's scope overrides the one we just
                # computed (see ``effective_scope`` below), so reusing one that
                # predates a scope change silently undoes it. That is how a
                # connection kept being issued tokens without
                # ``offline_access`` after we started asking for it, and died
                # at the first expiry with nothing in the logs to say why. An
                # empty stored scope overrides nothing and is fine to reuse.
                wanted_scope = set((client_metadata.scope or "").split())
                stored_scope = set((stored.scope or "").split())
                covers_scope = not stored_scope or wanted_scope <= stored_scope
                if wanted <= registered and covers_scope:
                    # client_secret is stored encrypted, outside the JSONB blob.
                    stored.client_secret = existing.client_secret
                    # Authorize with the scope computed just now, not the one
                    # the registration was made with. ``effective_scope`` below
                    # prefers the stored value, so reusing a registration made
                    # back when we asked for more restored precisely the
                    # permissions the current metadata stopped asking for --
                    # the widening half of the same bug the scope check above
                    # closes in the narrowing direction. Asking for a subset of
                    # what is registered is always allowed. A computed scope
                    # that is empty has nothing to narrow to, and keeps the
                    # stored value as its only signal.
                    #
                    # Carried beside the registration rather than written into
                    # it, because ``stored`` is what gets persisted: overwriting
                    # its scope makes the next connect believe the client is
                    # registered for the narrower set and re-register the moment
                    # we ask for anything the original already covered.
                    return _Registration(stored, client_metadata.scope or None)
                logger.info(
                    "[mcp_oauth] re-registering %s: stored registration lacks "
                    "the current %s",
                    server_name,
                    "redirect_uri" if not wanted <= registered else "scope",
                )
        except Exception:
            logger.info(
                "[mcp_oauth] stored client_info for %s unusable; re-registering",
                server_name,
            )
    if as_metadata.registration_endpoint is None:
        raise McpOAuthError(
            "This authorization server does not support Dynamic Client "
            "Registration. If it advertises a client metadata document, set "
            "MCP_CLIENT_METADATA_URL to one this deployment serves."
        )
    request = create_client_registration_request(
        as_metadata, client_metadata, auth_base_url
    )
    response = await pinned_send(client, request)
    if response.status_code not in (200, 201) and await _rejected_our_metadata(response):
        logger.info(
            "[mcp_oauth] %s refused the full registration metadata (%s); "
            "retrying with the interoperable core",
            server_name,
            response.status_code,
        )
        request = _minimal_registration_request(
            as_metadata, client_metadata, auth_base_url
        )
        response = await pinned_send(client, request)
    return _Registration(await handle_registration_response(response))


def _is_metadata_document_id(client_id: str | None) -> bool:
    """Whether a stored client id came from CIMD rather than from registration.

    A CIMD client id is the metadata document's own URL; a DCR one is opaque
    and issued by the AS. Both land in the same column, so a server that
    advertised CIMD once and has since stopped would otherwise have that URL
    replayed as though it were a registration: a document satisfies the issuer,
    redirect and scope checks trivially, no registration ever happens, and the
    authorize request carries an identity the server no longer accepts.

    Read from the shape rather than compared against the configured metadata
    URL, because the document this deployment serves can move while an id
    minted from the old one is still sitting in the row.

    The shape is a heuristic, and it errs toward re-registering: an AS that
    issues a URL as its opaque DCR client_id gets a fresh registration on every
    reconnect rather than a reused one. Recording which mechanism produced the
    id is the fix that needs no guess, and it needs a column to keep it in.
    """
    return bool(client_id) and client_id.lower().startswith(("http://", "https://"))


async def _rejected_our_metadata(response) -> bool:
    """Whether a failed registration is worth retrying with less metadata.

    Only when the server said the metadata was the problem. Retrying on any
    non-2xx meant a 429 was answered by immediately spending a second request
    against the limit that produced it, and a 5xx that had already created the
    client created a second one and orphaned the first. Neither is fixed by
    sending different metadata, and both are reported honestly by letting the
    response fall through to ``handle_registration_response``, which raises
    carrying the real status.

    A 400 with no parseable error still retries: RFC 7591 names the codes
    below, but a server that refuses our optional members with an empty 400 is
    exactly the case this fallback exists for. The body is drained either way,
    since the response is discarded on the retry path.

    ``invalid_redirect_uri`` is not one of them. The minimal body carries the
    same ``redirect_uris`` as the full one, so retrying asks the identical
    question and spends a second registration attempt to be told the same
    thing. The AS's own error, which names what it rejected, is the more useful
    answer to surface.
    """
    body = await response.aread()
    if response.status_code not in (400, 422):
        return False
    try:
        parsed = json.loads(body or b"")
    except ValueError:
        parsed = None
    error = parsed.get("error") if isinstance(parsed, dict) else None
    if error is None:
        return response.status_code == 400
    return error == "invalid_client_metadata"


def _inflight_scope(server_name: str, server_url: str | None) -> str:
    """What a connect holds while it is out, and so what starting it retires.

    Normally the row: two connects for one server are the same connect twice,
    and the newer wins. A vendor that allows a single connected AI platform per
    account is wider than that -- the grant it drops on a new one is the
    account's, not the row's -- and nothing stops a user from owning two rows
    at that vendor, the shipped one beside their own or one repointed onto the
    other's host. Held by row, those two connects could not see each other and
    both spent their codes; the second grant displaced the first as it landed,
    leaving a row that still read connected with nothing live behind it. Held by
    vendor, the older flow is retired before it spends anything.

    A row name can never collide with the prefixed form: ``NAME_RE`` admits no
    colon, which is also why the ordinary case stays the bare name rather than
    growing a prefix of its own -- an in-flight connect keeps working across the
    deploy that introduces this.
    """
    vendor = brokerage_for_url(server_url)
    if vendor and vendor.exclusive_connection:
        return f"vendor:{vendor.name}"
    return server_name


async def _supersede_inflight(user_id: str, scope: str, state: str) -> None:
    """Leave this the only connect for the scope a callback can still finish.

    Nothing stopped a user from running two connects for the same server at
    once -- two tabs, or a browser tab and the desktop window -- and both would
    finish: each exchanged its own code, and the connection row kept whichever
    landed last. The row itself was fine, but the losing flow had by then been
    granted real access that we then held no record of, and we do not revoke at
    the vendor (see ``disconnect_server``). It stayed live on the brokerage
    account, absent from every screen we show, until it expired on its own. A
    vendor that allows one connected AI platform per account made it worse: the
    second grant displaced the first, so one connection cost the user their
    other platform twice.

    Dropping the older flow's state record moves its failure to before the token
    exchange, so the surplus access is never granted rather than granted and
    forgotten. The marker holds a specific single-use state value, so a delete
    can only ever spend the flow it names -- which is why it is left to expire
    on its own TTL rather than cleared by the callback. Clearing it there would
    buy nothing and would let a slow first callback retire the marker a second
    flow had already replaced.

    The delete alone reaches only a flow that has not claimed its state yet;
    ``_is_superseded`` is the other half, and covers the one that had.
    """
    redis = _cache_client()
    key = f"{_INFLIGHT_KEY_PREFIX}{user_id}:{scope}"
    async with redis.pipeline(transaction=True) as pipe:
        pipe.get(key)
        pipe.set(key, state, ex=STATE_TTL_SECONDS)
        previous, _ = await pipe.execute()
    if not previous:
        return
    superseded = previous.decode() if isinstance(previous, bytes) else previous
    if await redis.delete(f"{_STATE_KEY_PREFIX}{superseded}"):
        logger.info(
            "[mcp_oauth] superseded an in-flight connect user=%s scope=%s",
            user_id, scope,
        )


async def _is_superseded(user_id: str, scope: str, state: str) -> bool:
    """Whether a newer connect for this scope started while this one was out.

    Retiring the older flow's state record only reaches a flow that has not
    claimed it yet. One that had -- its callback was already past the claim when
    the newer connect began -- is beyond what a delete can stop, and would go on
    to exchange its code and upsert over the connection the newer flow is about
    to write. Reading the marker is what catches that flow, and it is read twice:
    once before the code is spent, so an already-retired flow never gets a grant
    issued, and again immediately before the write, because the first read
    happens a whole round trip to the vendor earlier than the write does.

    Two reads narrow the window; they do not close it. Closing it would need the
    ownership test and the write to be one atomic step, and they cannot be: the
    marker is in Redis and the connection is in Postgres, so no single
    transaction sees both.

    An absent marker allows the flow. It means the marker outlived nothing --
    both it and the state record carry the same TTL, so a claimed state with no
    marker beside it is a connect old enough to be answering for itself.
    """
    key = f"{_INFLIGHT_KEY_PREFIX}{user_id}:{scope}"
    current = await _cache_client().get(key)
    if not current:
        return False
    return (current.decode() if isinstance(current, bytes) else current) != state


async def _drop_what_the_vendor_displaced(
    user_id: str, server_name: str, vendor: Brokerage
) -> None:
    """Put this user's other rows at ``vendor`` where the vendor already put them.

    Retiring an in-flight connect covers two of them running at once. Two a week
    apart are not a race and never meet: the second is an ordinary connect, and
    the first row goes on reading connected over a grant the vendor dropped the
    moment the second one landed. Nothing on our side notices -- the sweeper
    keeps trying to refresh it, every workspace still inherits it, and it fails
    only at the point a turn actually calls the broker.

    Which rows those are is a question about the address, not the name: the
    shipped row and one the user pointed at the same host are both this vendor.

    Already-revoked rows are skipped rather than re-revoked, so an ordinary
    reconnect does not bump every workspace's config for a row nothing changed.
    """
    from src.server.database.mcp_oauth import ConnectionStatus
    from src.server.services.mcp_oauth.lifecycle import disconnect_server

    for row in await list_catalog_servers(user_id):
        name = row.get("name")
        if name == server_name or brokerage_for_url(row.get("url")) is not vendor:
            continue
        connection = await get_connection(user_id, name)
        if connection is None or connection.status == ConnectionStatus.REVOKED:
            continue
        await disconnect_server(user_id, name)
        logger.info(
            "[mcp_oauth] disconnected user=%s server=%s: %s displaced it at %s",
            user_id, name, server_name, vendor.name,
        )


def _consented_capabilities(
    server_url: str | None, requested: Sequence[str] | None
) -> list[str] | None:
    """The subset of a vendor's real capability groups the caller asked for.

    Keyed on the address, not the row's name, and the two disagree exactly where
    it matters: a row the user named themselves at a broker's host was offered
    that broker's consent dialog and then stored nothing, so its grant carried
    no policy at all. Whatever the relay will dial is what the question was
    about.

    Intersected rather than trusted: these keys arrive from a browser, and one
    we do not curate would be stored as consent to something with no meaning.
    Order is the display order, so the stored record reads the way the dialog
    that produced it did.

    None only for a server that has no groups, which is every server that is
    not a shipped brokerage. A brokerage always gets a list, empty included:
    asking for nothing is a decision, and it must not be recorded as the
    absence of one.

    A caller that names no selection at all is refused rather than read as
    either extreme. It is not the user declining everything -- nobody was asked
    -- and it cannot be read as granting everything. An older page or a script
    posting straight to this endpoint lands here, and a connector that says why
    it stopped beats one that silently connects to a broker that then does
    nothing.
    """
    available = group_keys_for(vendor_for_url(server_url))
    if not available:
        return None
    if requested is None:
        raise McpOAuthError(
            "this brokerage needs a capability selection before it can be "
            "connected; reload the page (or update the app) and try again"
        )
    wanted = set(requested)
    return [key for key in available if key in wanted]


async def start_connect(
    user_id: str,
    server_name: str,
    *,
    return_to: str | None = None,
    web_origin: str | None = None,
    loopback_redirect: str | None = None,
    expected_url: str | None = None,
    granted_capabilities: Sequence[str] | None = None,
) -> StartedConnect:
    """Phase 1: discovery + DCR + state/PKCE persist.

    ``expected_url`` is the address the caller drew the row from, and connecting
    is refused if the row has moved off it since. Optional because a caller that
    does not name one has nothing to be wrong about; see ``McpServerMoved``.
    """
    row = await get_catalog_server(user_id, server_name)
    if row is None:
        raise McpServerNotFound("MCP server not found")
    server_url = row.get("url")
    # http only: the generated sandbox client rejects legacy `sse` transport
    # (it never implemented the real GET→endpoint-event→POST flow), so an
    # sse-bound OAuth connection could never be used through the relay.
    if row.get("transport") != "http" or not server_url:
        raise McpOAuthError("OAuth connect requires a remote (http) MCP server")
    # Compared the way the callback compares its own record against the row, so
    # the same edit reads the same on both ends of the flow.
    if expected_url is not None and not same_consented_url(server_url, expected_url):
        raise McpServerMoved(
            "This server's address changed since the page was loaded"
        )

    # Before the first packet leaves, not at the end beside the record it
    # feeds. This raises for a caller that named no selection, and raising it
    # after ``_register_client`` meant an older page or a script posting
    # straight here had already created a DCR client at the vendor -- one
    # nothing then persisted, since the state record is what carries it, and
    # one more per retry. Nothing below depends on the answer, so there is no
    # reason for it to wait.
    consented = _consented_capabilities(server_url, granted_capabilities)

    # One value, used for the authorize URL, the registration metadata and the
    # state record alike — an AS is entitled to compare all three, and the token
    # exchange in phase 2 reads it back from that record.
    redirect_uri = sanitize_loopback_redirect(loopback_redirect) or callback_uri()
    async with oauth_http_client() as client:
        prm, as_metadata, auth_server_url, www_scope = await _discover(
            client, server_url
        )

        # Passing the grant types is what makes the SDK append ``offline_access``
        # (SEP-2207). Without it, an AS that gates refresh tokens on that scope
        # issues an access token that cannot be renewed, and the connection dies
        # at the first expiry with no way back except a fresh authorize.
        grant_types = ["authorization_code", "refresh_token"]
        scope = get_client_metadata_scopes(www_scope, prm, as_metadata, grant_types)
        client_metadata = OAuthClientMetadata(
            client_name=CLIENT_NAME,
            redirect_uris=[redirect_uri],  # type: ignore[list-item]
            grant_types=grant_types,
            response_types=["code"],
            token_endpoint_auth_method="none",
            scope=scope,
        )
        context = build_context(
            server_url,
            client_metadata=client_metadata,
            prm=prm,
            as_metadata=as_metadata,
            auth_server_url=auth_server_url,
        )
        client_info, authorize_scope = await _register_client(
            client,
            user_id=user_id,
            server_name=server_name,
            as_metadata=as_metadata,
            client_metadata=client_metadata,
            auth_base_url=context.get_authorization_base_url(server_url),
        )

    # The authorize URL is opened by the user's browser, not by us — but it
    # must still be a public HTTPS endpoint, or the flow becomes an open
    # redirector into private address space.
    from src.server.utils.egress_guard import pin_public_url

    authorize_endpoint = str(as_metadata.authorization_endpoint)
    try:
        await pin_public_url(authorize_endpoint, require_https=True)
    except Exception as e:
        raise McpOAuthError(f"Refusing authorization endpoint: {e}")

    pkce = PKCEParameters.generate()
    state = secrets.token_urlsafe(32)
    # Empty on a loopback callback: the record then takes the same skip path as
    # a pre-control record, so the verification logic needs no dev branch.
    #
    # This asks about the DEPLOYMENT, not about `redirect_uri`, and the two now
    # differ: a loopback override means the AS answers a listener on the user's
    # machine, which then drives that same browser to this deployment's own
    # callback — where the cookie set here is present exactly as it always was.
    # Deriving the skip from `redirect_uri` instead would drop the binding on
    # precisely the flows that can still honor it.
    browser_nonce = "" if callback_is_loopback() else secrets.token_urlsafe(32)

    params: dict[str, str] = {
        "response_type": "code",
        "client_id": client_info.client_id or "",
        "redirect_uri": redirect_uri,
        "state": state,
        "code_challenge": pkce.code_challenge,
        "code_challenge_method": "S256",
    }
    include_resource = context.should_include_resource_param(
        context.protocol_version
    )
    if include_resource:
        params["resource"] = context.get_resource_url()
    effective_scope = authorize_scope or client_info.scope or scope
    if effective_scope:
        params["scope"] = effective_scope
        # Refresh tokens hinge on offline_access for many providers; ask for
        # explicit consent so the grant is durable.
        if "offline_access" in effective_scope.split():
            params["prompt"] = "consent"

    record = ConnectState(
        user_id=user_id,
        server_name=server_name,
        server_url=server_url,
        code_verifier=pkce.code_verifier,
        redirect_uri=redirect_uri,
        token_endpoint=str(as_metadata.token_endpoint),
        issuer=str(as_metadata.issuer),
        resource=params.get("resource"),
        scope=effective_scope,
        # client_secret is excluded here and carried in its own field — see
        # ConnectState.client_secret. Keeping it out of this blob is what stops
        # a confidential secret from being written plaintext to the client_info
        # JSONB column when phase 2 persists the connection.
        client_info=client_info.model_dump(
            mode="json", exclude_none=True, exclude={"client_secret"}
        ),
        as_metadata=as_metadata.model_dump(mode="json", exclude_none=True),
        resource_metadata=(
            prm.model_dump(mode="json", exclude_none=True) if prm else None
        ),
        return_to=sanitize_return_to(return_to),
        web_origin=sanitize_web_origin(web_origin),
        browser_nonce=browser_nonce,
        client_secret=client_info.client_secret or "",
        granted_capabilities=consented,
    )
    redis = _cache_client()
    stored = await redis.set(
        f"{_STATE_KEY_PREFIX}{state}",
        record.model_dump_json(),
        nx=True,
        ex=STATE_TTL_SECONDS,
    )
    if not stored:
        raise McpOAuthError("state collision — retry the connect")
    # Only now, with the new flow's record parked: an error on the way here
    # leaves an older connect intact rather than retiring it for one that turned
    # out not to exist.
    await _supersede_inflight(user_id, _inflight_scope(server_name, server_url), state)

    # RFC 6749 §3.1 lets the authorization endpoint publish its own query
    # (tenant routing, etc.) and requires it be retained — appending with a bare
    # `?` would emit a second one and break the flow. Ours win on collision.
    endpoint_parts = urlsplit(authorize_endpoint)
    kept = [(k, v) for k, v in parse_qsl(endpoint_parts.query) if k not in params]
    authorize_url = urlunsplit((
        endpoint_parts.scheme,
        endpoint_parts.netloc,
        endpoint_parts.path,
        urlencode(kept + list(params.items())),
        endpoint_parts.fragment,
    ))
    logger.info(
        "[mcp_oauth] connect started user=%s server=%s issuer=%s",
        user_id, server_name, record.issuer,
    )
    return StartedConnect(
        authorize_url=authorize_url,
        state=state,
        browser_nonce=browser_nonce,
        redirect_uri=redirect_uri,
    )


async def _claim_state(state: str) -> ConnectState | None:
    """Atomic single-use claim: at most one callback wins a given state."""
    redis = _cache_client()
    key = f"{_STATE_KEY_PREFIX}{state}"
    async with redis.pipeline(transaction=True) as pipe:
        pipe.get(key)
        pipe.delete(key)
        raw, _ = await pipe.execute()
    if not raw:
        return None
    try:
        return ConnectState.model_validate_json(raw)
    except ValidationError as e:
        # The key is already consumed, so an unusable record is spent — same
        # outcome as an expired one, and the caller must not leak the shape.
        # Field locations only: the rendered ValidationError embeds the input it
        # rejected, and this record carries the DCR client secret.
        logger.warning(
            "[mcp_oauth] unusable state record discarded: %d invalid field(s): %s",
            e.error_count(),
            ", ".join(
                ".".join(str(part) for part in err["loc"]) for err in e.errors()
            ),
        )
        return None


async def _resync_live_sandboxes(user_id: str) -> None:
    """Re-apply the config on the user's running workspaces after a connect.

    The version bump alone only makes sessions re-resolve; the generated MCP
    client — which embeds the relay binding and drops the configured headers —
    is re-uploaded by the asset sync the apply drives. Without this a warm
    sandbox keeps dialing the vendor directly with the headers the connection
    just displaced. Best-effort, like the catalog and vault mutation paths: a
    failure here delays convergence to the next turn.
    """
    try:
        # Lazy: the scheduler lives in a router, and a service must not import
        # an app module at import time.
        from src.server.app.mcp_servers import _schedule_proactive_apply

        for workspace_id in await get_running_workspace_ids_for_user(user_id):
            _schedule_proactive_apply(workspace_id, user_id)
    except Exception:
        logger.warning(
            "[mcp_oauth] post-connect sandbox resync failed for user=%s",
            user_id, exc_info=True,
        )


async def complete_callback(
    *,
    state: str | None,
    code: str | None,
    iss: str | None = None,
    error: str | None = None,
    error_description: str | None = None,
    browser_nonce: str | None = None,
) -> str:
    """Phase 2: claim state, exchange the code, persist the bundle.

    Returns the redirect target for the browser — absolute when the start
    request captured a web origin, relative otherwise. Never raises for
    user-visible outcomes — errors are encoded in the redirect.

    ``browser_nonce`` is the value from the initiating browser's HttpOnly
    cookie; it must match the one minted into the state record, so a callback
    replayed in a different browser (which carries no such cookie) is refused.
    """
    if not state:
        return redirect_to(mcp_error=CallbackError.MISSING_STATE)
    record = await _claim_state(state)
    if record is None:
        # Unknown, expired, or already used — uniform answer, no oracle.
        return redirect_to(mcp_error=CallbackError.INVALID_STATE)

    server_name = record.server_name

    def _fail(reason: CallbackError) -> str:
        logger.warning(
            "[mcp_oauth] callback failed user=%s server=%s reason=%s",
            record.user_id, server_name, reason,
        )
        # Absolute when the start request carried a browser Origin (split-port
        # dev: the callback's own origin is the API, which has no UI routes);
        # relative otherwise, resolving on the unified proxy/prod origin.
        return redirect_to(
            record.return_to, record.web_origin,
            mcp_error=reason, server=server_name,
        )

    # Everything past here can still fail in a way nothing above it planned
    # for, and that failure is this flow's. The route that calls this is the
    # last resort and has nothing to name -- the state is spent by now, so it
    # cannot look the server back up -- and a redirect that names no server is
    # one the return path refuses to attribute while another connect is out.
    # That leaves the row this flow switched on standing with nothing behind
    # it, and its marker to be reported as an abandoned connect on some later
    # visit. So the promise in the docstring above is kept here rather than
    # left to the caller.
    async def _settle() -> str:
        # Recomputed rather than carried on the record: the URL it reads is the
        # consented one, pinned at phase 1, so both halves of the flow ask the
        # same question of the same address.
        scope = _inflight_scope(server_name, record.server_url)

        # CSRF binding: the state is single-use and now claimed, so a mismatch here
        # spends it (no retry oracle). An older-shaped record has an empty nonce and
        # skips the check — it predates this control and can't be forged into one.
        if record.browser_nonce and not secrets.compare_digest(
            record.browser_nonce, browser_nonce or ""
        ):
            return _fail(CallbackError.STATE_MISMATCH)

        # A record parked by a build that predates the consent field carries
        # None here, and None is not a selection -- it is a flow that never
        # asked. Stored, it becomes a connection granting none of the
        # brokerage's curated groups: connected, and able to do nothing. The
        # backfill cannot reach it either, since the connection row it would
        # have corrected does not exist yet. So it fails the way ConnectState
        # says an older-shaped record must, like an expired one, and the retry
        # goes through a phase 1 that asks the question.
        if record.granted_capabilities is None and group_keys_for(
            vendor_for_url(record.server_url)
        ):
            return _fail(CallbackError.INVALID_STATE)

        if error:
            # The AS reported denial/failure (user hit cancel, etc.).
            logger.info(
                "[mcp_oauth] authorization denied server=%s error=%s (%s)",
                server_name, error, error_description or "",
            )
            return _fail(
                CallbackError.DENIED
                if error == "access_denied"
                else CallbackError.PROVIDER_ERROR
            )
        if not code:
            return _fail(CallbackError.MISSING_CODE)

        # The last thing checked before the code is spent. A newer connect for this
        # pair may have started while this callback was in flight, and its supersede
        # cannot reach a state record this flow had already claimed -- so ask here
        # instead. Exchanging now would grant access that nothing on our side ends
        # up pointing at, which is the whole failure the marker exists to prevent.
        if await _is_superseded(record.user_id, scope, state):
            return _fail(CallbackError.INVALID_STATE)

        as_metadata = OAuthMetadata.model_validate(record.as_metadata)
        try:
            validate_authorization_response_iss(iss, as_metadata)
        except Exception:
            return _fail(CallbackError.ISSUER_MISMATCH)

        client_info = OAuthClientInformationFull.model_validate(record.client_info)
        # Re-attach the out-of-band secret (stripped from the blob at persist) so a
        # confidential client authenticates its token exchange below.
        if record.client_secret:
            client_info.client_secret = record.client_secret

        grant: dict[str, str] = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": record.redirect_uri,
            "client_id": client_info.client_id or "",
            "code_verifier": record.code_verifier,
        }
        if record.resource:
            grant["resource"] = record.resource

        try:
            token = await exchange_token(
                record.token_endpoint, grant, client_info=client_info
            )
        except TokenExchangeError as e:
            # Both hop-block kinds: a policy rejection is always the pre-send one,
            # so naming only the post-send kind here would report the case this
            # error exists for as a generic exchange failure.
            if e.kind in (TokenFailure.BLOCKED, TokenFailure.BLOCKED_PRE_SEND):
                logger.warning("[mcp_oauth] token hop blocked: %s", e)
                return _fail(CallbackError.BLOCKED_ENDPOINT)
            logger.warning("[mcp_oauth] token exchange for %s: %s", server_name, e)
            return _fail(CallbackError.TOKEN_EXCHANGE_FAILED)
        except Exception:
            logger.exception("[mcp_oauth] token exchange errored for %s", server_name)
            return _fail(CallbackError.TOKEN_EXCHANGE_FAILED)

        # The catalog row was validated in phase 1, up to STATE_TTL_SECONDS ago —
        # long enough for the user to delete or re-point the server mid-consent.
        # Persisting anyway would resurrect a connection with no catalog row behind
        # it: invisible to the UI, refreshed forever by the sweeper, and silently
        # inherited by a same-name recreate. The freshly exchanged token is dropped
        # on the floor here; it simply expires.
        catalog_row = await get_catalog_server(record.user_id, server_name)
        if catalog_row is None or not same_consented_url(
            catalog_row.get("url"), record.server_url
        ):
            return _fail(CallbackError.SERVER_CHANGED)

        # Asked once more, with nothing but local statements left between here and
        # the write. The read above happened before the token exchange, which is a
        # network round trip to the vendor and room enough for a second connect to
        # start -- and one starting there cannot retire a state this flow has
        # already claimed. Without this the loser exchanges and then writes last,
        # over the connection the winner just made.
        if await _is_superseded(record.user_id, scope, state):
            return _fail(CallbackError.INVALID_STATE)

        # The consent and the policy that enforces it are one decision, so they
        # commit together or not at all. Split across two transactions, a worker
        # that dies between them leaves the row recording a narrowing that no
        # grant enforces, and nothing later notices: the version bump below had
        # not run either, so a warm session short-circuits on a matching version
        # and never re-resolves.
        #
        # The consent write has to be here rather than left to the resync at the
        # end, because everything below only makes sessions re-resolve
        # *eventually* and the relay authorizes against the grant, not the
        # connection. A reconnect that narrows consent must not leave the wider
        # denial standing while a bump, a discovery round trip and a swallowed
        # re-apply catch up -- that window is exactly long enough for the turn
        # the user narrowed it for.
        #
        # The price is that a database failure discards a token exchange that
        # already succeeded and the user authorizes again. That is the
        # recoverable half of the trade; a withdrawn permission left live is
        # not.
        try:
            async with get_db_connection() as db, db.transaction():
                connection_id = await upsert_connection(
                    record.user_id,
                    server_name,
                    server_url=record.server_url,
                    access_token=token.access_token,
                    refresh_token=token.refresh_token,
                    client_secret=client_info.client_secret,
                    token_type=token.token_type,
                    scope=token.scope or record.scope,
                    expires_at=token.expires_at,
                    client_info=record.client_info,
                    as_metadata=record.as_metadata,
                    resource_metadata=record.resource_metadata,
                    granted_capabilities=record.granted_capabilities,
                    conn=db,
                )
                await apply_consent_to_active_grants(connection_id, conn=db)
        except Exception:
            logger.exception(
                "[mcp_oauth] could not settle consent for user=%s server=%s; "
                "neither the connection nor its grant policy was written",
                record.user_id, server_name,
            )
            return _fail(CallbackError.INTERNAL)

        logger.info(
            "[mcp_oauth] connected user=%s server=%s connection=%s has_refresh=%s",
            record.user_id, server_name,
            connection_id, token.refresh_token is not None,
        )

        # Before the bump below rather than after it, so a session re-resolving
        # on this row going live already sees whatever this grant cost the user
        # elsewhere, instead of picking up a broker that is about to be torn
        # down a moment later.
        #
        # Best effort: the grant is won and written by now, and failing the
        # redirect over a sibling we could not tidy would report a connect that
        # worked as an error. The row is left overstating itself, which is where
        # it already was.
        vendor = brokerage_for_url(record.server_url)
        if vendor and vendor.exclusive_connection:
            try:
                await _drop_what_the_vendor_displaced(
                    record.user_id, server_name, vendor
                )
            except Exception:
                logger.warning(
                    "[mcp_oauth] could not retire rows displaced by %s at %s",
                    server_name, vendor.name, exc_info=True,
                )

        # Sessions must re-resolve: the server is now relay-bound.
        await bump_user_workspaces_mcp_version(record.user_id)

        # Best-effort host-side discovery so tools show up immediately; failure
        # leaves a pending/error schema row, never a broken connection.
        try:
            from src.server.services.mcp_oauth.discovery import (
                refresh_user_tool_schemas,
            )

            await refresh_user_tool_schemas(record.user_id, server_name)
        except Exception:
            logger.warning(
                "[mcp_oauth] post-connect discovery failed for %s",
                server_name, exc_info=True,
            )

        # After discovery either way: a success lands its schemas first, and the
        # failure path is precisely the one that needs this — nothing was written to
        # the user tier, so the read falls back to the pre-connect snapshot and the
        # warm sandbox would otherwise never learn it is relay-bound.
        await _resync_live_sandboxes(record.user_id)

        return redirect_to(
            record.return_to, record.web_origin, mcp_connected=server_name
        )

    try:
        return await _settle()
    except Exception:
        logger.exception(
            "[mcp_oauth] callback errored user=%s server=%s",
            record.user_id, server_name,
        )
        return _fail(CallbackError.INTERNAL)
