"""Per-user Firecrawl accounts through Firecrawl for Platforms (Gateway).

Every account the integration creates bills to the deployment's Firecrawl
org and counts toward the integration's account cap. Each user gets one,
created when they accept Firecrawl's terms with credits to spend, so one
user's key can be rotated or revoked without touching anyone else's and a
sign-up that never pays creates none. Firecrawl requires its terms be put
to the user before the account is created and asks for no record of the
answer, so the account is that record: the relay never creates one, it asks. The account's email is
only a contact address, but Firecrawl treats it as the idempotency key:
provisioning the same alias twice returns the same account and key, which is
what makes a second accept, or two tabs accepting at once, harmless.

The key is held encrypted and handed only to the egress relay. It is never
returned to a client or written to a sandbox.
"""

import hashlib
import logging
import os
import re

from typing import Any

import httpx

from src.config import env
from src.server.database import firecrawl_accounts as db
from src.server.services import vendor_terms
from src.server.services.egress import AccountUnavailable, RelayError, RelayRejection

logger = logging.getLogger(__name__)

_TIMEOUT_S = 15.0
_RETRYABLE_STATUSES = frozenset({500, 502, 503, 504})
_FORBIDDEN_CODES = {
    "partner_account_cap_reached": ("cap_reached", False),
    "partner_provisioning_email_rejected": ("email_rejected", False),
    "partner_integration_suspended": ("suspended", False),
}
# Refusals the partner API will repeat, remembered so each call does not ask
# again: the integration's own (its account cap, a suspension) for every
# user, a rejected address for its user. A rejected platform key is
# remembered only briefly and answered as retryable, since one 401 can be a
# blip and would otherwise stop every new user's first call.
_REFUSAL_PREFIX = "firecrawl:provision_refused:v2:"
_INTEGRATION_WIDE = frozenset({"cap_reached", "suspended", "platform_key_rejected"})
_REFUSAL_TTL_S = {
    "cap_reached": 300,
    "suspended": 300,
    "email_rejected": 300,
    "platform_key_rejected": 30,
}
# A stored key is tried on an endpoint that costs nothing and answers a
# revoked key with a plain 401, unlike the MCP server the relay fronts.
_CREDIT_USAGE_URL = "https://api.firecrawl.dev/v2/team/credit-usage"
_RECHECK_PREFIX = "firecrawl:key_recheck:v1:user:"
_RECHECK_INTERVAL_S = 60
# Lowercase only: mail providers fold case, so two ids differing in case
# would otherwise share one account.
_SAFE_LOCAL_PART = re.compile(r"[a-z0-9]([a-z0-9._-]{0,62}[a-z0-9])?")
#: The form a digest alias takes, which no id is passed through as.
_DIGEST_LOCAL_PART = re.compile(r"h-[0-9a-f]{32}")


_client: httpx.AsyncClient | None = None


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(timeout=_TIMEOUT_S, trust_env=False)
    return _client


async def close_partner_client() -> None:
    global _client
    if _client is not None and not _client.is_closed:
        await _client.aclose()
    _client = None


def alias_email(user_id: str, domain: str) -> str:
    """The user's stable, unique contact address at our domain."""
    local = user_id
    if (
        ".." in local
        or not _SAFE_LOCAL_PART.fullmatch(local)
        or _DIGEST_LOCAL_PART.fullmatch(local)
    ):
        # Not usable as a mail local part (case, symbols, length): a digest
        # keeps the alias valid and stable. An id already shaped like one is
        # digested too, so no id is passed through as another's digest:
        # Firecrawl gives one address one account.
        local = "h-" + hashlib.sha256(user_id.encode()).hexdigest()[:32]
    return f"{local}@{domain}"


def _error_code(response: httpx.Response) -> str | None:
    try:
        error = response.json().get("error")
    except (ValueError, AttributeError):
        return None
    return error.get("code") if isinstance(error, dict) else None


def _unavailable(response: httpx.Response) -> AccountUnavailable:
    status = response.status_code
    code = _error_code(response)
    logger.warning("Firecrawl partner API answered %s (%s)", status, code)
    if status == 403 and code in _FORBIDDEN_CODES:
        name, retryable = _FORBIDDEN_CODES[code]
        return AccountUnavailable(name, f"Firecrawl refused: {code}", retryable=retryable)
    if status == 400:
        return AccountUnavailable("email_rejected", "Firecrawl rejected the account email")
    if status == 401:
        return AccountUnavailable(
            "platform_key_rejected", "Firecrawl rejected the platform key", retryable=True
        )
    if status == 429:
        return AccountUnavailable("rate_limited", "Firecrawl rate limit", retryable=True)
    return AccountUnavailable(
        "upstream_error",
        f"Firecrawl partner API error {status}",
        retryable=status in _RETRYABLE_STATUSES,
    )


async def _partner_post(path: str, body: dict) -> dict:
    try:
        response = await _get_client().post(
            f"{env.FIRECRAWL_PARTNER_BASE_URL}{path}",
            json=body,
            headers={"Authorization": f"Bearer {env.FIRECRAWL_PLATFORM_KEY}"},
        )
    except httpx.HTTPError as exc:
        raise AccountUnavailable(
            "upstream_error",
            f"Firecrawl partner API unreachable: {type(exc).__name__}",
            retryable=True,
        ) from exc
    if response.status_code != 200:
        raise _unavailable(response)
    try:
        payload = response.json()
        if isinstance(payload.get("apiKey"), str) and payload["apiKey"]:
            return payload
    except (ValueError, AttributeError):
        pass
    raise AccountUnavailable("upstream_error", "Firecrawl answered without a key")


async def _partner_account(user_id: str) -> tuple[str, dict]:
    if not env.FIRECRAWL_ACCOUNT_EMAIL_DOMAIN:
        raise AccountUnavailable(
            "not_configured", "FIRECRAWL_ACCOUNT_EMAIL_DOMAIN is not set"
        )
    email = alias_email(user_id, env.FIRECRAWL_ACCOUNT_EMAIL_DOMAIN)
    return email, await _partner_post("/partner/v1/accounts", {"email": email})


async def _provision(user_id: str) -> str:
    email, payload = await _partner_account(user_id)
    logger.info(
        "Provisioned Firecrawl account user_id=%s alreadyExisted=%s",
        user_id,
        payload.get("alreadyExisted"),
    )
    return await db.save_account(
        user_id,
        api_key=payload["apiKey"],
        contact_email=email,
        gateway_status=payload.get("gatewayStatus"),
    )


def warn_if_discovery_keyless(config: Any) -> None:
    """Say at startup when per-user accounts are on but the shared key is not.

    The host lists a built-in's tools once at startup, before any user
    exists, with the headers its ``mcp.json`` names, which for Firecrawl is
    ``FIRECRAWL_API_KEY``. Without a key Firecrawl lists only some of its
    tools (2 of the 7 offered, missing both catalog tools, checked
    2026-11-16), so the server is offered without the rest, though every
    call would go out on a per-user key.
    """
    if not env.FIRECRAWL_PLATFORM_KEY or os.getenv("FIRECRAWL_API_KEY"):
        return
    servers = getattr(getattr(config, "mcp", None), "servers", None) or []
    if any(s.enabled and s.auth == "firecrawl_account" for s in servers):
        logger.warning(
            "FIRECRAWL_PLATFORM_KEY is set but FIRECRAWL_API_KEY is not: the "
            "Firecrawl server's tools are listed at startup with "
            "FIRECRAWL_API_KEY, and without it Firecrawl lists only some of "
            "them, so the rest are not offered"
        )


def _refusal_keys(user_id: str) -> list[str]:
    return [_REFUSAL_PREFIX + "integration", _REFUSAL_PREFIX + "user:" + user_id]


async def _remembered_refusal(user_id: str) -> AccountUnavailable | None:
    from src.utils.cache.redis_cache import get_cache_client

    for cached in await get_cache_client().mget(_refusal_keys(user_id)):
        if isinstance(cached, dict) and cached.get("code"):
            return AccountUnavailable(
                cached["code"],
                f"Firecrawl refused: {cached['code']} (remembered)",
                retryable=bool(cached.get("retryable")),
            )
    return None


async def _remember_refusal(user_id: str, e: AccountUnavailable) -> None:
    from src.utils.cache.redis_cache import get_cache_client

    ttl = _REFUSAL_TTL_S.get(e.code)
    if ttl is None:
        return
    integration, user = _refusal_keys(user_id)
    key = integration if e.code in _INTEGRATION_WIDE else user
    await get_cache_client().set(key, {"code": e.code, "retryable": e.retryable}, ttl=ttl)


async def _require_credits(user_id: str) -> None:
    """Refuse to create an account for a user the auth service would not let
    spend: denied, or with less left than the cheapest paid call is held at,
    which admission would refuse straight after. With no auth service (a
    standalone install) every user passes."""
    from src.server.services.egress.firecrawl_meter import MIN_ESTIMATE, FirecrawlMeter
    from src.server.services.egress.metering import spendable_for_paid_calls
    from src.server.services.metered_charges import user_credits

    spendable = await spendable_for_paid_calls(user_id)
    if spendable is None:
        return
    if spendable <= 0:
        raise RelayRejection(
            402, RelayError.CREDITS_EXHAUSTED, "the user has no credits left for paid calls"
        )
    cheapest = user_credits(FirecrawlMeter(), MIN_ESTIMATE)
    if spendable < cheapest:
        raise RelayRejection(
            402,
            RelayError.COST_EXCEEDS_BALANCE,
            f"the cheapest paid call costs {cheapest:g} credits, more than the user has left",
        )


def firecrawl_configured() -> bool:
    """Whether any user can have a key: the platform key with the domain their
    accounts' addresses are made at, or else the deployment's shared one. A
    platform key alone selects per-user accounts, which no one can then get."""
    if env.FIRECRAWL_PLATFORM_KEY:
        return bool(env.FIRECRAWL_ACCOUNT_EMAIL_DOMAIN)
    return bool(os.getenv("FIRECRAWL_API_KEY"))


async def firecrawl_key_for(user_id: str) -> str:
    """The Firecrawl key the relay sends for this user.

    A stored key is used whatever the user's balance, since its calls are
    metered one by one. Without one the user is asked to accept Firecrawl's
    terms (``terms_required``), which creates the account. That is asked last,
    so a user who could not get one anyway (a remembered partner refusal, or
    no credits, a ``RelayRejection``) is not asked. Without a platform key every user
    shares the deployment's ``FIRECRAWL_API_KEY``, which is how a
    single-tenant install runs.
    """
    if not env.FIRECRAWL_PLATFORM_KEY:
        shared = os.getenv("FIRECRAWL_API_KEY")
        if not shared:
            raise AccountUnavailable("not_configured", "No Firecrawl key is configured")
        return shared
    stored = await db.get_account_key(user_id)
    if stored:
        return stored
    await _admit(user_id)
    # Read again inside the ask: an accept that landed since cleared the ask,
    # which asking now would put back.
    return await vendor_terms.ask(user_id, "firecrawl", lambda: db.get_account_key(user_id))


async def provision_firecrawl_account(user_id: str) -> None:
    """Create the user's Firecrawl account, once they accept its terms.

    A user who already has one is left as is. A refusal the partner API will
    repeat is remembered briefly, so neither the relay nor another accept
    asks it again. Without a platform key there is nothing to create.
    """
    if not env.FIRECRAWL_PLATFORM_KEY or await db.get_account_key(user_id):
        return
    await _admit(user_id)
    try:
        await _provision(user_id)
    except AccountUnavailable as e:
        await _remember_refusal(user_id, e)
        raise


async def recheck_firecrawl_key(user_id: str) -> None:
    """Replace the user's stored key if Firecrawl no longer accepts it.

    Run after a call's reply came back as a tool error. Firecrawl's MCP
    answers a revoked key that way, inside a 200, like any other failed tool,
    so the key is tried on the free credit-usage endpoint, whose 401 is
    unambiguous. Only then is the alias provisioned again, which answers the
    account's current key, and the row, the record that the user accepted
    the terms, is kept. The call that failed has been answered and stays
    failed; the next call sends the replacement. At most once a minute per
    user, since every error reply would otherwise probe. Never raises: it
    runs behind a call already answered.
    """
    if not env.FIRECRAWL_PLATFORM_KEY:
        return
    try:
        if not await _claim_recheck(user_id):
            return
        key = await db.get_account_key(user_id)
        if key is None or not await _refused_by_firecrawl(key):
            return
        await _replace_refused_key(user_id, key)
    except AccountUnavailable as e:
        logger.warning(
            "Firecrawl refused user_id=%s's key and could not give another: %s",
            user_id,
            e.code,
        )
    except Exception:  # noqa: BLE001 - see docstring
        logger.warning("Could not recheck user_id=%s's Firecrawl key", user_id, exc_info=True)


async def _claim_recheck(user_id: str) -> bool:
    from src.utils.cache.redis_cache import get_cache_client

    # Without Redis, no check: unthrottled, every error reply would probe.
    return bool(
        await get_cache_client().acquire_lock(
            _RECHECK_PREFIX + user_id, "1", _RECHECK_INTERVAL_S * 1000
        )
    )


async def _refused_by_firecrawl(key: str) -> bool:
    try:
        response = await _get_client().get(
            _CREDIT_USAGE_URL, headers={"Authorization": f"Bearer {key}"}
        )
    except httpx.HTTPError:
        return False
    return response.status_code == 401


async def _replace_refused_key(user_id: str, rejected: str) -> None:
    _, payload = await _partner_account(user_id)
    if payload["apiKey"] == rejected:
        logger.warning(
            "Firecrawl refused user_id=%s's key and holds no other for the account", user_id
        )
        return
    await db.replace_account_key(user_id, rejected=rejected, api_key=payload["apiKey"])
    logger.info("Replaced the Firecrawl key Firecrawl refused, user_id=%s", user_id)


async def _admit(user_id: str) -> None:
    if (refused := await _remembered_refusal(user_id)) is not None:
        raise refused
    await _require_credits(user_id)
