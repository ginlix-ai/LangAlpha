"""``GET /api/v1/auth/cli``: how the ``la`` CLI learns to sign in, asked before
it holds any credential."""

from __future__ import annotations

from contextlib import ExitStack
from unittest.mock import patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from src.config import env, settings
from src.server.auth import oauth_resource
from src.server.services import cli_tokens

ISSUER = "https://issuer.example.test"
JWKS = f"{ISSUER}/oauth2/jwks"
AUDIENCE = "https://api.example.test"
CLI_CLIENT = "https://cli.example.test/client.json"
PLATFORM = "https://platform.example.test"


@pytest_asyncio.fixture
async def ask():
    """Asks the route under a server configured as the test says."""
    from src.server.app.users import router

    app = FastAPI()
    app.include_router(router)

    async def get(
        *,
        issuer: str = "",
        jwks: str = "",
        audiences: tuple[str, ...] = (),
        cli_client: str = "",
        platform: str = "",
        host_mode: str = "oss",
    ) -> dict:
        with ExitStack() as stack:
            for module, name, value in [
                (oauth_resource, "OAUTH_RESOURCE_ISSUER", issuer),
                (oauth_resource, "OAUTH_RESOURCE_JWKS_URL", jwks),
                (oauth_resource, "OAUTH_RESOURCE_AUDIENCES", audiences),
                (env, "OAUTH_RESOURCE_ISSUER", issuer),
                (env, "OAUTH_CLI_CLIENT_ID", cli_client),
                (env, "AUTH_SERVICE_URL", platform),
                (settings, "HOST_MODE", host_mode),
            ]:
                stack.enter_context(patch.object(module, name, value))
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                resp = await client.get("/api/v1/auth/cli")
        assert resp.status_code == 200
        return resp.json()

    return get


def _oauth(**more) -> dict:
    return {"issuer": ISSUER, "jwks": JWKS, "audiences": (AUDIENCE,), **more}


@pytest.mark.asyncio
async def test_it_names_the_issuer_and_client_when_both_are_configured(ask):
    body = await ask(**_oauth(cli_client=CLI_CLIENT))

    assert body["issuer"] == ISSUER
    assert body["client_id"] == CLI_CLIENT


@pytest.mark.asyncio
async def test_it_names_no_sign_in_without_the_cli_client_id(ask):
    body = await ask(**_oauth())

    assert body["issuer"] is None
    assert body["client_id"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "missing", ["issuer", "jwks", "audiences"]
)
async def test_it_names_no_sign_in_unless_the_oauth_resource_is_fully_configured(ask, missing):
    config = _oauth(cli_client=CLI_CLIENT)
    config[missing] = "" if missing != "audiences" else ()

    body = await ask(**config)

    assert body["issuer"] is None
    assert body["client_id"] is None


@pytest.mark.asyncio
async def test_a_bare_server_offers_neither_sign_in_nor_pats(ask):
    body = await ask()

    assert body["issuer"] is None
    assert body["client_id"] is None
    assert body["personal_access_tokens"] is False


@pytest.mark.asyncio
async def test_personal_access_tokens_follow_the_platform_address(ask):
    assert (await ask(platform=PLATFORM))["personal_access_tokens"] is True
    assert (await ask(platform=""))["personal_access_tokens"] is False


@pytest.mark.asyncio
async def test_personal_access_tokens_do_not_depend_on_the_oauth_sign_in(ask):
    body = await ask(**_oauth(cli_client=CLI_CLIENT))

    assert body["personal_access_tokens"] is False


@pytest.mark.asyncio
async def test_oss_asks_no_credential(ask):
    assert (await ask(host_mode="oss"))["auth_required"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("host_mode", ["platform", "enterprise"])
async def test_any_other_mode_asks_for_a_credential(ask, host_mode):
    assert (await ask(host_mode=host_mode))["auth_required"] is True


@pytest.mark.asyncio
async def test_it_has_the_cli_ask_for_the_api_scopes_where_the_token_carries_them(ask):
    """Without the platform, a token's ``scope`` claim is what it may do."""
    body = await ask(**_oauth(cli_client=CLI_CLIENT))

    assert body["scopes"] == list(cli_tokens.SCOPES)


@pytest.mark.asyncio
async def test_it_has_the_cli_ask_for_no_scopes_where_the_platform_grants_them(ask):
    body = await ask(platform=PLATFORM, **_oauth(cli_client=CLI_CLIENT))

    assert body["scopes"] == []


@pytest.mark.asyncio
async def test_it_answers_with_no_credential_even_where_one_is_required(ask):
    """The CLI asks this to learn how to sign in, so it cannot need one."""
    body = await ask(host_mode="platform", **_oauth(cli_client=CLI_CLIENT))

    assert body["auth_required"] is True
    assert body["issuer"] == ISSUER
