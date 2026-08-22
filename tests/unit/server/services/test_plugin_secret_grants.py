"""A plugin may reference a credential only if the user gave it that one.

Declaring a secret name is a request. It becomes a grant when the plugin
introduces the name (the wizard fills it) or when one of the plugin's own rows
already holds the reference from an earlier install. A name the vault holds for
anything else stays refused, because nothing on the way in would have shown the
user otherwise: the wizard lists only names the vault is missing, and the
exported document is the plugin's own text, not the row's.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.server.services.plugins.extension import (
    LangalphaExtension,
    materialize_binds,
    resolve_binds,
)
from src.server.services.plugins.grants import resolve_bind_grants
from src.server.services.plugins.mcp import McpEntryPlan
from src.server.services.plugins.package import (
    ValidatedPackage,
    pending_secret_declarations,
)

USER = "test-user-123"
PLUGIN_ID = "22222222-2222-2222-2222-222222222222"


def _extension(name: str = "POLYGON_API_KEY") -> LangalphaExtension:
    return LangalphaExtension(
        secrets=[
            {
                "name": name,
                "label": name,
                "bind": [{"server": "remote", "header": "Authorization"}],
            }
        ]
    )


def _remote_plan() -> McpEntryPlan:
    return McpEntryPlan(
        key="remote", name="remote", renamed=False, transport="http",
        config={"name": "remote", "transport": "http",
                "url": "https://example.com/mcp", "headers": {}},
    )


def _vault(
    held: list[str],
    referenced: set[str] | None = None,
    owned: set[str] | None = None,
):
    """The three vault facts a grant is decided from: what the user holds, what
    this plugin's rows reference, and what this plugin introduced."""
    return (
        patch(
            "src.server.services.plugins.grants.get_user_secret_names",
            new=AsyncMock(return_value=held),
        ),
        patch(
            "src.server.services.plugins.grants.list_plugin_referenced_secrets",
            new=AsyncMock(return_value=referenced or set()),
        ),
        patch(
            "src.server.services.plugins.grants.list_plugin_owned_secrets",
            new=AsyncMock(return_value=owned or set()),
        ),
    )


@pytest.mark.asyncio
async def test_a_name_the_plugin_introduces_is_granted():
    held, refs, owned = _vault([])
    with held, refs, owned:
        grants = await resolve_bind_grants(
            USER, _extension(), plugin_id=None
        )
    assert grants.granted == frozenset({"POLYGON_API_KEY"})
    assert grants.refused == ()


@pytest.mark.asyncio
async def test_a_credential_the_user_already_holds_is_refused():
    # The whole finding: without this the plugin's own endpoint receives a key
    # the user set up for something else, and no surface ever says so.
    held, refs, owned = _vault(["POLYGON_API_KEY"])
    with held, refs, owned:
        grants = await resolve_bind_grants(
            USER, _extension(), plugin_id=None
        )
    assert grants.granted == frozenset()
    assert grants.refused == ("POLYGON_API_KEY",)


@pytest.mark.asyncio
async def test_a_grant_carries_forward_through_an_update():
    # By install time the plugin created the secret, so on update the vault
    # holds it. What distinguishes that from the attack is that the plugin's
    # own row already references it.
    held, refs, owned = _vault(["POLYGON_API_KEY"], {"POLYGON_API_KEY"})
    with held, refs, owned:
        grants = await resolve_bind_grants(
            USER, _extension(), plugin_id=PLUGIN_ID
        )
    assert grants.granted == frozenset({"POLYGON_API_KEY"})


@pytest.mark.asyncio
async def test_a_grant_survives_having_reached_no_row():
    # The rows are not the only record. A plugin whose every entry was held
    # back at install has none of them, so the key its own wizard collected
    # would read afterwards as the user's key for something else, and the
    # plugin would be refused it on the upgrade that finally installs the
    # entry. The claim on the secret is what carries the grant instead.
    held, refs, owned = _vault(
        ["POLYGON_API_KEY"], set(), {"POLYGON_API_KEY"}
    )
    with held, refs, owned:
        grants = await resolve_bind_grants(
            USER, _extension(), plugin_id=PLUGIN_ID
        )
    assert grants.granted == frozenset({"POLYGON_API_KEY"})
    assert grants.refused == ()


@pytest.mark.asyncio
async def test_another_plugin_s_claim_grants_nothing():
    # The claim is per-plugin, so this plugin's queries return nothing for a
    # name a sibling introduced, and the refusal stands.
    held, refs, owned = _vault(["POLYGON_API_KEY"], set(), set())
    with held, refs, owned:
        grants = await resolve_bind_grants(
            USER, _extension(), plugin_id=PLUGIN_ID
        )
    assert grants.granted == frozenset()
    assert grants.refused == ("POLYGON_API_KEY",)


def test_validation_records_the_bind_without_writing_it():
    plan = _remote_plan()
    assert resolve_binds(_extension(), [plan])
    assert plan.config["headers"] == {}


def test_only_granted_binds_reach_the_config():
    granted, refused = _remote_plan(), _remote_plan()
    materialize_binds(_extension(), [granted], {"POLYGON_API_KEY"})
    materialize_binds(_extension(), [refused], set())
    assert granted.config["headers"]["Authorization"] == (
        "${vault:POLYGON_API_KEY}"
    )
    assert refused.config["headers"] == {}


def _package(secret: dict) -> ValidatedPackage:
    return ValidatedPackage(
        manifest={}, mcp_document=None, mcp_document_invalid=False,
        entry_plans=[], skill_plans=[],
        extension=LangalphaExtension(secrets=[secret]),
        diagnostics=[], dropped_files=[], content_hash="h",
    )


@pytest.mark.asyncio
async def test_the_bindings_step_asks_in_the_declaring_package_s_words():
    """A name is one vault slot but any number of requests for it.

    The merged blueprint catalog keys by name and keeps the first declarer's
    copy, so sourcing the wizard from there labelled this package's request
    with a builtin's, or with whichever plugin got installed first. Refusing
    the collision instead would refuse honest packages: reusing an upstream's
    obvious credential name is not an attack.
    """
    package = _package({
        "name": "X_BEARER_TOKEN",
        "label": "Acme relay token",
        "description": "Bearer token minted by the Acme relay console.",
        "bind": [{"server": "remote", "header": "Authorization"}],
    })
    with patch(
        "src.server.services.plugins.package.get_user_secret_names",
        new=AsyncMock(return_value=[]),
    ):
        pending = await pending_secret_declarations(USER, package)

    assert [s.name for s in pending] == ["X_BEARER_TOKEN"]
    assert pending[0].label == "Acme relay token"
    # Where the credential is injected is wiring, and the consent screen it
    # feeds neither renders nor asks about it.
    assert not hasattr(pending[0], "bind")


@pytest.mark.asyncio
async def test_a_credential_the_vault_already_holds_is_not_asked_for_again():
    package = _package({"name": "X_BEARER_TOKEN", "label": "Acme relay token"})
    with patch(
        "src.server.services.plugins.package.get_user_secret_names",
        new=AsyncMock(return_value=["X_BEARER_TOKEN"]),
    ):
        assert await pending_secret_declarations(USER, package) == []
