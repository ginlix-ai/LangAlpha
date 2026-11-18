"""The ``public_links`` rule: nothing turns a public link on, and a link
shared before stops opening for anyone but its owner while the policy stands,
then opens again unchanged once it is lifted. Nothing is read or asked on a
visitor's path when no policy is configured."""

from __future__ import annotations

from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from tests.unit.server.policy_standing import standing_under
from src.config.models import PolicyLayer
from src.server.database.artifacts import StoredVersionFile
from src.server.preview_proxy import artifacts as artifact_proxy
from src.server.services import file_grants
from src.server.services.artifacts import grants
from src.server.services.artifacts.grants import mint_artifact_share_grant
from src.server.services.policy import ALLOW_ALL, PUBLIC_LINKS, merge
from tests.unit.server.app.test_share_link_resolution import (
    CODE,
    OWNER,
    STRANGER,
    _client,
    _link,
    _versions,
    _workspace,
)

pytestmark = pytest.mark.asyncio

POLICY = "src.server.services.policy"
ACCESS = "src.server.app.share_access"
ARTIFACT = "11111111-2222-4333-8444-555555555555"

NO_LINKS = merge(None, PolicyLayer.model_validate({"public_links": False}))

THREAD = {
    "conversation_thread_id": "thread-fake-1",
    "workspace_id": "ws-fake-0001",
    "share_permissions": {"allow_files": True, "allow_download": True},
    "owner_id": OWNER,
    "title": "T",
}

HTML = b"<!doctype html><html><head></head><body>v</body></html>"
STORED = StoredVersionFile(
    user_id=OWNER,
    sha256="a" * 64,
    byte_len=len(HTML),
    content_type="text/html",
    content=HTML,
    owner_id=OWNER,
)


def _as_owner(thread_id, caller, **_):
    """The access check as it answers a thread's owner."""
    from src.server.services.shared_workspaces.access import Access

    return Access(caller)


@pytest.fixture(autouse=True)
def _keys(monkeypatch):
    monkeypatch.setattr(grants._OWNER, "_key", b"o" * 32)
    monkeypatch.setattr(grants._VISITOR, "_key", b"s" * 32)
    monkeypatch.setattr(file_grants._CODEC, "_key", b"k" * 32)



def _nothing_configured():
    """The teams flag off and no ``policy:``, with a cache that records any read."""
    cache, stack = MagicMock(), ExitStack()
    for p in (
        patch("src.server.services.account.is_feature_enabled_system", lambda key: False),
        patch("src.server.services.membership.is_feature_enabled_system", lambda key: False),
        patch("src.server.services.account.deployment_policy", return_value=None),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
    ):
        stack.enter_context(p)
    return cache, stack


# --- turning a link on -------------------------------------------------------


async def test_sharing_a_thread_is_refused_and_unsharing_is_not():
    from src.server.app.threads.sharing import update_thread_share
    from src.server.models.conversation import ThreadShareRequest

    update = AsyncMock(return_value={"is_shared": False, "share_token": "t"})
    with (
        standing_under(NO_LINKS),
        patch("src.server.services.shared_workspaces.access.thread_access", new_callable=AsyncMock, side_effect=_as_owner),
        patch(
            "src.server.app.threads.sharing.get_thread_by_id",
            new_callable=AsyncMock,
            return_value={"share_permissions": {}},
        ),
        patch("src.server.app.threads.sharing.update_thread_sharing", update),
    ):
        with pytest.raises(HTTPException) as refused:
            await update_thread_share("th", ThreadShareRequest(is_shared=True), "u")
        update.assert_not_awaited()
        stopped = await update_thread_share("th", ThreadShareRequest(is_shared=False), "u")
    assert refused.value.status_code == 403
    detail = refused.value.detail
    assert (detail["type"], detail["rule"], detail["set_by"]) == (
        "policy_denied", PUBLIC_LINKS, "organization",
    )
    assert stopped.is_shared is False


async def test_publishing_a_file_is_refused():
    from src.server.app.artifacts.models import PublishRequest
    from src.server.app.artifacts.records import publish_artifact

    publish = AsyncMock()
    with (
        standing_under(NO_LINKS),
        patch("src.server.app.artifacts.records.reached", new_callable=AsyncMock, return_value=(MagicMock(audience=None), _as_owner(None, "u"))),
        patch("src.server.app.artifacts.records.snapshot.publish", publish),
    ):
        with pytest.raises(HTTPException) as refused:
            await publish_artifact(ARTIFACT, PublishRequest(files=["a.html"]), "u")
    assert refused.value.status_code == 403
    assert refused.value.detail["rule"] == PUBLIC_LINKS
    publish.assert_not_awaited()


async def test_an_unknown_policy_refuses_sharing_as_unknown():
    from src.server.services.share_audience import refuse_public_link_of

    with standing_under(None), pytest.raises(HTTPException) as refused:
        await refuse_public_link_of("u")
    assert refused.value.status_code == 503


# --- a thread link -----------------------------------------------------------


@pytest.mark.parametrize("policy", [NO_LINKS, None], ids=["denied", "unknown"])
@pytest.mark.parametrize(
    "path", ["", "/replay", "/files", "/files/read?path=a.md", "/files/serve/a.html"]
)
async def test_a_shared_thread_answers_as_unshared_while_the_policy_stands(policy, path):
    replay = AsyncMock()
    with (
        standing_under(policy),
        patch(f"{ACCESS}.get_thread_by_share_token", AsyncMock(return_value=THREAD)),
        patch("src.server.app.public.get_thread_by_share_token", AsyncMock(return_value=THREAD)),
        patch("src.server.app.public.get_replay_thread_data", replay),
    ):
        async with _client(OWNER) as client:
            resp = await client.get(f"/api/v1/public/shared/share_abc123{path}")
    assert resp.status_code == 404
    replay.assert_not_awaited()


async def test_a_shared_thread_opens_again_once_the_policy_is_lifted():
    with (
        standing_under(ALLOW_ALL),
        patch(f"{ACCESS}.get_thread_by_share_token", AsyncMock(return_value=THREAD)),
        patch("src.server.app.public.get_thread_by_share_token", AsyncMock(return_value=THREAD)),
    ):
        async with _client(None) as client:
            resp = await client.get("/api/v1/public/shared/share_abc123")
    assert resp.status_code == 200
    assert resp.json()["thread_id"] == "thread-fake-1"


async def test_a_thread_row_without_its_owner_opens_for_no_one():
    thread = {k: v for k, v in THREAD.items() if k != "owner_id"}
    with (
        standing_under(ALLOW_ALL),
        patch(f"{ACCESS}.get_thread_by_share_token", AsyncMock(return_value=thread)),
        patch("src.server.app.public.get_thread_by_share_token", AsyncMock(return_value=thread)),
    ):
        async with _client(None) as client:
            resp = await client.get("/api/v1/public/shared/share_abc123")
    assert resp.status_code == 404


# --- a file link -------------------------------------------------------------


def _file_link():
    return (
        patch(f"{ACCESS}.get_link", AsyncMock(return_value=_link())),
        patch(f"{ACCESS}.db_get_workspace", AsyncMock(return_value=_workspace())),
        patch("src.server.app.public.artifacts_db", _versions()),
        patch("src.server.app.public.artifact_base", return_value=None),
    )


@pytest.mark.parametrize("viewer", [None, STRANGER])
@pytest.mark.parametrize("policy", [NO_LINKS, None], ids=["denied", "unknown"])
async def test_a_shared_file_link_is_not_found_for_a_visitor(viewer, policy):
    link, ws, db, base = _file_link()
    with link, ws, db, base, standing_under(policy) as asked:
        async with _client(viewer) as client:
            resp = await client.get(f"/api/v1/public/shared/{CODE}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Shared thread not found"
    asked.assert_awaited_once_with(OWNER)


async def test_the_owner_still_opens_their_link_and_as_a_visitor_does_not():
    link, ws, db, base = _file_link()
    with link, ws, db, base, standing_under(NO_LINKS):
        async with _client(OWNER) as client:
            own = await client.get(f"/api/v1/public/shared/{CODE}")
            as_visitor = await client.get(
                f"/api/v1/public/shared/{CODE}", params={"as": "visitor"}
            )
    assert own.status_code == 200 and own.json()["access"] == "owner"
    assert as_visitor.status_code == 404


async def test_a_shared_file_link_opens_again_once_the_policy_is_lifted():
    link, ws, db, base = _file_link()
    with link, ws, db, base, standing_under(ALLOW_ALL):
        async with _client(None) as client:
            resp = await client.get(f"/api/v1/public/shared/{CODE}")
    assert resp.status_code == 200
    assert resp.json()["access"] == "public"


# --- a visitor's version files ----------------------------------------------


async def _visitor_load(**kwargs):
    grant = await mint_artifact_share_grant(CODE, ARTIFACT, revision=3, seq=11)
    return await artifact_proxy.load_artifact_file(
        grant.token, "11", "index.html", host_artifact_id=ARTIFACT, **kwargs
    )


async def test_a_visitor_grant_minted_before_the_policy_opens_nothing_after(monkeypatch):
    monkeypatch.setattr(
        artifact_proxy, "get_shared_version_file", AsyncMock(return_value=STORED)
    )
    with standing_under(ALLOW_ALL):
        served = await _visitor_load()
    etag = dict(served.headers)["etag"]
    with standing_under(NO_LINKS) as asked:
        assert await _visitor_load() is None
        # Nor a 304 for a copy the browser kept.
        assert await _visitor_load(if_none_match=etag) is None
    assert [c.args for c in asked.await_args_list] == [(OWNER,), (OWNER,)]
    with standing_under(None):
        assert await _visitor_load() is None
    with standing_under(ALLOW_ALL):
        assert (await _visitor_load()).status == 200


async def test_the_owner_grant_is_not_asked(monkeypatch):
    from src.server.services.artifacts.grants import mint_artifact_grant

    monkeypatch.setattr(artifact_proxy, "get_version_file", AsyncMock(return_value=STORED))
    grant = await mint_artifact_grant(ARTIFACT)
    # An unknown policy serves no public link, so serving proves the owner's
    # grant never asks it.
    with standing_under(None):
        served = await artifact_proxy.load_artifact_file(
            grant.token, "11", "index.html", host_artifact_id=ARTIFACT
        )
    assert served.status == 200


# --- cost with nothing configured ------------------------------------------


async def test_nothing_configured_reads_nothing_on_a_visitor_path(monkeypatch):
    from src.server.app.share_access import get_shared_thread, resolve_link

    cache, nothing = _nothing_configured()
    monkeypatch.setattr(
        artifact_proxy, "get_shared_version_file", AsyncMock(return_value=STORED)
    )
    link, ws, _, _ = _file_link()
    with (
        nothing,
        link,
        ws,
        patch(f"{ACCESS}.get_thread_by_share_token", AsyncMock(return_value=THREAD)),
        patch("src.server.app.public.get_thread_by_share_token", AsyncMock(return_value=THREAD)),
    ):
        assert (await get_shared_thread("share_abc123"))["owner_id"] == OWNER
        assert (await resolve_link(CODE)).owner is False
        assert (await _visitor_load()).status == 200
    assert not cache.method_calls


async def test_the_rule_merges_strictly_and_is_reported():
    deployment = PolicyLayer.model_validate({"public_links": False})
    assert merge(deployment, PolicyLayer()).allows(PUBLIC_LINKS) is False
    assert merge(PolicyLayer(), PolicyLayer()).allows(PUBLIC_LINKS) is True
    assert merge(deployment, None).set_by(PUBLIC_LINKS) == "deployment"
    client = NO_LINKS.for_client()
    assert client["public_links"] is False
    assert client["set_by"] == {PUBLIC_LINKS: "organization"}
