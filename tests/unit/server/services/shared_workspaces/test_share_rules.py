"""Links from a shared workspace open to its organization's members only.

Its content is the holder's, so the audiences offered are the holder's: the
members one while the holder's organization allows ``teams``, never anyone.
A public link is refused whatever the member's own rules allow, a thread's
share and a file's publish alike, and a file's first publish takes the
members audience rather than being refused the default.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from src.server.database.shared_workspaces import SharedRow
from src.server.services import share_audience
from src.server.services.account_contract import Account, Org
from src.server.services.account import Standing
from src.server.services.policy import ALLOW_ALL
from src.server.services.share_audience import (
    ORGANIZATION,
    PUBLIC,
    SHARED_MEMBERS_ONLY,
    offered,
    refuse_public_link_of,
)
from src.server.services.shared_workspaces.access import Access, SharedAccess

pytestmark = pytest.mark.asyncio

HOLDER = "shared-00000000-0000-4000-8000-0000000000aa"
OWNER = "alice"
ORG = Account(org=Org(id="org-n", name="N"))
# Any member may share here; who may when its managers choose otherwise is
# pinned in ``test_share_out.py``.
SHARED = SharedRow("ws-1", HOLDER, "org-n", everyone=False, created_by=OWNER, share_out="members")


@pytest.fixture
def accounts(monkeypatch):
    monkeypatch.setattr(share_audience, "teams_door_open", lambda: True)
    monkeypatch.setattr(share_audience, "account_for", AsyncMock(return_value=ORG))
    monkeypatch.setattr("src.server.services.membership.teams_door_open", lambda: True)
    monkeypatch.setattr(
        "src.server.services.account.read_standing", AsyncMock(return_value=Standing(ORG, ORG))
    )


async def test_a_holder_is_offered_members_only(accounts):
    assert await offered(HOLDER, ORGANIZATION) == [ORGANIZATION]
    assert await offered(OWNER) == [PUBLIC, ORGANIZATION]


async def test_a_holders_public_link_is_refused_without_reading_its_policy():
    rule = AsyncMock(return_value=ALLOW_ALL)
    with patch.object(share_audience, "require_policy", rule):
        with pytest.raises(HTTPException) as refused:
            await refuse_public_link_of(HOLDER)
        await refuse_public_link_of(OWNER)
    assert refused.value.status_code == 403
    assert refused.value.detail["type"] == SHARED_MEMBERS_ONLY
    rule.assert_awaited_once_with(OWNER)


def _member(thread_id, caller, **_):
    return Access(HOLDER, SharedAccess(SHARED, caller, "member"))


async def test_a_member_shares_a_thread_with_members_and_never_with_anyone(accounts):
    from src.server.app.threads.sharing import update_thread_share
    from src.server.models.conversation import ThreadShareRequest

    update = AsyncMock(
        return_value={
            "is_shared": True,
            "share_token": "t",
            "share_permissions": {"audience": ORGANIZATION},
        }
    )
    with (
        patch(
            "src.server.services.shared_workspaces.access.thread_access",
            new_callable=AsyncMock,
            side_effect=_member,
        ),
        patch(
            "src.server.app.threads.sharing.get_thread_by_id",
            new_callable=AsyncMock,
            return_value={"share_permissions": {}},
        ),
        patch("src.server.app.threads.sharing.update_thread_sharing", update),
        patch("src.server.app.threads.sharing.activity.record", MagicMock()),
    ):
        with pytest.raises(HTTPException) as refused:
            await update_thread_share(
                "th", ThreadShareRequest(is_shared=True, permissions={"audience": PUBLIC}), "m"
            )
        update.assert_not_awaited()
        shared = await update_thread_share(
            "th",
            ThreadShareRequest(is_shared=True, permissions={"audience": ORGANIZATION}),
            "m",
        )
    assert refused.value.detail["type"] == SHARED_MEMBERS_ONLY
    assert shared.is_shared is True
    assert shared.audiences == [ORGANIZATION]


async def test_a_files_first_publish_takes_the_members_audience(accounts):
    from src.server.app.artifacts.models import PublishRequest
    from src.server.app.artifacts.records import publish_artifact

    artifact = MagicMock(artifact_id="a1", audience=None)
    set_audience = AsyncMock(return_value=MagicMock(artifact_id="a1", audience=ORGANIZATION))
    publish = AsyncMock(side_effect=RuntimeError("published"))
    with (
        patch(
            "src.server.app.artifacts.records.reached",
            new_callable=AsyncMock,
            return_value=(artifact, Access(HOLDER, SharedAccess(SHARED, OWNER, "member"))),
        ),
        patch("src.server.app.artifacts.records.artifacts_db.set_audience", set_audience),
        patch("src.server.app.artifacts.records.snapshot.publish", publish),
        pytest.raises(RuntimeError, match="published"),
    ):
        await publish_artifact("a1", PublishRequest(files=["a.html"]), "m")
    assert set_audience.await_args.kwargs == {
        "user_id": HOLDER,
        "artifact_id": "a1",
        "audience": ORGANIZATION,
    }
    assert publish.await_args.args[0].audience == ORGANIZATION
    assert publish.await_args.kwargs["user_id"] == HOLDER
    assert publish.await_args.kwargs["actor"] == "m"
