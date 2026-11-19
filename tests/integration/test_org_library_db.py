"""The organization library against a real Postgres.

Whether a thread or file item is listed is read off its link on every
listing, so these run the listing's joins as shipped: a link turned off, or
a workspace deleted, takes the item out, and turning the link back on brings
it back. Removal is scoped to the publisher or to the organization the
console names. A skill item keeps its own snapshot, and a member's copy
installed from it has an update once the snapshot moves on.
"""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio

from src.server.database import org_library as library

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

ORG = "org-lib-a"
OTHER_ORG = "org-lib-b"


@pytest_asyncio.fixture(loop_scope="session")
async def seeded(patched_get_db_connection, test_db_pool):
    """A publisher with a shared thread and a shared file in one workspace,
    and a second member."""
    from src.server.database.user import create_user
    from src.server.database.workspace import create_workspace

    tag = uuid.uuid4().hex[:10]
    publisher, member = f"lib-pub-{tag}", f"lib-mem-{tag}"
    for user_id in (publisher, member):
        await create_user(user_id=user_id, email=f"{user_id}@example.com", name=user_id)
    ws = str((await create_workspace(user_id=publisher, name="Lib", status="running"))["workspace_id"])
    thread, artifact = str(uuid.uuid4()), str(uuid.uuid4())
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "INSERT INTO conversation_threads (conversation_thread_id, workspace_id,"
            " current_status, thread_index, title, is_shared, share_token, share_permissions)"
            " VALUES (%s, %s, 'completed', 0, 'Q3 notes', true, %s,"
            " '{\"audience\": \"organization\"}'::jsonb)",
            (thread, ws, f"tok{tag}"),
        )
        await conn.execute(
            "INSERT INTO share_links (code, workspace_id, kind, path, shared_at, files, artifact_id)"
            " VALUES (%s, %s, 'file', 'out/report.html', now(), ARRAY['out/report.html'], %s)",
            (f"c{tag}x", ws, artifact),
        )
    try:
        yield {"publisher": publisher, "member": member, "ws": ws, "thread": thread, "artifact": artifact}
    finally:
        async with test_db_pool.connection() as conn:
            await conn.execute("DELETE FROM org_library_items WHERE publisher_id = %s", (publisher,))
            await conn.execute("DELETE FROM user_skills WHERE user_id IN (%s, %s)", (publisher, member))
            await conn.execute("DELETE FROM workspaces WHERE workspace_id = %s", (ws,))
            await conn.execute("DELETE FROM users WHERE user_id IN (%s, %s)", (publisher, member))


async def _listed(org: str = ORG, **kw) -> list[str]:
    rows, _ = await library.list_items(org, **kw)
    return [r["item_id"] for r in rows]


async def _publish(s: dict, kind: str, publisher: str | None = None, **kw):
    return await library.publish(
        org_id=ORG,
        publisher_id=publisher or s["publisher"],
        kind=kind,
        target_id=s["thread"] if kind == "thread" else s["artifact"],
        title=kw.get("title"),
        summary=kw.get("summary"),
    )


async def test_items_are_named_as_the_app_names_them_and_listed_to_their_organization(seeded):
    thread_item, created = await _publish(seeded, "thread")
    file_item, _ = await _publish(seeded, "file")
    assert created
    rows = {r["item_id"]: r for r in (await library.list_items(ORG))[0]}
    assert rows[thread_item]["title"] == "Q3 notes"
    assert rows[thread_item]["audience"] == "organization"
    assert rows[file_item]["title"] == "report.html"
    assert rows[file_item]["audience"] == "public"
    assert await _listed(OTHER_ORG) == []


async def test_publishing_again_edits_the_one_item_and_keeps_what_it_was_not_given(seeded):
    item, _ = await _publish(seeded, "thread", title="Mine", summary="About Q3")
    again, created = await _publish(seeded, "thread", title="Renamed")
    assert (again, created) == (item, False)
    row = await library.get_item(item)
    assert (row["title"], row["summary"]) == ("Renamed", "About Q3")


async def test_only_the_contents_owner_publishes_it(seeded):
    assert await _publish(seeded, "thread", publisher=seeded["member"]) is None
    assert await _publish(seeded, "file", publisher=seeded["member"]) is None


async def test_a_link_turned_off_takes_its_item_out_and_back_on_brings_it_back(seeded, test_db_pool):
    thread_item, _ = await _publish(seeded, "thread")
    file_item, _ = await _publish(seeded, "file")
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE conversation_threads SET is_shared = false WHERE conversation_thread_id = %s",
            (seeded["thread"],),
        )
        await conn.execute(
            "UPDATE share_links SET shared_at = NULL, files = NULL WHERE artifact_id = %s",
            (seeded["artifact"],),
        )
    assert await _listed() == []
    # The console still sees them, as not listed.
    console = {r["item_id"]: r["listed"] for r in (await library.list_items(ORG, listed_only=False))[0]}
    assert console == {thread_item: False, file_item: False}
    # A link that is off cannot be published.
    assert await _publish(seeded, "file") is None

    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE conversation_threads SET is_shared = true WHERE conversation_thread_id = %s",
            (seeded["thread"],),
        )
    assert await _listed() == [thread_item]


async def test_a_link_open_to_anyone_is_left_out_while_the_rule_stops_such_links(seeded):
    """The organization's rule stops a public link opening for anyone, its
    members too, so the item would list something no one can open."""
    thread_item, _ = await _publish(seeded, "thread")  # organization audience
    file_item, _ = await _publish(seeded, "file")  # public audience
    assert set(await _listed()) == {thread_item, file_item}
    assert await _listed(public_links=False) == [thread_item]
    # The console's list is the organization's whole library either way.
    assert set(await _listed(listed_only=False, public_links=False)) == {thread_item, file_item}


async def test_a_deleted_workspace_takes_its_items_out(seeded, test_db_pool):
    await _publish(seeded, "thread")
    async with test_db_pool.connection() as conn:
        await conn.execute("UPDATE workspaces SET status = 'deleted' WHERE workspace_id = %s", (seeded["ws"],))
    assert await _listed() == []


async def test_removal_is_scoped_to_the_publisher_or_the_organization(seeded):
    item, _ = await _publish(seeded, "thread")
    assert await library.remove(item, publisher_id=seeded["member"]) is None
    assert await library.remove(item, org_id=OTHER_ORG) is None
    assert await library.edit(item, publisher_id=seeded["member"], title="x", summary=None) is False
    removed = await library.remove(item, org_id=ORG)
    assert removed["publisher_id"] == seeded["publisher"]
    assert await library.get_item(item) is None
    with pytest.raises(ValueError):
        await library.remove(item)


async def test_a_skill_snapshot_is_listed_installed_and_updated(seeded, test_db_pool):
    async def publish(content_hash: str):
        return await library.publish_skill(
            org_id=ORG, publisher_id=seeded["publisher"], skill_name="dcf", title=None,
            summary="Discounted cash flow", content_hash=content_hash, archive_key=None,
            archive_blob=b"zip-" + content_hash.encode(), archive_bytes=8,
        )

    item, created, superseded = await publish("sha256:" + "1" * 64)
    assert created and superseded is None
    member = seeded["member"]
    assert await library.own_skill_origin(member, "dcf") == (False, None)
    row = await library.get_item(item, viewer=member)
    assert (row["title"], row["audience"], row["installed_hash"]) == ("dcf", "organization", None)

    async with test_db_pool.connection() as conn:
        await conn.execute(
            "INSERT INTO user_skills (user_id, name, content_hash, archive_blob, org_library_item_id)"
            " VALUES (%s, 'dcf', %s, 'zip', %s)",
            (member, "sha256:" + "1" * 64, item),
        )
    assert await library.own_skill_origin(member, "dcf") == (True, item)
    row = await library.get_item(item, viewer=member)
    assert row["installed_hash"] == row["content_hash"]

    again, created, _ = await publish("sha256:" + "2" * 64)
    assert (again, created) == (item, False)
    row = await library.get_item(item, viewer=member)
    assert row["installed_hash"] != row["content_hash"]
    assert await library.skill_archive(item) == (None, b"zip-sha256:" + b"2" * 64)
    # A skill item is listed whatever happens to links.
    assert item in await _listed(viewer=member, kind="skill")


async def test_a_shared_workspaces_member_publishes_its_thread_and_no_one_keeps_it_in(seeded, test_db_pool):
    """A member publishes a shared workspace's thread (the workspace is the
    holder's, not theirs), and under "no one" the item is listed only to the
    workspace's people while the workspace is restricted."""
    from src.server.database.workspace import create_workspace

    holder = f"shared-{uuid.uuid4()}"
    member, outsider = seeded["publisher"], seeded["member"]
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "INSERT INTO users (user_id, auth_provider) VALUES (%s, 'shared_workspace')", (holder,)
        )
    ws = str((await create_workspace(user_id=holder, name="Atlas", status="running"))["workspace_id"])
    thread = str(uuid.uuid4())
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "INSERT INTO shared_workspaces (workspace_id, holder_id, org_id, everyone, created_by)"
            " VALUES (%s, %s, %s, false, %s)",
            (ws, holder, ORG, member),
        )
        await conn.execute(
            "INSERT INTO shared_workspace_members (workspace_id, user_id, role, added_by)"
            " VALUES (%s, %s, 'member', %s)",
            (ws, member, member),
        )
        await conn.execute(
            "INSERT INTO conversation_threads (conversation_thread_id, workspace_id,"
            " current_status, thread_index, title, is_shared, share_token, share_permissions)"
            " VALUES (%s, %s, 'completed', 0, 'Atlas notes', true, %s,"
            " '{\"audience\": \"organization\"}'::jsonb)",
            (thread, ws, f"tok{thread[:8]}"),
        )
    try:
        publish = dict(org_id=ORG, publisher_id=member, kind="thread", target_id=thread, title=None, summary=None)
        assert await library.publish(**publish) is None
        item, _ = await library.publish(**publish, owner_id=holder)

        async def seen(viewer: str, kept_in: bool = True) -> bool:
            return item in await _listed(viewer=viewer, kept_in=kept_in)

        assert await seen(outsider) and await seen(member)
        async with test_db_pool.connection() as conn:
            await conn.execute("UPDATE shared_workspaces SET share_out = 'none' WHERE workspace_id = %s", (ws,))
        assert not await seen(outsider)
        assert await seen(member)
        assert await seen(outsider, kept_in=False)
        # Someone in the team the workspace names reaches it, so sees it too.
        async with test_db_pool.connection() as conn:
            await conn.execute(
                "UPDATE shared_workspaces SET team_id = 'team-a', team_since = now() WHERE workspace_id = %s",
                (ws,),
            )
        assert item in await _listed(viewer=outsider, viewer_teams={"team-a"}, kept_in=True)
        assert item not in await _listed(viewer=outsider, viewer_teams={"team-b"}, kept_in=True)
        assert not await seen(outsider)
        async with test_db_pool.connection() as conn:
            await conn.execute(
                "UPDATE shared_workspaces SET team_id = NULL, team_since = NULL, everyone = true"
                " WHERE workspace_id = %s",
                (ws,),
            )
        assert await seen(outsider)
    finally:
        async with test_db_pool.connection() as conn:
            await conn.execute("DELETE FROM org_library_items WHERE thread_id = %s", (thread,))
            await conn.execute("DELETE FROM conversation_threads WHERE conversation_thread_id = %s", (thread,))
            await conn.execute("DELETE FROM shared_workspaces WHERE workspace_id = %s", (ws,))
            await conn.execute("DELETE FROM workspaces WHERE workspace_id = %s", (ws,))
            await conn.execute("DELETE FROM users WHERE user_id = %s", (holder,))
