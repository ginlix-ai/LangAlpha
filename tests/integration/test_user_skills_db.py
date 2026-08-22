"""Integration tests for the user-skill writers against real PostgreSQL.

Focused on the alias seed, which is chosen outside any lock (the handler picks
it, then spends the object PUT) and so has to be re-checked at write time.
"""

from __future__ import annotations

import uuid

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _write(user_id: str, name: str, command: str | None):
    from src.server.database.user_skills import upsert_user_skill

    row, _ = await upsert_user_skill(
        user_id,
        name,
        description=f"probe {name}",
        license=None,
        frontmatter={"name": name, "description": "probe"},
        allowed_tools=[],
        confirmed=True,
        content_hash=uuid.uuid4().hex,
        archive_key=None,
        archive_blob=b"PK\x05\x06" + b"\x00" * 18,
        archive_bytes=22,
        file_count=1,
        command=command,
    )
    return row


class TestAliasSeedUnderTheLock:
    async def test_a_seed_a_sibling_name_already_answers_to_is_dropped(
        self, seed_user, patched_get_db_connection, test_user_id
    ):
        """The sibling holds the trigger as its NAME, with a NULL command, so
        neither the unique index nor the name-vs-alias check can see it.
        """
        await _write(test_user_id, "alpha", None)

        row = await _write(test_user_id, "beta", "alpha")

        assert row["command"] is None

    async def test_a_seed_a_sibling_alias_already_answers_to_is_dropped(
        self, seed_user, patched_get_db_connection, test_user_id
    ):
        await _write(test_user_id, "alpha", "shared")

        row = await _write(test_user_id, "beta", "shared")

        assert row["command"] is None

    async def test_a_free_seed_still_lands(
        self, seed_user, patched_get_db_connection, test_user_id
    ):
        await _write(test_user_id, "alpha", None)

        row = await _write(test_user_id, "beta", "gamma")

        assert row["command"] == "gamma"


class TestDeleteClearsWorkspaceDisableMarkers:
    async def test_account_tier_delete_clears_the_markers(
        self, seed_workspace, patched_get_db_connection, test_user_id
    ):
        """The markers describe the deleted identity; a later same-name upload
        must not inherit them."""
        from src.server.database.user_skills import (
            delete_user_skill,
            list_workspace_skill_disables,
            set_workspace_skill_disable,
        )

        ws_id = seed_workspace["workspace_id"]
        await _write(test_user_id, "alpha", None)
        await set_workspace_skill_disable(ws_id, "alpha", True)

        await delete_user_skill(test_user_id, "alpha")

        assert "alpha" not in await list_workspace_skill_disables(ws_id)

    async def test_workspace_scope_delete_keeps_them(
        self, seed_workspace, patched_get_db_connection, test_user_id
    ):
        """A workspace-scoped delete re-exposes the inherited skill the marker
        points at, so the marker must survive."""
        from src.server.database.user_skills import (
            delete_user_skill,
            list_workspace_skill_disables,
            set_workspace_skill_disable,
            upsert_user_skill,
        )

        ws_id = seed_workspace["workspace_id"]
        await _write(test_user_id, "alpha", None)
        await set_workspace_skill_disable(ws_id, "alpha", True)
        await upsert_user_skill(
            test_user_id,
            "alpha",
            description="shadow",
            license=None,
            frontmatter={"name": "alpha", "description": "shadow"},
            allowed_tools=[],
            confirmed=True,
            content_hash=uuid.uuid4().hex,
            archive_key=None,
            archive_blob=b"PK\x05\x06" + b"\x00" * 18,
            archive_bytes=22,
            file_count=1,
            workspace_id=ws_id,
        )

        await delete_user_skill(test_user_id, "alpha", workspace_id=ws_id)

        assert "alpha" in await list_workspace_skill_disables(ws_id)


class TestPlatformAliasReadUnderTheLock:
    async def _seed_override(self, user_id: str, alias: str) -> None:
        from src.server.database.user import upsert_user_preferences

        await upsert_user_preferences(
            user_id,
            other_preference={"skills": {"command_overrides": {"builtin-x": alias}}},
        )

    async def test_a_command_taken_by_a_platform_alias_is_rejected(
        self, seed_user, patched_get_db_connection, test_user_id
    ):
        """set_user_skill_command re-reads the override table under the lock,
        so an alias committed after the router's friendly pre-check still
        collides."""
        from src.server.database.user_skills import set_user_skill_command

        await _write(test_user_id, "alpha", None)
        await self._seed_override(test_user_id, "taken")

        with pytest.raises(ValueError, match="already in use"):
            await set_user_skill_command(test_user_id, "alpha", "taken")

    async def test_a_seed_taken_by_a_platform_alias_is_dropped(
        self, seed_user, patched_get_db_connection, test_user_id
    ):
        await self._seed_override(test_user_id, "taken")

        row = await _write(test_user_id, "beta", "taken")

        assert row["command"] is None


class TestPluginOwnedRowsStayAtTheAccountTier:
    """A plugin-owned skill cannot be moved into a workspace.

    Detaching it on the way down (the behavior this replaced) dropped the row
    out of the plugin's owned set while the manifest still declared the
    component, so the next update re-created it at the account tier and the
    plugin's copy went live in every other workspace under the name the user
    had just scoped down. Reproduced end to end before the guard existed.
    """

    async def _seed_owned(self, user_id: str, name: str) -> str:
        from src.server.database.plugins import create_plugin
        from src.server.database.user_skills import upsert_user_skill

        plugin = await create_plugin(
            user_id,
            "probe-plugin",
            version="1.0.0",
            source_type="upload",
            source_ref=None,
            manifest={"name": "probe-plugin", "version": "1.0.0"},
            mcp_document=None,
        )
        await upsert_user_skill(
            user_id,
            name,
            description="owned probe",
            license=None,
            frontmatter={"name": name, "description": "owned probe"},
            allowed_tools=[],
            confirmed=True,
            content_hash=uuid.uuid4().hex,
            archive_key=None,
            archive_blob=b"PK\x05\x06" + b"\x00" * 18,
            archive_bytes=22,
            file_count=1,
            workspace_id=None,
            plugin_id=plugin["user_plugin_id"],
            plugin_skill_dir=name,
        )
        return plugin["user_plugin_id"]

    async def test_moving_one_into_a_workspace_is_refused(
        self, seed_workspace, patched_get_db_connection, test_user_id
    ):
        from src.server.database.user_skills import get_user_skill, move_user_skill

        ws_id = seed_workspace["workspace_id"]
        plugin_id = await self._seed_owned(test_user_id, "owned")

        with pytest.raises(ValueError) as excinfo:
            await move_user_skill(
                test_user_id, "owned", from_workspace_id=None, to_workspace_id=ws_id
            )

        # The refusal names the plugin, so the user is told who owns the row
        # rather than being handed a bare rule.
        assert "probe-plugin" in str(excinfo.value)
        # And the row is genuinely untouched: still owned, still account tier.
        row = await get_user_skill(test_user_id, "owned")
        assert row is not None
        assert row["plugin_id"] == plugin_id
        assert await get_user_skill(test_user_id, "owned", workspace_id=ws_id) is None

    async def test_ownership_is_checked_before_the_name_collision(
        self, seed_workspace, patched_get_db_connection, test_user_id
    ):
        """Both checks would refuse, so the order decides which reason the user
        reads; the actionable one is the ownership."""
        from src.server.database.user_skills import move_user_skill

        ws_id = seed_workspace["workspace_id"]
        await self._seed_owned(test_user_id, "owned")
        await _write(test_user_id, "decoy", None)
        from src.server.database.user_skills import upsert_user_skill

        await upsert_user_skill(
            test_user_id,
            "owned",
            description="shadow",
            license=None,
            frontmatter={"name": "owned", "description": "shadow"},
            allowed_tools=[],
            confirmed=True,
            content_hash=uuid.uuid4().hex,
            archive_key=None,
            archive_blob=b"PK\x05\x06" + b"\x00" * 18,
            archive_bytes=22,
            file_count=1,
            workspace_id=ws_id,
        )

        with pytest.raises(ValueError) as excinfo:
            await move_user_skill(
                test_user_id, "owned", from_workspace_id=None, to_workspace_id=ws_id
            )

        assert "probe-plugin" in str(excinfo.value)

    async def test_an_owned_row_may_still_move_up(
        self, seed_workspace, patched_get_db_connection, test_user_id
    ):
        """The guard is one-directional. Rows that predate it can still be
        recovered out of the workspace tier, which is where they do not belong."""
        from src.server.database.user_skills import get_user_skill, move_user_skill

        ws_id = seed_workspace["workspace_id"]
        await self._seed_owned(test_user_id, "owned")
        async with patched_get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE user_skills SET workspace_id = %s "
                    "WHERE user_id = %s AND name = %s",
                    (ws_id, test_user_id, "owned"),
                )

        row = await move_user_skill(
            test_user_id, "owned", from_workspace_id=ws_id, to_workspace_id=None
        )

        assert row is not None
        assert await get_user_skill(test_user_id, "owned") is not None


class TestWritersReturnTheSameShapeAsReads:
    """A writer's row carries the owner's display fields, like a read's does.

    RETURNING cannot JOIN, so these rows used to come back with plugin_name
    and plugin_enabled absent while plugin_id was set: GET said the row had
    an owner and PATCH said it did not, for the same row. Nothing rendered
    wrong only because no caller consumed a mutation result. These run
    against real PostgreSQL because the projection is SQL text no unit test
    parses, and a correlated subquery in RETURNING is exactly the kind of
    thing a mocked cursor accepts and a server rejects.
    """

    async def _owned(self, user_id: str, name: str) -> str:
        from src.server.database.plugins import create_plugin
        from src.server.database.user_skills import upsert_user_skill

        plugin = await create_plugin(
            user_id,
            "shape-plugin",
            version="1.0.0",
            source_type="upload",
            source_ref=None,
            manifest={"name": "shape-plugin", "version": "1.0.0"},
            mcp_document=None,
        )
        # INSERT ... ON CONFLICT ... RETURNING, the first of the three forms.
        row, _ = await upsert_user_skill(
            user_id,
            name,
            description="shape probe",
            license=None,
            frontmatter={"name": name, "description": "shape probe"},
            allowed_tools=[],
            confirmed=True,
            content_hash=uuid.uuid4().hex,
            archive_key=None,
            archive_blob=b"PK\x05\x06" + b"\x00" * 18,
            archive_bytes=22,
            file_count=1,
            plugin_id=plugin["user_plugin_id"],
            plugin_skill_dir=name,
        )
        assert row["plugin_name"] == "shape-plugin"
        assert row["plugin_enabled"] is True
        return plugin["user_plugin_id"]

    async def test_update_and_delete_writers_match_the_read(
        self, seed_user, patched_get_db_connection, test_user_id
    ):
        from src.server.database.user_skills import (
            delete_user_skill,
            get_user_skill,
            set_user_skill_enabled,
        )

        await self._owned(test_user_id, "shaped")
        read = await get_user_skill(test_user_id, "shaped")

        written = await set_user_skill_enabled(test_user_id, "shaped", False)

        assert written is not None
        for field in ("plugin_id", "plugin_name", "plugin_enabled"):
            assert written[field] == read[field], field

        # DELETE ... RETURNING, the third form. The plugin row outlives the
        # skill row, so the subselect still resolves on the way out.
        dropped = await delete_user_skill(test_user_id, "shaped")
        assert dropped is not None
        assert dropped["plugin_name"] == "shape-plugin"

    async def test_a_disabled_owner_is_reported_by_writers_too(
        self, seed_user, patched_get_db_connection, test_user_id
    ):
        """plugin_enabled=False is what explains a row the delivery predicate
        is suppressing, so a writer that could not say it would render an
        unexplained dead skill."""
        from src.server.database.plugins import set_plugin_enabled
        from src.server.database.user_skills import set_user_skill_command

        await self._owned(test_user_id, "shaped")
        toggled = await set_plugin_enabled(test_user_id, "shape-plugin", False)
        assert toggled is not None

        written = await set_user_skill_command(test_user_id, "shaped", "alias-x")

        assert written is not None
        assert written["plugin_name"] == "shape-plugin"
        assert written["plugin_enabled"] is False

    async def test_an_unowned_row_still_reads_as_unowned(
        self, seed_user, patched_get_db_connection, test_user_id
    ):
        """The subselect must answer None for a hand-made row rather than
        failing or inventing an owner."""
        from src.server.database.user_skills import set_user_skill_enabled

        await _write(test_user_id, "plain", None)

        written = await set_user_skill_enabled(test_user_id, "plain", False)

        assert written is not None
        assert written["plugin_id"] is None
        assert written["plugin_name"] is None
        assert written["plugin_enabled"] is None
