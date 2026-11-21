"""Saving one skill folder on request: the verdict ``_saved`` gives, the lock
and error handling around it, and what a pass reports of the watched folder."""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.server.database.user_skills import SkillSyncLockBusy
from src.server.services.user_skills import reconcile as R
from src.server.services.workspace_layout import WorkspaceLayoutUnavailable

WHERE = "/home/workspace/acme/.agents/skills/demo"


def _rep(**over):
    return {
        "present": True,
        "syncable": True,
        "wellFormed": True,
        "treeHash": "tree-1",
        "entry": None,
        **over,
    }


def _watch(rep=..., row=None, **over):
    rep = _rep() if rep is ... else rep
    return R._Watch("demo", rep=rep, row=row, **over)


def _saved(watch, row=None):
    return R._saved(watch, row, where=WHERE)


def _row(**over):
    return {"enabled": True, "content_hash": "h-1", **over}


class TestSavedVerdict:
    @pytest.mark.parametrize("rep", [None, _rep(present=False)])
    def test_an_absent_folder_is_not_saved_and_names_where_it_looked(self, rep):
        got = _saved(_watch(rep))

        assert not got.saved
        assert f"{WHERE}/SKILL.md" in got.reason

    def test_a_name_that_belongs_to_a_built_in_skill_is_refused(self):
        got = _saved(_watch(_rep(entry={"owner": "platform"})), _row())

        assert not got.saved
        assert "platform skill or command" in got.reason
        # A real folder is in the way of the skill's link, so it is renamed.
        assert "Rename the folder" in got.reason
        assert "linked again" in got.reason

    @pytest.mark.parametrize(
        ("present", "advice"),
        [(True, "Rename the folder"), (False, "Use a name no skill has")],
    )
    def test_a_command_s_name_has_no_skill_to_copy_or_link_back(
        self, monkeypatch, present, advice
    ):
        # `compact` and the like are reserved with no skill folder behind
        # them, so neither the link nor the copy the other answers name exists.
        monkeypatch.setattr(R, "reserved_skill_names", lambda: frozenset({"demo"}))
        monkeypatch.setattr(R, "reserved_for_a_command_only", lambda name: True)

        got = _saved(_watch(_rep(present=present)), _row())

        assert not got.saved
        assert "platform skill or command" in got.reason
        assert advice in got.reason
        assert "linked again" not in got.reason and "cp -rL" not in got.reason

    def test_a_built_in_linked_into_the_workspace_is_named_as_one(self, monkeypatch):
        # The report does not count a link as a folder, so without the name
        # check this read "there is no folder" for a path that plainly exists.
        monkeypatch.setattr(R, "reserved_skill_names", lambda: frozenset({"demo"}))

        got = _saved(_watch(_rep(present=False)))

        assert not got.saved
        assert "platform skill or command" in got.reason
        assert "no folder" not in got.reason
        assert "cp -rL" in got.reason

    def test_an_account_skill_linked_into_the_workspace_says_how_to_change_it_here(self):
        got = _saved(_watch(_rep(present=False), user_row=_row()))

        assert not got.saved
        assert "account's skills" in got.reason
        assert WHERE in got.reason

    def test_a_folder_that_cannot_sync_carries_the_reports_reason(self):
        got = _saved(_watch(_rep(syncable=False, reason="symlink escapes the folder")))

        assert not got.saved
        assert "symlink escapes the folder" in got.reason

    def test_a_malformed_skill_md_names_frontmatter_and_the_folder(self):
        got = _saved(_watch(_rep(wellFormed=False)), _row())

        assert not got.saved
        assert "frontmatter" in got.reason
        assert "`name: demo`" in got.reason

    def test_an_unfinished_restore_is_not_saved(self):
        got = _saved(_watch(restore_incomplete=True), _row())

        assert not got.saved
        assert "restored" in got.reason

    def test_a_failure_of_the_folders_own_step_is_its_reason(self):
        got = _saved(_watch(failure="too many files"), _row())

        assert (got.saved, got.reason) == (False, "too many files")

    def test_a_failed_write_to_the_computer_is_its_reason(self):
        got = _saved(_watch(apply_failure="disk full"), _row())

        assert (got.saved, got.reason) == (False, "disk full")

    @pytest.mark.parametrize(
        ("before", "db_hash"), [(None, None), (_row(content_hash="h-1"), "h-1")]
    )
    def test_an_earlier_refusal_that_still_matches_is_the_recorded_reason(
        self, before, db_hash
    ):
        refused = {"treeHash": "tree-1", "dbHash": db_hash, "reason": "over the size cap"}
        rep = _rep(entry={"sync": {"lastFailedSync": refused}})

        got = _saved(_watch(rep, before), _row())

        assert (got.saved, got.reason) == (False, "over the size cap")

    def test_an_earlier_refusal_of_other_content_does_not_block_a_save(self):
        refused = {"treeHash": "tree-0", "dbHash": None, "reason": "over the size cap"}
        rep = _rep(entry={"sync": {"lastFailedSync": refused}})

        assert _saved(_watch(rep), _row()).saved

    def test_no_row_after_the_pass_is_not_saved(self):
        assert not _saved(_watch(), None).saved

    def test_a_copy_equal_to_the_accounts_skill_says_to_change_it_first(self):
        # The pass takes such a copy for the account skill's delivered one and
        # puts the link back, so "not this workspace's own" would send the
        # agent looking for a folder that is gone.
        got = _saved(_watch(user_row=_row()), None)

        assert not got.saved
        assert "Change the copy before you save it" in got.reason

    def test_a_disabled_row_is_not_saved(self):
        got = _saved(_watch(), _row(enabled=False))

        assert not got.saved
        assert "turned off" in got.reason

    def test_a_new_row_is_saved_and_changed(self):
        got = _saved(_watch(), _row())

        assert (got.saved, got.changed, got.reason) == (True, True, None)

    def test_a_row_with_new_content_is_saved_and_changed(self):
        got = _saved(_watch(row=_row(content_hash="h-0")), _row())

        assert (got.saved, got.changed) == (True, True)

    def test_a_row_with_the_same_content_is_saved_unchanged(self):
        got = _saved(_watch(row=_row()), _row())

        assert (got.saved, got.changed) == (True, False)


class TestSaveWorkspaceSkill:
    @pytest.fixture
    def seam(self, monkeypatch):
        held: list[tuple[str, float]] = []
        seen = SimpleNamespace(held=held, run=AsyncMock(), error=None)

        @asynccontextmanager
        async def hold(lock_id, *, wait_s):
            if seen.error is not None:
                raise seen.error
            held.append((lock_id, wait_s))
            yield

        layout = SimpleNamespace(skills="/home/workspace/acme/.agents/skills/")
        monkeypatch.setattr(R, "hold_workspace_skill_sync", hold)
        monkeypatch.setattr(R, "_run_pass", seen.run)
        monkeypatch.setattr(R, "_resolve_layout", AsyncMock(return_value=layout))
        monkeypatch.setattr(
            R,
            "list_user_skills",
            AsyncMock(return_value=[{"name": "demo", "enabled": True, "content_hash": "h"}]),
        )
        return seen

    @pytest.mark.asyncio
    async def test_it_waits_on_both_locks_and_watches_the_named_skill(self, seam):
        got = await R.save_workspace_skill(
            object(), user_id="u-1", workspace_id="ws-1", name="demo"
        )

        assert {lock for lock, _ in seam.held} == {"ws-1", R.acting_for("u-1", "ws-1")}
        assert {wait for _, wait in seam.held} == {R.SAVE_LOCK_WAIT_SECONDS}
        watch = seam.run.await_args.args[-1]
        assert watch.name == "demo"
        assert "/home/workspace/acme/.agents/skills/demo" in got.reason

    @pytest.mark.asyncio
    async def test_a_busy_lock_answers_not_saved(self, seam):
        seam.error = SkillSyncLockBusy()

        got = await R.save_workspace_skill(
            object(), user_id="u-1", workspace_id="ws-1", name="demo"
        )

        assert not got.saved
        seam.run.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_unavailable_layout_answers_not_saved(self, seam, monkeypatch):
        monkeypatch.setattr(
            R, "_resolve_layout", AsyncMock(side_effect=WorkspaceLayoutUnavailable("x"))
        )

        got = await R.save_workspace_skill(
            object(), user_id="u-1", workspace_id="ws-1", name="demo"
        )

        assert not got.saved

    @pytest.mark.asyncio
    async def test_a_timeout_answers_not_saved(self, seam):
        seam.run.side_effect = TimeoutError()

        got = await R.save_workspace_skill(
            object(), user_id="u-1", workspace_id="ws-1", name="demo"
        )

        assert not got.saved
        assert "too long" in got.reason

    @pytest.mark.asyncio
    async def test_a_computer_that_cannot_be_read_answers_not_saved(self, seam):
        seam.run.side_effect = RuntimeError("transport closed")

        got = await R.save_workspace_skill(
            object(), user_id="u-1", workspace_id="ws-1", name="demo"
        )

        assert not got.saved
        assert "run it again" in got.reason


class TestPassFillsTheWatch:
    @pytest.fixture
    def world(self, monkeypatch):
        state = SimpleNamespace(results=[], decide=None)

        async def _decide(ctx, name):
            if state.decide:
                await state.decide(ctx, name)

        async def _none(*a, **k):
            return None

        async def _flag(workspace_id):
            return False

        async def _rows(user_id, *, workspace_id=None):
            return []

        async def _apply(*a, **k):
            return state.results

        report = {"demo": _rep(), "other": _rep()}
        monkeypatch.setattr(R.skill_sync, "report", AsyncMock(return_value=report))
        monkeypatch.setattr(R.skill_sync, "apply_actions", _apply)
        monkeypatch.setattr(R, "list_user_skills", _rows)
        monkeypatch.setattr(R, "files_restore_incomplete", _flag)
        monkeypatch.setattr(R, "_link_shared", _none)
        monkeypatch.setattr(R, "_decide", _decide)
        return state

    async def _run(self, watch):
        await R._run_pass(
            object(),
            "u-1",
            "ws-1",
            "save",
            SimpleNamespace(skills="/home/workspace/acme/.agents/skills"),
            watch,
        )

    @pytest.mark.asyncio
    async def test_a_failure_for_the_watched_name_lands_in_the_watch(self, world):
        async def decide(ctx, name):
            raise R._SyncFailure("caps", f"{name} is too big", suppress=False)

        world.decide = decide
        watch = R._Watch("demo")

        await self._run(watch)

        assert watch.failure == "demo is too big"
        assert watch.rep is not None

    @pytest.mark.asyncio
    async def test_a_failure_for_another_name_stays_out_of_the_watch(self, world):
        async def decide(ctx, name):
            if name == "other":
                raise R._SyncFailure("caps", "other is too big", suppress=False)

        world.decide = decide
        watch = R._Watch("demo")

        await self._run(watch)

        assert watch.failure is None

    @pytest.mark.asyncio
    async def test_a_drift_on_the_watched_name_sets_the_apply_failure(self, world):
        async def decide(ctx, name):
            ctx.actions.append({"op": "swap", "name": name})

        world.decide = decide
        world.results = [{"name": "demo", "ok": False, "drift": True, "op": "swap"}]
        watch = R._Watch("demo")

        await self._run(watch)

        assert "changed while it was being saved" in watch.apply_failure


def test_a_reserved_name_is_a_command_s_alone_only_with_no_skill_behind_it():
    from ptc_agent.agent.middleware.skills.registry import SKILL_REGISTRY
    from src.server.services.user_skills.validate import reserved_for_a_command_only

    a_skill = next(iter(SKILL_REGISTRY))

    assert reserved_for_a_command_only("compact")
    assert not reserved_for_a_command_only(a_skill)
    assert not reserved_for_a_command_only("a-name-nothing-reserves")
