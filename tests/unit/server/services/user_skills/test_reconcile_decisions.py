"""The reconciler's decision matrix, with the destructive branches pinned.

``_decide`` is the whole two-way sync contract in one function: which of
pull-up / push-down / import / arbitrate / delete a given (sandbox report, DB
row) pair earns. The operations themselves are stubbed here so each case
asserts the *decision*, not the transfer.

Two invariants get explicit coverage because a regression in either destroys
user content silently: every destructive action carries the state it was
decided against (``expectTreeHash`` / ``expectAbsent``), and content beats
deletion on both sides.
"""

import pytest

from ptc_agent.agent.middleware.skills.lock import MANAGED_SOURCE_TYPE
from src.server.services.user_skills import reconcile


def _rep(
    *,
    present: bool = True,
    tree_hash: str | None = "tree-1",
    entry: dict | None = None,
    syncable: bool = True,
    well_formed: bool = True,
) -> dict:
    return {
        "present": present,
        "treeHash": tree_hash,
        "entry": entry,
        "syncable": syncable,
        "wellFormed": well_formed,
    }


def _entry(owner: str = "user", source_type: str = MANAGED_SOURCE_TYPE, **sync) -> dict:
    entry: dict = {"name": "demo", "owner": owner, "sourceType": source_type}
    if sync:
        entry["sync"] = dict(sync)
    return entry


def _linked_entry(skill_id: str = "sk-1", tree: str = "tree-1", db: str = "db-1") -> dict:
    return _entry(
        linkedSkillId=skill_id, syncedTreeHash=tree, syncedDbHash=db
    )


def _row(
    *,
    skill_id: str = "sk-1",
    content_hash: str = "db-1",
    enabled: bool = True,
    name: str = "demo",
) -> dict:
    return {
        "user_skill_id": skill_id,
        "name": name,
        "content_hash": content_hash,
        "enabled": enabled,
        "archive_key": "user-skills/u/abc.zip",
        "workspace_id": "ws-1",
    }


class _Calls(dict):
    """Records which stubbed operation ran, with its keyword arguments."""

    def recorder(self, key):
        async def _run(ctx, name, *args, **kwargs):
            self.setdefault(key, []).append(kwargs)

        return _run


@pytest.fixture
def ctx(monkeypatch):
    calls = _Calls()
    for op in ("_pull_up", "_push_down", "_import_new", "_absorb_user_shadow"):
        monkeypatch.setattr(reconcile, op, calls.recorder(op))
    pass_ctx = reconcile._Pass(
        sandbox=object(),
        user_id="u-1",
        workspace_id="ws-1",
        report={},
        ws_rows={},
        user_rows={},
    )
    pass_ctx.calls = calls  # type: ignore[attr-defined]
    return pass_ctx


def _ops(ctx) -> list[str]:
    return [a["op"] for a in ctx.actions]


def _action(ctx, op: str) -> dict:
    return next(a for a in ctx.actions if a["op"] == op)


# ---------------------------------------------------------------------------
# Ownership
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_platform_entry_is_not_the_reconcilers_business(ctx):
    ctx.report["demo"] = _rep(entry=_entry(owner="platform", source_type="platform"))
    ctx.ws_rows["demo"] = _row()

    await reconcile._decide(ctx, "demo")

    assert ctx.actions == [] and ctx.calls == {}


# ---------------------------------------------------------------------------
# Disabled rows: withdraw the copy, keep the content
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_disabled_row_with_clean_tree_deletes_the_dir_under_guard(ctx):
    ctx.report["demo"] = _rep(entry=_linked_entry())
    ctx.ws_rows["demo"] = _row(enabled=False)

    await reconcile._decide(ctx, "demo")

    assert _action(ctx, "delete_dir")["expectTreeHash"] == "tree-1"
    assert ctx.stats.dir_deletes == 1


@pytest.mark.asyncio
async def test_disabled_row_with_unpulled_edits_pulls_up_before_deleting(ctx):
    ctx.report["demo"] = _rep(tree_hash="tree-2", entry=_linked_entry(tree="tree-1"))
    ctx.ws_rows["demo"] = _row(enabled=False)

    await reconcile._decide(ctx, "demo")

    assert "_pull_up" in ctx.calls
    assert "delete_dir" not in _ops(ctx)


@pytest.mark.asyncio
async def test_disabled_row_with_unsyncable_tree_touches_nothing(ctx):
    ctx.report["demo"] = _rep(entry=_linked_entry(), syncable=False)
    ctx.ws_rows["demo"] = _row(enabled=False)

    await reconcile._decide(ctx, "demo")

    assert ctx.actions == [] and ctx.calls == {}
    assert ctx.stats.skipped == 1


@pytest.mark.asyncio
async def test_disabled_row_with_no_dir_drops_only_the_ledger_entry(ctx):
    ctx.report["demo"] = _rep(present=False, tree_hash=None, entry=_linked_entry())
    ctx.ws_rows["demo"] = _row(enabled=False)

    await reconcile._decide(ctx, "demo")

    assert _ops(ctx) == ["remove_entry"]


# ---------------------------------------------------------------------------
# Linked: both sides present
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_both_sides_unchanged_is_a_no_op(ctx):
    ctx.report["demo"] = _rep(entry=_linked_entry())
    ctx.ws_rows["demo"] = _row()

    await reconcile._decide(ctx, "demo")

    assert ctx.actions == [] and ctx.calls == {}


@pytest.mark.asyncio
async def test_tree_dirty_alone_pulls_up_without_conflict(ctx):
    ctx.report["demo"] = _rep(tree_hash="tree-2", entry=_linked_entry())
    ctx.ws_rows["demo"] = _row()

    await reconcile._decide(ctx, "demo")

    assert ctx.calls["_pull_up"] == [{"conflict": False}]


@pytest.mark.asyncio
async def test_row_dirty_alone_pushes_down(ctx):
    ctx.report["demo"] = _rep(entry=_linked_entry())
    ctx.ws_rows["demo"] = _row(content_hash="db-2")

    await reconcile._decide(ctx, "demo")

    assert "_push_down" in ctx.calls and ctx.stats.pushed == 1


@pytest.mark.asyncio
async def test_both_dirty_goes_through_the_arbiter(ctx):
    ctx.report["demo"] = _rep(tree_hash="tree-2", entry=_linked_entry())
    ctx.ws_rows["demo"] = _row(content_hash="db-2")

    await reconcile._decide(ctx, "demo")

    assert ctx.calls["_pull_up"] == [{"conflict": True}]


@pytest.mark.asyncio
async def test_relinked_name_arbitrates_rather_than_trusting_the_ref(ctx):
    """The name maps to a different row now — deleted and recreated over the
    API while this sandbox slept."""
    ctx.report["demo"] = _rep(entry=_linked_entry(skill_id="sk-old"))
    ctx.ws_rows["demo"] = _row(skill_id="sk-new")

    await reconcile._decide(ctx, "demo")

    assert ctx.calls["_pull_up"] == [{"conflict": True}]


# ---------------------------------------------------------------------------
# Linked: the row is gone
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_row_deleted_and_tree_clean_removes_the_dir_under_guard(ctx, monkeypatch):
    monkeypatch.setattr(
        reconcile, "get_user_skill_by_id", _async_return(None)
    )
    ctx.report["demo"] = _rep(entry=_linked_entry())

    await reconcile._decide(ctx, "demo")

    assert _action(ctx, "delete_dir")["expectTreeHash"] == "tree-1"
    assert ctx.stats.dir_deletes == 1


@pytest.mark.asyncio
async def test_row_deleted_but_tree_edited_survives_as_a_new_row(ctx, monkeypatch):
    monkeypatch.setattr(reconcile, "get_user_skill_by_id", _async_return(None))
    ctx.report["demo"] = _rep(tree_hash="tree-2", entry=_linked_entry())

    await reconcile._decide(ctx, "demo")

    assert "_import_new" in ctx.calls
    assert "delete_dir" not in _ops(ctx)


@pytest.mark.asyncio
async def test_row_deleted_and_tree_unsyncable_is_kept_as_agent_content(ctx, monkeypatch):
    monkeypatch.setattr(reconcile, "get_user_skill_by_id", _async_return(None))
    ctx.report["demo"] = _rep(entry=_linked_entry(), syncable=False)

    await reconcile._decide(ctx, "demo")

    assert _ops(ctx) == ["set_entry"]


@pytest.mark.asyncio
async def test_row_promoted_to_the_user_tier_only_drops_the_link(ctx, monkeypatch):
    promoted = _row()
    promoted["workspace_id"] = None
    monkeypatch.setattr(reconcile, "get_user_skill_by_id", _async_return(promoted))
    ctx.report["demo"] = _rep(entry=_linked_entry())

    await reconcile._decide(ctx, "demo")

    assert _action(ctx, "update_sync")["sync"] is None
    assert ctx.stats.healed == 1


# ---------------------------------------------------------------------------
# Linked: the dir is gone
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dir_deleted_with_a_dirty_row_re_delivers_it(ctx):
    ctx.report["demo"] = _rep(present=False, tree_hash=None, entry=_linked_entry())
    ctx.ws_rows["demo"] = _row(content_hash="db-2")

    await reconcile._decide(ctx, "demo")

    assert "_push_down" in ctx.calls and ctx.stats.pushed == 1


@pytest.mark.asyncio
async def test_dir_deleted_with_a_clean_row_propagates_the_deletion(ctx, monkeypatch):
    deleted: list[tuple] = []

    async def _cas(user_id, skill_id, expect):
        deleted.append((user_id, skill_id, expect))
        return {"archive_key": "user-skills/u/abc.zip"}

    dropped: list[str] = []

    async def _drop(user_id, key):
        dropped.append(key)

    monkeypatch.setattr(reconcile, "delete_user_skill_cas", _cas)
    monkeypatch.setattr(reconcile, "drop_archive_if_unused", _drop)
    ctx.report["demo"] = _rep(present=False, tree_hash=None, entry=_linked_entry())
    ctx.ws_rows["demo"] = _row()

    await reconcile._decide(ctx, "demo")

    assert deleted == [("u-1", "sk-1", "db-1")]
    assert dropped == ["user-skills/u/abc.zip"]
    assert _ops(ctx) == ["remove_entry"]


# ---------------------------------------------------------------------------
# Unlinked state
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_row_with_no_sandbox_trace_is_delivered(ctx):
    ctx.ws_rows["demo"] = _row()

    await reconcile._decide(ctx, "demo")

    assert "_push_down" in ctx.calls and ctx.stats.pushed == 1


@pytest.mark.asyncio
async def test_managed_delivery_of_a_workspace_row_is_adopted(ctx):
    ctx.report["demo"] = _rep(entry=_entry())
    ctx.ws_rows["demo"] = _row()

    await reconcile._decide(ctx, "demo")

    assert "_push_down" in ctx.calls and ctx.stats.adopted == 1


@pytest.mark.asyncio
async def test_agent_installed_dir_shadowing_a_user_skill_absorbs_it(ctx):
    ctx.report["demo"] = _rep(entry=None)
    ctx.user_rows["demo"] = _row()

    await reconcile._decide(ctx, "demo")

    assert "_absorb_user_shadow" in ctx.calls


@pytest.mark.asyncio
async def test_brand_new_agent_installed_dir_is_imported(ctx):
    ctx.report["demo"] = _rep(entry=None)

    await reconcile._decide(ctx, "demo")

    assert "_import_new" in ctx.calls


@pytest.mark.asyncio
async def test_agent_installed_entry_without_a_dir_is_forgotten(ctx):
    ctx.report["demo"] = _rep(
        present=False, tree_hash=None, entry=_entry(source_type="agent")
    )

    await reconcile._decide(ctx, "demo")

    assert _ops(ctx) == ["remove_entry"]


@pytest.mark.asyncio
async def test_managed_entry_without_a_dir_or_row_is_left_to_the_prune_path(ctx):
    """User-tier delivery and orphan cleanup both belong to the generic
    managed path; the reconciler owns workspace rows only."""
    ctx.report["demo"] = _rep(present=False, tree_hash=None, entry=_entry())

    await reconcile._decide(ctx, "demo")

    assert ctx.actions == [] and ctx.calls == {}


def _async_return(value):
    async def _run(*args, **kwargs):
        return value

    return _run


class TestFailuresThatDependOnOtherRows:
    """A failure the fingerprint cannot see the remedy for must not suppress.

    ``lastFailedSync`` keys on this skill's tree hash and this row's content
    hash. The per-user caps and the sibling-alias check read *other* rows, so
    deleting a skill to free quota or renaming the sibling moves neither half
    of the key: suppressing there wedges the skill for the life of the sandbox,
    silently, and no user action clears it.
    """

    @staticmethod
    def _pass():
        # Not the shared ``ctx`` fixture: that one stubs ``_pull_up`` itself,
        # and these two tests call the real transfer functions.
        return reconcile._Pass(
            sandbox=object(),
            user_id="u-1",
            workspace_id="ws-1",
            report={},
            ws_rows={},
            user_rows={},
        )

    @staticmethod
    def _validated():
        from src.server.services.user_skills.validate import ValidatedSkill

        return ValidatedSkill(
            name="demo",
            description="d",
            license=None,
            frontmatter={},
            allowed_tools=[],
            skill_md="---\nname: demo\n---\n",
            canonical_zip=b"PK\x05\x06" + b"\x00" * 18,
            content_hash="db-2",
            file_count=1,
        )

    @pytest.mark.asyncio
    async def test_the_import_insert_retries(self, monkeypatch):
        monkeypatch.setattr(
            reconcile, "_store", _async_return((None, b"zip"))
        )
        monkeypatch.setattr(
            reconcile, "drop_archive_if_unused", _async_return(None)
        )

        async def _raise(*a, **k):
            raise ValueError("Maximum of 50 skills per user reached")

        monkeypatch.setattr(reconcile, "create_user_skill", _raise)

        with pytest.raises(reconcile._SyncFailure) as exc:
            await reconcile._create_linked_row(
                self._pass(), "demo", self._validated(), "tree-2"
            )
        assert exc.value.suppress is False

    @pytest.mark.asyncio
    async def test_the_pull_up_write_retries(self, monkeypatch):
        monkeypatch.setattr(
            reconcile, "_store", _async_return((None, b"zip"))
        )
        monkeypatch.setattr(
            reconcile, "drop_archive_if_unused", _async_return(None)
        )
        monkeypatch.setattr(
            reconcile,
            "_download_validated",
            _async_return((self._validated(), "tree-2")),
        )

        async def _raise(*a, **k):
            raise ValueError("Skill storage limit reached")

        monkeypatch.setattr(reconcile, "update_user_skill_content_cas", _raise)

        with pytest.raises(reconcile._SyncFailure) as exc:
            await reconcile._pull_up(
                self._pass(), "demo", _row(), conflict=False
            )
        assert exc.value.suppress is False
