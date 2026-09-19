"""Which cache view a turn resolves, and which tier lands in each one.

Two rules carry the whole design here. The delivery view (``dir``) must hold
the user tier only, because the reconciler is the sole writer of workspace
skill dirs and the generic managed upload would fight it; and the two views
must live in separate scope namespaces, because each scope's resolve deletes
its own siblings.

The scope *choice* is the third rule: the delivery view is the whole enabled
user tier and resolves in the plain user scope for every workspace, because it
materialises the one shared skill directory a computer has. A workspace's
shadows and disables shape its effective set and its link view, never that
directory.
"""

import pytest

from src.server.database.account_disables import AccountDisables
from src.server.services.plugins import bundled
from src.server.services.user_skills import materialize


def _row(
    name: str,
    *,
    workspace_id: str | None = None,
    content: str = "c",
    enabled: bool = True,
) -> dict:
    return {
        "name": name,
        "description": f"{name} skill",
        "content_hash": f"sha256:{content}",
        "confirmed": True,
        "enabled": enabled,
        "workspace_id": workspace_id,
        "user_skill_id": f"id-{name}-{workspace_id or 'user'}",
        "command": None,
    }


@pytest.fixture
def resolved(monkeypatch):
    """Record every ``resolve_user_skill_dir`` call as ``{scope: [names]}``."""
    seen: dict[str, list[str]] = {}

    async def _resolve(user_id, rows, *, scope="user"):
        seen[scope] = [r["name"] for r in rows]
        return (f"/cache/{scope}" if rows else None), rows

    monkeypatch.setattr(materialize, "resolve_user_skill_dir", _resolve)
    return seen


@pytest.fixture
def db(monkeypatch):
    """Stub the reads ``load_user_skill_bundle`` makes.

    ``rows`` is the enabled set; ``ws_rows`` is every workspace-scoped row
    including the disabled ones, which is a distinct read because shadowing is
    by name rather than by enabled state. It defaults to the enabled workspace
    rows so a test that does not care about disables says nothing about it.
    """
    state: dict = {
        "rows": [],
        "ws_rows": None,
        "ws_disabled": set(),
        # Account-tier disables, the per-skill kind. The bundle kind below
        # reaches the same tier through the boot snapshot instead.
        "disabled_builtins": set(),
        # Bundle name -> the platform skills it ships, plus the ones this user
        # switched off. Both empty is the ordinary account.
        "bundle_owns": {},
        "disabled_bundles": set(),
    }

    async def _rows(user_id, workspace_id=None):
        return state["rows"]

    async def _all_ws_rows(user_id, workspace_id=None):
        if state["ws_rows"] is not None:
            return state["ws_rows"]
        return [r for r in state["rows"] if r["workspace_id"] == workspace_id]

    async def _ws_disabled(workspace_id):
        return state["ws_disabled"]

    async def _disabled_builtins(user_id):
        return frozenset(state["disabled_builtins"])

    async def _empty_dict(user_id):
        return {}

    async def _disabled_bundles(user_id):
        return AccountDisables(
            servers=frozenset(), bundles=frozenset(state["disabled_bundles"])
        )

    def _owners():
        return bundled.ComponentOwners(
            servers={},
            skills={
                name: bundle
                for bundle, names in state["bundle_owns"].items()
                for name in names
            },
        )

    monkeypatch.setattr(materialize, "list_enabled_user_skills", _rows)
    monkeypatch.setattr(materialize, "list_user_skills", _all_ws_rows)
    monkeypatch.setattr(materialize, "list_workspace_skill_disables", _ws_disabled)
    monkeypatch.setattr(
        materialize, "get_disabled_builtin_skills", _disabled_builtins
    )
    monkeypatch.setattr(materialize, "get_skill_command_overrides", _empty_dict)
    monkeypatch.setattr(materialize, "list_account_disables", _disabled_bundles)
    monkeypatch.setattr(bundled, "component_owners", _owners)
    return state


@pytest.mark.asyncio
async def test_user_tier_only_resolves_the_plain_user_scope(db, resolved):
    db["rows"] = [_row("alpha"), _row("beta")]

    bundle = await materialize.load_user_skill_bundle("u-1")

    assert resolved["user"] == ["alpha", "beta"]
    assert bundle.dir == "/cache/user"
    assert bundle.workspace_dir is None
    assert [s.name for s in bundle.skills] == ["alpha", "beta"]


@pytest.mark.asyncio
async def test_workspace_rows_stay_out_of_the_delivery_view(db, resolved):
    db["rows"] = [_row("alpha"), _row("ws-only", workspace_id="ws-1")]

    bundle = await materialize.load_user_skill_bundle("u-1", "ws-1")

    assert resolved["user"] == ["alpha"], "delivery view is the user tier only"
    assert resolved[materialize._own_scope_key("ws-1")] == ["ws-only"]
    assert bundle.dir == "/cache/user"
    assert bundle.workspace_dir is not None and bundle.workspace_dir != bundle.dir
    # Both tiers are still one effective set for the manifest and slash menu.
    assert {s.name for s in bundle.skills} == {"alpha", "ws-only"}
    assert {s.name for s in bundle.skills if s.workspace_scoped} == {"ws-only"}


@pytest.mark.asyncio
async def test_untouched_user_tier_reuses_the_shared_user_view(db, resolved):
    """A workspace resolves the delivery view in the plain user scope, never a
    private copy of identical bytes."""
    db["rows"] = [_row("alpha"), _row("beta"), _row("ws-only", workspace_id="ws-1")]

    await materialize.load_user_skill_bundle("u-1", "ws-1")

    assert resolved["user"] == ["alpha", "beta"]
    assert materialize._scope_key("ws-1") not in resolved


@pytest.mark.asyncio
async def test_a_shadowed_user_skill_stays_in_the_delivery_view(db, resolved):
    """The workspace row hides the same-named user row from this workspace's
    effective set only. The shared directory keeps serving the user-tier body,
    which is the one every sibling workspace reads, and the two tiers land in
    different sandbox directories so neither overwrites the other."""
    db["rows"] = [_row("alpha"), _row("alpha", workspace_id="ws-1", content="d")]

    bundle = await materialize.load_user_skill_bundle("u-1", "ws-1")

    assert resolved["user"] == ["alpha"], "the shadowed user row stays delivered"
    assert materialize._scope_key("ws-1") not in resolved
    assert bundle.dir == "/cache/user"
    assert resolved[materialize._own_scope_key("ws-1")] == ["alpha"]
    # One effective entry, and it is the workspace copy.
    assert [(s.name, s.workspace_scoped) for s in bundle.skills] == [("alpha", True)]


@pytest.mark.asyncio
async def test_a_disabled_workspace_row_still_shadows_its_user_twin(db, resolved):
    """Shadowing is by name, not by enabled state.

    Turning the workspace copy off turns that name off in the workspace rather
    than falling it back to the inherited body, which is what the management
    list already shows. The delivery view is unmoved either way: it feeds the
    shared directory the sibling workspaces read.
    """
    db["rows"] = [_row("alpha"), _row("beta")]
    db["ws_rows"] = [_row("alpha", workspace_id="ws-1", content="d", enabled=False)]

    bundle = await materialize.load_user_skill_bundle("u-1", "ws-1")

    assert resolved["user"] == ["alpha", "beta"]
    assert materialize._scope_key("ws-1") not in resolved
    assert [s.name for s in bundle.skills] == ["beta"]


@pytest.mark.asyncio
async def test_a_workspace_disable_leaves_the_delivery_view_alone(db, resolved):
    """The defect this locks: subtracting the disable from the delivery view
    had the asset sync prune the name from the shared directory, taking the
    skill from every sibling workspace on the computer (and flip-flopping the
    directory turn by turn). The disable reaches the agent as an absent link in
    this workspace's own view and as a name in ``disabled_builtins``.
    """
    db["rows"] = [_row("alpha"), _row("beta")]
    db["ws_disabled"] = {"alpha"}

    bundle = await materialize.load_user_skill_bundle("u-1", "ws-1")

    assert resolved["user"] == ["alpha", "beta"]
    assert materialize._scope_key("ws-1") not in resolved
    assert bundle.dir == "/cache/user"
    assert [s.name for s in bundle.skills] == ["beta"]
    assert "alpha" in bundle.disabled_builtins
    assert bundle.workspace_disabled_builtins == frozenset({"alpha"})


@pytest.mark.asyncio
async def test_sibling_workspaces_share_one_delivery_view(db, resolved):
    """One computer, one shared skill directory: the workspace that disables a
    skill and the sibling that does not must deliver the same view, or the two
    turns prune and re-upload it against each other."""
    db["rows"] = [_row("alpha"), _row("beta")]

    db["ws_disabled"] = {"alpha"}
    disabling = await materialize.load_user_skill_bundle("u-1", "ws-1")
    db["ws_disabled"] = set()
    sibling = await materialize.load_user_skill_bundle("u-1", "ws-2")

    assert disabling.dir == sibling.dir
    assert resolved["user"] == ["alpha", "beta"]


@pytest.mark.asyncio
async def test_a_row_whose_archive_is_unfetchable_drops_from_the_manifest(
    db, monkeypatch
):
    """A skill with no body on disk must not advertise a slash trigger that
    would resolve to nothing."""
    db["rows"] = [_row("alpha"), _row("broken")]

    async def _resolve(user_id, rows, *, scope="user"):
        kept = [r for r in rows if r["name"] != "broken"]
        return (f"/cache/{scope}" if kept else None), kept

    monkeypatch.setattr(materialize, "resolve_user_skill_dir", _resolve)

    bundle = await materialize.load_user_skill_bundle("u-1")

    assert [s.name for s in bundle.skills] == ["alpha"]


class TestScopeNamespaces:
    def test_delivery_and_own_scopes_never_collide(self):
        """They GC their own siblings, so sharing a namespace would make each
        resolve delete the other's view."""
        assert materialize._own_scope_key("ws-1") != materialize._scope_key("ws-1")
        assert materialize._own_scope_key(None) != materialize._scope_key(None)

    def test_scopes_are_per_workspace(self):
        assert materialize._scope_key("ws-1") != materialize._scope_key("ws-2")
        assert materialize._scope_key(None) == "user"

    def test_view_hash_is_content_addressed_and_order_free(self):
        a, b = _row("alpha"), _row("beta", content="d")
        assert materialize._view_hash([a, b]) == materialize._view_hash([b, a])
        assert materialize._view_hash([a, b]) != materialize._view_hash([a])
        assert materialize._view_hash([a]) != materialize._view_hash(
            [_row("alpha", content="changed")]
        )


class TestTheListingAgreesWithDelivery:
    """A switched-off bundle has to subtract the same names from both halves.

    ``materialize`` folds a disabled bundle's skills into the same set a
    per-skill disable writes to, so the agent never loads them. The default
    ``/skills`` response is the slash-command menu, and it is assembled
    separately -- from the registry rather than from the delivery bundle -- so
    nothing but this makes the two agree. When they disagree the menu offers a
    command whose skill has already been removed from the registry the turn
    runs against.

    The management view is deliberately the other way: the row stays, with its
    own switch still on and ``plugin_enabled`` false, because that pair is what
    the Plugins page draws as "suppressed by its package".
    """

    @pytest.fixture
    def listing(self, monkeypatch):
        from src.server.app import skills as skills_app

        state = {"platform": ["alpha", "beta"], "disabled_bundles": set()}

        async def _none(user_id):
            return set()

        async def _empty(user_id):
            return {}

        async def _disables(user_id):
            return AccountDisables(
                servers=frozenset(), bundles=frozenset(state["disabled_bundles"])
            )

        async def _rows(user_id, workspace_id=None):
            return []

        monkeypatch.setattr(
            skills_app,
            "list_skills",
            lambda mode=None: [
                {
                    "name": n,
                    "description": f"{n} skill",
                    "tool_count": 0,
                    "tools": [],
                    "command": f"/{n}",
                }
                for n in state["platform"]
            ],
        )
        monkeypatch.setattr(skills_app, "get_disabled_builtin_skills", _none)
        monkeypatch.setattr(skills_app, "get_skill_command_overrides", _empty)
        monkeypatch.setattr(skills_app, "list_account_disables", _disables)
        monkeypatch.setattr(skills_app, "list_user_skills", _rows)
        monkeypatch.setattr(skills_app, "list_enabled_user_skills", _rows)
        monkeypatch.setattr(
            bundled,
            "component_owners",
            lambda: bundled.ComponentOwners(servers={}, skills={"alpha": "pack"}),
        )
        return state

    async def _names(self, *, include_disabled: bool) -> list[str]:
        from src.server.app.skills import _assemble_skills

        payload = await _assemble_skills(
            "u-1", None, include_disabled, None
        )
        return [s.name for s in payload["skills"]]

    @pytest.mark.asyncio
    async def test_a_suppressed_skill_leaves_the_slash_menu(self, listing):
        listing["disabled_bundles"] = {"pack"}
        assert await self._names(include_disabled=False) == ["beta"]

    @pytest.mark.asyncio
    async def test_it_stays_in_the_management_view_wearing_its_reason(
        self, listing
    ):
        from src.server.app.skills import _assemble_skills

        listing["disabled_bundles"] = {"pack"}
        payload = await _assemble_skills("u-1", None, True, None)
        (alpha,) = [s for s in payload["skills"] if s.name == "alpha"]
        assert (alpha.enabled, alpha.plugin_name, alpha.plugin_enabled) == (
            True, "pack", False,
        )

    @pytest.mark.asyncio
    async def test_an_enabled_bundle_subtracts_nothing(self, listing):
        assert await self._names(include_disabled=False) == ["alpha", "beta"]


@pytest.mark.asyncio
async def test_a_bundle_disable_subtracts_against_the_boot_snapshot(
    db, resolved, monkeypatch
):
    """Materialization withdraws, so it reads the map the running set came from.

    A live re-read of ``plugins/`` can stop naming a bundle the process is
    still delivering skills for -- renamed on disk, or momentarily unreadable.
    Subtracting against that map hands the user back skills they switched off,
    with the registry still carrying them; the server-side twin already reads
    the snapshot, and this path was the one left behind.
    """
    from src.server.app import setup

    db["disabled_bundles"] = {"market"}

    monkeypatch.setattr(
        setup,
        "bundle_owners",
        bundled.ComponentOwners(servers={}, skills={"morning-note": "market"}),
    )
    # Disk has moved on: the bundle answers under a different name now, so a
    # live read attributes nothing to the name the disable was written under.
    monkeypatch.setattr(
        bundled,
        "component_owners",
        lambda: bundled.ComponentOwners(
            servers={}, skills={"morning-note": "market-data"}
        ),
    )

    bundle = await materialize.load_user_skill_bundle("u-1")

    assert bundle.disabled_builtins == frozenset({"morning-note"})


class TestWorkspaceDisablesStayOffTheSharedTier:
    """A workspace disable hides a skill from one workspace, not from the disk.

    The shared skill directory at the computer root serves every workspace on
    it, so deleting a skill there for one workspace's sake takes it from the
    siblings. The bundle keeps the two disable tiers apart for that reason: the
    registry reads their union, the upload reads the account tier alone.
    """

    @pytest.mark.asyncio
    async def test_the_bundle_carries_the_workspace_disable_separately(self, db):
        db["bundle_owns"] = {"market": ["morning-note"]}
        db["ws_disabled"] = {"morning-note"}

        bundle = await materialize.load_user_skill_bundle("u-1", "ws-1")

        assert bundle.disabled_builtins == frozenset({"morning-note"})
        assert bundle.workspace_disabled_builtins == frozenset({"morning-note"})
        assert bundle.account_disabled_builtins == frozenset()

    @pytest.mark.asyncio
    async def test_the_user_tier_bundle_has_no_workspace_disables(self, db):
        db["ws_disabled"] = {"morning-note"}

        bundle = await materialize.load_user_skill_bundle("u-1")

        assert bundle.workspace_disabled_builtins == frozenset()

    @pytest.mark.asyncio
    async def test_a_workspace_disable_does_not_prune_the_shared_upload(self, db):
        db["bundle_owns"] = {"market": ["morning-note"]}
        db["ws_disabled"] = {"morning-note"}

        params = await materialize.sandbox_skill_sync_params(
            "u-1", "/home/workspace/.agents/skills", "ws-1"
        )

        assert "disabled_skills" not in params

    @pytest.mark.asyncio
    async def test_an_account_disable_still_prunes_the_shared_upload(self, db):
        db["bundle_owns"] = {"market": ["morning-note"]}
        db["disabled_bundles"] = {"market"}

        params = await materialize.sandbox_skill_sync_params(
            "u-1", "/home/workspace/.agents/skills", "ws-1"
        )

        assert params["disabled_skills"] == frozenset({"morning-note"})

    @pytest.mark.asyncio
    async def test_both_tiers_naming_one_skill_still_prunes_the_upload(self, db):
        """The two tiers are stored apart, so an overlap cannot cancel itself.

        While the account tier was reconstructed by subtracting the workspace
        tier out of the union, a name switched off at both levels came back
        empty and the shared directory kept shipping it.
        """
        db["disabled_builtins"] = {"morning-note"}
        db["ws_disabled"] = {"morning-note"}

        bundle = await materialize.load_user_skill_bundle("u-1", "ws-1")
        params = await materialize.sandbox_skill_sync_params(
            "u-1", "/home/workspace/.agents/skills", "ws-1"
        )

        assert bundle.account_disabled_builtins == frozenset({"morning-note"})
        assert bundle.workspace_disabled_builtins == frozenset({"morning-note"})
        assert bundle.disabled_builtins == frozenset({"morning-note"})
        assert params["disabled_skills"] == frozenset({"morning-note"})
