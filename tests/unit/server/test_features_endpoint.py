"""GET/PUT /api/v1/features — per-user feature-flag state and overrides.

DB access is patched at the service's imports (get/upsert preferences, cache
invalidation); resolution logic runs for real against the code catalog.
"""

from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

USER_ID = "test-user-123"  # matches create_test_app's auth override


def _prefs(overrides=None):
    other = {} if overrides is None else {"feature_overrides": overrides}
    return {"user_id": USER_ID, "other_preference": other}


@pytest_asyncio.fixture
async def features_client():
    from src.server.app.features import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


class TestGetFeatures:
    @pytest.mark.asyncio
    async def test_lists_catalog_with_effective_state(self, features_client):
        with patch(
            "src.server.services.features.get_user_preferences",
            new=AsyncMock(return_value=_prefs()),
        ):
            resp = await features_client.get("/api/v1/features")
        assert resp.status_code == 200
        by_key = {f["key"]: f for f in resp.json()["features"]}
        mw = by_key["market_watch"]
        assert mw["enabled"] is False  # opt_in: off until the user opts in
        assert mw["gate"] == "opt_in"
        assert mw["min_tier"] is None
        assert mw["user_override"] is None
        assert mw["label"] and mw["description"]
        assert mw["tradeoffs"]

    @pytest.mark.asyncio
    async def test_kill_switched_feature_is_omitted(self, features_client):
        """Deployment ``enabled: false`` hides every surface — the list must
        not advertise a toggle that can never turn on."""
        from src.config.features import FeatureGate, SystemFlag

        killed = SystemFlag(enabled=False, gate=FeatureGate.OPT_IN, min_tier=None)
        with (
            patch(
                "src.server.services.features.get_user_preferences",
                new=AsyncMock(return_value=_prefs()),
            ),
            patch("src.server.services.features.system_flag", lambda key: killed),
        ):
            resp = await features_client.get("/api/v1/features")
        assert resp.status_code == 200
        assert all(
            f["key"] != "market_watch" for f in resp.json()["features"]
        )

    @pytest.mark.asyncio
    async def test_user_override_wins_and_is_reported(self, features_client):
        with patch(
            "src.server.services.features.get_user_preferences",
            new=AsyncMock(return_value=_prefs({"market_watch": True})),
        ):
            resp = await features_client.get("/api/v1/features")
        mw = {f["key"]: f for f in resp.json()["features"]}["market_watch"]
        assert mw["enabled"] is True
        assert mw["user_override"] is True
        assert mw["gate"] == "opt_in"

    @pytest.mark.asyncio
    async def test_an_override_for_a_retired_feature_is_inert(self, features_client):
        """A feature leaves the catalog while users still hold overrides for it."""
        with patch(
            "src.server.services.features.get_user_preferences",
            new=AsyncMock(
                return_value=_prefs({"retired_feature": False, "market_watch": True})
            ),
        ):
            resp = await features_client.get("/api/v1/features")
        assert resp.status_code == 200
        by_key = {f["key"]: f for f in resp.json()["features"]}
        assert "retired_feature" not in by_key
        assert by_key["market_watch"]["enabled"] is True


class TestPutFeatureOverride:
    @pytest.mark.asyncio
    async def test_sets_override_and_returns_new_state(self, features_client):
        upsert = AsyncMock(return_value={})
        invalidate = AsyncMock()
        with (
            patch(
                "src.server.services.features.get_user_preferences",
                new=AsyncMock(side_effect=[_prefs(), _prefs({"market_watch": True})]),
            ),
            patch("src.server.services.features.upsert_user_preferences", new=upsert),
            patch(
                "src.server.services.features.invalidate_user_prefs_cache",
                new=invalidate,
            ),
        ):
            resp = await features_client.put(
                "/api/v1/features/market_watch", json={"enabled": True}
            )
        assert resp.status_code == 200
        mw = {f["key"]: f for f in resp.json()["features"]}["market_watch"]
        assert mw["enabled"] is True
        upsert.assert_awaited_once_with(
            USER_ID, other_preference={"feature_overrides": {"market_watch": True}}
        )
        invalidate.assert_awaited_once_with(USER_ID)

    @pytest.mark.asyncio
    async def test_setting_one_keeps_a_retired_features_override(self, features_client):
        """Stored data is left alone: the bag is rewritten whole, retired keys too."""
        upsert = AsyncMock(return_value={})
        with (
            patch(
                "src.server.services.features.get_user_preferences",
                new=AsyncMock(return_value=_prefs({"retired_feature": False})),
            ),
            patch("src.server.services.features.upsert_user_preferences", new=upsert),
            patch(
                "src.server.services.features.invalidate_user_prefs_cache",
                new=AsyncMock(),
            ),
        ):
            resp = await features_client.put(
                "/api/v1/features/market_watch", json={"enabled": True}
            )
        assert resp.status_code == 200
        upsert.assert_awaited_once_with(
            USER_ID,
            other_preference={
                "feature_overrides": {"retired_feature": False, "market_watch": True}
            },
        )

    @pytest.mark.asyncio
    async def test_clearing_last_override_deletes_the_key(self, features_client):
        upsert = AsyncMock(return_value={})
        with (
            patch(
                "src.server.services.features.get_user_preferences",
                new=AsyncMock(side_effect=[_prefs({"market_watch": True}), _prefs()]),
            ),
            patch("src.server.services.features.upsert_user_preferences", new=upsert),
            patch(
                "src.server.services.features.invalidate_user_prefs_cache",
                new=AsyncMock(),
            ),
        ):
            resp = await features_client.put(
                "/api/v1/features/market_watch", json={"enabled": None}
            )
        assert resp.status_code == 200
        # Emptied dict is written as None so the DB layer deletes the field.
        upsert.assert_awaited_once_with(
            USER_ID, other_preference={"feature_overrides": None}
        )

    @pytest.mark.asyncio
    async def test_unknown_feature_404s(self, features_client):
        resp = await features_client.put(
            "/api/v1/features/no_such_feature", json={"enabled": True}
        )
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_non_opt_gate_403s(self, features_client):
        from src.config.features import FeatureGate, SystemFlag

        locked = SystemFlag(enabled=True, gate=FeatureGate.NONE, min_tier=None)
        with patch(
            "src.server.services.features.system_flag", lambda key: locked
        ):
            resp = await features_client.put(
                "/api/v1/features/market_watch", json={"enabled": True}
            )
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_a_grant_is_not_the_users_to_set(self, features_client):
        from src.config.features import FeatureGate, SystemFlag

        granted_only = SystemFlag(enabled=True, gate=FeatureGate.GRANT, min_tier=None)
        upsert = AsyncMock()
        with (
            patch("src.server.services.features.system_flag", lambda key: granted_only),
            patch("src.server.services.features.upsert_user_preferences", new=upsert),
        ):
            resp = await features_client.put(
                "/api/v1/features/claude_oauth", json={"enabled": True}
            )
        assert resp.status_code == 403
        upsert.assert_not_awaited()


class TestPlanGate:
    @pytest.mark.asyncio
    async def test_no_access_tier_fails_plan_gate_closed(
        self, features_client, monkeypatch
    ):
        """A -1 no-access tier normalizes to None, so a plan-gated feature
        fails closed (resolves False) instead of leaking the sentinel."""
        import src.config.settings as settings_module
        from src.config.features import FEATURES, FeatureGate, FeatureSpec

        monkeypatch.setitem(
            FEATURES,
            "pro_widget",
            FeatureSpec(
                key="pro_widget",
                label="Pro widget",
                description="tier-gated",
                enabled=True,
                gate=FeatureGate.PLAN,
                min_tier=2,
            ),
        )
        monkeypatch.setattr(settings_module, "HOST_MODE", "platform")

        async def _fetch(_user_id, tier):
            monkeypatch.setattr(
                "src.server.services.membership.access_tier",
                AsyncMock(return_value=tier),
            )
            with patch(
                "src.server.services.features.get_user_preferences",
                new=AsyncMock(return_value=_prefs()),
            ):
                resp = await features_client.get("/api/v1/features")
            assert resp.status_code == 200
            return {f["key"]: f for f in resp.json()["features"]}["pro_widget"]

        # -1 (no platform access) → None path → fail closed.
        no_access = await _fetch(USER_ID, -1)
        assert no_access["gate"] == "plan"
        assert no_access["enabled"] is False

        # Unknown (the platform down, no membership on record) fails closed too.
        unknown = await _fetch(USER_ID, None)
        assert unknown["enabled"] is False

        # Positive control: a tier meeting min_tier flows through the same
        # wiring and unlocks the feature.
        granted = await _fetch(USER_ID, 2)
        assert granted["enabled"] is True


class TestOrganizationTier:
    @staticmethod
    def _member(**features):
        from src.server.services.account_contract import Account, Org

        return Account(org=Org(id="org-1", name="Acme"), features={"teams": True, **features})

    @pytest.mark.asyncio
    async def test_an_org_off_is_listed_as_the_orgs_and_refuses_the_users_toggle(
        self, features_client
    ):
        upsert = AsyncMock()
        with (
            patch(
                "src.server.services.account.account_for",
                new=AsyncMock(return_value=self._member(market_watch=False)),
            ),
            patch(
                "src.server.services.features.get_user_preferences",
                new=AsyncMock(return_value=_prefs({"market_watch": True})),
            ),
            patch("src.server.services.features.upsert_user_preferences", new=upsert),
        ):
            listed = await features_client.get("/api/v1/features")
            put = await features_client.put(
                "/api/v1/features/market_watch", json={"enabled": True}
            )
        mw = {f["key"]: f for f in listed.json()["features"]}["market_watch"]
        assert mw["enabled"] is False
        assert mw["set_by_organization"] is True
        assert put.status_code == 403
        assert "organization" in put.json()["detail"]
        upsert.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_teams_resolves_on_exactly_when_the_door_is_open(self):
        from src.server.services.account_contract import NO_ORG
        from src.server.services.features import effective_flags_for_user

        for account, expected in ((self._member(), True), (NO_ORG, False), (None, False)):
            with patch(
                "src.server.services.account.account_for",
                new=AsyncMock(return_value=account),
            ):
                flags = await effective_flags_for_user(USER_ID, {"teams": True})
            assert flags["teams"] is expected

    @pytest.mark.asyncio
    async def test_an_unknown_account_lists_every_feature_off_and_keeps_the_toggle(
        self, features_client
    ):
        """Off, whatever the gate; the user's own setting is still theirs to save."""
        upsert = AsyncMock()
        with (
            patch(
                "src.server.services.account.account_for",
                new=AsyncMock(return_value=None),
            ),
            patch(
                "src.server.services.features.get_user_preferences",
                new=AsyncMock(return_value=_prefs({"market_watch": True})),
            ),
            patch("src.server.services.features.upsert_user_preferences", new=upsert),
            patch("src.server.services.features.invalidate_user_prefs_cache", new=AsyncMock()),
        ):
            listed = await features_client.get("/api/v1/features")
            put = await features_client.put(
                "/api/v1/features/market_watch", json={"enabled": True}
            )
        assert listed.status_code == 200
        assert not any(f["enabled"] for f in listed.json()["features"])
        assert put.status_code == 200
        upsert.assert_awaited_once()


class TestRequires:
    @staticmethod
    def _member(**features):
        from src.server.services.account_contract import Account, Org

        return Account(org=Org(id="org-1", name="Acme"), features={"teams": True, **features})

    @pytest.fixture
    def child(self, monkeypatch):
        """A grant feature that requires ``teams``, on in the deployment."""
        import src.config.features as features_module
        from src.config.features import FEATURES, FeatureGate, FeatureSpec
        from src.config.models import FeatureFlagOverride, InfrastructureConfig

        monkeypatch.setitem(
            FEATURES,
            "child",
            FeatureSpec(key="child", label="c", description="", enabled=True,
                        gate=FeatureGate.GRANT, requires=("teams",)),
        )
        cfg = InfrastructureConfig(features={"teams": FeatureFlagOverride(enabled=True)})
        monkeypatch.setattr(features_module, "get_infrastructure_config", lambda: cfg)

    @pytest.mark.asyncio
    async def test_a_child_of_teams_follows_the_door(self, child):
        from src.server.services.account_contract import NO_ORG
        from src.server.services.features import effective_flags_for_user

        cases = ((self._member(child=True), True), (NO_ORG, False), (None, False))
        for account, expected in cases:
            with patch(
                "src.server.services.account.account_for",
                new=AsyncMock(return_value=account),
            ):
                # A user grant alone never opens it while the door is shut.
                flags = await effective_flags_for_user(USER_ID, {"child": True})
            assert flags["child"] is expected, account

    @pytest.mark.asyncio
    async def test_the_listing_omits_a_child_whose_parent_is_kill_switched(
        self, child, features_client, monkeypatch
    ):
        import src.config.features as features_module
        from src.config.models import InfrastructureConfig

        with patch(
            "src.server.services.features.get_user_preferences",
            new=AsyncMock(return_value=_prefs()),
        ):
            listed = await features_client.get("/api/v1/features")
            by_key = {f["key"]: f for f in listed.json()["features"]}
            assert by_key["child"]["requires"] == ["teams"]
            monkeypatch.setattr(
                features_module, "get_infrastructure_config", InfrastructureConfig
            )
            listed = await features_client.get("/api/v1/features")
        assert "child" not in {f["key"] for f in listed.json()["features"]}
