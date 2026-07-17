"""Feature-flag catalog and gate resolution semantics (src/config/features.py)."""

import src.config.features as features_module
from src.config.features import (
    FEATURES,
    FeatureGate,
    FeatureSpec,
    default_feature_enabled,
    effective_flags,
    is_feature_enabled_system,
    system_flag,
)
from src.config.models import FeatureFlagOverride, InfrastructureConfig


def _stub_config(monkeypatch, **overrides):
    cfg = InfrastructureConfig(
        features={k: FeatureFlagOverride(**v) for k, v in overrides.items()}
    )
    monkeypatch.setattr(features_module, "get_infrastructure_config", lambda: cfg)


def _add_feature(monkeypatch, key, **kwargs):
    monkeypatch.setitem(
        FEATURES, key, FeatureSpec(key=key, label=key, description="", **kwargs)
    )


def test_market_watch_is_cataloged_enabled_opt_in():
    spec = FEATURES["market_watch"]
    assert spec.enabled is True
    assert spec.gate is FeatureGate.OPT_IN
    assert spec.label and spec.description
    assert spec.tradeoffs  # experiments state their cost


def test_system_flag_partial_override_merges_with_catalog(monkeypatch):
    _stub_config(monkeypatch, market_watch={"gate": "opt_out"})
    flag = system_flag("market_watch")
    assert flag.enabled is True  # unset field inherits the catalog
    assert flag.gate is FeatureGate.OPT_OUT

    _stub_config(monkeypatch, market_watch={"enabled": False})
    assert is_feature_enabled_system("market_watch") is False


def test_kill_switch_beats_every_gate(monkeypatch):
    _stub_config(monkeypatch, market_watch={"enabled": False, "gate": "none"})
    assert effective_flags({"market_watch": True})["market_watch"] is False


def test_gate_none_is_on_for_everyone_and_ignores_overrides(monkeypatch):
    _stub_config(monkeypatch, market_watch={"gate": "none"})
    assert effective_flags(None)["market_watch"] is True
    assert effective_flags({"market_watch": False})["market_watch"] is True


def test_gate_opt_in_defaults_off_until_user_opts_in(monkeypatch):
    _stub_config(monkeypatch)  # catalog default: opt_in
    assert effective_flags(None)["market_watch"] is False
    assert effective_flags({"market_watch": True})["market_watch"] is True
    assert effective_flags({"market_watch": "yes"})["market_watch"] is False


def test_gate_opt_out_defaults_on_until_user_opts_out(monkeypatch):
    _stub_config(monkeypatch, market_watch={"gate": "opt_out"})
    assert effective_flags(None)["market_watch"] is True
    assert effective_flags({"market_watch": False})["market_watch"] is False
    assert effective_flags({"market_watch": "no"})["market_watch"] is True


def test_gate_plan_platform_mode_requires_tier(monkeypatch):
    _add_feature(
        monkeypatch, "pro_widget", enabled=True, gate=FeatureGate.PLAN, min_tier=2
    )
    _stub_config(monkeypatch)
    kw = {"platform_mode": True}
    assert effective_flags(None, access_tier=None, **kw)["pro_widget"] is False
    assert effective_flags(None, access_tier=1, **kw)["pro_widget"] is False
    assert effective_flags(None, access_tier=2, **kw)["pro_widget"] is True
    # User overrides never move a plan gate.
    assert (
        effective_flags({"pro_widget": True}, access_tier=1, **kw)["pro_widget"]
        is False
    )


def test_gate_plan_bypassed_outside_platform_mode(monkeypatch):
    _add_feature(
        monkeypatch, "pro_widget", enabled=True, gate=FeatureGate.PLAN, min_tier=2
    )
    _stub_config(monkeypatch)
    assert effective_flags(None, platform_mode=False)["pro_widget"] is True


def test_gate_plan_without_min_tier_fails_closed_in_platform(monkeypatch):
    _add_feature(monkeypatch, "pro_widget", enabled=True, gate=FeatureGate.PLAN)
    _stub_config(monkeypatch)
    assert (
        effective_flags(None, access_tier=99, platform_mode=True)["pro_widget"]
        is False
    )


def test_unknown_override_keys_are_ignored(monkeypatch):
    _stub_config(monkeypatch)
    resolved = effective_flags({"no_such_feature": True})
    assert "no_such_feature" not in resolved


def test_default_feature_enabled_no_user_context(monkeypatch):
    _stub_config(monkeypatch)
    assert default_feature_enabled("market_watch") is False  # opt_in
    _stub_config(monkeypatch, market_watch={"gate": "opt_out"})
    assert default_feature_enabled("market_watch") is True
    assert default_feature_enabled("no_such_feature") is False
