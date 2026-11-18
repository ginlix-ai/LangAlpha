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


def test_scratchpad_is_on_unless_the_user_stored_false():
    spec = FEATURES["scratchpad"]
    assert spec.enabled is True
    assert spec.gate is FeatureGate.OPT_OUT
    assert spec.label and spec.description
    assert spec.tradeoffs
    assert effective_flags(None)["scratchpad"] is True
    assert effective_flags({"scratchpad": False})["scratchpad"] is False
    assert default_feature_enabled("scratchpad") is True


def test_claude_code_runtime_is_on_for_everyone_unless_a_deployment_gates_it(monkeypatch):
    _stub_config(monkeypatch)
    assert effective_flags(None)["claude_code_runtime"] is True
    assert effective_flags({"claude_code_runtime": False})["claude_code_runtime"] is True
    _stub_config(monkeypatch, claude_code_runtime={"gate": "opt_in"})
    assert effective_flags(None)["claude_code_runtime"] is False
    assert effective_flags({"claude_code_runtime": True})["claude_code_runtime"] is True
    _stub_config(monkeypatch, claude_code_runtime={"enabled": False})
    assert is_feature_enabled_system("claude_code_runtime") is False


def test_idle_compaction_is_off_until_the_user_opts_in(monkeypatch):
    _stub_config(monkeypatch)
    spec = FEATURES["idle_compaction"]
    assert spec.gate is FeatureGate.OPT_IN
    assert spec.tradeoffs
    assert is_feature_enabled_system("idle_compaction") is True
    assert effective_flags(None)["idle_compaction"] is False
    assert effective_flags({"idle_compaction": True})["idle_compaction"] is True


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


def test_reset_preferences_keeps_only_the_overrides_on_the_grant_gate(monkeypatch):
    from src.server.services.features import granted_override_keys

    _stub_config(monkeypatch)
    assert granted_override_keys() == [
        "claude_oauth", "teams", "org_connectors", "org_library",
        "shared_workspaces",
    ]
    _stub_config(
        monkeypatch,
        claude_oauth={"gate": "none"},
        teams={"gate": "none"},
        org_connectors={"gate": "none"},
        org_library={"gate": "none"},
        shared_workspaces={"gate": "none"},
    )
    assert granted_override_keys() == []


def test_gate_grant_is_off_until_granted_and_takes_only_true(monkeypatch):
    _stub_config(monkeypatch, market_watch={"gate": "grant"})
    assert effective_flags(None)["market_watch"] is False
    assert effective_flags({"market_watch": True})["market_watch"] is True
    assert effective_flags({"market_watch": "yes"})["market_watch"] is False
    assert default_feature_enabled("market_watch") is False


def test_claude_sign_in_is_only_for_users_granted_it_unless_a_deployment_opens_it(monkeypatch):
    _stub_config(monkeypatch)
    assert effective_flags(None)["claude_oauth"] is False
    assert effective_flags({"claude_oauth": True})["claude_oauth"] is True
    _stub_config(monkeypatch, claude_oauth={"gate": "none"})
    assert effective_flags(None)["claude_oauth"] is True


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


def test_teams_is_dark_until_a_deployment_turns_it_on(monkeypatch):
    _stub_config(monkeypatch)
    assert FEATURES["teams"].gate is FeatureGate.GRANT
    assert is_feature_enabled_system("teams") is False
    assert effective_flags(None, org_features={"teams": True})["teams"] is False


def test_org_connectors_is_granted_by_the_organization_behind_teams(monkeypatch):
    spec = FEATURES["org_connectors"]
    assert spec.enabled is False
    assert spec.gate is FeatureGate.GRANT
    assert spec.requires == ("teams",)
    _stub_config(monkeypatch)
    assert is_feature_enabled_system("org_connectors") is False
    # Turned on without teams it stays off: its parent's kill switch wins.
    _stub_config(monkeypatch, org_connectors={"enabled": True})
    assert is_feature_enabled_system("org_connectors") is False
    _stub_config(monkeypatch, org_connectors={"enabled": True}, teams={"enabled": True})
    assert is_feature_enabled_system("org_connectors") is True
    granted = effective_flags(
        None, org_features={"org_connectors": True}, forced={"teams": True}
    )
    assert granted["org_connectors"] is True
    assert effective_flags(None, forced={"teams": True})["org_connectors"] is False
    assert (
        effective_flags(
            None, org_features={"org_connectors": True}, forced={"teams": False}
        )["org_connectors"]
        is False
    )


def test_the_org_tier_sits_between_the_deployment_and_the_user(monkeypatch):
    _stub_config(
        monkeypatch,
        market_watch={"gate": "opt_in"},
        scratchpad={"gate": "opt_out"},
        claude_code_runtime={"gate": "none"},
        claude_oauth={"gate": "grant"},
    )
    user = {"market_watch": True, "scratchpad": True, "claude_oauth": True}
    off = {
        "market_watch": False,
        "scratchpad": False,
        "claude_code_runtime": False,
        "claude_oauth": False,
    }
    # The organization's off wins over every gate and the user's own choice.
    assert not any(effective_flags(user, org_features=off)[k] for k in off)
    # Its on grants a grant gate, and nothing else.
    on = dict.fromkeys(off, True)
    resolved = effective_flags(None, org_features=on)
    assert resolved["claude_oauth"] is True
    assert resolved["market_watch"] is False
    assert resolved["scratchpad"] is True
    # Absent inherits, and the deployment kill switch still wins.
    assert effective_flags(user, org_features={})["market_watch"] is True
    _stub_config(monkeypatch, claude_oauth={"enabled": False})
    assert effective_flags(None, org_features=on)["claude_oauth"] is False


def test_an_unknown_account_resolves_every_feature_off(monkeypatch):
    """Any feature might be one the organization turned off, whatever its gate."""
    _stub_config(monkeypatch)
    user = {"claude_oauth": True, "market_watch": True}
    resolved = effective_flags(user, account_unknown=True, platform_mode=False)
    assert resolved and not any(resolved.values())


def test_a_child_is_off_while_its_parent_is_kill_switched(monkeypatch):
    _add_feature(monkeypatch, "child", enabled=True, gate=FeatureGate.NONE,
                 requires=("market_watch",))
    _stub_config(monkeypatch, market_watch={"gate": "none"})
    assert system_flag("child").enabled is True
    assert effective_flags(None)["child"] is True
    _stub_config(monkeypatch, market_watch={"enabled": False})
    assert system_flag("child").enabled is False
    assert is_feature_enabled_system("child") is False
    assert effective_flags(None)["child"] is False


def test_a_child_is_off_wherever_its_parent_resolves_off(monkeypatch):
    _add_feature(monkeypatch, "child", enabled=True, gate=FeatureGate.NONE,
                 requires=("market_watch",))
    _stub_config(monkeypatch)  # market_watch: opt_in
    assert effective_flags(None)["child"] is False
    assert effective_flags({"market_watch": True})["child"] is True
    assert effective_flags({"market_watch": True},
                           org_features={"market_watch": False})["child"] is False


def test_a_forced_parent_carries_its_children(monkeypatch):
    _add_feature(monkeypatch, "child", enabled=True, gate=FeatureGate.NONE,
                 requires=("teams",))
    _stub_config(monkeypatch, teams={"enabled": True})
    assert effective_flags(None, forced={"teams": False})["child"] is False
    resolved = effective_flags(None, forced={"teams": True})
    assert resolved["teams"] is True and resolved["child"] is True


def test_a_parent_must_be_declared_before_its_child():
    import pytest

    from src.config.features import _check_requires

    spec = FeatureSpec(key="a", label="a", description="", requires=("b",))
    with pytest.raises(AssertionError):
        _check_requires({"a": spec, "b": FeatureSpec(key="b", label="b", description="")})
    with pytest.raises(AssertionError):
        _check_requires({"a": spec})
    _check_requires({"b": FeatureSpec(key="b", label="b", description=""), "a": spec})
