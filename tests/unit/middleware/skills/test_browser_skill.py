"""The browser skill is offered only to a person who turned the browser on."""

import ptc_agent.agent.middleware.skills.registry as registry
from ptc_agent.agent.middleware.skills.registry import get_skill_registry
from src.config.features import FEATURES, FeatureGate


def test_the_browser_is_opt_in_and_owns_its_skill():
    assert FEATURES["browser"].gate is FeatureGate.OPT_IN
    assert registry.SKILL_REGISTRY["browser"].feature == "browser"


def test_the_skill_follows_each_person_s_switch(monkeypatch):
    # The agent's build passes the turn's user's resolver, which wins over the
    # deployment's default either way.
    monkeypatch.setattr(registry, "is_feature_enabled_system", lambda key: True)
    assert "browser" not in get_skill_registry(feature_resolver=lambda key: key != "browser")
    monkeypatch.setattr(registry, "is_feature_enabled_system", lambda key: False)
    assert "browser" in get_skill_registry(feature_resolver=lambda key: True)
