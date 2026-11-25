"""A start installs the token the host sent and points it at the server
address it was given as one pair: another start between the two cannot
leave its token paired with this start's address."""

from __future__ import annotations

import json
from types import SimpleNamespace

from ptc_agent.core.sandbox.livefs_runtime import lifecycle


def test_a_start_between_install_and_rebase_cannot_split_the_pair(tmp_path, monkeypatch):
    token = lifecycle.Paths(private=str(tmp_path / "token"))
    paths = lifecycle.Paths(private=str(tmp_path / "drive"))
    a = {"base_url": "https://a.example.com", "token": "token-a"}
    b = {"base_url": "https://b.example.com", "token": "token-b"}

    def another_start_lands():
        with lifecycle.locked(token):
            lifecycle.install_config(json.dumps(b), token)
        return True

    monkeypatch.setattr(lifecycle, "unsupported", lambda: None)
    monkeypatch.setattr(lifecycle, "has_libfuse", another_start_lands)
    args = SimpleNamespace(config=json.dumps(a), base_url=a["base_url"])
    assert lifecycle.prepare(args, paths, token)[0] is None
    with open(token.config) as f:
        config = json.load(f)
    assert config in (a, b), f"a token paired with another start's address: {config}"
