"""cl100k_base fetches its BPE file on first use, which the unit suite's
blocked sockets refuse on a cold cache (CI runners). The prefix tests count
what no call measured through it, so the loader is stubbed for the whole
folder."""

from types import SimpleNamespace

import pytest


@pytest.fixture(autouse=True)
def _offline_token_counter(monkeypatch):
    from ptc_agent.agent.middleware.compaction import utils

    fake = SimpleNamespace(encode=lambda text, **_: [0] * (len(text) // 4 + 1))
    monkeypatch.setattr(utils, "_get_tiktoken_encoder", lambda: fake)


@pytest.fixture(autouse=True)
def _release_at_zero(monkeypatch):
    """The rebuild tests read a build at version 0 with nothing urgent, so a
    bump in prompts.yaml does not move their expectations; a test about the
    urgency marker sets its own release over this."""
    from ptc_agent.agent.prompts import release

    monkeypatch.setattr(release, "_config", lambda: {"prompt_version": 0, "rebuild_before": 0})
