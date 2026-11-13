from types import SimpleNamespace

import pytest


@pytest.fixture(autouse=True)
def _offline_token_counter(monkeypatch):
    """cl100k_base fetches its BPE file on first use, which the unit suite's
    blocked sockets refuse on a cold cache (CI runners); a brief of the thread
    itself is fitted with Claude's count, which reads it."""
    from ptc_agent.agent.middleware.compaction import utils

    fake = SimpleNamespace(encode=lambda text, **_: [0] * (len(text) // 4 + 1))
    monkeypatch.setattr(utils, "_get_tiktoken_encoder", lambda: fake)


@pytest.fixture(autouse=True)
def _release_at_zero(monkeypatch):
    """The summary-hint tests build threads with no recorded prompt build, which
    reads as version 0; pinning the release there keeps a bump in prompts.yaml
    from turning every hint into a forced rebuild."""
    from ptc_agent.agent.prompts import release

    monkeypatch.setattr(release, "_config", lambda: {"prompt_version": 0, "rebuild_before": 0})
