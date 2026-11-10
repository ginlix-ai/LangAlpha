import pytest


@pytest.fixture(autouse=True)
def _release_at_zero(monkeypatch):
    """The summary-hint tests build threads with no recorded prompt build, which
    reads as version 0; pinning the release there keeps a bump in prompts.yaml
    from turning every hint into a forced rebuild."""
    from ptc_agent.agent.prompts import release

    monkeypatch.setattr(release, "_config", lambda: {"prompt_version": 0, "rebuild_before": 0})
