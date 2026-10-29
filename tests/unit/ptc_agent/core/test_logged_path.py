"""An automation's file is named after the automation, which operator logs
leave out however the path is spelled."""

from __future__ import annotations

import pytest

from ptc_agent.core.paths import logged_path


@pytest.mark.parametrize(
    ("path", "logged"),
    [
        (".agents/user/automations/daily-brief.json", ".agents/user/automations"),
        ("/mnt/livefs/user/automations/daily-brief.json", "/mnt/livefs/user/automations"),
        ("/mnt/livefs/user/./automations/daily-brief.json", "/mnt/livefs/user/automations"),
        ("/mnt/livefs//user//automations/daily-brief.json", "/mnt/livefs/user/automations"),
        ("/mnt/livefs/user/automations/daily-brief/../../profile/a.json", "/mnt/livefs/user/profile/a.json"),
        ("/mnt/livefs/user/memory/notes.md", "/mnt/livefs/user/memory/notes.md"),
    ],
    ids=["workspace", "mount", "dot", "double-slash", "walks-out", "elsewhere"],
)
def test_a_path_keeps_no_automation_name(path, logged):
    assert logged_path(path) == logged
