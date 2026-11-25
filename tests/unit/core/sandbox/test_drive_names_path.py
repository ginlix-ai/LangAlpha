"""Which command text names the Drive: its link or its mount at a path
boundary, never a longer path that only ends the same way."""

from __future__ import annotations

import pytest

from ptc_agent.core.sandbox.drive_mount import names_drive


@pytest.mark.parametrize("text", ["ls /mnt/drive", "cat /mnt/drive/a.txt", "cd .agents/drive", "x=.agents/drive/b"])
def test_drive_paths_are_named(text):
    assert names_drive(text)


@pytest.mark.parametrize("text", ["cat ./fixtures/mnt/drive/report.txt", "ls /mnt/drive2", "ls .agents/drive.local"])
def test_longer_paths_are_not(text):
    assert not names_drive(text)
