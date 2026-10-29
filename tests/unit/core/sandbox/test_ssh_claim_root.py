"""A folder taken back from another computer is marked as the taker's at once.

Until a start marked it, the cleared computer's mark stayed, and that
computer's own uninstall empties a folder its mark names. A folder the taker
never worked in also loses the restore markers that vouched for the cleared
computer's files, or its own backup would never be laid over them.
"""

from __future__ import annotations

import subprocess

from ptc_agent.core.sandbox.providers._ssh_shell import ROOT_MARKER, claim_root_script

ME = "a" * 32
GONE = "3cd2aabcfce04ecf83f69a2e2952866c"


def _run(script: str) -> int:
    return subprocess.run(["bash", "-c", script], check=False).returncode


def _claim(root, computers, **kwargs) -> str:
    return claim_root_script(
        str(root), ME, computers=str(computers), cleared=GONE, **kwargs
    )


def test_the_folder_is_marked_as_the_takers_and_its_restores_forgotten(tmp_path):
    root = tmp_path / "la"
    (root / "Research" / "large").mkdir(parents=True)
    (root / ROOT_MARKER).write_text(GONE)
    (root / "Research" / ".file_sync_marker").write_text("x")
    (root / "Research" / "large" / ".restored").write_text("x")
    (root / "Research" / "notes.md").write_text("kept")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (elsewhere / ".file_sync_marker").write_text("x")
    (root / "link").symlink_to(elsewhere)

    assert _run(_claim(root, tmp_path / "computers", forget=(".file_sync_marker", "large/.restored"))) == 0

    assert (root / ROOT_MARKER).read_text() == ME
    assert not (root / "Research" / ".file_sync_marker").exists()
    assert not (root / "Research" / "large" / ".restored").exists()
    assert (root / "Research" / "notes.md").read_text() == "kept"
    # A link out of the root is not followed.
    assert (elsewhere / ".file_sync_marker").exists()


def test_without_forget_only_the_mark_changes(tmp_path):
    root = tmp_path / "la"
    (root / "Research").mkdir(parents=True)
    (root / ROOT_MARKER).write_text(GONE)
    (root / "Research" / ".file_sync_marker").write_text("x")

    assert _run(_claim(root, tmp_path / "computers")) == 0

    assert (root / ROOT_MARKER).read_text() == ME
    assert (root / "Research" / ".file_sync_marker").exists()


def test_a_computer_with_no_id_yet_forgets_without_marking(tmp_path):
    # Its first build failed before the machine gave it an id: the restore
    # markers still go, and its first start marks the folder.
    root = tmp_path / "la"
    (root / "Research").mkdir(parents=True)
    (root / ROOT_MARKER).write_text(GONE)
    (root / "Research" / ".file_sync_marker").write_text("x")

    script = claim_root_script(
        str(root), None, computers=str(tmp_path / "computers"), cleared=GONE,
        forget=(".file_sync_marker",),
    )
    assert _run(script) == 0

    assert not (root / "Research" / ".file_sync_marker").exists()
    assert (root / ROOT_MARKER).read_text() == GONE


def test_a_start_that_took_the_folder_since_keeps_it(tmp_path):
    # Between the take-over's check and this claim, another computer's start
    # found the cleared mark naming no computer and marked the folder.
    other = "b" * 32
    root = tmp_path / "la"
    (root / "Research").mkdir(parents=True)
    (root / ROOT_MARKER).write_text(other)
    (root / "Research" / ".file_sync_marker").write_text("x")

    done = subprocess.run(
        ["bash", "-c", _claim(root, tmp_path / "computers", forget=(".file_sync_marker",))],
        capture_output=True,
        check=False,
    )

    assert done.returncode == 10
    assert done.stdout.decode().strip() == other
    assert (root / ROOT_MARKER).read_text() == other
    assert (root / "Research" / ".file_sync_marker").exists()


def test_the_claim_waits_on_the_hold_starts_take(tmp_path):
    root = tmp_path / "la"
    root.mkdir()
    computers = tmp_path / "computers"
    (computers / ".claiming").mkdir(parents=True)
    script = _claim(root, computers)

    held = subprocess.Popen(["bash", "-c", script])
    try:
        held.wait(timeout=1.5)
    except subprocess.TimeoutExpired:
        pass
    assert held.poll() is None
    assert not (root / ROOT_MARKER).exists()

    (computers / ".claiming").rmdir()
    assert held.wait(timeout=5) == 0
    assert (root / ROOT_MARKER).read_text() == ME
    # The hold goes with the claim.
    assert not (computers / ".claiming").exists()


def test_a_missing_folder_is_left_for_the_start_to_make(tmp_path):
    assert _run(_claim(tmp_path / "missing", tmp_path / "computers")) == 0

    assert not (tmp_path / "missing").exists()


def _knows(home, root=None) -> bool:
    from ptc_agent.core.sandbox.providers._ssh_shell import knows_computer_script

    done = subprocess.run(
        ["bash", "-c", knows_computer_script(str(home), ME, str(root) if root else None)],
        capture_output=True,
        check=False,
    )
    return done.stdout.decode().strip() == "1"


def test_a_machine_holds_a_computer_by_its_own_folder_or_its_mark(tmp_path):
    home = tmp_path / "home"
    root = home / "langalpha"
    root.mkdir(parents=True)
    assert not _knows(home, root)

    (root / ROOT_MARKER).write_text(GONE)
    assert not _knows(home, root)

    (root / ROOT_MARKER).write_text(ME)
    assert _knows(home, root)

    (root / ROOT_MARKER).unlink()
    (home / ".langalpha" / "computers" / ME).mkdir(parents=True)
    # Its own folder in the home outlives a workspace folder deleted by hand.
    assert _knows(home, None)
