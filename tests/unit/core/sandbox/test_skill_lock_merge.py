"""The sandbox-side authoritative merge must match lock.py's merge_lock_files.

The asset-sync path used to compute merge_lock_files on the host from a stale
lock snapshot and blind-upload the result; the ``install`` script mode
re-reads under the reconciler's flock instead. Its merge logic is a
deliberate inline copy of the lock.py predicates, so this test binds the two:
for any lock state, script merge == host merge — plus the one divergence the
script adds on purpose (a racing agent/linked claim on an authoritative name
keeps the claim and reports it, rather than being overwritten). Nothing is
staged here, so the install is the merge alone; the command the host builds
runs for real in a subprocess.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from ptc_agent.agent.middleware.skills.lock import (
    LOCK_FILE_VERSION,
    merge_lock_files,
)
from ptc_agent.core.sandbox import skill_sync
from tests.unit.restored_overlay import archive, python3_on_path


class _ShellSandbox:
    """Runs the command the host builds, on this machine's own filesystem."""

    def __init__(self, working_dir: Path) -> None:
        self._work_dir = str(working_dir)
        self.runtime = self

    async def upload_file(self, content: bytes, dest_path: str):
        path = Path(dest_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)

    async def exec(self, command: str, *args, **kwargs):
        proc = subprocess.run(
            command, shell=True, capture_output=True, text=True, check=False
        )
        return SimpleNamespace(
            stdout=proc.stdout, stderr=proc.stderr, exit_code=proc.returncode
        )

    async def _runtime_call(self, fn, *args, retry_policy=None, **kwargs):
        return await fn(*args, **kwargs)


def _run_merge(tmp_path, entries: dict, auth: dict) -> tuple[dict, dict]:
    """Run an install with nothing staged against a real lock file in
    tmp_path; returns (result, lock_file_on_disk)."""
    lock_path = tmp_path / "skills-lock.json"
    lock_path.write_text(
        json.dumps({"version": LOCK_FILE_VERSION, "skills": entries})
    )
    merged, skipped = asyncio.run(
        skill_sync.install_staged_skills(
            _ShellSandbox(tmp_path),
            str(tmp_path / ".staging" / "sync-test"),
            [],
            auth,
            base=str(tmp_path),
        )
    )
    payload = {"skills": merged["skills"], "skipped": skipped}
    return payload, json.loads(lock_path.read_text())


_AGENT = {"owner": "user", "sourceType": "agent"}
_LINKED = {
    "owner": "user",
    "sourceType": "langalpha-user",
    "sync": {"linkedSkillId": "abc"},
}
_PLATFORM_OLD = {"owner": "platform", "sourceType": "langalpha"}
_PLATFORM_NEW = {"owner": "platform", "sourceType": "langalpha", "v": 2}
_MANAGED = {"owner": "user", "sourceType": "langalpha-user"}


def test_script_merge_matches_host_merge(tmp_path):
    entries = {
        "agent-skill": _AGENT,
        "linked-skill": _LINKED,
        "stale-platform": _PLATFORM_OLD,
        "managed-old": _MANAGED,
    }
    auth = {"fresh-platform": _PLATFORM_NEW, "managed-old": _MANAGED}

    payload, on_disk = _run_merge(tmp_path, entries, auth)
    host = merge_lock_files(auth, entries)

    assert payload["skills"] == host["skills"]
    assert on_disk == {"version": LOCK_FILE_VERSION, "skills": host["skills"]}
    assert payload["skipped"] == []
    # Stale platform + managed entries purged, protected owners preserved.
    assert set(payload["skills"]) == {
        "agent-skill",
        "linked-skill",
        "fresh-platform",
        "managed-old",
    }


def test_racing_protected_claim_keeps_the_claim_and_reports_it(tmp_path):
    # The one deliberate divergence from merge_lock_files: an agent/linked
    # entry that appeared under an authoritative name since the caller's
    # snapshot wins the lock entry; the reconciler arbitrates the bytes.
    entries = {"contested": _AGENT}
    auth = {"contested": _PLATFORM_NEW, "other": _PLATFORM_NEW}

    payload, on_disk = _run_merge(tmp_path, entries, auth)

    assert payload["skipped"] == ["contested"]
    assert payload["skills"]["contested"] == _AGENT
    assert payload["skills"]["other"] == _PLATFORM_NEW
    assert on_disk["skills"] == payload["skills"]


def test_empty_authoritative_set_purges_only_unprotected(tmp_path):
    entries = {"agent-skill": _AGENT, "stale-platform": _PLATFORM_OLD}
    payload, _ = _run_merge(tmp_path, entries, {})
    assert set(payload["skills"]) == {"agent-skill"}


@pytest.mark.parametrize("occupant", ["symlink", "file"])
def test_install_over_a_non_dir_name_leaves_no_trash(tmp_path, occupant):
    """A link or file an agent left at a skill's name is replaced, not kept as trash."""
    if occupant == "symlink":
        (tmp_path / "elsewhere").mkdir()
        (tmp_path / "pdf").symlink_to(tmp_path / "elsewhere")
    else:
        (tmp_path / "pdf").write_text("not a skill")
    staged = tmp_path / ".staging" / "sync-test" / "pdf"
    staged.mkdir(parents=True)
    (staged / "SKILL.md").write_text("new")
    asyncio.run(
        skill_sync.install_staged_skills(
            _ShellSandbox(tmp_path),
            str(tmp_path / ".staging" / "sync-test"),
            ["pdf"],
            {"pdf": _PLATFORM_NEW},
            base=str(tmp_path),
        )
    )
    assert not (tmp_path / "pdf").is_symlink()
    assert (tmp_path / "pdf" / "SKILL.md").read_text() == "new"
    assert not [p for p in tmp_path.iterdir() if p.name.startswith(".trash-")]


def _restored_skill(base, monkeypatch, bin_dir, extra=""):
    """A live ``pdf`` skill on a sandbox since restored from its archive,
    which refuses to rename the dir aside (EXDEV)."""
    (base / "pdf" / "scripts").mkdir(parents=True)
    (base / "pdf" / "SKILL.md").write_text("old")
    (base / "pdf" / "scripts" / "gone.py").write_text("old")
    python3_on_path(monkeypatch, bin_dir, archive(base), extra=extra)
    staged = base / ".staging" / "sync-test" / "pdf"
    staged.mkdir(parents=True)
    (staged / "SKILL.md").write_text("new")
    return staged


def test_install_over_a_restored_skill_deletes_it_where_it_is(tmp_path, monkeypatch):
    base = tmp_path / "skills"
    _restored_skill(base, monkeypatch, tmp_path / "bin")
    merged, _ = asyncio.run(
        skill_sync.install_staged_skills(
            _ShellSandbox(tmp_path),
            str(base / ".staging" / "sync-test"),
            ["pdf"],
            {"pdf": _PLATFORM_NEW},
            base=str(base),
        )
    )
    assert "pdf" in merged["skills"]
    assert (base / "pdf" / "SKILL.md").read_text() == "new"
    assert not (base / "pdf" / "scripts").exists()
    assert not [p for p in base.iterdir() if p.name.startswith(".trash-")]


def test_a_restored_skill_holding_a_read_only_dir_is_still_replaced(tmp_path, monkeypatch):
    """A dir in it the agent made read-only refuses a delete inside until it
    is opened up; left there, the skill could never be replaced."""
    base = tmp_path / "skills"
    _restored_skill(base, monkeypatch, tmp_path / "bin")
    (base / "pdf" / "scripts").chmod(0o500)
    merged, _ = asyncio.run(
        skill_sync.install_staged_skills(
            _ShellSandbox(tmp_path),
            str(base / ".staging" / "sync-test"),
            ["pdf"],
            {"pdf": _PLATFORM_NEW},
            base=str(base),
        )
    )
    assert "pdf" in merged["skills"]
    assert (base / "pdf" / "SKILL.md").read_text() == "new"
    assert not (base / "pdf" / "scripts").exists()


def test_a_swap_over_a_restored_skill_deletes_it_where_it_is(tmp_path, monkeypatch):
    base = tmp_path / "skills"
    staged = _restored_skill(base, monkeypatch, tmp_path / "bin")
    (base / "skills-lock.json").write_text(
        json.dumps({"version": LOCK_FILE_VERSION, "skills": {}})
    )
    [result] = asyncio.run(
        skill_sync.apply_actions(
            _ShellSandbox(tmp_path),
            [{"op": "swap_staged", "name": "pdf", "staged": str(staged), "entry": dict(_MANAGED)}],
            base=str(base),
        )
    )
    assert result["ok"], result
    assert (base / "pdf" / "SKILL.md").read_text() == "new"
    assert not (base / "pdf" / "scripts").exists()
    assert not [p for p in base.iterdir() if p.name.startswith(".trash-")]



# The sync dies at the first file it deletes after SKILL.md, as a timeout or a
# stopping machine can cut it off.
_DIES_MID_DELETE = (
    "_unlink_dies = _os.unlink\n"
    "def _dies(path, *, dir_fd=None):\n"
    "    if _os.path.basename(str(path)) != 'SKILL.md':\n"
    "        _os._exit(9)\n"
    "    return _unlink_dies(path, dir_fd=dir_fd)\n"
    "_os.unlink = _dies\n"
)


def test_a_restored_skill_whose_delete_dies_midway_holds_no_skill(tmp_path, monkeypatch):
    """Its SKILL.md goes first, so what is left reads as no skill, which the
    next sync installs again, rather than as an edit made in the sandbox that
    would go up over the server's copy. That install expects the name empty,
    as the report saw it, so the report finishes the delete."""
    base = tmp_path / "skills"
    for i in range(40):
        (base / "pdf" / f"ref-{i:02d}.md").parent.mkdir(parents=True, exist_ok=True)
        (base / "pdf" / f"ref-{i:02d}.md").write_text("old")
    staged = _restored_skill(base, monkeypatch, tmp_path / "bin", extra=_DIES_MID_DELETE)
    (base / "skills-lock.json").write_text(
        json.dumps({"version": LOCK_FILE_VERSION, "skills": {}})
    )
    with pytest.raises(Exception):
        asyncio.run(
            skill_sync.apply_actions(
                _ShellSandbox(tmp_path),
                [{"op": "swap_staged", "name": "pdf", "staged": str(staged),
                  "entry": dict(_MANAGED)}],
                base=str(base),
            )
        )
    assert (base / "pdf").is_dir()
    assert not (base / "pdf" / "SKILL.md").exists()

    python3_on_path(monkeypatch, tmp_path / "bin", tmp_path / ".skills.restored")
    assert "pdf" not in asyncio.run(skill_sync.report(_ShellSandbox(tmp_path), base=str(base)))
    staged = base / ".staging" / "sync-again" / "pdf"
    staged.mkdir(parents=True)
    (staged / "SKILL.md").write_text("new")
    [result] = asyncio.run(
        skill_sync.apply_actions(
            _ShellSandbox(tmp_path),
            [{"op": "swap_staged", "name": "pdf", "staged": str(staged),
              "entry": dict(_MANAGED), "expectAbsent": True}],
            base=str(base),
        )
    )
    assert result["ok"], result
    assert sorted(p.name for p in (base / "pdf").iterdir()) == ["SKILL.md"]
    assert not [p for p in base.iterdir() if p.name.startswith(".deleting-")]


# The disk takes no new file, as a full one may not.
_NO_ROOM_FOR_A_MARK = (
    "_open_any = open\n"
    "def open(path, *args, **kwargs):\n"
    "    if _os.path.basename(str(path)).startswith('.deleting-'):\n"
    "        raise OSError(_errno.ENOSPC, 'No space left on device', path)\n"
    "    return _open_any(path, *args, **kwargs)\n"
)


def test_a_restored_skill_whose_delete_cannot_be_marked_is_left_whole(tmp_path, monkeypatch):
    """Unmarked, a delete cut short would leave a dir no report finishes and
    no install may replace, so it is not begun: the skill stays as it was,
    and the swap says it failed."""
    base = tmp_path / "skills"
    staged = _restored_skill(base, monkeypatch, tmp_path / "bin", extra=_NO_ROOM_FOR_A_MARK)
    (base / "skills-lock.json").write_text(
        json.dumps({"version": LOCK_FILE_VERSION, "skills": {}})
    )
    [result] = asyncio.run(
        skill_sync.apply_actions(
            _ShellSandbox(tmp_path),
            [{"op": "swap_staged", "name": "pdf", "staged": str(staged), "entry": dict(_MANAGED)}],
            base=str(base),
        )
    )
    assert not result["ok"]
    assert (base / "pdf" / "SKILL.md").read_text() == "old"
    assert (base / "pdf" / "scripts" / "gone.py").read_text() == "old"


@pytest.mark.parametrize("name", ["", "..", ".staging"])
def test_a_mark_of_no_skill_name_deletes_nothing(tmp_path, name):
    """The report finishes only a delete of a skill's own name: a mark the
    agent made of another reaches the tier itself, its parent or staging."""
    base = tmp_path / ".agents" / "skills"
    (base / "pdf").mkdir(parents=True)
    (base / "pdf" / "SKILL.md").write_text("---\nname: pdf\ndescription: d\n---\n")
    (base / ".staging" / "push-1").mkdir(parents=True)
    (tmp_path / ".agents" / "memory.md").write_text("mine")
    (base / f".deleting-{name}").write_text("")
    assert asyncio.run(skill_sync.report(_ShellSandbox(tmp_path), base=str(base)))["pdf"]["present"]
    assert (base / ".staging" / "push-1").is_dir()
    assert (tmp_path / ".agents" / "memory.md").read_text() == "mine"
    assert not (base / f".deleting-{name}").exists()


def test_a_mark_beside_a_dir_that_holds_its_skill_md_keeps_it(tmp_path):
    """The delete takes SKILL.md first, so a dir with one is a skill made at
    the name since, or one whose delete was cut short before anything went."""
    base = tmp_path / ".agents" / "skills"
    (base / "pdf" / "scripts").mkdir(parents=True)
    (base / "pdf" / "SKILL.md").write_text("---\nname: pdf\ndescription: d\n---\n")
    (base / "pdf" / "scripts" / "run.py").write_text("mine")
    (base / ".deleting-pdf").write_text("")
    assert asyncio.run(skill_sync.report(_ShellSandbox(tmp_path), base=str(base)))["pdf"]["present"]
    assert (base / "pdf" / "scripts" / "run.py").read_text() == "mine"
    assert not (base / ".deleting-pdf").exists()
