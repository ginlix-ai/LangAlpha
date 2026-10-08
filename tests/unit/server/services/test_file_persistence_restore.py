"""
Unit tests for restore_to_sandbox on the relay path.

Rows that still carry inline bytes (and every file row when blob transfer
is ``relay``) are uploaded from this process. That path:

1. Uploads every file to a staging name through a semaphore-bounded worker
   pool, so the next upload starts the instant any slot frees up.
2. Hands the staged names to one runtime op that verifies, places and
   stamps them, with directory modes applied after the last file is in.

These tests pin that behavior so regressions of the restore latency
budget are visible in CI.
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.core.paths import SandboxLayout, WorkspaceLayout
from ptc_agent.core.sandbox.assets import _read_unified_manifest
from src.server.database.conversation import ThreadPrefixes
from src.server.database.workspace_file import WorkspaceSyncBusy
from src.server.services.persistence import restore
from src.server.services.persistence.resolve import FileBytesUnavailable
from src.server.services.persistence.transfer import TransferRuntimeError

import hashlib

ROOT = "/workspace"
DIR_NAME = "relay-ab12"
# The folder this workspace owns on its computer. Staging names and the sync
# marker are reserved at the root of the walk, which is this folder.
LAYOUT = SandboxLayout.for_root(ROOT).for_workspace(DIR_NAME)


@pytest.fixture(autouse=True)
def restore_flag():
    """Restore records its own completeness in Postgres; unit tests have no DB."""
    with patch(
        "src.server.services.persistence.restore.set_files_restore_incomplete",
        new_callable=AsyncMock,
    ) as flag:
        yield flag


@pytest.fixture(autouse=True)
def flag_state():
    """maybe_restore reads the flag beside a marker; unit tests have no DB."""
    with patch(
        "src.server.services.persistence.restore.files_restore_incomplete",
        new=AsyncMock(return_value=False),
    ) as read:
        yield read


@pytest.fixture(autouse=True)
def owner():
    """Restore resolves the owning user once per pass; unit tests have no DB."""
    with patch(
        "src.server.services.persistence.restore.workspace_owner",
        new=AsyncMock(return_value="user-restore"),
    ):
        yield


@pytest.fixture(autouse=True)
def sync_lock():
    """Restore serializes on a Postgres advisory lock; unit tests have no DB."""

    @asynccontextmanager
    async def _lock(_workspace_id, *, conn=None):
        yield "conn"

    @asynccontextmanager
    async def _hold(_workspace_id, *, wait_s):
        yield "conn"

    with (
        patch("src.server.services.persistence.restore.workspace_sync_lock", _lock),
        patch("src.server.services.persistence.restore.hold_workspace_skill_sync", _hold),
    ):
        yield


def _place_everything(_sandbox, items, **_kw):
    """What the runtime answers when every staged file verifies."""
    out = {}
    for i in items:
        if i.get("kind") == "pack":
            out.update({m["path"]: {"status": "ok"} for m in i["members"]})
        else:
            out[i["path"]] = {"status": "ok"}
    return out


@pytest.fixture(autouse=True)
def no_runtime():
    """The placement op runs the sandbox runtime; these tests have none."""
    with patch(
        "src.server.services.persistence.restore.pull_direct",
        new=AsyncMock(side_effect=_place_everything),
    ) as pull:
        yield pull


def _file(path: str, text: str = "hello") -> dict:
    return {
        "file_path": path,
        "is_binary": False,
        "content_binary": None,
        "content_text": text,
        "content_hash": hashlib.sha256(text.encode()).hexdigest(),
        "file_size": len(text),
    }


def _mock_sandbox(manifest: dict | None = None) -> MagicMock:
    sandbox = MagicMock()
    sandbox.working_dir = ROOT
    sandbox.acreate_directories = AsyncMock(return_value=True)
    sandbox.acreate_directory = AsyncMock(return_value=True)
    sandbox.aupload_file_bytes = AsyncMock(return_value=True)
    # Through the real reader: the lookup sees only what it lets through.
    sandbox._runtime_call = AsyncMock(
        return_value=None if manifest is None else json.dumps(manifest).encode()
    )
    sandbox._read_unified_manifest = lambda: _read_unified_manifest(sandbox)
    return sandbox


def _synced(*names: str, collisions: tuple[str, ...] = ()) -> dict:
    """The manifest of an asset sync that delivered ``names`` to the tier."""
    return {
        "schema_version": 1,
        "modules": {
            "skills": {
                "version": "v",
                "files": {f"{n}/SKILL.md": "h" for n in (*names, *collisions)},
                "skills": {n: {} for n in (*names, *collisions)},
                "collisions": list(collisions),
            }
        },
    }


@pytest.mark.asyncio
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_relayed_files_are_staged_then_placed_by_the_runtime(mock_get, no_runtime):
    """Nothing is uploaded to its final path. Each file goes to a scan-excluded
    staging name, and one runtime op verifies it against the manifest, moves it
    into place and stamps it, carrying the directory items so their modes land
    after the last file. A truncated upload can then never be mistaken for the
    file's next content."""
    mock_get.return_value = [
        _file("a/one.txt", "one"),
        _file("two.txt", "two"),
        {"file_path": "a", "kind": "dir", "permissions": "0555"},
    ]
    sandbox = _mock_sandbox()

    result = await restore.restore_to_sandbox("ws-1", sandbox, layout=LAYOUT)

    uploads = {c.args[0]: c.args[1] for c in sandbox.aupload_file_bytes.await_args_list}
    staged = {p: b for p, b in uploads.items() if not p.endswith(".file_sync_marker")}
    assert set(staged.values()) == {b"one", b"two"}
    assert all(p.startswith(f"{LAYOUT.workspace}/.wsfiles-relay-") for p in staged)
    # No final path was ever written directly.
    assert f"{LAYOUT.workspace}/a/one.txt" not in uploads
    assert f"{LAYOUT.workspace}/two.txt" not in uploads

    structure, placement = [c.args[1] for c in no_runtime.await_args_list]
    assert [i["path"] for i in structure] == ["a"]
    assert no_runtime.await_args_list[0].kwargs == {
        "defer_dir_modes": True,
        "layout": LAYOUT,
    }
    by_path = {i["path"]: i for i in placement}
    assert set(by_path) == {"a/one.txt", "two.txt", "a"}
    one = by_path["a/one.txt"]
    staged_one = f"{LAYOUT.workspace}/{one['file']}"
    assert staged_one in staged and staged[staged_one] == b"one"
    assert one["sha256"] == hashlib.sha256(b"one").hexdigest() and one["size"] == 3
    assert by_path["a"]["kind"] == "dir" and by_path["a"]["mode"] == 0o555
    assert result == {"restored": 3, "errors": 0}


@pytest.mark.asyncio
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_a_row_whose_file_size_disagrees_with_its_bytes_is_still_placed(
    mock_get, no_runtime
):
    """The placement check asks whether the upload arrived whole, so it has to
    describe what was sent. A row written before ``file_size`` was taken from
    the content itself can state a length its own bytes disagree with, and
    checking against the column would refuse that file on every start, which
    also leaves the manifest unable to ever prune."""
    row = _file("stale.txt", "hello")
    row["file_size"] = 999_999
    mock_get.return_value = [row]

    result = await restore.restore_to_sandbox("ws-1", _mock_sandbox(), layout=LAYOUT)

    item = no_runtime.await_args_list[0].args[1][0]
    assert item["size"] == 5
    assert item["sha256"] == hashlib.sha256(b"hello").hexdigest()
    assert result == {"restored": 1, "errors": 0}


@pytest.mark.asyncio
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_bytes_that_miss_their_own_hash_are_placed_but_named(
    mock_get, no_runtime, caplog
):
    """Deriving the digest from the bytes drops the only reading the column
    could still have given, so the disagreement is logged rather than lost."""
    row = _file("drifted.txt", "hello")
    row["content_hash"] = "0" * 64
    mock_get.return_value = [row]

    with caplog.at_level(logging.WARNING):
        result = await restore.restore_to_sandbox("ws-1", _mock_sandbox(), layout=LAYOUT)

    item = no_runtime.await_args_list[0].args[1][0]
    assert item["sha256"] == hashlib.sha256(b"hello").hexdigest()
    assert "does not reproduce" in caplog.text or "do not reproduce" in caplog.text
    assert result == {"restored": 1, "errors": 0}


@pytest.mark.asyncio
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_a_failed_directory_stamp_in_a_relay_restore_is_an_error(mock_get, no_runtime, restore_flag):
    """The structure pass creates the directory, the placement op applies its
    final mode and mtime. When that last step fails the files are back but the
    metadata is not, and a restore that clears the flag anyway lets the next
    backup record the wrong mode as the user's own."""
    mock_get.return_value = [
        _file("a/one.txt", "one"),
        _file("two.txt", "two"),
        {"file_path": "a", "kind": "dir", "permissions": "0555"},
    ]

    def _place(sandbox, items, **kw):
        out = _place_everything(sandbox, items, **kw)
        if not kw.get("defer_dir_modes"):
            out["a"] = {"status": "failed", "error": "chmod: EPERM"}
        return out

    no_runtime.side_effect = _place
    sandbox = _mock_sandbox()

    result = await restore.restore_to_sandbox("ws-1", sandbox, layout=LAYOUT)

    # The structure pass already counted the directory; the failed stamp
    # adds an error rather than taking that count back.
    assert result == {"restored": 3, "errors": 1}
    assert [c.args for c in restore_flag.await_args_list] == [("ws-1", True)]
    assert not any(
        c.args[0].endswith(".file_sync_marker") for c in sandbox.aupload_file_bytes.await_args_list
    )


@pytest.mark.asyncio
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_directory_modes_are_still_applied_when_every_file_fails_to_stage(mock_get, no_runtime, restore_flag):
    """The structure pass leaves the directories open for the relay pass to
    place files under, and only the placement op closes them again. Returning
    early because no file survived staging strands them at their working modes,
    and the next backup records those as the user's own."""
    mock_get.return_value = [
        _file("a/one.txt", "one"),
        _file("two.txt", "two"),
        {"file_path": "a", "kind": "dir", "permissions": "0555"},
    ]
    sandbox = _mock_sandbox()
    sandbox.aupload_file_bytes = AsyncMock(
        side_effect=lambda path, _content: ".wsfiles-relay-" not in path
    )

    result = await restore.restore_to_sandbox("ws-1", sandbox, layout=LAYOUT)

    structure, placement = [c.args[1] for c in no_runtime.await_args_list]
    assert [i["path"] for i in structure] == ["a"]
    assert no_runtime.await_args_list[0].kwargs == {
        "defer_dir_modes": True,
        "layout": LAYOUT,
    }
    # Nothing but the directory items, and the op ran all the same.
    assert [(i["path"], i["kind"], i["mode"]) for i in placement] == [("a", "dir", 0o555)]
    assert result == {"restored": 1, "errors": 2}
    assert [c.args for c in restore_flag.await_args_list] == [("ws-1", True)]


@pytest.mark.asyncio
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_a_staged_file_the_runtime_rejects_is_an_error_and_keeps_the_flag(mock_get, no_runtime, restore_flag):
    mock_get.return_value = [_file("a.txt", "aaa"), _file("b.txt", "bbb")]
    no_runtime.side_effect = lambda sb, items, **kw: {
        i["path"]: {"status": "mismatch" if i["path"] == "a.txt" else "ok", "error": "got bytes=1"} for i in items
    }

    result = await restore.restore_to_sandbox("ws-1", _mock_sandbox(), layout=LAYOUT)

    assert result == {"restored": 1, "errors": 1}
    assert [c.args for c in restore_flag.await_args_list] == [("ws-1", True)]


@pytest.mark.asyncio
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_a_placement_op_that_raises_counts_every_staged_file(mock_get, no_runtime, restore_flag):
    mock_get.return_value = [_file("a.txt"), _file("b.txt")]
    no_runtime.side_effect = TransferRuntimeError("sandbox went away")

    result = await restore.restore_to_sandbox("ws-1", _mock_sandbox(), layout=LAYOUT)

    assert result == {"restored": 0, "errors": 2}
    assert [c.args for c in restore_flag.await_args_list] == [("ws-1", True)]


@pytest.mark.asyncio
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_restore_isolates_per_file_failures(mock_get, restore_flag):
    """A single failed upload doesn't block the rest. 5 files, one
    raises, one returns False — healthy 3 still restore, error count
    tallied correctly, method does not raise.

    Incompleteness is recorded in Postgres and the sandbox marker is
    WITHHELD. Pruning against a sandbox that failed to restore deletes the
    manifest rows for exactly the files that never came back, losing them
    from both tiers, so the flag that prevents it has to be durable and
    visible to another worker. The marker only claims "this sandbox has
    been populated"; withholding it makes the next start retry.
    """
    mock_get.return_value = [_file(f"f_{i}.txt") for i in range(5)]
    sandbox = _mock_sandbox()

    call_count = {"n": 0}

    async def flaky_upload(_path, _content):
        call_count["n"] += 1
        if call_count["n"] == 2:
            raise RuntimeError("transient network blip")
        if call_count["n"] == 3:
            return False
        return True

    sandbox.aupload_file_bytes = AsyncMock(side_effect=flaky_upload)

    result = await restore.restore_to_sandbox("ws-1", sandbox, layout=LAYOUT)

    assert result["restored"] == 3
    assert result["errors"] == 2
    # 5 file uploads and no marker — restore stays retryable next start.
    assert sandbox.aupload_file_bytes.await_count == 5
    upload_paths = [c.args[0] for c in sandbox.aupload_file_bytes.await_args_list]
    assert not any(p.endswith(".file_sync_marker") for p in upload_paths)
    # Raised up front and never cleared.
    assert [c.args for c in restore_flag.await_args_list] == [("ws-1", True)]


@pytest.mark.asyncio
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_a_clean_restore_raises_the_flag_then_clears_it(mock_get, restore_flag):
    """The flag is durable before the first byte moves, not after the last one."""
    mock_get.return_value = [_file("a.txt")]
    result = await restore.restore_to_sandbox("ws-1", _mock_sandbox(), layout=LAYOUT)

    assert result == {"restored": 1, "errors": 0}
    assert [c.args for c in restore_flag.await_args_list] == [("ws-1", True), ("ws-1", False)]


@pytest.fixture
def served(monkeypatch):
    """``scopes["ws-1"]`` is the workspace's own rows and ``disabled["ws-1"]``
    the inherited names it switched off, both read on the restore's lock
    connection."""
    scopes = {"ws-1": []}
    disabled = {"ws-1": set()}
    calls = []
    disable_calls = []

    async def _list(user_id, *, workspace_id=None, conn=None):
        calls.append((user_id, workspace_id, conn))
        return scopes[workspace_id]

    async def _disables(workspace_id, *, conn=None):
        disable_calls.append((workspace_id, conn))
        return disabled[workspace_id]

    monkeypatch.setattr(restore, "list_user_skills", _list)
    monkeypatch.setattr(restore, "list_workspace_skill_disables", _disables)
    return SimpleNamespace(
        scopes=scopes, disabled=disabled, calls=calls, disable_calls=disable_calls
    )


@pytest.mark.asyncio
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_skills_the_shared_tier_serves_are_left_out(mock_get, no_runtime, served):
    """A backup taken while the workspace lived at the computer root carries
    the skills the shared tier served it there, and a copy restored into a
    folder outranks the shared skill for good. The backup's own ledger names
    them; a name it says nothing about is shared when the sync delivered it."""
    ledger = {
        "pdf": {"owner": "platform", "sourceType": "platform"},
        "house-style": {"owner": "user", "sourceType": "langalpha-user"},
        "mine": {"owner": "user", "sourceType": "local"},
        "docx": {"owner": "user", "sourceType": "local"},
        "synced": {
            "owner": "user",
            "sourceType": "langalpha-user",
            "sync": {"linkedSkillId": "row-1"},
        },
    }
    mock_get.return_value = [
        _file(
            ".agents/skills/skills-lock.json",
            json.dumps({"version": 1, "skills": ledger}),
        ),
        {"file_path": ".agents/skills/pdf", "kind": "dir", "permissions": "0755"},
        _file(".agents/skills/pdf/SKILL.md"),
        _file(".agents/skills/house-style/SKILL.md"),
        _file(".agents/skills/mine/SKILL.md"),
        _file(".agents/skills/docx/SKILL.md"),
        _file(".agents/skills/synced/SKILL.md"),
        _file(".agents/skills/unledgered/SKILL.md"),
        _file(".agents/skills/team-voice/SKILL.md"),
        {
            "file_path": ".agents/skills/xlsx",
            "kind": "symlink",
            "symlink_target": "../../../.agents/skills/xlsx",
        },
        _file(".agents/skills-archive/pdf/SKILL.md"),
        _file("work/notes.md"),
    ]
    sandbox = _mock_sandbox(_synced("docx", "pdf", "xlsx", "team-voice"))

    result = await restore.restore_to_sandbox("ws-1", sandbox, layout=LAYOUT)

    placed = {i["path"] for c in no_runtime.await_args_list for i in c.args[1]}
    assert placed == {
        ".agents/skills/skills-lock.json",
        ".agents/skills/mine/SKILL.md",
        ".agents/skills/docx/SKILL.md",
        ".agents/skills/synced/SKILL.md",
        ".agents/skills/unledgered/SKILL.md",
        ".agents/skills/xlsx",
        ".agents/skills-archive/pdf/SKILL.md",
        "work/notes.md",
    }
    assert result == {"restored": 8, "errors": 0}


@pytest.mark.asyncio
@pytest.mark.parametrize("unreadable", ["unfetchable", "oversized"])
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_without_a_ledger_the_delivered_names_are_left_out(
    mock_get, no_runtime, served, unreadable
):
    """A ledger the store cannot hand back, or one too large for the server to
    read whole, names no skill. Holding every skill back would wait forever on
    a lost blob, and restoring them all pins stale copies, so what the last
    sync delivered stands in for it."""
    ledger_row = _file(
        ".agents/skills/skills-lock.json",
        json.dumps({"version": 1, "skills": {"mine": {"owner": "platform"}}}),
    )
    if unreadable == "oversized":
        ledger_row["file_size"] = restore._LEDGER_MAX_BYTES + 1
    mock_get.return_value = [
        ledger_row,
        _file(".agents/skills/docx/SKILL.md"),
        _file(".agents/skills/team-voice/SKILL.md"),
        _file(".agents/skills/mine/SKILL.md"),
    ]
    real_resolve = restore.resolve_file_bytes

    async def _resolve(row, *, user_id):
        if unreadable == "unfetchable" and row is ledger_row:
            raise FileBytesUnavailable("store unreachable")
        return await real_resolve(row, user_id=user_id)

    with patch.object(restore, "resolve_file_bytes", _resolve):
        await restore.restore_to_sandbox(
            "ws-1", _mock_sandbox(_synced("docx", "team-voice")), layout=LAYOUT
        )

    placed = {i["path"] for c in no_runtime.await_args_list for i in c.args[1]}
    assert placed - {".agents/skills/skills-lock.json"} == {
        ".agents/skills/mine/SKILL.md"
    }
    assert served.calls == [("user-restore", "ws-1", "conn")]


@pytest.mark.asyncio
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_an_unledgered_name_the_sync_did_not_deliver_is_kept(
    mock_get, no_runtime, served
):
    """Only what the sync delivered stands in for a lost entry. A user-tier
    skill whose archive it could not fetch, a disabled or Flash-only skill,
    and a name a collision kept from upload never reached the tier as a
    delivery, so a copy under that name may be the only one there is."""
    mock_get.return_value = [
        _file(".agents/skills/docx/SKILL.md"),
        _file(".agents/skills/broken-archive/SKILL.md"),
        _file(".agents/skills/pdf/SKILL.md"),
        _file(".agents/skills/collided/SKILL.md"),
    ]
    sandbox = _mock_sandbox(_synced("docx", collisions=("collided",)))

    await restore.restore_to_sandbox("ws-1", sandbox, layout=LAYOUT)

    placed = {i["path"] for c in no_runtime.await_args_list for i in c.args[1]}
    assert placed == {
        ".agents/skills/broken-archive/SKILL.md",
        ".agents/skills/pdf/SKILL.md",
        ".agents/skills/collided/SKILL.md",
    }


@pytest.mark.asyncio
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_an_unledgered_name_the_workspace_switched_off_is_kept(
    mock_get, no_runtime, served
):
    """The shared tier holds a skill the workspace switched off for its
    siblings, but no pass links it into this folder, so a copy under that
    name is no delivery that lost its entry, and nothing replaces it."""
    served.disabled["ws-1"] = {"docx"}
    mock_get.return_value = [
        _file(".agents/skills/docx/SKILL.md"),
        _file(".agents/skills/pdf/SKILL.md"),
    ]

    await restore.restore_to_sandbox(
        "ws-1", _mock_sandbox(_synced("docx", "pdf")), layout=LAYOUT
    )

    placed = {i["path"] for c in no_runtime.await_args_list for i in c.args[1]}
    assert placed == {".agents/skills/docx/SKILL.md"}
    assert served.disable_calls == [("ws-1", "conn")]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "manifest",
    [
        None,
        {"schema_version": 1, "modules": ["skills"]},
        {"schema_version": 1, "modules": {"skills": ["docx"]}},
        {"schema_version": 1, "modules": {"skills": {"files": [1]}}},
        {
            "schema_version": 1,
            "modules": {
                "skills": {"files": {"docx/SKILL.md": "x"}, "collisions": [["pdf"]]}
            },
        },
        {"schema_version": 1, "modules": {"skills": {"files": ["docx/SKILL.md"]}}},
        {
            "schema_version": 1,
            "modules": {
                "skills": {"files": {"docx/SKILL.md": "x"}, "collisions": "pdf"}
            },
        },
    ],
    ids=[
        "unreadable",
        "modules-list",
        "skills-list",
        "file-not-a-path",
        "collision-list",
        "files-list",
        "collisions-string",
    ],
)
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_with_no_record_of_the_sync_unledgered_copies_are_restored(
    mock_get, no_runtime, served, manifest
):
    """A sandbox whose manifest cannot be read, or holds a shape no sync
    writes, says nothing about what it was delivered. The ledger still decides
    the names it covers; the rest are restored, since a stale copy pinned in
    the folder costs less than a lost one."""
    ledger = {"pdf": {"owner": "platform", "sourceType": "platform"}}
    mock_get.return_value = [
        _file(
            ".agents/skills/skills-lock.json",
            json.dumps({"version": 1, "skills": ledger}),
        ),
        _file(".agents/skills/pdf/SKILL.md"),
        _file(".agents/skills/docx/SKILL.md"),
    ]

    await restore.restore_to_sandbox("ws-1", _mock_sandbox(manifest), layout=LAYOUT)

    placed = {i["path"] for c in no_runtime.await_args_list for i in c.args[1]}
    assert placed == {
        ".agents/skills/skills-lock.json",
        ".agents/skills/docx/SKILL.md",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("ledger", ["shared entry", "unfetchable"])
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_a_skill_the_workspace_owns_is_never_left_out(
    mock_get, no_runtime, served, ledger
):
    """A workspace row shadowing a user-tier skill keeps its copy whatever the
    ledger says or fails to say. Left out, its linked entry would come back
    over a missing directory, which the reconcile reads as a deletion."""
    served.scopes["ws-1"] = [{"name": "team-voice"}]
    entry = {"owner": "user", "sourceType": "langalpha-user"}
    ledger_row = _file(
        ".agents/skills/skills-lock.json",
        json.dumps({"version": 1, "skills": {"team-voice": entry, "docx": entry}}),
    )
    mock_get.return_value = [
        ledger_row,
        _file(".agents/skills/team-voice/SKILL.md"),
        _file(".agents/skills/docx/SKILL.md"),
    ]
    real_resolve = restore.resolve_file_bytes

    async def _resolve(row, *, user_id):
        if ledger == "unfetchable" and row is ledger_row:
            raise FileBytesUnavailable("store unreachable")
        return await real_resolve(row, user_id=user_id)

    with patch.object(restore, "resolve_file_bytes", _resolve):
        await restore.restore_to_sandbox(
            "ws-1", _mock_sandbox(_synced("docx", "team-voice")), layout=LAYOUT
        )

    placed = {i["path"] for c in no_runtime.await_args_list for i in c.args[1]}
    assert placed - {".agents/skills/skills-lock.json"} == {
        ".agents/skills/team-voice/SKILL.md"
    }


@pytest.mark.asyncio
@patch("src.server.services.persistence.restore.pull_direct", new_callable=AsyncMock)
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_a_transfer_that_raises_leaves_the_workspace_flagged(
    mock_get, mock_pull, restore_flag
):
    """The workspace manager swallows a TransferRuntimeError from a restore
    as a warning. If the flag were written after the transfers, the sandbox would
    be left a partial mirror with nothing recording it, and the next sync would
    prune the manifest rows for every file that never arrived."""
    mock_get.return_value = [{"file_path": "d", "kind": "dir", "permissions": "0755"}]
    mock_pull.side_effect = TransferRuntimeError("sandbox went away")

    with pytest.raises(TransferRuntimeError):
        await restore.restore_to_sandbox("ws-1", _mock_sandbox(), layout=LAYOUT)

    assert [c.args for c in restore_flag.await_args_list] == [("ws-1", True)]


@pytest.mark.asyncio
async def test_a_lock_wait_that_times_out_still_flags_the_workspace(restore_flag):
    """The lock wait is the one step before the flag is raised. Both restore
    paths end ``WorkspaceSyncBusy`` in a warning, so without the flag the sandbox
    starts as an empty mirror of a full manifest and the next sync prunes it."""

    @asynccontextmanager
    async def _busy(_workspace_id, *, conn=None):
        raise WorkspaceSyncBusy("held")
        yield  # pragma: no cover

    with patch("src.server.services.persistence.restore.workspace_sync_lock", _busy):
        with pytest.raises(WorkspaceSyncBusy):
            await restore.restore_to_sandbox("ws-1", _mock_sandbox(), layout=LAYOUT)

    assert [c.args for c in restore_flag.await_args_list] == [("ws-1", True)]


@pytest.mark.asyncio
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_maybe_restore_counts_structural_rows_as_files_to_restore(mock_get):
    """A workspace of directories and symlinks is not an empty one. Reading it as
    empty writes the marker and clears the flag, and the next backup then prunes
    the structural rows the sandbox was never given."""
    mock_get.return_value = [{"file_path": "d", "kind": "dir", "permissions": "0755"}]
    sandbox = _mock_sandbox()
    sandbox.adownload_file_bytes = AsyncMock(return_value=None)  # no sync marker

    with patch.object(
        restore, "restore_to_sandbox", new_callable=AsyncMock
    ) as restore_fn:
        await restore.maybe_restore("ws-1", sandbox, layout=LAYOUT)

    restore_fn.assert_awaited_once_with(
        "ws-1", sandbox, expected_sandbox_id=sandbox.sandbox_id, layout=LAYOUT
    )
    assert mock_get.await_args.kwargs["all_kinds"] is True


@pytest.mark.asyncio
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_restore_caps_concurrency_at_semaphore_size(mock_get):
    """Worker pool semaphore caps concurrent uploads at 16. With 40
    files each taking a tick, peak in-flight must never exceed 16."""
    mock_get.return_value = [_file(f"f_{i}.txt") for i in range(40)]
    sandbox = _mock_sandbox()

    inflight = 0
    peak = 0

    async def tracking_upload(_path, _content):
        nonlocal inflight, peak
        inflight += 1
        peak = max(peak, inflight)
        await asyncio.sleep(0.002)
        inflight -= 1
        return True

    sandbox.aupload_file_bytes = AsyncMock(side_effect=tracking_upload)

    await restore.restore_to_sandbox("ws-1", sandbox, layout=LAYOUT)

    assert peak <= 16, f"Concurrency cap breached: peak={peak}"
    # Sanity: parallelism actually happened (not forced to 1).
    assert peak > 1, f"Expected parallel uploads but peak={peak}"


@pytest.mark.asyncio
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_restore_empty_file_list_is_noop(mock_get, restore_flag):
    """Zero files → no transfer, no errors, the marker, and nothing left flagged.

    The marker matters when every row is deferred: without it each start
    would find no marker and run this restore again."""
    mock_get.return_value = []
    sandbox = _mock_sandbox()

    result = await restore.restore_to_sandbox("ws-1", sandbox, layout=LAYOUT)

    assert result == {"restored": 0, "errors": 0}
    sandbox.acreate_directories.assert_not_awaited()
    assert [c.args[0] for c in sandbox.aupload_file_bytes.await_args_list] == [
        restore._sync_marker_path(LAYOUT)
    ]
    assert [c.args for c in restore_flag.await_args_list] == [("ws-1", True), ("ws-1", False)]


@pytest.mark.asyncio
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_a_manifest_read_that_raises_leaves_the_workspace_flagged(
    mock_get, restore_flag
):
    """The window the flag has to cover starts at the manifest read, not at the
    first transfer: a sandbox that was recreated is empty either way."""
    mock_get.side_effect = RuntimeError("db blip")

    with pytest.raises(RuntimeError):
        await restore.restore_to_sandbox("ws-1", _mock_sandbox(), layout=LAYOUT)

    assert [c.args for c in restore_flag.await_args_list] == [("ws-1", True)]


@pytest.mark.asyncio
async def test_the_flag_is_raised_before_the_lock_is_requested(restore_flag):
    """A worker whose wait times out must not flag a restore another worker
    completed meanwhile. The holder clears the flag as its last write before
    releasing the lock, so a flag written before the wait began is always the
    older of the two; one written after the timeout could be the newer."""
    order: list[str] = []
    restore_flag.side_effect = lambda *a, **k: order.append(f"flag={a[1]}") or True

    @asynccontextmanager
    async def _busy(_workspace_id, *, conn=None):
        order.append("lock")
        raise WorkspaceSyncBusy("held")
        yield  # pragma: no cover

    with patch("src.server.services.persistence.restore.workspace_sync_lock", _busy):
        with pytest.raises(WorkspaceSyncBusy):
            await restore.restore_to_sandbox("ws-1", _mock_sandbox(), layout=LAYOUT)

    assert order == ["flag=True", "lock"]


@pytest.mark.asyncio
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_a_clean_restore_clears_the_flag_after_the_marker(mock_get, restore_flag):
    """The clear is the holder's last write: see the test above."""
    mock_get.return_value = [_file("a.txt", "a")]
    order: list[str] = []
    restore_flag.side_effect = lambda *a, **k: order.append(f"flag={a[1]}") or True
    sandbox = _mock_sandbox()

    async def upload(path, _data):
        if path.endswith(".file_sync_marker"):
            order.append("marker")
        return True

    sandbox.aupload_file_bytes = AsyncMock(side_effect=upload)

    await restore.restore_to_sandbox("ws-1", sandbox, layout=LAYOUT)

    assert order == ["flag=True", "marker", "flag=False"]


@pytest.mark.asyncio
async def test_a_flag_left_standing_beside_a_marker_is_cleared(restore_flag, flag_state):
    """A restore writes the marker and then clears the flag; a process that dies
    between the two leaves a populated sandbox whose every later backup would
    withhold pruning. The marker is written only after a zero-error restore, so
    the flag beside it is stale."""
    flag_state.return_value = True
    sandbox = _mock_sandbox()
    sandbox.adownload_file_bytes = AsyncMock(return_value=b"2026")

    with patch.object(restore, "restore_to_sandbox", new_callable=AsyncMock) as restore_fn:
        await restore.maybe_restore("ws-1", sandbox, layout=LAYOUT)

    restore_fn.assert_not_awaited()
    assert [c.args for c in restore_flag.await_args_list] == [("ws-1", False)]


@pytest.mark.asyncio
async def test_a_marker_with_no_flag_writes_nothing(restore_flag):
    sandbox = _mock_sandbox()
    sandbox.adownload_file_bytes = AsyncMock(return_value=b"2026")

    with patch.object(restore, "restore_to_sandbox", new_callable=AsyncMock) as restore_fn:
        await restore.maybe_restore("ws-1", sandbox, layout=LAYOUT)

    restore_fn.assert_not_awaited()
    restore_flag.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_flag_write_that_fails_aborts_before_the_lock(restore_flag):
    """Without the guard an empty sandbox reads as an emptied workspace. The
    caller has to abort provisioning, so the failure must not look like any
    other restore error, which the callers swallow."""
    restore_flag.side_effect = RuntimeError("db away")
    requested = []

    @asynccontextmanager
    async def _lock(_workspace_id, *, conn=None):
        requested.append(True)
        yield "conn"

    with patch("src.server.services.persistence.restore.workspace_sync_lock", _lock):
        with pytest.raises(restore.RestoreGuardUnavailable):
            await restore.restore_to_sandbox("ws-1", _mock_sandbox(), layout=LAYOUT)

    assert requested == []


@pytest.mark.asyncio
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_the_raise_and_the_clear_each_name_their_sandbox(mock_get, restore_flag):
    """A restore runs on a provisional sandbox before the identity CAS. The
    raise names the sandbox the CAS expects to replace; the clear lands only
    while the row names this sandbox, so a clean restore that then loses the
    race cannot vouch for the winner."""
    mock_get.return_value = [_file("a.txt")]
    sandbox = _mock_sandbox()
    sandbox.sandbox_id = "sb-provisional"

    await restore.restore_to_sandbox(
        "ws-1", sandbox, expected_sandbox_id="sb-previous", layout=LAYOUT
    )

    raised, cleared = restore_flag.await_args_list
    assert raised.args == ("ws-1", True)
    assert raised.kwargs["sandbox_id"] == "sb-previous"
    assert cleared.args == ("ws-1", False)
    assert cleared.kwargs["sandbox_id"] == "sb-provisional"


@pytest.mark.asyncio
async def test_a_raise_that_lands_nowhere_aborts_as_identity_lost(restore_flag):
    """Another provisioner bound the workspace while this one was still
    building; a restore now would fill a sandbox the CAS is about to discard,
    and its flag would stand on the winner's row with nothing left to clear it."""
    restore_flag.return_value = False
    requested = []

    @asynccontextmanager
    async def _lock(_workspace_id, *, conn=None):
        requested.append(True)
        yield "conn"

    with patch("src.server.services.persistence.restore.workspace_sync_lock", _lock):
        with pytest.raises(restore.RestoreIdentityLost):
            await restore.restore_to_sandbox(
                "ws-1",
                _mock_sandbox(),
                expected_sandbox_id="sb-previous",
                layout=LAYOUT,
            )

    assert requested == []
    assert issubclass(restore.RestoreIdentityLost, restore.RestoreGuardUnavailable)


@pytest.mark.asyncio
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_a_reconcile_on_a_bound_sandbox_expects_the_row_to_name_it(mock_get, restore_flag):
    mock_get.return_value = [_file("a.txt")]
    sandbox = _mock_sandbox()
    sandbox.sandbox_id = "sb-bound"
    sandbox.adownload_file_bytes = AsyncMock(return_value=None)

    await restore.maybe_restore("ws-1", sandbox, layout=LAYOUT)

    raised = restore_flag.await_args_list[0]
    assert raised.args == ("ws-1", True)
    assert raised.kwargs["sandbox_id"] == "sb-bound"


@pytest.mark.asyncio
async def test_a_stale_flag_beside_a_marker_is_cleared_for_that_sandbox(restore_flag, flag_state):
    flag_state.return_value = True
    sandbox = _mock_sandbox()
    sandbox.sandbox_id = "sb-bound"
    sandbox.adownload_file_bytes = AsyncMock(return_value=b"2026")

    await restore.maybe_restore("ws-1", sandbox, layout=LAYOUT)

    assert restore_flag.await_args.kwargs["sandbox_id"] == "sb-bound"


@pytest.mark.asyncio
async def test_a_failed_clear_beside_the_marker_is_retried(restore_flag, flag_state):
    """The reconcile after the bind is the last one a warm session gets; a
    single transient failure there must not leave pruning withheld until the
    sandbox is recreated, when the restore would bring deleted files back."""
    flag_state.return_value = True
    restore_flag.side_effect = [RuntimeError("db away"), True]
    sandbox = _mock_sandbox()
    sandbox.sandbox_id = "sb-bound"
    sandbox.adownload_file_bytes = AsyncMock(return_value=b"2026")

    with patch("src.server.services.persistence.restore._FLAG_CLEAR_BACKOFF_S", 0):
        await restore.maybe_restore("ws-1", sandbox, layout=LAYOUT)

    assert restore_flag.await_count == 2
    assert all(c.args == ("ws-1", False) for c in restore_flag.await_args_list)


@pytest.mark.asyncio
async def test_a_clear_that_keeps_failing_is_logged_as_an_error(restore_flag, flag_state, caplog):
    flag_state.return_value = True
    restore_flag.side_effect = RuntimeError("db away")
    sandbox = _mock_sandbox()
    sandbox.adownload_file_bytes = AsyncMock(return_value=b"2026")

    with patch("src.server.services.persistence.restore._FLAG_CLEAR_BACKOFF_S", 0):
        await restore.maybe_restore("ws-1", sandbox, layout=LAYOUT)

    assert restore_flag.await_count == restore._FLAG_CLEAR_ATTEMPTS
    assert any(
        r.levelname == "ERROR" and "completeness flag" in r.getMessage()
        for r in caplog.records
    )


@pytest.mark.asyncio
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_maybe_restore_lets_a_missing_guard_reach_the_caller(mock_get, restore_flag):
    """On the lazy-start and reconnect paths this is the only restore; the
    generic handler must not turn the guard's absence into a warning."""
    mock_get.return_value = [_file("a.txt")]
    restore_flag.side_effect = RuntimeError("db away")
    sandbox = _mock_sandbox()
    sandbox.adownload_file_bytes = AsyncMock(return_value=None)

    with pytest.raises(restore.RestoreGuardUnavailable):
        await restore.maybe_restore("ws-1", sandbox, layout=LAYOUT)


@pytest.mark.asyncio
@patch("src.server.services.persistence.restore.get_files_for_workspace", new_callable=AsyncMock)
async def test_maybe_restore_treats_an_unreadable_manifest_as_a_missing_guard(mock_get, restore_flag):
    """A manifest read that fails leaves the flag unraised and the sandbox
    empty, so it has to unwind provisioning the same way a failed raise does."""
    mock_get.side_effect = RuntimeError("db away")
    sandbox = _mock_sandbox()
    sandbox.adownload_file_bytes = AsyncMock(return_value=None)

    with pytest.raises(restore.RestoreGuardUnavailable):
        await restore.maybe_restore("ws-1", sandbox, layout=LAYOUT)
    restore_flag.assert_not_awaited()


def _kept(prefixes: set[str]) -> ThreadPrefixes:
    return ThreadPrefixes(all=frozenset(prefixes), open=frozenset(prefixes))


@pytest.mark.asyncio
async def test_a_deferred_batch_sends_only_what_is_still_missing_under_its_lock():
    """Two workers' bring-ups can restore the same folder at once, and a row
    sent twice would undo an edit or a delete made in between, so each batch
    looks again under the lock, at its own paths, before it transfers."""
    base = f"{WorkspaceLayout.LARGE_TOOL_RESULTS_DIR}/abcd1234"
    rows = [_file(f"{base}/a.txt", "aaaaa"), _file(f"{base}/b.txt", "bbbbb")]
    probes = iter(["", f"f 5 {LAYOUT.join(base, 'a.txt')}\n"])
    commands = []

    def probe(cmd):
        commands.append(cmd)
        return MagicMock(stdout=next(probes), exit_code=0)

    sandbox = _mock_sandbox()
    sandbox.runtime.exec = AsyncMock(side_effect=probe)
    fetched = []

    async def get_files(_workspace_id, *, paths=None, under=None, **_kw):
        if paths is None:
            return [r for r in rows if r["file_path"].startswith(under + "/")]
        fetched.append(paths)
        return [r for r in rows if r["file_path"] in paths]

    @asynccontextmanager
    async def lock(_workspace_id, *, wait):
        yield "conn"

    transfer = AsyncMock(return_value={"restored": 1, "errors": 0})
    with (
        patch.object(restore, "get_files_for_workspace", get_files),
        patch.object(restore, "workspace_sync_lock", lock),
        patch.object(restore, "_transfer_rows", transfer),
    ):
        result = await restore.restore_deferred(
            "ws-1", sandbox, layout=LAYOUT, kept=_kept({"abcd1234"})
        )

    assert fetched == [[f"{base}/b.txt"]]
    assert result == {"restored": 1, "errors": 0, "skipped": 1, "done": True}
    recheck = commands[1]
    assert "-maxdepth 0" in recheck
    assert LAYOUT.join(base, "a.txt") in recheck and LAYOUT.join(base, "b.txt") in recheck


@pytest.mark.asyncio
async def test_a_deferred_pass_leaves_a_note_the_turn_already_rewrote():
    """The pass runs beside the first turn, which rewrites notes at fixed paths
    without the sync lock. A note there at another size than its row's is that
    later write, and sending the backup's copy over it would undo it."""
    note = WorkspaceLayout.scratchpad_subdir("abcd1234", "note", "plan.md")
    rows = [_file(note, "the plan as backed up")]
    sandbox = _mock_sandbox()
    sandbox.runtime.exec = AsyncMock()

    async def get_files(_workspace_id, *, under=None, **_kw):
        return [r for r in rows if r["file_path"].startswith(under + "/")]

    transfer = AsyncMock()
    with (
        patch.object(restore, "get_files_for_workspace", get_files),
        patch.object(restore, "_transfer_rows", transfer),
    ):
        result = await restore.restore_deferred(
            "ws-1",
            sandbox,
            layout=LAYOUT,
            kept=_kept({"abcd1234"}),
            inventory=restore.parse_deferred(f"f 7 {note}\n"),
        )

    transfer.assert_not_awaited()
    assert result == {"restored": 0, "errors": 0, "skipped": 1, "done": True}


@pytest.mark.asyncio
async def test_a_deferred_batch_whose_probe_failed_sends_nothing():
    """A timed-out exec comes back empty, which would read as every path
    missing and send the backup over what the sandbox holds."""
    base = f"{WorkspaceLayout.LARGE_TOOL_RESULTS_DIR}/abcd1234"
    rows = [_file(f"{base}/a.txt", "aaaaa")]
    probes = iter([MagicMock(stdout="", exit_code=0), MagicMock(stdout="", exit_code=-1)])
    sandbox = _mock_sandbox()
    sandbox.runtime.exec = AsyncMock(side_effect=lambda _cmd: next(probes))

    @asynccontextmanager
    async def lock(_workspace_id, *, wait):
        yield "conn"

    transfer = AsyncMock()
    with (
        patch.object(restore, "get_files_for_workspace", AsyncMock(side_effect=[rows, []])),
        patch.object(restore, "workspace_sync_lock", lock),
        patch.object(restore, "_transfer_rows", transfer),
        pytest.raises(RuntimeError, match="probe failed"),
    ):
        await restore.restore_deferred(
            "ws-1", sandbox, layout=LAYOUT, kept=_kept({"abcd1234"})
        )

    transfer.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_deferred_pass_given_an_inventory_does_not_probe_the_tree_again():
    """The bring-up takes the inventory in the exec that lists the thread
    dirs; a marker already there ends the pass with no exec at all."""
    sandbox = _mock_sandbox()
    sandbox.runtime.exec = AsyncMock()

    with patch.object(restore, "get_files_for_workspace", AsyncMock()) as get_files:
        result = await restore.restore_deferred(
            "ws-1",
            sandbox,
            layout=LAYOUT,
            kept=_kept(set()),
            inventory=restore.parse_deferred("#done\n"),
        )

    assert result["done"] is True
    sandbox.runtime.exec.assert_not_awaited()
    get_files.assert_not_awaited()
