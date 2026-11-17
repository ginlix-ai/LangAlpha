"""Handing Home's stand-in files back into Home.

Pins what is carried (a diff against what went before, what a backup of the
folder keeps less the agent's own files, size caps, nothing the cloud packed beyond the regular files asked for), that the
folder they land in appears whole and is always a new one, and when a worker
hands back at all: Home's computer answering, nothing of Home's running, the
per-user lock held, and a failure retried later rather than at once.
"""

import io
import subprocess
import tarfile
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services import home_handback, home_handback_transfer
from src.server.services.home_handback_transfer import Moved

USER = "user-1"
_P = "src.server.services.home_handback"
_DB = "src.server.database.home_away"
MAC = {"computer_id": "mac-1", "name": "MacBook Pro", "kind": "ssh"}
CLOUD = {"computer_id": "cloud-1", "name": "Cloud computer", "kind": "daytona"}
AT = datetime(2026, 11, 15, 9, 0, tzinfo=timezone.utc)
THROUGH = datetime(2026, 11, 15, 10, 0, tzinfo=timezone.utc)


def test_the_listing_splits_on_nul_so_any_name_survives():
    names = ["a\nb.md", "line sep.txt", "tab\there.csv", "sub/report.md"]
    data = b"".join(
        f"{i}\t17000000{i}.5\t{name}\0".encode() for i, name in enumerate(names)
    )
    data += b"3\t1.0\t.agents/threads/x.log\0" + b"3\t1.0\twork/.cache/blob\0"
    data += b"3\t1.0\tbad-\xff.bin\0"

    listed, unreadable = home_handback_transfer.parse_listing(data)

    assert sorted(listed) == sorted(names)
    assert listed["tab\there.csv"] == (2, "170000002.5")
    assert unreadable == ["bad-�.bin"]


def test_what_went_before_unchanged_stays_and_the_rest_goes_within_the_caps():
    mb = 1024 * 1024
    listed = {
        "same.md": (3, "100.5"),
        "touched.md": (3, "200.0"),
        "grown.md": (9, "100.0"),
        "new.csv": (5, "300.0"),
        "huge.bin": (201 * mb, "1.0"),
        "a.parquet": (150 * mb, "1.0"),
        "b.parquet": (150 * mb, "1.0"),
        "c.parquet": (150 * mb, "1.0"),
        "d.parquet": (150 * mb, "1.0"),
    }
    before = {
        "same.md": [3, "100.5"],
        "touched.md": [3, "150.0"],
        "grown.md": [3, "100.0"],
    }

    pick = home_handback_transfer.pick_files(listed, before)

    assert sorted(pick.chosen) == [
        "a.parquet",
        "b.parquet",
        "c.parquet",
        "grown.md",
        "new.csv",
        "touched.md",
    ]
    assert pick.too_large == ["huge.bin"]
    assert pick.deferred == ["d.parquet"]
    assert pick.unchanged == ["same.md"]


@pytest.mark.parametrize(
    ("path", "carried"),
    [
        (".gitignore", True),
        (".env.example", True),
        (".github/workflows/ci.yml", True),
        ("sub/.hidden.md", True),
        ("results/.file_sync_marker", True),
        (".agents/threads/x.log", False),
        # A thread's checkpoint notes go back, to their own path.
        (".agents/scratchpad/ab12cd34/note/n.md", True),
        (".agents/scratchpad/ab12cd34/draft.csv", False),
        (".file_sync_marker", False),
        (".wsfiles-abc", False),
        ("proj/.git/HEAD", False),
        ("node_modules/x.js", False),
        ("pkg/__pycache__/m.pyc", False),
        ("x.pyc", False),
        (".DS_Store", False),
        ("a//b", False),
        ("../x", False),
    ],
)
def test_a_hand_back_carries_what_a_backup_keeps_but_the_agents_own(path, carried):
    """A file the user made hidden goes like any other: one left out is in
    neither the copy nor the notice, and the stand-in's computer can then go."""
    listed, _ = home_handback_transfer.parse_listing(f"3\t1.0\t{path}\0".encode())
    assert (path in listed) is carried


def test_an_unreadable_name_in_the_agents_own_files_is_not_named():
    _, unreadable = home_handback_transfer.parse_listing(b"3\t1.0\t.agents/bad-\xff\0")
    assert unreadable == []


def _member(tar, name, data=b"", **attrs):
    info = tarfile.TarInfo(name)
    info.size = len(data)
    for key, value in attrs.items():
        setattr(info, key, value)
    tar.addfile(info, io.BytesIO(data) if data else None)


def test_the_cap_on_the_packed_archive_holds_long_names_and_odd_sizes(tmp_path):
    raw = tmp_path / "raw.tar"
    chosen = {}
    with tarfile.open(raw, "w", format=tarfile.GNU_FORMAT) as tar:
        for i in range(40):
            name = "/".join(["d" * 60] * 5) + f"/{i}.csv"
            data = b"x" * (i * 37 + 1)
            _member(tar, name, data, mtime=1)
            chosen[name] = (len(data), "1.0")
    over = home_handback_transfer.MAX_TOTAL_BYTES - sum(s for s, _ in chosen.values())
    assert raw.stat().st_size + over <= home_handback_transfer.packed_cap(chosen)
    # 4,000 files of 128 KiB under 900-byte names: header, long name, data.
    batch = {f"{i:04d}" + "n" * 896: (128 * 1024, "1.0") for i in range(4000)}
    assert home_handback_transfer.packed_cap(batch) >= 4000 * (512 + 512 + 1024 + 128 * 1024)


def test_rebuilding_an_archive_of_many_headers_keeps_none_in_memory(tmp_path):
    import tracemalloc

    raw, clean = tmp_path / "raw.tar", tmp_path / "clean.tgz"
    with tarfile.open(raw, "w", format=tarfile.GNU_FORMAT) as tar:
        for i in range(50_000):
            _member(tar, f"e{i}")
        _member(tar, "report.md", b"report", mtime=1)

    tracemalloc.start()
    try:
        moved = home_handback_transfer.rebuild_archive(
            raw, clean, {"report.md": (6, "1.0")}
        )
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert list(moved.carried) == ["report.md"]
    assert peak < 5 * 1024 * 1024


def test_the_archive_is_rebuilt_from_the_regular_files_asked_for_alone(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(home_handback_transfer, "MAX_FILE_BYTES", 10)
    raw, clean = tmp_path / "raw.tar", tmp_path / "clean.tgz"
    with tarfile.open(raw, "w", format=tarfile.GNU_FORMAT) as tar:
        _member(tar, "report.md", b"report", mode=0o4755, uid=0, uname="root", mtime=1)
        _member(tar, "report.md", b"second copy")
        _member(tar, "link.md", type=tarfile.SYMTYPE, linkname="/etc/passwd")
        _member(tar, "up.md", type=tarfile.SYMTYPE, linkname="../../.ssh/id_rsa", mtime=1)
        _member(tar, "sub/latest.png", type=tarfile.SYMTYPE, linkname="chart.png", mtime=1)
        _member(tar, "hard.md", type=tarfile.LNKTYPE, linkname="report.md")
        _member(tar, "dev.bin", type=tarfile.CHRTYPE, devmajor=1, devminor=3)
        _member(tar, "../escape.md", b"out")
        _member(tar, "/etc/cron.d/x", b"abs")
        _member(tar, "not-asked.md", b"x")
        _member(tar, "big.bin", b"x" * 11, mtime=1)
        _member(tar, "sub/chart.png", b"png", mtime=1)
    # As listed: the packed member matches on size and whole seconds.
    chosen = {
        "report.md": (6, "1.5"),
        "link.md": (0, "1.0"),
        "up.md": (17, "1.0"),
        "sub/latest.png": (9, "1.0"),
        "hard.md": (0, "1.0"),
        "dev.bin": (0, "1.0"),
        "big.bin": (11, "1.0"),
        "sub/chart.png": (3, "1.0"),
        "gone.md": (1, "1.0"),
    }

    moved = home_handback_transfer.rebuild_archive(raw, clean, chosen)

    with tarfile.open(clean, "r:gz") as tar:
        members = {m.name: m for m in tar.getmembers()}
        assert set(members) == {"report.md", "sub/chart.png", "sub/latest.png"}
        assert tar.extractfile("report.md").read() == b"report"
        link = members.pop("sub/latest.png")
        assert link.issym() and link.linkname == "chart.png" and link.uid == 0
        assert all(
            m.isreg() and m.mode == 0o644 and m.uid == 0 and not m.uname
            for m in members.values()
        )
    assert sorted(moved.carried) == ["report.md", "sub/chart.png", "sub/latest.png"]
    assert moved.too_large == ["big.bin"]
    # A link out of the folder holds nothing of it: dropped, not left behind.
    assert moved.unreadable == ["dev.bin", "gone.md", "hard.md"]


def test_a_file_changed_since_its_listing_waits_rather_than_be_recorded(tmp_path):
    """Recorded as listed, a file packed with other bytes would read as
    handed back once restored to its listed size and mtime, and never go."""
    raw, clean = tmp_path / "raw.tar", tmp_path / "clean.tgz"
    with tarfile.open(raw, "w", format=tarfile.GNU_FORMAT) as tar:
        _member(tar, "kept.md", b"abc", mtime=100)
        _member(tar, "rewritten.md", b"", mtime=200)
    chosen = {"kept.md": (3, "100.25"), "rewritten.md": (3, "100.0")}

    moved = home_handback_transfer.rebuild_archive(raw, clean, chosen)

    assert moved.carried == {"kept.md": [3, "100.25"]}
    assert moved.deferred == ["rewritten.md"]


def test_names_a_mac_spells_alike_go_one_per_hand_back():
    """A Mac's volume unpacks Report.csv over report.csv, or a file over a
    folder so spelled, so one hand-back carries only the first of them."""
    listed = {
        "Report.csv": (1, "1.0"),
        "report.csv": (1, "1.0"),
        "cafe\u0301.md": (1, "1.0"),
        "caf\u00e9.md": (1, "1.0"),
        "Data": (1, "1.0"),
        "data/x.csv": (1, "1.0"),
        "Notes/a.md": (1, "1.0"),
        "notes/b.md": (1, "1.0"),
    }

    pick = home_handback_transfer.pick_files(listed, {})

    assert sorted(pick.chosen) == [
        "Data",
        "Notes/a.md",
        "Report.csv",
        "cafe\u0301.md",
        "notes/b.md",
    ]
    assert sorted(pick.deferred) == ["caf\u00e9.md", "data/x.csv", "report.csv"]

    # Once the first has gone, the next pass carries the other.
    handed = {path: list(listed[path]) for path in pick.chosen}
    assert sorted(home_handback_transfer.pick_files(listed, handed).chosen) == sorted(
        pick.deferred
    )


def _parts(tmp_path, stage, files, links=None):
    """A gzipped archive of ``files`` and symlinks ``links`` cut into parts,
    as the server uploads it."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz", format=tarfile.PAX_FORMAT) as tar:
        for name, data in files.items():
            _member(tar, name, data, mode=0o644)
        for name, target in (links or {}).items():
            _member(tar, name, type=tarfile.SYMTYPE, linkname=target)
    data = buf.getvalue()
    parts = stage / "parts"
    parts.mkdir(parents=True)
    for index, start in enumerate(range(0, len(data), 7)):
        (parts / f"{index:06d}").write_bytes(data[start : start + 7])


def _unpack(home, stage):
    script = home_handback_transfer.unpack_script(
        str(home), str(stage), "From the cloud", "2026-11-15"
    )
    return subprocess.run(["sh", "-c", script], capture_output=True, text=True)


def test_each_hand_back_lands_whole_in_a_folder_nothing_holds_yet(tmp_path):
    home = tmp_path / "Home"
    made = []
    for i in range(3):
        stage = home / f".handback-{i}"
        _parts(tmp_path, stage, {"report.md": b"hi", "sub/a\nb.csv": b"1,2"})
        out = _unpack(home, stage)
        assert out.returncode == 0, out.stdout + out.stderr
        made.append(out.stdout.rpartition("HANDBACK_DEST=")[2])

    base = home / "From the cloud"
    assert made == [
        str(base / "2026-11-15"),
        str(base / "2026-11-15 2"),
        str(base / "2026-11-15 3"),
    ]
    assert (base / "2026-11-15" / "sub" / "a\nb.csv").read_bytes() == b"1,2"


def test_a_folder_made_at_the_name_just_before_the_rename_is_left_alone(tmp_path):
    """``mv`` onto a folder moves into it: the copy comes back out and goes
    to the next name, whole."""
    home = tmp_path / "Home"
    stage = home / ".handback-race"
    _parts(tmp_path, stage, {"report.md": b"hi"})
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    shim = bin_dir / "mv"
    shim.write_text(
        '#!/bin/sh\n'
        'if [ ! -e "$MARK" ]; then : > "$MARK"; mkdir -p "$2"; fi\n'
        'exec /bin/mv "$@"\n'
    )
    shim.chmod(0o755)
    script = home_handback_transfer.unpack_script(
        str(home), str(stage), "From the cloud", "2026-11-15"
    )
    out = subprocess.run(
        ["sh", "-c", script],
        capture_output=True,
        text=True,
        env={"PATH": f"{bin_dir}:/usr/bin:/bin", "MARK": str(tmp_path / "raced")},
    )

    assert out.returncode == 0, out.stdout + out.stderr
    base = home / "From the cloud"
    assert out.stdout.rpartition("HANDBACK_DEST=")[2] == str(base / "2026-11-15 2")
    assert (base / "2026-11-15 2" / "report.md").read_bytes() == b"hi"
    assert list((base / "2026-11-15").iterdir()) == []


def test_notes_go_back_to_their_own_path_and_links_stay_links(tmp_path):
    """A summary names a thread's notes where they were, so they go there in
    Home, never over a note written there since; the rest lands in the
    dated folder, a link as a link."""
    home = tmp_path / "Home"
    note = home / ".agents/scratchpad/ab12cd34/note"
    note.mkdir(parents=True)
    (note / "newer.md").write_text("mac")
    stage = home / ".handback-0123456789ab"
    _parts(
        tmp_path,
        stage,
        {
            ".agents/scratchpad/ab12cd34/note/task.md": b"cloud",
            ".agents/scratchpad/ab12cd34/note/newer.md": b"older",
            "sub/chart.png": b"png",
        },
        links={"sub/latest.png": "chart.png"},
    )
    import os

    os.utime(note / "newer.md", (2_000_000_000, 2_000_000_000))

    out = _unpack(home, stage)

    assert out.returncode == 0, out.stdout + out.stderr
    dest = home / "From the cloud" / "2026-11-15"
    assert out.stdout.rpartition("HANDBACK_DEST=")[2] == str(dest)
    assert (note / "task.md").read_text() == "cloud"
    assert (note / "newer.md").read_text() == "mac"
    assert not (dest / ".agents").exists()
    assert os.readlink(dest / "sub/latest.png") == "chart.png"


def test_notes_alone_fill_no_folder(tmp_path):
    home = tmp_path / "Home"
    stage = home / ".handback-0123456789ab"
    _parts(tmp_path, stage, {".agents/scratchpad/ab12cd34/note/task.md": b"cloud"})

    out = _unpack(home, stage)

    assert out.returncode == 0, out.stdout + out.stderr
    assert out.stdout.rpartition("HANDBACK_DEST=")[2] == ""
    assert (home / ".agents/scratchpad/ab12cd34/note/task.md").read_text() == "cloud"
    assert not (home / "From the cloud").exists()


def test_a_new_stage_clears_only_what_earlier_hand_backs_left(tmp_path):
    home = tmp_path / "Home"
    stale = home / ".handback-aaaaaaaaaaaa"
    (stale / "parts").mkdir(parents=True)
    (home / ".handback-notes").mkdir()
    (home / ".handback-bbbbbbbbbbbb").mkdir()
    stage = home / ".handback-0123456789ab"

    out = subprocess.run(
        ["sh", "-c", home_handback_transfer.stage_script(str(home), str(stage))],
        capture_output=True,
        text=True,
    )

    assert out.returncode == 0, out.stdout + out.stderr
    assert sorted(p.name for p in home.iterdir()) == [
        ".handback-0123456789ab",
        ".handback-bbbbbbbbbbbb",
        ".handback-notes",
    ]
    assert (stage / "parts").is_dir()


def test_an_archive_that_does_not_unpack_leaves_no_folder(tmp_path):
    home = tmp_path / "Home"
    stage = home / ".handback-x"
    (stage / "parts").mkdir(parents=True)
    (stage / "parts" / "000000").write_bytes(b"not an archive")

    out = _unpack(home, stage)

    assert out.returncode != 0
    assert not (home / "From the cloud").exists()


@pytest.mark.parametrize(
    ("locale", "folder"),
    [
        ("zh-CN", "来自云端"),
        ("zh", "来自云端"),
        ("en-US", "From the cloud"),
        (None, "From the cloud"),
    ],
)
def test_the_folder_is_named_in_the_account_language(locale, folder):
    assert home_handback.handback_folder(locale) == folder


@pytest.fixture
def owed():
    """A user owed a hand-back, with Home on a machine that answers again."""
    order = []
    state = {
        "held": True,
        "busy": False,
        "live_run": False,
        "unreachable": False,
        "stand_in_computer": CLOUD,
        "order": order,
    }

    async def computer_for(workspace_id, conn=None):
        from src.server.database.home_workspace import get_home_stand_in_id

        return (
            state["stand_in_computer"]
            if workspace_id == get_home_stand_in_id(USER)
            else MAC
        )

    @asynccontextmanager
    async def lock(key):
        yield state["held"]

    row = {
        "user_id": USER,
        "last_away_at": AT,
        "away_since": AT,
        "handed_back_through": None,
        "last_handback": None,
    }

    async def begin(user_id):
        return None if state["live_run"] else (dict(row), THROUGH)

    moved = Moved(
        folder="From the cloud/2026-11-15",
        carried={"a.md": [3, "1.0"]},
        computer_id="mac-1",
    )
    moved.clean_up = AsyncMock(side_effect=lambda: order.append("clean_up"))
    finish = AsyncMock(side_effect=lambda *a, **k: order.append("finish") or True)
    mirror = AsyncMock(side_effect=lambda _: order.append("mirror"))
    with (
        patch(f"{_P}.mirror_computer", mirror),
        patch(f"{_P}.get_computer_for_workspace", side_effect=computer_for),
        patch(
            f"{_P}.computer_unreachable",
            AsyncMock(side_effect=lambda c, **_: state["unreachable"]),
        ),
        patch(f"{_P}._home_busy", AsyncMock(side_effect=lambda _: state["busy"])),
        patch(f"{_P}._user_locale", AsyncMock(return_value="en-US")),
        patch("src.server.database.session_lock.try_own_session_lock", lock),
        patch(f"{_DB}.get_home_away", AsyncMock(return_value=row)),
        patch(f"{_DB}.begin_handback", side_effect=begin),
        patch(f"{_DB}.handed_back", AsyncMock(return_value={"old.md": [1, "0.5"]})),
        patch(f"{_DB}.finish_handback", finish),
        patch(f"{_DB}.fail_handback", AsyncMock(return_value=False)) as fail,
        patch(f"{_DB}.defer_handback", AsyncMock()) as defer,
        patch(
            "src.server.services.home_handback_transfer.move_into_home",
            AsyncMock(return_value=moved),
        ) as copy,
    ):
        state.update(
            finish=finish, fail=fail, defer=defer, copy=copy, moved=moved, mirror=mirror
        )
        yield state


@pytest.mark.asyncio
async def test_a_hand_back_records_what_it_copied_before_cleaning_up(owed):
    assert await home_handback.hand_back_owed(USER) is True

    owed["copy"].assert_awaited_once()
    assert owed["copy"].await_args.args == (USER, {"old.md": [1, "0.5"]})
    kwargs = owed["finish"].await_args.kwargs
    assert kwargs["previous"] is None and kwargs["through"] == THROUGH
    assert kwargs["carried"] == {"a.md": [3, "1.0"]} and kwargs["done"] is True
    assert kwargs["record"]["folder"] == "From the cloud/2026-11-15"
    assert kwargs["record"]["files"] == 1
    assert owed["order"] == ["finish", "clean_up", "mirror"]
    owed["mirror"].assert_awaited_once_with("mac-1")


@pytest.mark.asyncio
@pytest.mark.parametrize("why", ["nothing_copied", "watermark_moved"])
async def test_home_is_backed_up_only_after_a_hand_back_it_took(owed, why):
    if why == "nothing_copied":
        owed["moved"].folder, owed["moved"].carried = None, {}
    else:
        owed["finish"].side_effect = lambda *a, **k: False

    await home_handback.hand_back_owed(USER)

    owed["mirror"].assert_not_awaited()


@pytest.mark.asyncio
async def test_what_a_hand_back_found_unchanged_is_recorded(owed):
    owed["moved"].kept = ["old.md"]

    await home_handback.hand_back_owed(USER)

    assert owed["finish"].await_args.kwargs["kept"] == ["old.md"]


@pytest.mark.asyncio
async def test_files_with_no_room_this_time_stay_owed(owed):
    owed["moved"].deferred = ["d.parquet"]

    await home_handback.hand_back_owed(USER)

    assert owed["finish"].await_args.kwargs["done"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("why", ["unreachable", "not_held"])
async def test_nothing_is_copied_while_home_cannot_take_it(owed, why):
    if why == "not_held":
        owed["held"] = False
    else:
        owed[why] = True

    assert await home_handback.hand_back_owed(USER) is False

    owed["copy"].assert_not_awaited()
    owed["finish"].assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("why", ["busy", "live_run"])
async def test_a_busy_home_steps_aside_without_counting_a_failure(owed, why):
    owed[why] = True

    assert await home_handback.hand_back_owed(USER) is False

    owed["copy"].assert_not_awaited()
    owed["defer"].assert_awaited_once()
    owed["fail"].assert_not_awaited()


@pytest.mark.asyncio
async def test_a_failed_copy_is_counted_and_retried_later(owed):
    owed["copy"].side_effect = RuntimeError("tar failed")

    assert await home_handback.hand_back_owed(USER) is False

    owed["finish"].assert_not_awaited()
    kwargs = owed["fail"].await_args.kwargs
    assert kwargs["max_attempts"] == home_handback.MAX_ATTEMPTS
    assert kwargs["given_up"]["failed"] is True
    assert kwargs["given_up"]["cloud_name"] == "Cloud computer"


@pytest.mark.asyncio
async def test_with_the_stand_ins_computer_gone_nothing_stays_owed(owed):
    owed["stand_in_computer"] = None

    assert await home_handback.hand_back_owed(USER) is False

    owed["copy"].assert_not_awaited()
    # To the watermark read under Home's row lock, not the last mark read
    # before it: a turn that placed the stand-in again since stays owed.
    owed["finish"].assert_awaited_once_with(
        USER, previous=None, through=THROUGH, record=None
    )


@pytest.mark.asyncio
async def test_with_the_stand_ins_computer_gone_a_busy_home_still_waits(owed):
    owed["stand_in_computer"] = None
    owed["live_run"] = True

    assert await home_handback.hand_back_owed(USER) is False

    owed["finish"].assert_not_awaited()


@pytest.mark.asyncio
async def test_home_is_backed_up_even_when_the_cleanup_is_cancelled(owed):
    import asyncio

    owed["moved"].clean_up = AsyncMock(side_effect=asyncio.CancelledError)

    with pytest.raises(asyncio.CancelledError):
        await home_handback.hand_back_owed(USER)

    owed["mirror"].assert_awaited_once_with("mac-1")


@pytest.mark.asyncio
async def test_files_left_on_the_cloud_are_named_once():
    left = Moved(too_large=["huge.bin"])
    names = {"computer_name": "Mac", "cloud_name": "Cloud", "cloud_computer_id": "c1"}
    with patch(f"{_P}._names", AsyncMock(return_value=names)):
        first = await home_handback._record(USER, left, None)
        again = await home_handback._record(USER, left, first)
        nothing = await home_handback._record(USER, Moved(), first)
    # The stand-in stands on another computer now, which holds its own copy.
    with patch(f"{_P}._names", AsyncMock(return_value={**names, "cloud_computer_id": "c2"})):
        moved = await home_handback._record(USER, left, first)

    assert first["folder"] is None and first["left_on_cloud"] == ["huge.bin"]
    assert again is None and nothing is None
    assert moved["cloud_computer_id"] == "c2"


def _stand_in(*, owed=False, left=False):
    from src.server.database.home_stand_in import StandIn

    return StandIn(
        row={}, computer=_RETIRING, kept=True, runs=True, owed=owed, left=left
    )


_RETIRING = {
    **CLOUD,
    "user_id": USER,
    "is_primary": False,
    "status": "running",
    "spec_change": None,
    "config": {"retire_when_empty": True},
}
_R = "src.server.services.computer_manager._retirement"


@pytest.fixture
def retiring():
    """A cloud computer with no workspace of the user's on it, Home's stand-in
    on it as each read of it (``stand_in_on``) finds it in turn."""
    from src.server.services.computer_manager._retirement import RetirementMixin

    state = {"reads": [], "held": True, "holding": [], "torn_down": [], "needed": False}

    class Manager(RetirementMixin):
        @asynccontextmanager
        async def _machine_decision_lock(self, computer_id):
            yield True

        def _binding_from_computer(self, workspace_id, claimed):
            return claimed

        async def _teardown_machine(self, binding):
            state["torn_down"].append(list(state["holding"]))

    @asynccontextmanager
    async def stand_in_held(user_id):
        state["holding"].append(user_id)
        try:
            yield state["held"]
        finally:
            state["holding"].remove(user_id)

    async def update_status(computer_id, status, **_):
        return {**_RETIRING, "status": status}

    with (
        patch(f"{_R}.get_computer", AsyncMock(return_value=_RETIRING)),
        patch(f"{_R}.mark_computer_retirement_pending", AsyncMock(return_value=True)),
        patch(f"{_R}.clear_computer_retirement_pending", AsyncMock()) as cleared,
        patch(f"{_R}.update_computer_status", side_effect=update_status),
        patch(f"{_R}.get_live_workspace_ids_for_computer", AsyncMock(return_value=[])),
        patch(f"{_R}.stand_in_on", AsyncMock(side_effect=lambda c: state["reads"].pop(0))),
        patch(f"{_R}.stand_in_held", stand_in_held),
        patch(f"{_R}.stand_in_needed", AsyncMock(side_effect=lambda _: state["needed"])),
    ):
        state.update(manager=Manager(), cleared=cleared)
        yield state


async def _decide(state, **kwargs):
    async with state["manager"]._retirement_decision("cloud-1", **kwargs) as (outcome, _):
        return outcome


@pytest.mark.asyncio
async def test_owed_stand_in_files_keep_their_computer_and_its_marker(retiring):
    from src.server.services.computer_errors import Retirement

    retiring["reads"] = [_stand_in(), _stand_in(owed=True)]

    assert await _decide(retiring, sweep=True) is Retirement.HANDBACK_OWED
    assert retiring["torn_down"] == []
    # The sweep retires it once they are in Home.
    retiring["cleared"].assert_not_awaited()


@pytest.mark.asyncio
async def test_a_turn_admitted_before_the_hold_keeps_the_owners_remove_off(retiring):
    """Read again under the hold: a Home turn admitted after the first read
    marked Home owed, so the remove is refused rather than taking the files."""
    from src.server.services.computer_errors import Retirement

    retiring["reads"] = [_stand_in(), _stand_in(owed=True)]

    assert await _decide(retiring, owner_request=True) is Retirement.HANDBACK_OWED
    assert retiring["torn_down"] == []


@pytest.mark.asyncio
async def test_a_turn_being_admitted_keeps_the_stand_ins_computer(retiring):
    from src.server.services.computer_errors import Retirement

    retiring["reads"] = [_stand_in()]
    retiring["held"] = False

    assert await _decide(retiring, owner_request=True) is Retirement.BUSY
    assert retiring["torn_down"] == []


@pytest.mark.asyncio
async def test_the_stand_in_alone_goes_with_its_computer_under_the_hold(retiring):
    from src.server.services.computer_errors import Retirement

    retiring["reads"] = [_stand_in(), _stand_in()]

    assert await _decide(retiring, owner_request=True) is Retirement.RETIRED
    # No Home turn could be admitted onto it through the teardown.
    assert retiring["torn_down"] == [[USER]]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("ask", "outcome"),
    [({"sweep": True}, "handback_left"), ({"owner_request": True}, "retired")],
    ids=["sweep", "owner"],
)
async def test_files_a_hand_back_left_keep_their_computer_from_retiring_itself(
    retiring, ask, outcome
):
    """Not in Home, so only their owner's remove, told so, takes them."""
    retiring["reads"] = [_stand_in(left=True), _stand_in(left=True)]

    assert await _decide(retiring, **ask) == outcome


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("needed", "ask", "outcome"),
    [
        (True, {"sweep": True}, "stand_in"),
        (True, {"owner_request": True}, "retired"),
        (False, {"sweep": True}, "retired"),
    ],
    ids=["own-primary-sweep", "own-primary-owner", "cloud-primary-sweep"],
)
async def test_the_stand_ins_computer_stays_for_a_primary_that_can_be_away(
    retiring, needed, ask, outcome
):
    """With the primary a machine of the user's own, it is where Home's turns
    run while that machine is away, so only its owner's remove takes it."""
    retiring["reads"] = [_stand_in(), _stand_in()]
    retiring["needed"] = needed

    assert await _decide(retiring, **ask) == outcome


def test_what_a_hand_back_left_is_read_off_its_notice():
    from src.server.database.home_away import left_on_stand_in

    notice = home_handback._notice(computer_name="Mac", cloud_name="Cloud", cloud_computer_id="c1")
    left = {**notice, "left_on_cloud": ["huge.bin"]}

    assert left_on_stand_in(None, "c1") is None
    assert left_on_stand_in(notice, "c1") is None
    assert left_on_stand_in(left, "c1") == {"failed": False, "paths": ["huge.bin"], "computer_id": "c1"}
    assert left_on_stand_in({**notice, "failed": True}, "c1") == {
        "failed": True,
        "paths": [],
        "computer_id": "c1",
    }
    # The stand-in stands elsewhere now: what was left went with that computer.
    assert left_on_stand_in(left, "c2") is None
    assert left_on_stand_in(left, None) is None


@pytest.mark.asyncio
async def test_a_cleanup_that_fails_after_the_copy_is_recorded_raises_nothing():
    runtime = MagicMock()
    runtime.exec = AsyncMock(side_effect=ConnectionError("tunnel dropped"))

    await Moved(stage=(runtime, "/home/.handback-x")).clean_up()


@pytest.mark.asyncio
async def test_home_is_backed_up_on_the_computer_the_files_went_to(owed):
    """Home moved since the hand-back first read its computer: the files
    went where it is now, so that computer is the one backed up."""
    owed["moved"].computer_id = "mac-2"

    await home_handback.hand_back_owed(USER)

    owed["mirror"].assert_awaited_once_with("mac-2")


@pytest.mark.asyncio
@pytest.mark.parametrize("grown", [False, True], ids=["seed", "grown"])
async def test_the_seed_check_reads_no_further_than_the_seed(grown):
    """The notes file may have grown since its listing; the check stops one
    byte past the seed rather than read it all into memory."""
    from src.server.services.computer_manager._types import _SEEDED_AGENT_MD

    seed = _SEEDED_AGENT_MD.encode()
    read = []

    class Runtime:
        async def download_file_stream(self, path):
            yield seed
            read.append("seed")
            if grown:
                for _ in range(1000):
                    read.append("more")
                    yield b"x" * 65536

    seeded = await home_handback_transfer._is_seeded(Runtime(), "/f", len(seed))

    assert seeded is (not grown)
    assert read.count("more") <= 1
