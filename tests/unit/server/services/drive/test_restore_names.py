"""Where a restored trash item goes (src/server/database/drive/writes.py)."""

import pytest

from src.server.database.drive.keys import MAX_PATH_BYTES, MAX_SEGMENT_BYTES, check
from src.server.database.drive.writes import _roots, beside
from src.server.services.drive import DriveError


def test_a_file_goes_beside_with_a_number_before_its_extension():
    assert beside("notes/plan.md", 1, folder=False) == "notes/plan (1).md"
    assert beside("notes/v1.2", 2, folder=True) == "notes/v1.2 (2)"


def test_a_full_length_name_is_shortened_to_fit():
    name = "a" * 251 + ".txt"
    out = beside(f"long/{name}", 1, folder=False)
    named = out.rsplit("/", 1)[1]
    assert len(named.encode()) <= MAX_SEGMENT_BYTES
    assert named.endswith(" (1).txt")
    check(out)


def test_shortening_cuts_on_a_character():
    name = "é" * 127  # 254 bytes
    named = beside(name, 12, folder=True)
    assert len(named.encode()) <= MAX_SEGMENT_BYTES
    named.encode().decode()  # whole characters only


def test_the_paths_under_a_folder_are_kept_inside_the_limit():
    parent = "/".join(["d" * 200] * 3)
    out = beside(f"{parent}/top", 1, folder=True, below=200)
    assert len(out.encode()) + 200 <= MAX_PATH_BYTES


def test_no_room_for_any_name_is_a_clear_refusal():
    with pytest.raises(DriveError) as caught:
        beside("x", 1, folder=True, below=1022)
    assert caught.value.code == "invalid_path"


def _row(path, deleted=1):
    return {"entry_id": path, "path": path, "changed_seq": deleted}


def test_one_delete_has_one_top_entry():
    groups = _roots([_row("a/b/c.md"), _row("a"), _row("a/b")])
    assert [(top["path"], sorted(r["path"] for r in members)) for top, members in groups] == [
        ("a", ["a", "a/b", "a/b/c.md"])
    ]


def test_a_burst_of_deletes_keeps_each_delete_apart():
    groups = _roots(
        [
            _row("x.md", 1),
            _row("dir/a.md", 2),
            _row("dir", 2),
            _row("y.md", 3),
            _row("other/z.md", 4),
        ]
    )
    assert {top["path"]: len(members) for top, members in groups} == {
        "x.md": 1,
        "y.md": 1,
        "dir": 2,
        "other/z.md": 1,
    }


def test_two_generations_of_a_path_come_back_apart_the_shallowest_first():
    groups = _roots([_row("d/x.md", 1), _row("d", 2), _row("d/x.md", 2)])
    assert [(top["path"], len(members)) for top, members in groups] == [("d", 2), ("d/x.md", 1)]
