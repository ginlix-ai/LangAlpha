"""What a Drive path may be (src/server/services/drive/paths.py)."""

import unicodedata

import pytest

from src.server.services.drive import DriveError
from src.server.database.drive.keys import normalize, path_key


@pytest.mark.parametrize(
    ("given", "stored"),
    [
        ("notes/plan.md", "notes/plan.md"),
        ("/notes/plan.md", "notes/plan.md"),
        ("notes/", "notes"),
        ("a b/c d.txt", "a b/c d.txt"),
        ("名前/ファイル.md", "名前/ファイル.md"),
        ("..hidden/x", "..hidden/x"),
    ],
)
def test_accepted_paths(given, stored):
    assert normalize(given) == stored


def test_a_path_is_stored_nfc():
    decomposed = unicodedata.normalize("NFD", "café.md")
    assert normalize(decomposed) == unicodedata.normalize("NFC", "café.md")


@pytest.mark.parametrize(
    "given",
    [
        "",
        "/",
        "a//b",
        "../x",
        "a/./b",
        "a/..",
        " lead.md",
        "trail.md ",
        "tab\there",
        "nul\x00",
        "x" * 256,
        "/".join(["d"] * 33),
        "/".join(["d" * 200] * 6),
        # 1025 bytes in 517 characters: the limit is bytes.
        "/".join(["é" * 127] * 4) + "/" + "x" * 5,
    ],
)
def test_refused_paths(given):
    with pytest.raises(DriveError) as caught:
        normalize(given)
    assert caught.value.code == "invalid_path"


def test_a_segment_is_counted_in_bytes():
    assert normalize("é" * 127)
    with pytest.raises(DriveError):
        normalize("é" * 128)


def test_the_root_only_where_a_folder_is_read():
    assert normalize("", root=True) == ""
    assert normalize("/", root=True) == ""


def test_the_whole_path_is_counted_in_bytes():
    at_limit = "/".join(["d" * 200] * 5) + "/" + "f" * 19
    assert len(at_limit.encode()) == 1024
    assert normalize(at_limit) == at_limit
    with pytest.raises(DriveError):
        normalize(at_limit + "f")


@pytest.mark.parametrize(
    ("one", "other"),
    [
        ("straße.txt", "STRASSE.txt"),
        ("aς.txt", "aσ.txt"),
        ("AΣ.txt", "aς.txt"),
        ("Notes/Plan.md", "notes/PLAN.md"),
        (unicodedata.normalize("NFD", "Café"), "CAFÉ"),
    ],
)
def test_names_a_case_insensitive_disk_takes_for_one_share_a_key(one, other):
    assert path_key(normalize(one)) == path_key(normalize(other))


def test_a_folders_key_prefixes_the_keys_under_it():
    """A move rewrites keys under a folder by swapping the prefix."""
    assert path_key("Straße/X.md").startswith(path_key("STRASSE") + "/")
