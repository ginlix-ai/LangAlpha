"""The refusal a strict backup shows the person who asked for the change."""

from src.server.services.persistence.sync_result import BackupIncomplete, UnsavedFile


def test_names_the_files_and_hides_the_ids():
    e = BackupIncomplete(
        "File backup left 1 of 2 project(s) on computer 7f0c... unmirrored",
        [
            UnsavedFile("data/ticks.parquet", "too_large", 6_000_000_000),
            UnsavedFile("logs/run.log", "changed"),
            UnsavedFile("a.bin", "failed"),
            UnsavedFile("b.bin", "unreadable"),
        ],
    )
    assert e.user_message == (
        "Nothing was changed: 4 files could not be backed up first: "
        "data/ticks.parquet (too large to back up), logs/run.log (changed while "
        "saving), a.bin (didn't upload), and 1 more. Move or delete them, or try again."
    )
    assert "7f0c" in str(e) and "7f0c" not in e.user_message


def test_without_a_file_list_says_so_plainly():
    assert BackupIncomplete("sibling list unavailable: OSError").user_message == (
        "Nothing was changed: this computer's files could not be backed up "
        "first. Try again in a moment."
    )


def test_a_path_too_long_to_back_up_is_shortened_in_the_sentence():
    deep = "nest/" * 900 + "leaf.txt"
    message = BackupIncomplete("x", [UnsavedFile(deep, "path_too_long")]).user_message
    assert "(path too long to back up)" in message
    assert "leaf.txt" in message and len(message) < 300
