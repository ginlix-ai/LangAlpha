"""Large files: what counts as data, how a file is cut into parts, and the
limits a deployment's storage sets."""

import pytest

from src.config.models import DriveConfig
from src.server.services import drive
from src.server.services.drive import Limits
from src.server.services.drive import content_bytes, uploads
from src.server.services.drive.data_files import is_data_file

MIB = 1024 * 1024


@pytest.mark.parametrize(
    ("name", "data"),
    [
        ("prices.csv", True),
        ("deep/folder/Trades.PARQUET", True),
        ("ticks.csv.gz", True),
        ("panel.jsonl", True),
        ("notes.md", False),
        ("report.json", False),
        (".csv", False),
        ("csv", False),
    ],
)
def test_data_files_are_told_by_their_last_suffix(name, data):
    assert is_data_file(name) is data


def test_a_data_file_is_never_text_with_object_storage(monkeypatch):
    monkeypatch.setattr(content_bytes, "is_storage_enabled", lambda: True)
    assert not content_bytes.kept_as_text("a/prices.csv", b"date,close\n")
    assert content_bytes.kept_as_text("a/notes.md", b"# notes\n")


def test_without_a_store_a_data_file_follows_its_bytes(monkeypatch):
    monkeypatch.setattr(content_bytes, "is_storage_enabled", lambda: False)
    assert content_bytes.kept_as_text("a/prices.csv", b"date,close\n")


@pytest.mark.parametrize(
    ("size", "part_size"),
    [
        (2 * 1024 * MIB, 16 * MIB),
        (160 * 1024 * MIB, 17 * MIB),
        (1000 * 1024 * MIB, 103 * MIB),
    ],
)
def test_a_file_is_cut_into_whole_mib_parts_within_the_stores_count(size, part_size):
    assert uploads.part_size_for(size) == part_size
    assert -(-size // part_size) <= 10_000


def _deployment(monkeypatch, *, store: bool, parts: bool, given: int | None = None, **cfg):
    """``given``: the allowance the account service gives, None for none."""
    monkeypatch.setattr(drive, "_config", lambda: DriveConfig(**cfg))
    monkeypatch.setattr(drive, "is_storage_enabled", lambda: store)
    monkeypatch.setattr(drive, "multipart_enabled", lambda: parts)

    async def drive_allowance(user_id):
        return given

    monkeypatch.setattr(drive.membership, "drive_allowance", drive_allowance)


async def _caps() -> tuple[int | None, int, int]:
    bounds = await drive._limits("u1")
    return bounds.max_space_bytes, bounds.max_file_bytes, bounds.max_request_bytes


@pytest.mark.asyncio
async def test_a_file_may_fill_the_allowance_where_the_store_takes_parts(monkeypatch):
    _deployment(monkeypatch, store=True, parts=True, max_space_bytes=5 * 1024 * MIB)
    assert await _caps() == (5 * 1024 * MIB, 5 * 1024 * MIB, 50 * MIB)


@pytest.mark.asyncio
async def test_a_set_file_limit_binds_below_the_allowance(monkeypatch):
    _deployment(monkeypatch, store=True, parts=True, max_file_bytes=200 * MIB)
    assert (await _caps())[1] == 200 * MIB


@pytest.mark.asyncio
async def test_a_store_without_parts_keeps_what_one_request_carries(monkeypatch):
    _deployment(monkeypatch, store=True, parts=False)
    assert (await _caps())[1:] == (50 * MIB, 50 * MIB)


@pytest.mark.asyncio
async def test_without_a_store_the_database_limits_hold(monkeypatch):
    _deployment(monkeypatch, store=False, parts=False)
    assert (await _caps())[1:] == (10 * MIB, 10 * MIB)


def test_the_refusal_names_the_limit_in_mb():
    bounds = Limits(max_space_bytes=None, max_entries=1, max_trash_bytes=None,
                    max_file_bytes=5 * 1024 * MIB, max_request_bytes=50 * MIB)
    with pytest.raises(drive.DriveError) as refused:
        bounds.too_large(5 * 1024 * MIB + 1)
    assert refused.value.message == "A file is at most 5120 MB"
    bounds.too_large(5 * 1024 * MIB)
