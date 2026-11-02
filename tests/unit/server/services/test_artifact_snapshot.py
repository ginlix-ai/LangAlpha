"""Snapshot rules that keep a shared link from costing a read per request: a
link shared before versions is tried once under the caps it was shared with,
a keep-updated read never lands over a newer version, and a path whose file
may not be shared is as missing as one that is not there."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.server.services.share_manifest import (
    MAX_FILES,
    MAX_TOTAL_BYTES,
    ManifestEntryMissing,
    Reader,
)
from src.server.database.artifacts import Artifact
from src.server.services.artifacts import snapshot
from src.server.services.served_paths import may_be_artifact

_NOW = datetime(2026, 10, 31, tzinfo=timezone.utc)


def _captured(*paths: str) -> SimpleNamespace:
    return SimpleNamespace(files=[SimpleNamespace(path=p) for p in paths])


def _artifact(**over) -> Artifact:
    fields = dict(
        artifact_id="00000000-0000-0000-0000-00000000000a",
        code="c0de",
        workspace_id="00000000-0000-0000-0000-00000000000f",
        path="reports/q3.html",
        created_thread_id=None,
        bound_thread_id=None,
        keep_updated=True,
        latest_seq=0,
        shared_at=_NOW,
        files=("reports/q3.html", "reports/big.bin"),
        revision=1,
        created_at=_NOW,
    )
    fields.update(over)
    return Artifact(**fields)


@pytest.fixture
def legacy(monkeypatch):
    monkeypatch.setattr(
        snapshot,
        "db_get_workspace",
        AsyncMock(return_value={"user_id": "u", "status": "stopped"}),
    )
    capture = AsyncMock(side_effect=snapshot.SnapshotRefused("too_many_bytes"))
    refuse = AsyncMock()
    published = AsyncMock()
    monkeypatch.setattr(snapshot, "_capture", capture)
    monkeypatch.setattr(snapshot.artifacts_db, "refuse_legacy", refuse)
    monkeypatch.setattr(snapshot, "publish_artifact_change", published)
    return capture, refuse, published


def test_a_legacy_link_keeps_the_caps_it_was_shared_under():
    assert (snapshot._LEGACY_CAPS.files, snapshot._LEGACY_CAPS.bytes) == (
        MAX_FILES,
        MAX_TOTAL_BYTES,
    )
    assert snapshot._VERSION_CAPS.bytes == snapshot.MAX_VERSION_BYTES
    assert snapshot._VERSION_CAPS.bytes < snapshot._LEGACY_CAPS.bytes


@pytest.mark.asyncio
async def test_a_legacy_link_past_the_caps_is_recorded_and_told_once(legacy):
    capture, refuse, published = legacy
    refuse.side_effect = [_artifact(legacy_refused="too_many_bytes"), None]

    for _ in range(2):
        with pytest.raises(snapshot.LegacyRefused):
            await snapshot.legacy_snapshot(_artifact())

    assert capture.await_args.kwargs["source"].caps is snapshot._LEGACY_CAPS
    assert refuse.await_count == 2
    published.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [True, False])
async def test_other_views_hear_of_a_publish_only_when_it_changed_the_record(
    monkeypatch, changed
):
    record = _artifact(latest_seq=1)
    monkeypatch.setattr(snapshot, "store_unregistered_blobs", AsyncMock())
    monkeypatch.setattr(
        snapshot.artifacts_db,
        "publish_version",
        AsyncMock(
            return_value=snapshot.artifacts_db.PublishResult(
                record, 1, False, False, changed
            )
        ),
    )
    published = AsyncMock()
    monkeypatch.setattr(snapshot, "publish_artifact_change", published)

    await snapshot._cut(
        record,
        SimpleNamespace(files=[], title=None, blobs=[], total=0),
        user_id="u",
        origin="owner",
        keep_updated=False,
    )

    assert published.await_count == (1 if changed else 0)


@pytest.mark.asyncio
async def test_an_owners_publish_names_the_version_its_read_began_from(monkeypatch):
    record = _artifact(latest_seq=7)
    monkeypatch.setattr(snapshot, "_capture", AsyncMock(return_value=SimpleNamespace()))
    cut = AsyncMock()
    monkeypatch.setattr(snapshot, "_cut", cut)

    await snapshot.publish(
        record, user_id="u", reviewed=[record.path], label=None, keep_updated=None
    )

    assert cut.await_args.kwargs["based_on"] == 7


@pytest.mark.asyncio
async def test_a_recorded_refusal_reads_nothing(legacy):
    capture, refuse, _ = legacy

    with pytest.raises(snapshot.LegacyRefused):
        await snapshot.legacy_snapshot(_artifact(legacy_refused="too_many_files"))

    capture.assert_not_awaited()
    refuse.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_missing_legacy_file_is_not_remembered(legacy):
    capture, refuse, _ = legacy
    capture.side_effect = snapshot.SnapshotRefused("file_missing")

    with pytest.raises(snapshot.LegacyRefused):
        await snapshot.legacy_snapshot(_artifact())

    refuse.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_keep_updated_cut_is_based_on_the_version_before_the_read(
    monkeypatch,
):
    artifact = _artifact(latest_seq=7)
    monkeypatch.setattr(
        snapshot, "_capture", AsyncMock(return_value=_captured(*artifact.files))
    )
    cut = AsyncMock()
    monkeypatch.setattr(snapshot, "_cut", cut)

    await snapshot._keep_one(artifact, "u")

    assert cut.await_args.kwargs["origin"] == "keep_updated"
    assert cut.await_args.kwargs["based_on"] == 7


@pytest.mark.asyncio
async def test_keep_updated_cuts_no_version_without_a_listed_file(monkeypatch):
    """The list never widens on its own, so a version cut while a file is being
    rebuilt would leave the link without it for good."""
    monkeypatch.setattr(
        snapshot, "_capture", AsyncMock(return_value=_captured("reports/q3.html"))
    )
    cut = AsyncMock()
    monkeypatch.setattr(snapshot, "_cut", cut)

    await snapshot._keep_one(_artifact(), "u")

    cut.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_file_past_one_read_is_refused_rather_than_unavailable():
    """It will not fit next time either, so a legacy link records the refusal
    instead of answering 503 and reading everything again on each view."""
    reader = SimpleNamespace(files=SimpleNamespace(read=AsyncMock(return_value=None)))
    big = SimpleNamespace(path="big.bin", size=snapshot.EXEC_READ_MAX_BYTES + 1)
    small = SimpleNamespace(path="gone.bin", size=1)

    with pytest.raises(snapshot.SnapshotRefused) as refused:
        await snapshot._read_all(reader, [big])
    assert refused.value.code == "too_many_bytes"
    with pytest.raises(snapshot.SnapshotUnavailable):
        await snapshot._read_all(reader, [small])


def test_a_title_is_read_in_one_pass():
    """On the event loop, on anyone's bytes: a pattern tried at each of a run
    of unclosed tags took seconds."""
    assert snapshot._html_title(b'<title lang="en">Q3 &amp; Q4</TITLE >') == "Q3 & Q4"
    assert snapshot._html_title(b"<title>open") is None
    started = time.monotonic()
    assert snapshot._html_title(b"<title>" * 40_000) is None
    assert time.monotonic() - started < 5


class _LinkedReader:
    def __init__(self, found: dict[str, tuple[str, int]]) -> None:
        self.found = found

    async def stat(self, paths):
        return {p: self.found[p] for p in paths if p in self.found}


@pytest.mark.asyncio
async def test_a_listed_path_resolving_to_a_hidden_file_is_left_out():
    linked = _LinkedReader(
        {
            "reports/q3.html": ("reports/q3.html", 10),
            "reports/data.json": (".agents/data.json", 5),
            "reports/style.css": ("reports/style.css", 3),
        }
    )
    reader = Reader(linked, "/home/workspace/proj", ())
    artifact = _artifact(files=("reports/q3.html", "reports/data.json"))

    entries = await snapshot._listed(
        reader, artifact, ("reports/data.json", "reports/style.css")
    )

    assert [e.path for e in entries] == ["reports/q3.html", "reports/style.css"]

    linked.found["reports/q3.html"] = (".agents/q3.html", 10)
    with pytest.raises(ManifestEntryMissing):
        await snapshot._listed(reader, artifact, ())


def test_a_path_past_what_the_schema_holds_may_not_be_an_artifact():
    """It would get a record, then fail the check on its first comment or
    version with a 500."""
    assert may_be_artifact("reports/" + "a" * 1016)
    assert not may_be_artifact("reports/" + "a" * 1017)
