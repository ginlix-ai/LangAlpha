"""The status and publish routes' edges: a tab on the build before the status
read became a POST still reads it, a stopped computer's page that needs its
libraries says so, and the page's report is refused past a path's length."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from src.server.app.artifacts import records
from src.server.app.artifacts.models import PublishRequest, StatusRequest
from src.server.database.artifacts.records import Artifact
from src.server.services.artifacts import snapshot
from src.server.services.artifacts.manifest import ManifestEntry

_NOW = datetime(2026, 10, 31, tzinfo=timezone.utc)
_ID = "00000000-0000-0000-0000-00000000000a"


def _artifact() -> Artifact:
    return Artifact(
        artifact_id=_ID,
        code="c0de",
        workspace_id="00000000-0000-0000-0000-00000000000f",
        path="reports/q3.html",
        created_thread_id=None,
        bound_thread_id=None,
        keep_updated=False,
        latest_seq=0,
        shared_at=None,
        files=None,
        revision=1,
        created_at=_NOW,
    )


@pytest.fixture
def status(monkeypatch):
    monkeypatch.setattr(records, "owned", AsyncMock(return_value=_artifact()))
    entry = ManifestEntry("reports/q3.html", 3, "entry")
    found = snapshot.Status(
        snapshot.Snapshot(
            entries=[entry],
            left_out=[],
            files=[SimpleNamespace(sha256="0" * 64, byte_len=3)],
            blobs={},
            title=None,
            root_refs=["/assets/app.js"],
        ),
        0,
        None,
        None,
    )
    read = AsyncMock(return_value=found)
    monkeypatch.setattr(records.snapshot, "status", read)
    return read


@pytest.mark.asyncio
async def test_a_tab_on_the_previous_build_reads_status_without_a_report(status):
    posted = await records.get_status(_ID, StatusRequest(), "u")
    got = await records.get_status_unreported(_ID, "u")

    assert got == posted
    assert status.await_args.kwargs["loaded"] == ()
    methods = {
        method
        for route in records.router.routes
        if route.path == "/{artifact_id}/status"
        for method in route.methods
    }
    assert methods == {"GET", "POST"}


@pytest.mark.asyncio
async def test_what_an_earlier_document_loaded_reaches_the_walk_apart(status):
    await records.get_status(
        _ID, StatusRequest(loaded=["data/now.csv"], earlier=["data/old.csv"]), "u"
    )

    assert status.await_args.kwargs["loaded"] == ("data/now.csv",)
    assert status.await_args.kwargs["earlier"] == ("data/old.csv",)


@pytest.mark.asyncio
async def test_what_the_page_loads_from_the_origin_root_comes_with_the_list(status):
    got = await records.get_status(_ID, StatusRequest(), "u")

    assert got.root_refs == ["/assets/app.js"]


@pytest.mark.asyncio
async def test_a_list_past_a_cap_is_answered_with_the_cap_and_no_code(monkeypatch):
    monkeypatch.setattr(records, "owned", AsyncMock(return_value=_artifact()))
    rows = [
        ManifestEntry("reports/q3.html", 3, "entry"),
        ManifestEntry("reports/raw.zip", 500, "link", optional=True),
    ]
    over = snapshot.OverCap("too_many_bytes", 100, rows, [])
    monkeypatch.setattr(
        records.snapshot,
        "status",
        AsyncMock(return_value=snapshot.Status(None, 0, None, None, over)),
    )

    got = await records.get_status(_ID, StatusRequest(), "u")

    assert got.over_cap.model_dump() == {"code": "too_many_bytes", "limit": 100}
    assert [(f.path, f.optional) for f in got.files] == [
        ("reports/q3.html", False),
        ("reports/raw.zip", True),
    ]
    # Nothing was read, so nothing names the page's code for a publish.
    assert (got.live_sha256, got.page_sha256, got.total_size) == (None, None, 503)
    assert got.root_refs == []


def test_a_page_that_needs_its_computer_is_a_conflict_the_web_can_name():
    with pytest.raises(HTTPException) as answered:
        with records._snapshot_errors():
            raise snapshot.SnapshotRefused("needs_computer")

    assert answered.value.status_code == 409
    assert answered.value.detail == {"code": "needs_computer"}


def test_a_publish_names_the_page_code_by_its_digest_or_not_at_all():
    files = {"files": ["reports/q3.html"]}
    assert PublishRequest(**files).page_sha256 is None
    assert PublishRequest(**files, page_sha256="ab" * 32).page_sha256 == "ab" * 32
    with pytest.raises(ValidationError):
        PublishRequest(**files, page_sha256="not a digest")


@pytest.mark.parametrize("model", [StatusRequest, PublishRequest])
def test_a_reported_path_is_refused_past_what_a_record_holds(model):
    extra = {"files": ["reports/q3.html"]} if model is PublishRequest else {}

    assert model(loaded=["a" * 1024], **extra).loaded == ["a" * 1024]
    with pytest.raises(ValidationError):
        model(loaded=["a" * 1025], **extra)
