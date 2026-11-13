"""An attachment uploaded before its message reaches the turn as an inline one
does: a content block when the model takes it, a saved file always, fetched
by the sandbox from the store when it can. An id that resolves to nothing is
said to the agent, and the turn still runs."""

from __future__ import annotations

import base64
import io
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from PIL import Image

from ptc_agent.core.project_context import ProjectContext
from src.server.handlers.chat.attachments import attach_request_files
from src.server.models.chat import ChatRequest
from src.server.models.chat_attachment import NATIVE_MAX_BYTES, MissingAttachment, TurnAttachments
from src.server.services import chat_attachment_turns as turns
from src.server.utils.multimodal_context import build_attachment_metadata

pytestmark = pytest.mark.asyncio

USER = "user-1"
IMAGE_ID = "00000000-0000-4000-8000-000000000001"
CSV_ID = "00000000-0000-4000-8000-000000000002"
GONE_ID = "00000000-0000-4000-8000-000000000003"
XLSX_ID = "00000000-0000-4000-8000-000000000004"
VISION = SimpleNamespace(input_modalities=["text", "image", "pdf"])
PROJECT = ProjectContext(workspace_id="ws-1", dir_name="Research")


def _png() -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (8, 8), "white").save(buffer, "PNG")
    return buffer.getvalue()


PNG = _png()


def _row(attachment_id: str, name: str, mime: str, size: int, **overrides) -> dict:
    return {
        "attachment_id": attachment_id,
        "user_id": USER,
        "name": name,
        "mime_type": mime,
        "size": size,
        "object_key": None,
        "has_content": True,
        "state": "ready",
        **overrides,
    }


def _stored(attachment_id: str, name: str, mime: str, size: int) -> dict:
    return _row(
        attachment_id, name, mime, size, object_key=f"attachments/{attachment_id}/{name}", has_content=False
    )


def _request(*items: tuple[str, str, str]) -> ChatRequest:
    return ChatRequest(
        messages=[{"role": "user", "content": "look"}],
        additional_context=[
            {"type": kind, "attachment_id": attachment_id, "description": name}
            for kind, attachment_id, name in items
        ],
    )


class _Sandbox:
    """A sandbox the server writes bytes into; ``provider`` makes it one
    that fetches from the store itself."""

    def __init__(self, provider: str | None = None):
        self.files: dict[str, bytes] = {}
        self.config = SimpleNamespace(sandbox=SimpleNamespace(provider=provider))

    def workspace(self, project=None):
        return SimpleNamespace(workspace="/home/u/Research", root="/home/u")

    def normalize_path(self, path, project=None):
        return f"/home/u/Research/{path}"

    async def aupload_file_bytes(self, path, data):
        self.files[path] = data
        return True


async def _attach(request, rows, sandbox, *, content=None):
    with (
        patch.object(turns.db, "find_for_turn", AsyncMock(return_value=rows)) as take,
        patch.object(turns.db, "get_content", AsyncMock(return_value=content)),
    ):
        attachments = await turns.resolve_turn_attachments(request.additional_context, USER)
        [message] = await attach_request_files(
            [{"role": "user", "content": "look"}],
            attachments,
            SimpleNamespace(sandbox=sandbox),
            "model",
            VISION,
            project=PROJECT,
        )
    return message["content"], attachments, take


@asynccontextmanager
async def _store_pulls(results=None):
    """A sandbox that fetches from the store; records each pull and each hold
    of the folder's lock."""
    seen: dict = {"pulls": [], "locks": []}

    async def pull_direct(_sandbox, items, *, layout):
        seen["pulls"].append(items)
        return {i["path"]: {"status": "ok"} for i in items} if results is None else results(items)

    @asynccontextmanager
    async def lock(workspace_id, *, wait):
        seen["locks"].append(workspace_id)
        yield None

    get_bytes = MagicMock()
    with (
        patch.object(turns, "sandbox_pulls_from_store", MagicMock(return_value=True)),
        patch.object(turns, "pull_direct", pull_direct),
        patch.object(turns, "workspace_sync_lock", lock),
        patch.object(turns, "get_signed_url", lambda key, _expires: f"https://store.invalid/{key}"),
        patch.object(turns, "get_bytes", get_bytes),
    ):
        seen["get_bytes"] = get_bytes
        yield seen


async def test_an_uploaded_image_goes_in_as_itself_and_is_saved_from_its_bytes():
    sandbox = _Sandbox()
    content, _, take = await _attach(
        _request(("image", IMAGE_ID, "chart.png")),
        [_row(IMAGE_ID, "chart.png", "image/png", len(PNG))],
        sandbox,
        content=PNG,
    )

    take.assert_awaited_once_with([IMAGE_ID], USER)
    assert [b["type"] for b in content] == ["text", "image_url", "text"]
    assert content[0]["text"].startswith("[Attached image: chart.png (saved to work/uploads/")
    assert content[1]["image_url"]["url"] == "data:image/png;base64," + base64.b64encode(PNG).decode()
    [(path, saved)] = sandbox.files.items()
    assert path.startswith("/home/u/Research/work/uploads/chart_") and saved == PNG


async def test_a_stored_file_is_fetched_by_the_sandbox_and_never_read_here():
    sandbox = _Sandbox(provider="daytona")
    async with _store_pulls() as seen:
        content, _, _ = await _attach(
            _request(("file", CSV_ID, "prices.csv")),
            [_stored(CSV_ID, "prices.csv", "text/csv", 40)],
            sandbox,
        )

    [[item]] = seen["pulls"]
    assert item["url"] == f"https://store.invalid/attachments/{CSV_ID}/prices.csv"
    assert item["size"] == 40
    assert item["path"].startswith("work/uploads/prices_") and item["path"].endswith(".csv")
    assert seen["locks"] == ["ws-1"]
    seen["get_bytes"].assert_not_called()
    assert sandbox.files == {}
    assert f"It has been saved to {item['path']}." in content


async def test_several_stored_files_are_fetched_in_one_pull_under_one_lock():
    """Pulled one by one, each would wait out the others' holds of the
    folder's lock, and one waiting too long would fall back to a copy capped
    well under what the store takes."""
    sandbox = _Sandbox(provider="daytona")
    big = 600 * 1024 * 1024

    def placed_but_last(items):
        return {i["path"]: {"status": "ok" if n < 2 else "failed"} for n, i in enumerate(items)}

    async with _store_pulls(placed_but_last) as seen:
        content, attachments, _ = await _attach(
            _request(
                ("file", CSV_ID, "prices.csv"),
                ("file", XLSX_ID, "book.xlsx"),
                ("image", IMAGE_ID, "chart.png"),
            ),
            [
                _stored(CSV_ID, "prices.csv", "text/csv", big),
                _stored(XLSX_ID, "book.xlsx", "application/octet-stream", big),
                _row(IMAGE_ID, "chart.png", "image/png", len(PNG), object_key=f"attachments/{IMAGE_ID}/chart.png"),
            ],
            sandbox,
            content=PNG,
        )

    [items] = seen["pulls"]
    assert len(items) == 3 and seen["locks"] == ["ws-1"]
    csv, xlsx, image = attachments.files
    assert csv.path == items[0]["path"] and xlsx.path == items[1]["path"]
    # The one the pull did not place is copied from the bytes read for its block.
    [(saved, raw)] = sandbox.files.items()
    assert image.path and saved.endswith(image.path) and raw == PNG
    seen["get_bytes"].assert_not_called()
    assert content[1]["type"] == "image_url"


async def test_an_image_too_large_for_a_block_is_only_a_file():
    sandbox = _Sandbox()
    size = NATIVE_MAX_BYTES + 1
    with patch.object(turns, "_load_bytes", AsyncMock(return_value=b"x" * size)) as load:
        content, _, _ = await _attach(
            _request(("image", IMAGE_ID, "scan.png")),
            [_row(IMAGE_ID, "scan.png", "image/png", size)],
            sandbox,
        )

    assert isinstance(content, str)
    assert "The user attached a file (scan.png, image/png). It has been saved to work/uploads/scan_" in content
    assert "cannot view" not in content
    load.assert_awaited_once()  # for the copy, not for a block


async def test_an_id_that_resolves_to_nothing_is_said_and_the_rest_still_goes_in():
    sandbox = _Sandbox()
    content, attachments, _ = await _attach(
        _request(("image", IMAGE_ID, "chart.png"), ("file", GONE_ID, "old.xlsx")),
        [_row(IMAGE_ID, "chart.png", "image/png", len(PNG))],
        sandbox,
        content=PNG,
    )

    assert [a.name for a in attachments.files] == ["chart.png"]
    assert [m.name for m in attachments.missing] == ["old.xlsx"]
    assert content[1]["type"] == "image_url"
    assert "The user attached a file (old.xlsx), but it is no longer available" in content[-1]["text"]
    assert len(sandbox.files) == 1


async def test_history_links_a_stored_upload_without_storing_it_again():
    request = _request(("file", CSV_ID, "prices.csv"), ("pdf", IMAGE_ID, "memo.pdf"))
    rows = [
        _stored(CSV_ID, "prices.csv", "text/csv", 40),
        _row(IMAGE_ID, "memo.pdf", "application/pdf", 9),
    ]
    with patch.object(turns.db, "find_for_turn", AsyncMock(return_value=rows)):
        resolved = await turns.resolve_turn_attachments(request.additional_context, USER)
    attachments = TurnAttachments(
        resolved.files, [MissingAttachment("gone.png", "image", "it is gone")]
    )
    upload = MagicMock()
    with (
        patch("src.server.utils.multimodal_context.upload_bytes", upload),
        patch(
            "src.server.utils.multimodal_context.get_public_url",
            lambda key: f"https://cdn.invalid/{key}",
        ),
    ):
        meta = await build_attachment_metadata(attachments, "thread-1")

    upload.assert_not_called()
    assert meta == [
        {"name": "prices.csv", "type": "file", "size": 40, "url": f"https://cdn.invalid/attachments/{CSV_ID}/prices.csv"},
        {"name": "memo.pdf", "type": "pdf", "size": 9},
        {"name": "gone.png", "type": "image", "size": 0, "missing": True},
    ]


async def test_inline_data_still_goes_in_as_itself_and_is_saved_from_its_bytes():
    sandbox = _Sandbox()
    data = "data:image/png;base64," + base64.b64encode(PNG).decode()
    request = ChatRequest(
        messages=[{"role": "user", "content": "look"}],
        additional_context=[{"type": "image", "data": data, "description": "chart.png"}],
    )
    take = AsyncMock()
    with patch.object(turns.db, "find_for_turn", take):
        attachments = await turns.resolve_turn_attachments(request.additional_context, USER)
    [message] = await attach_request_files(
        [{"role": "user", "content": "look"}],
        attachments,
        SimpleNamespace(sandbox=sandbox),
        "model",
        VISION,
        project=PROJECT,
    )

    take.assert_not_awaited()
    content = message["content"]
    assert content[1] == {"type": "image_url", "image_url": {"url": data}}
    [(path, saved)] = sandbox.files.items()
    assert saved == PNG and content[0]["text"] == f"[Attached image: chart.png (saved to {path.removeprefix('/home/u/Research/')})]"


async def test_a_composer_item_names_bytes_one_way_only():
    with pytest.raises(ValueError):
        ChatRequest(
            messages=[{"role": "user", "content": "x"}],
            additional_context=[{"type": "image", "data": "data:image/png;base64,AA", "attachment_id": IMAGE_ID}],
        )
    with pytest.raises(ValueError):
        ChatRequest(
            messages=[{"role": "user", "content": "x"}],
            additional_context=[{"type": "file", "description": "neither"}],
        )


async def test_a_raw_item_naming_neither_is_dropped_not_fatal():
    attachments = await turns.resolve_turn_attachments(
        [{"type": "file", "description": "neither"}, {"type": "image", "data": "data:image/png;base64,AA=="}], USER
    )
    assert [a.size for a in attachments.files] == [1] and not attachments.missing


async def test_an_id_named_twice_is_one_file_and_past_the_cap_is_one_note():
    """An id costs the request nothing, so the count of distinct ids is what
    bounds what a turn reads and keeps."""
    ids = [f"00000000-0000-4000-8000-1000000000{n:02d}" for n in range(turns.MAX_UPLOADED_PER_TURN + 3)]
    request = _request(*[("file", i, f"f{n}.csv") for n, i in enumerate(ids)], ("file", ids[0], "again.csv"))
    rows = [_row(i, f"f{n}.csv", "text/csv", 4) for n, i in enumerate(ids[: turns.MAX_UPLOADED_PER_TURN])]

    with patch.object(turns.db, "find_for_turn", AsyncMock(return_value=rows)) as take:
        attachments = await turns.resolve_turn_attachments(request.additional_context, USER)

    taken = take.await_args.args[0]
    assert sorted(taken) == sorted(ids[: turns.MAX_UPLOADED_PER_TURN])
    assert len(attachments.files) == turns.MAX_UPLOADED_PER_TURN
    assert [m.name for m in attachments.missing] == ["3 more"]


async def test_a_sent_file_whose_bytes_postgres_dropped_is_missing():
    request = _request(("file", CSV_ID, "prices.csv"))
    rows = [_row(CSV_ID, "prices.csv", "text/csv", 40, has_content=False)]

    with patch.object(turns.db, "find_for_turn", AsyncMock(return_value=rows)):
        attachments = await turns.resolve_turn_attachments(request.additional_context, USER)

    assert attachments.files == []
    assert [m.name for m in attachments.missing] == ["prices.csv"]


async def test_images_past_the_turns_block_budget_go_in_by_path():
    from src.server.models.chat_attachment import NATIVE_TURN_MAX_BYTES

    size = NATIVE_TURN_MAX_BYTES // 2 - 1
    ids = [IMAGE_ID, CSV_ID, XLSX_ID]
    request = _request(*[("image", i, f"p{n}.png") for n, i in enumerate(ids)])
    rows = [_stored(i, f"p{n}.png", "image/png", size) for n, i in enumerate(ids)]
    read = AsyncMock(return_value=None)

    with patch("src.server.handlers.chat.attachments.read_for_model", read):
        await _attach(request, rows, _Sandbox())

    assert read.await_count == 2


async def test_a_file_that_could_not_be_saved_is_said_so():
    sandbox = _Sandbox()
    sandbox.aupload_file_bytes = AsyncMock(return_value=False)

    content, _, _ = await _attach(
        _request(("file", CSV_ID, "prices.csv")), [_row(CSV_ID, "prices.csv", "text/csv", 4)], sandbox, content=b"a,b\n"
    )

    assert isinstance(content, str)
    assert "could not be saved where you can open it" in content


async def test_a_steer_naming_uploaded_files_waits_for_a_turn_of_its_own():
    from src.server.handlers.chat.admission_gate import steer_allowed

    assert not steer_allowed(_request(("file", CSV_ID, "prices.csv")), dispatched=False, steerable=True)
    inline = ChatRequest(
        messages=[{"role": "user", "content": "look"}],
        additional_context=[{"type": "image", "data": "data:image/png;base64,AA==", "description": "x.png"}],
    )
    assert steer_allowed(inline, dispatched=False, steerable=True)


async def test_a_history_link_keeps_a_name_with_url_characters_whole():
    key = f"attachments/{CSV_ID}/chart#1 v2?.png"
    attachments = TurnAttachments([turns._from_row(_stored(CSV_ID, "chart#1 v2?.png", "image/png", 4) | {"object_key": key}, None)])
    with patch("src.server.utils.multimodal_context.get_public_url", lambda k: f"https://cdn.invalid/{k}"):
        [entry] = await build_attachment_metadata(attachments, "thread-1")

    assert entry["url"] == f"https://cdn.invalid/attachments/{CSV_ID}/chart%231%20v2%3F.png"


def test_an_empty_file_may_be_attached():
    from src.server.app.attachments import BeginAttachmentRequest

    assert BeginAttachmentRequest(name="empty.csv", size=0).size == 0


@pytest.mark.parametrize(
    ("workspace", "pulls", "refused"),
    [
        ({"user_id": USER, "computer_kind": "ssh"}, False, True),
        ({"user_id": USER, "computer_kind": "daytona"}, True, False),
        ({"user_id": USER, "computer_kind": None}, False, False),
        ({"user_id": "someone-else", "computer_kind": "ssh"}, False, False),
        (None, False, False),
    ],
    ids=["cannot-fetch", "fetches", "no-computer-yet", "not-theirs", "unknown"],
)
async def test_a_file_larger_than_the_server_copies_is_refused_for_a_computer_that_cannot_fetch_it(
    workspace, pulls, refused
):
    """Refused before the upload starts, rather than uploaded and then only
    noted to the agent as not saved."""
    from src.server.database.blob_keys import RELAY_MAX_BYTES
    from src.server.services import chat_attachments as svc

    created = AsyncMock(return_value={"part_size": 1, "part_count": 1})
    with (
        patch.object(svc, "plan_upload", MagicMock(return_value=("relay", RELAY_MAX_BYTES * 8))),
        patch.object(svc, "get_workspace", AsyncMock(return_value=workspace)),
        patch.object(svc, "kind_pulls_from_store", MagicMock(return_value=pulls)),
        patch.object(svc, "is_storage_enabled", MagicMock(return_value=False)),
        patch.object(svc.db, "create_attachment", created),
    ):
        if refused:
            with pytest.raises(svc.TooLarge, match="too large for this workspace's computer"):
                await svc.begin(USER, name="big.bin", size=RELAY_MAX_BYTES + 1, mime_type="", workspace_id="ws-1")
            created.assert_not_awaited()
        else:
            await svc.begin(USER, name="big.bin", size=RELAY_MAX_BYTES + 1, mime_type="", workspace_id="ws-1")
            created.assert_awaited_once()


def test_a_file_the_browser_sent_without_a_type_takes_the_type_its_name_implies():
    from src.server.models.chat_attachment import declared_type

    assert declared_type("", "chart.png") == "image/png"
    assert declared_type("application/octet-stream", "report.PDF") == "application/pdf"
    assert declared_type("text/csv", "odd.png") == "text/csv"
    assert declared_type("", "blob.unknownext") == "application/octet-stream"


async def test_two_inline_files_of_one_name_are_two_objects():
    data = "data:text/csv;base64," + base64.b64encode(b"a,b\n").decode()
    ctx = [{"type": "file", "data": data, "description": "same.csv"}] * 2
    attachments = await turns.resolve_turn_attachments(ctx, USER)
    keys: list[str] = []
    with (
        patch("src.server.utils.multimodal_context.is_storage_enabled", lambda: True),
        patch("src.server.utils.multimodal_context.upload_bytes", lambda key, *_a, **_k: keys.append(key) or True),
        patch("src.server.utils.multimodal_context.get_public_url", lambda k: f"https://cdn.invalid/{k}"),
    ):
        meta = await build_attachment_metadata(attachments, "thread-1")

    assert len(set(keys)) == 2
    assert meta[0]["url"] != meta[1]["url"]


async def test_files_are_stamped_sent_in_admission_and_one_discarded_meanwhile_is_missing():
    """Resolution stamps nothing: a send that fails before its run exists
    leaves its files for a discard or the sweep. The stamp goes on START's
    connection, so it commits with the run."""
    request = _request(("file", CSV_ID, "prices.csv"), ("file", XLSX_ID, "book.xlsx"))
    rows = [_row(CSV_ID, "prices.csv", "text/csv", 4), _row(XLSX_ID, "book.xlsx", "application/zip", 4)]
    stamped = AsyncMock(return_value={CSV_ID})
    with (
        patch.object(turns.db, "find_for_turn", AsyncMock(return_value=rows)),
        patch.object(turns.db, "mark_sent", stamped),
    ):
        attachments = await turns.resolve_turn_attachments(request.additional_context, USER)
        stamped.assert_not_awaited()
        conn = object()
        await turns.mark_sent(attachments, conn)

    assert sorted(stamped.await_args.args[0]) == sorted([CSV_ID, XLSX_ID])
    assert stamped.await_args.args[1] is conn
    assert [a.name for a in attachments.files] == ["prices.csv"]
    assert [m.name for m in attachments.missing] == ["book.xlsx"]


async def test_a_file_too_large_for_the_turns_computer_is_missing_with_the_reason():
    """Staged for another target, or before the composer knew one, a file the
    server would have to copy and cannot is said so before history records it."""
    big = 300 * 1024 * 1024
    request = _request(("file", CSV_ID, "big.csv"), ("file", XLSX_ID, "book.xlsx"))
    rows = [_row(CSV_ID, "big.csv", "text/csv", big), _row(XLSX_ID, "book.xlsx", "application/zip", 4)]
    takes = AsyncMock(return_value=False)
    with (
        patch.object(turns.db, "find_for_turn", AsyncMock(return_value=rows)),
        patch.object(turns, "computer_takes_large", takes),
    ):
        attachments = await turns.resolve_turn_attachments(request.additional_context, USER)
        await turns.refuse_too_large(attachments, USER, "ws-1")

    takes.assert_awaited_once_with(USER, "ws-1")
    assert [a.name for a in attachments.files] == ["book.xlsx"]
    assert [(m.name, "256 MB" in m.why) for m in attachments.missing] == [("big.csv", True)]


async def test_a_large_file_stays_for_a_computer_that_fetches_it_from_the_store():
    request = _request(("file", CSV_ID, "big.csv"))
    rows = [_row(CSV_ID, "big.csv", "text/csv", 300 * 1024 * 1024)]
    with (
        patch.object(turns.db, "find_for_turn", AsyncMock(return_value=rows)),
        patch.object(turns, "computer_takes_large", AsyncMock(return_value=True)),
    ):
        attachments = await turns.resolve_turn_attachments(request.additional_context, USER)
        await turns.refuse_too_large(attachments, USER, "ws-1")

    assert [a.name for a in attachments.files] == ["big.csv"]
    assert attachments.missing == []


async def test_a_relay_upload_starts_with_time_for_its_bytes_to_arrive():
    """Its bytes all arrive before the handler runs, so nothing moves its
    deadline meanwhile; a direct one moves its own as it signs parts."""
    from datetime import timedelta

    from src.server.services import chat_attachments as svc

    created = AsyncMock(return_value={"part_size": 1, "part_count": 1})
    size = 256 * 1024 * 1024
    with (
        patch.object(svc, "plan_upload", MagicMock(return_value=("relay", size))),
        patch.object(svc, "is_storage_enabled", MagicMock(return_value=False)),
        patch.object(svc.db, "create_attachment", created),
    ):
        await svc.begin(USER, name="big.csv", size=size, mime_type="text/csv")

    ttl = created.await_args.kwargs["ttl"]
    assert ttl >= svc.IDLE_TTL + timedelta(hours=2)
