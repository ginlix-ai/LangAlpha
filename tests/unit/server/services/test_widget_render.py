"""The pure parts of the widget render: what the report says, which file the
page's origin serves, how a grant is kept out of the model's view, and which
grant a render is minted."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.server.services import widget_render as wr

_BASE = "http://127.0.0.1:8000/api/v1/wsfiles/g/v1.ws.123.sig/"


# --- the build on disk ---------------------------------------------------------


def test_render_dir_prefers_the_checkout_build(tmp_path, monkeypatch):
    own, fallback = tmp_path / "own", tmp_path / "opt"
    for folder in (own, fallback):
        folder.mkdir()
        (folder / "widget-render.html").write_text("<!doctype html>")
    monkeypatch.setattr(wr, "_DEFAULT_RENDER_DIR", own)
    monkeypatch.setenv("WIDGET_RENDER_DIR", str(fallback))
    assert wr.render_dir() == own


def test_render_dir_falls_back_to_the_configured_build(tmp_path, monkeypatch):
    (tmp_path / "widget-render.html").write_text("<!doctype html>")
    monkeypatch.setattr(wr, "_DEFAULT_RENDER_DIR", tmp_path / "missing")
    monkeypatch.setenv("WIDGET_RENDER_DIR", str(tmp_path))
    assert wr.render_dir() == tmp_path


def test_render_dir_is_none_without_a_build(tmp_path, monkeypatch):
    monkeypatch.setenv("WIDGET_RENDER_DIR", str(tmp_path))
    monkeypatch.setattr(wr, "_DEFAULT_RENDER_DIR", tmp_path / "missing")
    assert wr.render_dir() is None


def test_bundle_file_serves_only_inside_the_build(tmp_path):
    (tmp_path / "assets").mkdir()
    (tmp_path / "assets" / "a.js").write_text("")
    (tmp_path.parent / "secret.txt").write_text("")
    assert (
        wr.bundle_file(tmp_path, "/assets/a.js")
        == (tmp_path / "assets" / "a.js").resolve()
    )
    assert wr.bundle_file(tmp_path, "/../secret.txt") is None
    assert wr.bundle_file(tmp_path, "/%2e%2e/secret.txt") is None
    assert wr.bundle_file(tmp_path, "/") is None
    assert wr.bundle_file(tmp_path, "/memes/01.jpg") is None


@pytest.mark.asyncio
async def test_render_without_a_build_is_unavailable(monkeypatch):
    monkeypatch.setattr(wr, "render_dir", lambda: None)
    with pytest.raises(wr.WidgetRenderUnavailable):
        await wr.render_widget(
            html="<p>x</p>", data=None, workspace_id=None, user_id=None
        )


# --- what reaches the model ------------------------------------------------------


def test_redact_keeps_workspace_paths_and_drops_every_grant():
    assert wr.redact(f"<img> {_BASE}memes/01.jpg", _BASE) == "<img> memes/01.jpg"
    # A grant other than the render's own (a URL the widget hard-coded).
    assert (
        wr.redact("http://x/api/v1/wsfiles/g/w1.a.b.c.d/data/x.json", None)
        == "data/x.json"
    )
    assert wr.redact("/api/v1/wsfiles/g/v1.zz/a.png", None) == "a.png"
    # No workspace: a relative URL resolved against the render page's origin.
    assert (
        wr.redact("https://widget-render.invalid/memes/01.jpg", None) == "memes/01.jpg"
    )


@pytest.mark.parametrize(
    ("diag", "expected"),
    [
        (
            {
                "kind": "error",
                "message": "x is not defined",
                "source": "widget html",
                "line": 4,
            },
            ("errors", "x is not defined (line 4 of the widget html)"),
        ),
        (
            {
                "kind": "error",
                "message": "boom",
                "source": "https://cdn.x/lib.js",
                "line": 9,
            },
            ("errors", "boom (https://cdn.x/lib.js:9)"),
        ),
        (
            {"kind": "error", "message": "boom", "source": "about:srcdoc"},
            ("errors", "boom"),
        ),
        (
            {"kind": "blocked", "directive": "img-src", "url": f"{_BASE}a.png"},
            ("blocked", "img-src: a.png"),
        ),
        (
            {"kind": "blocked", "directive": "script-src", "url": ""},
            ("blocked", "script-src: (inline)"),
        ),
        (
            {"kind": "load-failed", "tag": "img", "url": f"{_BASE}memes/01.jpg"},
            ("load_failed", "<img> memes/01.jpg"),
        ),
        (
            {
                "kind": "load-failed",
                "tag": "fetch",
                "url": f"{_BASE}d.json",
                "status": 404,
            },
            ("load_failed", "<fetch> d.json (HTTP 404)"),
        ),
        (
            {
                "kind": "load-failed",
                "tag": "fetch",
                "url": "https://h/x",
                "failure": "net::ERR_FAILED",
            },
            ("load_failed", "<fetch> https://h/x (net::ERR_FAILED)"),
        ),
    ],
)
def test_describe_diag(diag, expected):
    assert wr.describe_diag(diag, _BASE) == expected


@pytest.mark.parametrize(
    "diag",
    [
        "boom",
        {"kind": "error", "message": ""},
        {"kind": "error", "message": 3},
        {"kind": "blocked", "directive": None, "url": "x"},
        {"kind": "load-failed", "tag": "img"},
        {"kind": "resize", "height": 3},
    ],
)
def test_describe_diag_drops_what_is_not_the_contract(diag):
    assert wr.describe_diag(diag, None) is None


def test_describe_diag_clips_long_messages():
    _, line = wr.describe_diag({"kind": "error", "message": "x" * 2000}, None)
    assert len(line) == 300 and line.endswith("…")


_INSPECTION = {
    "height": 383,
    "width": 768,
    "scrollWidth": 900,
    "textChars": 18,
    "elements": 5,
    "images": {"total": 2, "broken": 1},
    "canvases": 1,
    "svgs": 0,
    "fixedElements": 1,
    "dataKeys": ["rev.json", 3],
    "visibleText": f"see {_BASE}a.png",
}


def _report(raw, diags=(), serve_base=None):
    diagnostics = wr.Diagnostics(serve_base)
    for diag in diags:
        diagnostics.add(diag)
    return wr.build_report(wr.RenderResult.read(raw, serve_base), diagnostics)


def test_the_report_reads_the_inspection():
    report = _report(
        {"settled": True, "height": 380, "frameHeight": 380, "inspection": _INSPECTION},
        serve_base=_BASE,
    )
    seen = report.inspection
    assert (seen.height, seen.width, seen.scroll_width) == (383, 768, 900)
    assert (seen.images.total, seen.images.broken, seen.fixed_elements) == (2, 1, 1)
    assert seen.data_keys == ["rev.json"]
    assert seen.visible_text == "see a.png"
    assert report.settled and report.inspected
    assert not report.hung and not report.unsettled
    assert report.overflow == wr.Overflow(frame=768, content=900)


def test_a_field_not_of_the_contracts_shape_reads_as_nothing():
    seen = wr.Inspection.model_validate(
        {
            "height": "tall",
            "width": -3,
            "scrollWidth": float("nan"),
            "textChars": None,
            "images": "two",
            "dataKeys": "rev.json",
            "visibleText": 7,
        }
    )
    assert (seen.height, seen.width, seen.scroll_width, seen.text_chars) == (0, 0, 0, 0)
    assert (seen.images.total, seen.images.broken) == (0, 0)
    assert seen.data_keys == [] and seen.visible_text == ""


def test_a_result_that_is_not_an_object_is_no_answer():
    assert wr.RenderResult.read("done", None) is None
    assert wr.RenderResult.read(None, None) is None


def test_without_a_narrow_pass_the_phone_is_unmeasured():
    report = _report({"settled": True, "inspection": _INSPECTION, "narrow": None})
    assert report.narrow is None and report.narrow_overflow is None


def test_the_report_reads_the_narrow_pass():
    narrow = {**_INSPECTION, "width": 360, "scrollWidth": 702, "height": 640.6}
    report = _report({"settled": True, "inspection": _INSPECTION, "narrow": narrow})
    assert (report.narrow.width, report.narrow.scroll_width, report.narrow.height) == (
        360,
        702,
        640,
    )
    assert report.narrow_overflow == wr.Overflow(frame=360, content=702)
    # The main pass is what the rest of the report describes.
    assert (report.width, report.inspection.scroll_width, report.height) == (768, 900, 383)


def test_a_narrow_pass_without_a_main_answer_is_ignored():
    report = _report({"inspection": None, "narrow": {"width": 360, "scrollWidth": 900}})
    assert report.narrow is None and report.narrow_overflow is None


def test_without_an_answer_the_widget_is_hung():
    report = _report(None, [{"kind": "error", "message": "early"}])
    assert report.height is None and report.width is None
    assert report.seen.elements == 0 and report.errors == ["early"]
    assert report.hung and report.blank and not report.unsettled


def test_an_answer_past_the_cap_is_unsettled():
    report = _report({"settled": False, "inspection": _INSPECTION})
    assert report.unsettled and not report.hung


def test_an_unanswered_frame_keeps_the_reported_height():
    report = _report({"height": 240.4, "inspection": None})
    assert report.height == 240 and report.width is None


def test_diagnostics_are_deduped_and_capped_per_list():
    diags = [{"kind": "error", "message": "same"}] * 5 + [
        {"kind": "load-failed", "tag": "img", "url": f"u{i}"} for i in range(40)
    ]
    report = _report(None, diags)
    assert report.errors == ["same"]
    assert len(report.load_failed) == 20


@pytest.mark.parametrize(
    ("frame", "expected"), [(383.0, 383), (5000, 1400), (0, 150), (None, 150), (10, 40)]
)
def test_screenshot_height(frame, expected):
    assert wr.screenshot_height(wr.RenderResult.read({"frameHeight": frame}, None)) == expected
    assert wr.screenshot_height(None) == 150


# --- what the page may tell the render ------------------------------------------


@pytest.mark.asyncio
async def test_only_the_render_page_may_report_or_ask():
    # Bindings reach every frame, the widget's own included; a widget calling
    # one must not write its own report.
    page, widget_frame = MagicMock(), object()
    page.expose_binding = AsyncMock()
    observer = wr._Observer(None)
    await observer.attach(page)
    bindings = {call.args[0]: call.args[1] for call in page.expose_binding.await_args_list}

    observer.in_flight = 2
    bindings["__laWidgetDiag"]({"frame": widget_frame}, {"kind": "error", "message": "FORGED"})
    assert bindings["__laWidgetNetBusy"]({"frame": widget_frame}) == 0

    bindings["__laWidgetDiag"]({"frame": page.main_frame}, {"kind": "error", "message": "real"})
    assert bindings["__laWidgetNetBusy"]({"frame": page.main_frame}) == 2
    assert observer.diagnostics.lists["errors"] == ["real"]


# --- the grant ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_workspace_no_grant():
    assert await wr._serve_base(None, "u1") is None


@pytest.mark.asyncio
async def test_owner_gets_a_short_owner_grant():
    grant = type("G", (), {"token": "v1.tok"})()
    with (
        patch(
            "src.server.database.workspace.get_workspace_placement",
            AsyncMock(return_value={"workspace_id": "ws-1", "user_id": "u1"}),
        ),
        patch(
            "src.server.services.file_grants.mint_file_grant",
            AsyncMock(return_value=grant),
        ) as owner,
        patch(
            "src.server.services.file_grants.mint_shared_file_grant", AsyncMock()
        ) as member,
    ):
        base = await wr._serve_base("ws-1", "u1")
    owner.assert_awaited_once_with("ws-1", ttl=wr._GRANT_TTL_S)
    member.assert_not_awaited()
    assert base.endswith("/api/v1/wsfiles/g/v1.tok/")


@pytest.mark.asyncio
async def test_a_shared_workspace_with_no_user_gets_no_grant():
    # An owner's grant there would be bound to no one.
    with (
        patch(
            "src.server.database.workspace.get_workspace_placement",
            AsyncMock(return_value={"workspace_id": "ws-1", "user_id": "shared-org"}),
        ),
        patch("src.server.services.file_grants.mint_file_grant", AsyncMock()) as owner,
        patch(
            "src.server.services.file_grants.mint_shared_file_grant", AsyncMock()
        ) as member,
    ):
        assert await wr._serve_base("ws-1", None) is None
    owner.assert_not_awaited()
    member.assert_not_awaited()


@pytest.mark.asyncio
async def test_member_of_a_shared_workspace_gets_a_grant_bound_to_them():
    grant = type("G", (), {"token": "w1.tok"})()
    with (
        patch(
            "src.server.database.workspace.get_workspace_placement",
            AsyncMock(return_value={"workspace_id": "ws-1", "user_id": "shared-org"}),
        ),
        patch("src.server.services.file_grants.mint_file_grant", AsyncMock()) as owner,
        patch(
            "src.server.services.file_grants.mint_shared_file_grant",
            AsyncMock(return_value=grant),
        ) as member,
    ):
        base = await wr._serve_base("ws-1", "member-1")
    member.assert_awaited_once_with("ws-1", "member-1", ttl=wr._GRANT_TTL_S)
    owner.assert_not_awaited()
    assert base.endswith("/api/v1/wsfiles/g/w1.tok/")


@pytest.mark.asyncio
async def test_a_gone_workspace_gets_no_grant():
    with patch(
        "src.server.database.workspace.get_workspace_placement",
        AsyncMock(return_value=None),
    ):
        assert await wr._serve_base("ws-gone", "u1") is None


@pytest.mark.asyncio
async def test_workspace_files_spend_the_renders_budget(monkeypatch):
    # Each file is capped on its own, and all of them together with what the
    # render fetched from the internet.
    monkeypatch.setattr("src.config.env.PDF_RENDER_INTERNAL_BASE", "http://127.0.0.1:8000")
    base = f"{wr._FILES_ORIGIN}/api/v1/wsfiles/g/v1.tok/"
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _r: httpx.Response(200, content=b"x" * 600))
    )
    budget = wr.Budget(1000)
    own = wr._OwnRequests(Path("/nonexistent"), base, client, budget)

    async def ask(path):
        route = MagicMock(abort=AsyncMock(), fulfill=AsyncMock())
        assert await own.answer(route, MagicMock(url=f"{base}{path}", headers={}))
        return route

    first, second = await ask("a.bin"), await ask("b.bin")
    first.fulfill.assert_awaited_once()
    second.fulfill.assert_not_awaited()
    second.abort.assert_awaited_once()
    await client.aclose()
    # Cut off by the check's own cap, which the user's browser does not have:
    # noted as such, and its failed load is not reported as a failure.
    assert budget.too_large == [f"{base}b.bin"]
    diagnostics = wr.Diagnostics(base, budget.too_large)
    diagnostics.add({"kind": "load-failed", "tag": "script", "url": f"{base}b.bin"})
    diagnostics.add({"kind": "load-failed", "tag": "img", "url": f"{base}c.png"})
    assert diagnostics.too_large() == ["b.bin"]
    assert diagnostics.lists["load_failed"] == ["<img> c.png"]


def test_internal_url_maps_only_what_sits_under_the_grant(monkeypatch):
    monkeypatch.setattr("src.config.env.PDF_RENDER_INTERNAL_BASE", "http://127.0.0.1:8000")
    base = f"{wr._FILES_ORIGIN}/api/v1/wsfiles/g/v1.tok/"

    assert (
        wr.internal_url(f"{base}memes/01.jpg", base)
        == "http://127.0.0.1:8000/api/v1/wsfiles/g/v1.tok/memes/01.jpg"
    )
    assert wr.internal_url(f"{wr._FILES_ORIGIN}/api/v1/wsfiles/g/other/x.jpg", base) is None
    assert wr.internal_url("http://127.0.0.1:8000/api/v1/health", base) is None
    # A base that is not the stand-in host is never fetched from loopback.
    assert wr.internal_url("https://example.com/a/x", "https://example.com/a/") is None
