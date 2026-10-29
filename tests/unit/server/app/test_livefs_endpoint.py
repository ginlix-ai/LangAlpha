"""The HTTP contract the sandbox file daemon is built against.

The daemon turns a refusal's ``code`` into an errno (401 and 429 by status
alone), sends back the ETag of a read as the next write's If-Match, and
counts on a provisional save's refusal staying out of the tool result. A save
runs for the conversation its command filed, and what it changed reaches the
tool result. The real tree runs over an in-memory store so each status comes
from the same route checks the file tools use; only the token row, Redis and
the automations rows are faked.
"""

from __future__ import annotations

import hashlib
import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from langgraph.store.memory import InMemoryStore

from ptc_agent.agent.backends import db_json_route
from ptc_agent.agent.backends.automations import AutomationsBackend
from ptc_agent.agent.backends.db_json_route import Plan
from ptc_agent.agent.backends.langgraph_store import MAX_CONTENT_BYTES
from ptc_agent.core.sandbox.livefs_mount import CallContext
from ptc_agent.core.sandbox.livefs_runtime.protocol import MAX_FILE_BYTES
from src.config import logging_config
from src.observability.private_query import drop_span_query
from src.server.app import livefs as livefs_app
from src.server.app import setup as setup_mod
from src.server.database import user as user_db
from src.server.database.home_workspace import get_home_workspace_id
from src.server.services.automations.file import AutomationFile, Document, FilePlan
from src.server.services.livefs import cache, outcomes, tokens
from tests.conftest import create_test_app

COMPUTER = "c0ffee00-0000-4000-8000-00000000000a"
OTHER_COMPUTER = "c0ffee00-0000-4000-8000-00000000000b"
USER = "user-livefs-endpoint-test"
ROOT = "/home/workspace"
SECRET = "unit-test-mount-secret"
TOKEN = f"lfs1.{COMPUTER}.{SECRET}"
CALL = "call0000test"

NOTES = "user/memory/notes.md"
FILE_NAME = "morning-brief.json"
AUTOMATIONS = f"user/automations/{FILE_NAME}"
SERVED = '{"name": "Morning brief", "status": "active"}\n'
ROWS_VERSION = "sha256:rows-v1"
REPORT = 'Saved morning-brief.json: updated "Morning brief": paused'
DELETED = 'Deleted "Morning brief" (morning-brief.json), with its run history'
WORKSPACE = "00000000-0000-4000-8000-00000000aaaa"
THREAD = "00000000-0000-4000-8000-00000000bbbb"

# Taken before the ``reported`` fixture fakes it.
_RECORD = outcomes.record


class _Tokens:
    """The token row ``authenticate`` compares against: one live computer."""

    async def load_token(self, computer_id):
        if computer_id != COMPUTER:
            return None
        return {
            "user_id": USER,
            "token_sha256": hashlib.sha256(SECRET.encode()).digest(),
            "expires_at": datetime.now(UTC) + timedelta(hours=1),
            "prev_token_sha256": None,
            "prev_expires_at": None,
            "root_dir": ROOT,
        }


class _NoCache:
    enabled = False
    client = None


@pytest.fixture
def reported(monkeypatch) -> list[tuple[str | None, dict]]:
    """The save outcomes filed for tool results, as (call id, outcome)."""
    filed: list[tuple[str | None, dict]] = []

    async def record(computer_id, call_id, outcome, context=None):
        filed.append((call_id, outcome))

    monkeypatch.setattr(outcomes, "record", record)
    return filed


@pytest_asyncio.fixture
async def client(monkeypatch, reported):
    monkeypatch.setattr(setup_mod, "store", InMemoryStore(), raising=False)
    monkeypatch.setattr(tokens, "db", _Tokens())
    monkeypatch.setattr(cache, "get_cache_client", lambda: _NoCache())
    # Each command filed that it runs for no conversation.
    monkeypatch.setattr(outcomes, "call_context", AsyncMock(return_value=CallContext()))
    app = create_test_app(livefs_app.router)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {TOKEN}"},
    ) as c:
        yield c


@pytest_asyncio.fixture
async def through_gzip(client):
    """The endpoint behind the app's GZip, asked as a CDN in front of it asks:
    for gzip, whatever its own client asked. Shares ``client``'s store."""
    app = create_test_app(livefs_app.router)
    app.add_middleware(setup_mod._SelectiveGZip, minimum_size=1000)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {TOKEN}", "Accept-Encoding": "gzip"},
    ) as c:
        yield c


@pytest.fixture
def automation(monkeypatch) -> MagicMock:
    """One automation's file, saved as planned and committed as nothing
    changing; its row never changes, so a save settles on what was served."""
    conn = MagicMock()

    @asynccontextmanager
    async def _open(*_, **__):
        yield conn

    conn.transaction = conn.cursor = _open
    monkeypatch.setattr(db_json_route, "get_db_connection", _open)
    file = MagicMock(spec=AutomationFile(FILE_NAME))
    file.unchanged = None
    file.fetch = AsyncMock(return_value=["row"])
    file.render = MagicMock(return_value=(SERVED, ROWS_VERSION))
    file.parse = AsyncMock(return_value=Document(fields={}, shown=None, timezone="UTC", model_pref=None, home_id=get_home_workspace_id(USER)))
    file.plan = MagicMock(return_value=Plan(FilePlan()))
    file.plan_delete = MagicMock(return_value=Plan(FilePlan()))
    file.hold = AsyncMock(return_value=None)
    file.commit = AsyncMock(return_value=REPORT)
    monkeypatch.setattr(
        AutomationsBackend, "file_named", classmethod(lambda cls, name: file if name == FILE_NAME else None)
    )
    return file


@pytest.fixture
def parse(automation) -> AsyncMock:
    """The save's context reaches the file first where it parses the
    document, which this returns."""
    return automation.parse


async def _put(client, path, body: bytes, **headers):
    return await client.put(
        "/api/v1/livefs/write",
        params={"path": path},
        content=body,
        headers={"X-Livefs-Call": CALL, **headers},
    )


async def _create(client, path, text: str) -> str:
    resp = await _put(client, path, text.encode(), **{"If-None-Match": "*"})
    assert resp.status_code == 200, resp.text
    return resp.json()["version"]


async def _read(client, path):
    return await client.get("/api/v1/livefs/read", params={"path": path})


async def _save_automations(client):
    resp = await _put(
        client, AUTOMATIONS, SERVED.encode(), **{"If-Match": f'"{ROWS_VERSION}"'}
    )
    assert resp.status_code == 200, resp.text
    return resp


async def _make_stale(client) -> str:
    """Create NOTES, change it, and return the version it had before."""
    stale = await _create(client, NOTES, "first\n")
    changed = await _put(client, NOTES, b"elsewhere\n", **{"If-Match": f'"{stale}"'})
    assert changed.status_code == 200, changed.text
    return stale


def _refusal(resp) -> tuple[int, str]:
    return resp.status_code, resp.json()["code"]


# -- versions --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_etag_a_read_returns_is_accepted_as_the_next_writes_if_match(client):
    await _create(client, NOTES, "first\n")

    read = await _read(client, NOTES)
    assert read.status_code == 200
    assert read.content == b"first\n"

    resp = await _put(client, NOTES, b"second\n", **{"If-Match": read.headers["etag"]})

    assert resp.status_code == 200, resp.text
    assert resp.json()["as_sent"] is True
    assert (await _read(client, NOTES)).headers["etag"] == f'"{resp.json()["version"]}"'


@pytest.mark.asyncio
async def test_a_save_naming_its_version_in_a_weak_tag_lands(client):
    # A CDN that re-encodes a read marks its ETag weak on the way to the daemon.
    version = await _create(client, NOTES, "first\n")

    resp = await _put(client, NOTES, b"second\n", **{"If-Match": f'W/"{version}"'})

    assert resp.status_code == 200, resp.text
    assert (await _read(client, NOTES)).content == b"second\n"


@pytest.mark.asyncio
@pytest.mark.parametrize("tag", ['""', 'W/""'])
async def test_a_tag_naming_no_version_is_refused_and_saves_nothing(client, tag):
    """Taken as no version, it would save only where no file is."""
    resp = await _put(client, NOTES, b"blind\n", **{"If-Match": tag})

    assert _refusal(resp) == (428, "precondition_required")
    assert (await _read(client, NOTES)).status_code == 404


@pytest.mark.asyncio
async def test_a_read_leaves_gzip_and_any_cdn_its_body_and_strong_etag(client, through_gzip):
    # Regression: a CDN asked for gzip and decompressed it for the daemon,
    # which asks for no encoding, so the ETag came back weak and the save
    # over it was refused as changed.
    text = "line of notes\n" * 200
    version = await _create(client, NOTES, text)

    resp = await through_gzip.get("/api/v1/livefs/read", params={"path": NOTES})

    assert resp.status_code == 200
    assert "content-encoding" not in resp.headers
    assert resp.content == text.encode()
    assert resp.headers["etag"] == f'"{version}"'
    assert "no-transform" in resp.headers["cache-control"]


@pytest.mark.parametrize(
    ("path", "exempt"),
    [
        ("/api/v1/livefs/read", True),
        ("/api/v1/livefs/list", True),
        ("/api/v1/workspaces/w/files/download", True),
        ("/api/v1/livefsx/read", False),
        ("/api/v1/threads", False),
    ],
)
def test_gzip_leaves_the_file_mount_and_downloads_alone(path, exempt):
    assert setup_mod._gzip_exempt(path) is exempt


# -- what a request's URL leaves in logs and spans ------------------------------------
# The daemon names files in the query, and an automation's file name is its name.


@pytest.fixture
def access_logger(monkeypatch) -> logging.Logger:
    """uvicorn's access logger as the app's logging setup leaves it, without
    touching the root logger's handlers or any level."""
    monkeypatch.setattr(logging_config, "_logging_configured", False)
    monkeypatch.setattr(logging_config, "get_module_log_levels", lambda: {})
    monkeypatch.setattr(logging_config, "is_sse_event_log_enabled", lambda: False)
    monkeypatch.setattr(logging, "basicConfig", lambda **_: None)
    access = logging.getLogger("uvicorn.access")
    for logger in (access, logging.getLogger("uvicorn.error")):
        monkeypatch.setattr(logger, "filters", list(logger.filters))
    logging_config.configure_logging()
    return access


@pytest.mark.parametrize(
    ("target", "logged"),
    [
        ("/api/v1/livefs/read?path=user%2Fautomations%2Fbrief.json", "/api/v1/livefs/read"),
        ("/api/v1/livefs/rename?path=a.md&to=b.md", "/api/v1/livefs/rename"),
        (
            "/api/v1/workspaces/w1/files/read?path=.agents%2Fuser%2F.%2Fautomations%2Fbrief.json",
            "/api/v1/workspaces/w1/files/read",
        ),
        ("/api/v1/workspaces/w1/files/read?path=.agents%5Cuser%5Cautomations%5Cbrief.json", "/api/v1/workspaces/w1/files/read"),
        (
            "/api/v1/workspaces/w1/files/read?path=file%3A%2F%2F%2Fhome%2Fworkspace%2F.agents%252Fuser%252Fautomations%252Fbrief.json",
            "/api/v1/workspaces/w1/files/read",
        ),
        ("/api/v1/workspaces/w1/files/read?path=report.md", "/api/v1/workspaces/w1/files/read?path=report.md"),
        ("/api/v1/threads?limit=5", "/api/v1/threads?limit=5"),
    ],
    ids=["mount", "mount-rename", "panel-automation", "panel-backslashes", "panel-file-url", "panel-file", "elsewhere"],
)
def test_the_access_log_keeps_a_mount_request_to_its_path(access_logger, target, logged):
    # The record as uvicorn logs a request.
    record = logging.LogRecord(
        "uvicorn.access", logging.INFO, __file__, 0, '%s - "%s %s HTTP/%s" %d',
        ("127.0.0.1:50000", "GET", target, "1.1", 200), None,
    )

    assert access_logger.filter(record)
    assert record.getMessage() == f'127.0.0.1:50000 - "GET {logged} HTTP/1.1" 200'


def test_the_access_log_drops_the_query_wherever_the_target_sits(access_logger):
    # A later uvicorn may log other arguments; the query goes all the same.
    record = logging.LogRecord(
        "uvicorn.access", logging.INFO, __file__, 0, '"%s %s" %s',
        ("GET", "/api/v1/livefs/read?path=user%2Fautomations%2Fbrief.json", 200), None,
    )

    assert access_logger.filter(record)
    assert record.getMessage() == '"GET /api/v1/livefs/read" 200'


def test_an_uncaught_database_error_is_logged_by_its_sqlstate(access_logger):
    import psycopg

    refused = psycopg.errors.CheckViolation("Failing row contains (Sell all TSLA).")
    record = logging.LogRecord(
        "uvicorn.error", logging.ERROR, __file__, 0, "Exception in ASGI application\n", (),
        (type(refused), refused, None),
    )

    assert logging.getLogger("uvicorn.error").filter(record)
    assert record.exc_info is None
    assert record.getMessage() == "Exception in ASGI application (CheckViolation, sqlstate 23514)"


class _Span:
    def __init__(self, attributes: dict[str, str]) -> None:
        self.attributes = attributes

    def is_recording(self) -> bool:
        return True

    def set_attribute(self, key: str, value: str) -> None:
        self.attributes[key] = value


@pytest.mark.parametrize(
    ("path", "query", "private"),
    [
        ("/api/v1/livefs/write", "path=a.md", True),
        ("/api/v1/workspaces/w1/files/read", "path=.agents%2Fuser%2Fautomations%2Fbrief.json", True),
        ("/api/v1/workspaces/w1/files/read", "path=report.md", False),
    ],
    ids=["mount", "panel-automation", "panel-file"],
)
def test_a_private_requests_span_keeps_its_url_without_the_query(path, query, private):
    sent = {"http.url": f"http://test{path}?{query}", "url.query": query}
    span = _Span(dict(sent))

    drop_span_query(span, {"path": path, "query_string": query.encode()})

    assert span.attributes == ({"http.url": f"http://test{path}", "url.query": ""} if private else sent)


@pytest.mark.asyncio
async def test_a_write_naming_a_stale_version_is_refused_as_changed(client):
    stale = await _make_stale(client)

    resp = await _put(client, NOTES, b"mine\n", **{"If-Match": f'"{stale}"'})

    assert _refusal(resp) == (412, "changed")
    assert (await _read(client, NOTES)).content == b"elsewhere\n"


@pytest.mark.asyncio
async def test_creating_a_file_that_already_exists_is_refused_as_exists(client):
    await _create(client, NOTES, "first\n")

    resp = await _put(client, NOTES, b"again\n", **{"If-None-Match": "*"})

    assert _refusal(resp) == (412, "exists")


@pytest.mark.asyncio
async def test_a_write_without_a_precondition_is_refused_and_saves_nothing(client):
    """A blind write would land over a change the daemon never saw."""
    resp = await _put(client, NOTES, b"blind\n")

    assert _refusal(resp) == (428, "precondition_required")
    assert (await _read(client, NOTES)).status_code == 404


# -- refusals ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_memo_write_is_refused_as_read_only(client):
    resp = await _put(client, "user/memo/brief.md", b"x", **{"If-None-Match": "*"})

    assert _refusal(resp) == (403, "read_only")


@pytest.mark.parametrize(
    ("path", "body"),
    [
        ("user/memory/../memo/brief.md", b"x"),
        (NOTES, b"\xff\xfe not utf-8"),
        ("user/profile/notes.txt", b"{}"),
        ("user/memory/a\x00b.md", b"x"),
        ("user/memory/a\nb.md", b"x"),
    ],
    ids=["traversal", "non-utf8", "unknown-profile-file", "nul", "control"],
)
@pytest.mark.asyncio
async def test_a_bad_path_body_or_file_name_is_refused_as_invalid(client, path, body):
    resp = await _put(client, path, body, **{"If-None-Match": "*"})

    assert _refusal(resp) == (422, "invalid")


@pytest.mark.asyncio
async def test_a_save_holding_a_nul_is_refused_with_why_and_logged_by_code(
    client, reported, caplog
):
    """Postgres refuses a NUL in text, which would answer as an outage to
    retry and quote the text in its error."""
    caplog.set_level(logging.INFO, logger=livefs_app.logger.name)

    resp = await _put(client, NOTES, b"private\x00text", **{"If-None-Match": "*"})

    assert _refusal(resp) == (422, "invalid")
    told = outcomes.describe([o for _, o in reported])
    assert "NOT SAVED" in told and "binary content (NUL bytes)" in told
    (logged,) = [r for r in caplog.records if r.getMessage() == "livefs write"]
    assert (logged.ok, logged.code) == (False, "invalid")
    # The refusal's text quotes what it refused; the tool result carries it.
    assert not hasattr(logged, "error")


@pytest.mark.asyncio
async def test_a_body_over_the_file_cap_is_refused_as_too_large(client):
    resp = await _put(
        client, NOTES, b"x" * (MAX_FILE_BYTES + 1), **{"If-None-Match": "*"}
    )

    assert _refusal(resp) == (413, "too_large")


@pytest.mark.asyncio
async def test_a_path_past_the_cap_is_refused_without_quoting_it_whole(client):
    resp = await _put(client, f"user/memory/{'a' * 5000}.md", b"x", **{"If-None-Match": "*"})

    assert _refusal(resp) == (422, "invalid")
    assert len(resp.json()["message"]) < 200


def test_the_daemon_refuses_a_write_at_the_cap_the_store_keeps():
    # A larger daemon cap accepts the write and fails it at close instead.
    assert MAX_FILE_BYTES == MAX_CONTENT_BYTES


@pytest.mark.parametrize(
    "authorization",
    [
        f"Bearer lfs1.{COMPUTER}.forged-secret",
        f"Bearer lfs1.{OTHER_COMPUTER}.{SECRET}",
        "Bearer lfs1.not-a-uuid.secret",
        "",
    ],
    ids=["forged", "moved", "malformed", "empty"],
)
@pytest.mark.asyncio
async def test_a_bad_token_is_refused_with_401(client, authorization):
    resp = await client.get(
        "/api/v1/livefs/read",
        params={"path": NOTES},
        headers={"Authorization": authorization},
    )

    assert resp.status_code == 401
    assert resp.headers["www-authenticate"] == "Bearer"


# -- save outcomes -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_refused_save_is_reported_under_its_call_id(client, reported):
    """A save through the mount fails at close() with only an errno, so the
    refusal's message reaches the agent through this report, by the sandbox
    path it used."""
    stale = await _make_stale(client)
    reported.clear()

    resp = await _put(client, NOTES, b"mine\n", **{"If-Match": f'"{stale}"'})

    assert [(call, o["op"], o["path"], o["ok"], o["error"]) for call, o in reported] == [
        (CALL, "write", f"{ROOT}/.agents/user/memory/notes.md", False, resp.json()["message"])
    ]


@pytest.mark.asyncio
async def test_a_refusal_where_nothing_is_linked_names_the_mount_path(client, reported):
    resp = await _put(client, "elsewhere/notes.md", b"x", **{"If-None-Match": "*"})

    assert _refusal(resp) == (404, "not_found")
    assert [o["path"] for _, o in reported] == ["/mnt/livefs/elsewhere/notes.md"]


@pytest.mark.asyncio
async def test_a_refused_provisional_save_is_not_reported(client, reported):
    """The empty save a shell's ``> file`` makes before its command writes;
    the daemon retries it at release, and only that retry is reported."""
    stale = await _make_stale(client)
    reported.clear()

    resp = await _put(
        client, NOTES, b"", **{"If-Match": f'"{stale}"', "X-Livefs-Provisional": "1"}
    )

    assert _refusal(resp) == (412, "changed")
    assert reported == []


@pytest.mark.asyncio
async def test_a_refused_provisional_save_logs_no_automation_name(
    client, reported, automation, caplog
):
    caplog.set_level(logging.INFO, logger=livefs_app.logger.name)

    resp = await _put(
        client,
        AUTOMATIONS,
        SERVED.encode(),
        **{"If-Match": '"sha256:stale"', "X-Livefs-Provisional": "1"},
    )

    assert resp.status_code == 412
    (logged,) = [
        r for r in caplog.records if r.getMessage() == "livefs provisional write refused"
    ]
    assert logged.path == f"{ROOT}/.agents/user/automations"
    assert not hasattr(logged, "error")
    assert reported == []


@pytest.mark.parametrize("provisional", [False, True], ids=["save", "provisional"])
@pytest.mark.asyncio
async def test_a_save_the_server_failed_on_is_reported_not_saved(
    client, reported, monkeypatch, provisional
):
    """Once answered, error or not, the daemon does not send a save again;
    only a provisional one is sent again at release."""
    monkeypatch.setattr(
        livefs_app.LivefsTree, "write", AsyncMock(side_effect=RuntimeError("store down"))
    )
    headers = {"If-None-Match": "*", **({"X-Livefs-Provisional": "1"} if provisional else {})}

    with pytest.raises(RuntimeError):
        await _put(client, NOTES, b"x", **headers)

    failed = {"op": "write", "path": f"/mnt/livefs/{NOTES}", "ok": False, "code": "unavailable", "error": "the server failed; retry"}
    assert reported == ([] if provisional else [(CALL, failed)])


@pytest.mark.asyncio
async def test_what_a_save_changed_reaches_its_outcome_but_not_the_log(
    client, reported, parse, caplog
):
    """The report names the user's automations; the tool result is its only
    reader."""
    caplog.set_level(logging.INFO, logger=livefs_app.logger.name)

    await _save_automations(client)

    assert reported == [
        (
            CALL,
            {
                "op": "write",
                "path": f"{ROOT}/.agents/{AUTOMATIONS}",
                "ok": True,
                "size": len(SERVED.encode()),
                "report": REPORT,
            },
        )
    ]
    (logged,) = [r for r in caplog.records if r.getMessage() == "livefs write"]
    assert logged.ok is True and not hasattr(logged, "report")
    # The file name is derived from the automation's name, so only the
    # folder is logged.
    assert logged.path == f"{ROOT}/.agents/user/automations"


@pytest.mark.asyncio
async def test_the_log_names_no_automation_a_move_or_refusal_touches(
    client, reported, automation, monkeypatch, caplog
):
    caplog.set_level(logging.INFO, logger=livefs_app.logger.name)
    automation.rename = AsyncMock(return_value="Renamed morning-brief.json to brief.json")
    absent = MagicMock(spec=AutomationFile("brief.json"))
    absent.fetch = AsyncMock(return_value=None)
    absent.render = MagicMock(return_value=None)
    names = {FILE_NAME: automation, "brief.json": absent}
    monkeypatch.setattr(AutomationsBackend, "file_named", classmethod(lambda cls, name: names.get(name)))

    moved = await client.post(
        "/api/v1/livefs/rename",
        params={"path": AUTOMATIONS, "to": "user/automations/brief.json"},
        headers={"X-Livefs-Call": CALL},
    )
    refused = await _put(client, AUTOMATIONS, SERVED.encode(), **{"If-Match": '"sha256:stale"'})

    assert (moved.status_code, refused.status_code) == (204, 412)
    folder = f"{ROOT}/.agents/user/automations"
    logged = [r for r in caplog.records if r.getMessage() in ("livefs rename", "livefs write")]
    assert [(r.op, r.ok, r.path, getattr(r, "from", None)) for r in logged] == [
        ("rename", True, folder, folder),
        ("write", False, folder, None),
    ]
    assert not any(hasattr(r, "error") or hasattr(r, "report") for r in logged)
    # The tool result still gets the whole outcome.
    assert reported[1][1]["path"] == f"{ROOT}/.agents/{AUTOMATIONS}"
    assert "morning-brief.json" in reported[1][1]["error"]


@pytest.mark.asyncio
async def test_a_save_that_deletes_the_automation_answers_removed_and_reports_it(
    client, reported, automation
):
    """The daemon drops the file from its listings, as after an unlink,
    rather than hold a version of a file that is gone."""
    automation.commit.return_value = DELETED
    automation.render.side_effect = [(SERVED, ROWS_VERSION), None]

    resp = await _put(
        client, AUTOMATIONS, b'{"status": "deleted"}', **{"If-Match": f'"{ROWS_VERSION}"'}
    )

    assert (resp.status_code, resp.json()) == (200, {"removed": True})
    assert reported == [
        (CALL, {"op": "write", "path": f"{ROOT}/.agents/{AUTOMATIONS}", "ok": True, "report": DELETED})
    ]


@pytest.mark.asyncio
async def test_a_save_over_an_automation_deleted_since_is_refused_as_not_found(client, reported, automation):
    automation.render.return_value = None

    resp = await _put(client, AUTOMATIONS, SERVED.encode(), **{"If-Match": f'"{ROWS_VERSION}"'})

    assert _refusal(resp) == (404, "not_found")
    assert "was deleted since it was read" in resp.json()["message"]
    automation.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_removing_an_automations_file_reports_the_delete(client, reported, automation):
    """``rm`` deletes the automation; the command's tool result says which."""
    automation.commit.return_value = DELETED

    resp = await client.post("/api/v1/livefs/delete", params={"path": AUTOMATIONS}, headers={"X-Livefs-Call": CALL})

    assert resp.status_code == 204, resp.text
    assert reported == [
        (CALL, {"op": "delete", "path": f"{ROOT}/.agents/{AUTOMATIONS}", "ok": True, "report": DELETED})
    ]


@pytest.mark.asyncio
async def test_removing_a_file_that_reports_nothing_files_no_report(client, reported):
    await _create(client, NOTES, "first\n")
    reported.clear()

    resp = await client.post("/api/v1/livefs/delete", params={"path": NOTES}, headers={"X-Livefs-Call": CALL})

    assert resp.status_code == 204, resp.text
    assert reported == [(CALL, {"op": "delete", "path": f"{ROOT}/.agents/{NOTES}", "ok": True})]


@pytest.mark.asyncio
async def test_renaming_an_automations_file_reports_the_move(client, reported, automation, monkeypatch):
    automation.rename = AsyncMock(return_value="Renamed morning-brief.json to brief.json")
    absent = MagicMock(spec=AutomationFile("brief.json"))
    absent.fetch = AsyncMock(return_value=None)
    absent.render = MagicMock(return_value=None)
    names = {FILE_NAME: automation, "brief.json": absent}
    monkeypatch.setattr(AutomationsBackend, "file_named", classmethod(lambda cls, name: names.get(name)))

    resp = await client.post(
        "/api/v1/livefs/rename",
        params={"path": AUTOMATIONS, "to": "user/automations/brief.json"},
        headers={"X-Livefs-Call": CALL},
    )

    assert resp.status_code == 204, resp.text
    [(call, outcome)] = reported
    assert (call, outcome["op"], outcome["report"]) == (CALL, "rename", "Renamed morning-brief.json to brief.json")


# -- who a save runs for -----------------------------------------------------------


@pytest.mark.parametrize("timezone", ["Asia/Tokyo", None], ids=["filed-clock", "no-clock"])
@pytest.mark.asyncio
async def test_a_save_runs_for_the_workspace_thread_and_clock_its_command_filed(
    client, parse, monkeypatch, timezone
):
    """A command that filed no clock leaves it unset, for the automations
    file to fill from the user's own before the save."""
    context = CallContext(workspace_id=WORKSPACE, thread_id=THREAD, timezone=timezone)
    filed = AsyncMock(return_value=context)
    monkeypatch.setattr(outcomes, "call_context", filed)

    await _save_automations(client)

    filed.assert_awaited_once_with(COMPUTER, CALL)
    assert parse.await_args.args[:2] == (USER, context)


@pytest.mark.asyncio
async def test_a_save_and_its_report_run_for_one_read_of_what_its_command_filed(
    client, parse, monkeypatch
):
    """A second read for the report could find the filing expired since the
    save, and hand the report to whichever conversation runs next."""
    context = CallContext(workspace_id=WORKSPACE, thread_id=THREAD)
    filed = AsyncMock(side_effect=[context, None])
    monkeypatch.setattr(outcomes, "call_context", filed)
    monkeypatch.setattr(outcomes, "record", _RECORD)
    redis = MagicMock(eval=AsyncMock(return_value=0))
    monkeypatch.setattr(outcomes, "cache", SimpleNamespace(client=lambda: redis, tag=cache.tag))

    await _save_automations(client)

    filed.assert_awaited_once_with(COMPUTER, CALL)
    assert parse.await_args.args[:2] == (USER, context)
    late_key = redis.eval.await_args.args[4]
    assert late_key == f"{cache.tag(COMPUTER)}:late:{THREAD}"


@pytest.mark.asyncio
async def test_a_save_no_command_filed_runs_and_reports_as_no_calls(
    client, parse, reported, monkeypatch
):
    """The filing expired, Redis lost it, or the sandbox's code made the id
    up, which it can do for every request: nothing is kept under it."""
    monkeypatch.setattr(outcomes, "call_context", AsyncMock(return_value=None))

    await _save_automations(client)

    assert parse.await_args.args[:2] == (USER, CallContext())
    assert [call for call, _ in reported] == [None]


@pytest.mark.asyncio
async def test_the_endpoint_never_looks_up_the_user(client, parse, monkeypatch):
    """The user's own clock is the automation files' to read, and only when
    the command filed none, so no other save waits on, or fails with, the
    users table."""
    get_user = AsyncMock(side_effect=RuntimeError("database down"))
    monkeypatch.setattr(user_db, "get_user", get_user)

    await _create(client, NOTES, "first")
    await _save_automations(client)

    get_user.assert_not_awaited()


# -- rate limit --------------------------------------------------------------------


class _DownRedis:
    enabled = True

    def __init__(self) -> None:
        self.client = self

    async def eval(self, *_):
        raise ConnectionError("redis down")

    async def get(self, *_):
        raise ConnectionError("redis down")


@pytest.mark.asyncio
async def test_the_rate_limit_fails_open_when_redis_is_down(client, monkeypatch):
    """Every file op goes through here; a Redis outage must not stop them."""
    monkeypatch.setattr(cache, "get_cache_client", lambda: _DownRedis())

    resp = await client.get("/api/v1/livefs/list", params={"path": ""})

    assert resp.status_code == 200
    assert resp.json()["structural"] is True


@pytest.mark.asyncio
async def test_a_throttled_request_runs_nothing_and_its_command_is_told(client, monkeypatch):
    """Refused before the handler, so the daemon's retry cannot land a save
    twice, and noted for the command's tool result, since the program sees
    only EAGAIN."""
    identity = tokens.LivefsIdentity(COMPUTER, USER, ROOT)
    throttle = AsyncMock(side_effect=tokens.LivefsThrottled(identity, 3))
    monkeypatch.setattr(livefs_app, "authenticate", throttle)
    noted = AsyncMock()
    monkeypatch.setattr(outcomes, "throttled", noted)

    resp = await _put(client, NOTES, b"x", **{"If-None-Match": "*"})

    assert resp.status_code == 429
    assert resp.headers["retry-after"] == "3"
    noted.assert_awaited_once_with(COMPUTER, CALL)
    monkeypatch.setattr(livefs_app, "authenticate", tokens.authenticate)
    assert (await _read(client, NOTES)).status_code == 404
