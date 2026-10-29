"""The shared import loop: what one entry costs the entries around it.

Two things travel between entries and both are load-bearing: the name an entry
holds once the import takes it, and the vault ref a literal earns only by
landing. A literal that is really a blank the user has yet to fill must travel
nowhere at all.
"""

from contextlib import asynccontextmanager
from unittest.mock import patch

import pytest

from src.server.models.mcp_server import ParsedMcpServer
from src.server.services.mcp_import import ImportScope, run_mcp_import

pytestmark = pytest.mark.asyncio


class _Conn:
    @asynccontextmanager
    async def transaction(self):
        yield


@pytest.fixture(autouse=True)
def _no_db():
    @asynccontextmanager
    async def connection():
        yield _Conn()

    with patch("src.server.services.mcp_import.get_db_connection", connection):
        yield


def _scope(persist, *, existing_names=(), create_secret=None, check=None):
    async def _drop(conn, secret):
        return None

    return ImportScope(
        reserved_names=set(),
        existing_names=set(existing_names),
        cap=50,
        cap_message="cap",
        exists_message="exists",
        existing_secret_names=set(),
        create_secret=create_secret or _drop,
        persist=persist,
        check=check,
    )


def _entry(original: str, name: str, **config) -> ParsedMcpServer:
    return ParsedMcpServer(
        original_name=original,
        name=name,
        renamed=original != name,
        config={"name": name, **config},
    )


async def test_an_entry_that_fails_validation_does_not_hold_its_name():
    landed: list[str] = []

    async def persist(conn, server, entry):
        landed.append(server.name)
        return True

    # ``foo-bar`` normalizes to ``foo_bar`` and then fails validation on a
    # plain-http url; the later ``foo_bar`` is the one that gets to land.
    report = await run_mcp_import(
        [
            _entry("foo-bar", "foo_bar", transport="http", url="http://example.com/mcp"),
            _entry("foo_bar", "foo_bar", transport="stdio", command="npx"),
        ],
        scope=_scope(persist),
    )

    assert [r["status"] for r in report.results] == ["invalid", "created"]
    assert landed == ["foo_bar"]
    assert report.created == 1


async def test_a_landed_entry_still_shadows_a_later_duplicate():
    async def persist(conn, server, entry):
        return True

    report = await run_mcp_import(
        [
            _entry("foo-bar", "foo_bar", transport="stdio", command="npx"),
            _entry("foo_bar", "foo_bar", transport="stdio", command="npx"),
        ],
        scope=_scope(persist),
    )

    assert [r["status"] for r in report.results] == ["created", "skipped"]
    assert report.results[1]["reason"] == "duplicate name after normalization"


def _remote(name: str, key_value: str) -> ParsedMcpServer:
    return _entry(
        name,
        name,
        transport="http",
        url=f"https://{name}.example.com/mcp",
        headers={"X-Api-Key": key_value},
    )


async def _import_two(key_value: str):
    """Import two remote servers whose api-key header carries the same literal."""
    created: list[str] = []
    headers: dict[str, str] = {}

    async def create_secret(conn, secret):
        created.append(secret.name)

    async def persist(conn, server, entry):
        headers[server.name] = server.headers["X-Api-Key"]
        return True

    report = await run_mcp_import(
        [_remote("srv_a", key_value), _remote("srv_b", key_value)],
        scope=_scope(persist, create_secret=create_secret),
    )
    assert [r["status"] for r in report.results] == ["created", "created"]
    return created, headers


async def test_a_shared_placeholder_becomes_one_blank_per_server():
    # Every vendor's docs print `<your-api-key>`, so the same spelling twice is
    # two credentials. One shared secret would send the key filled in for the
    # first vendor to the second the moment its row was switched on.
    created, headers = await _import_two("<your-api-key>")

    assert created == ["SRV_A_X_API_KEY", "SRV_B_X_API_KEY"]
    assert headers["srv_a"] != headers["srv_b"]


async def test_one_real_token_in_two_servers_is_still_stored_once():
    created, headers = await _import_two("sk-live-9f8e7d6c5b4a3210")

    assert created == ["SRV_A_X_API_KEY"]
    assert headers["srv_a"] == headers["srv_b"] == "${vault:SRV_A_X_API_KEY}"


async def test_an_uncoerced_reserved_name_does_not_land():
    # The model checks shape only, so the loop is where every import and
    # install door refuses a name the sandbox reserves.
    landed: list[str] = []

    async def persist(conn, server, entry):
        landed.append(server.name)
        return True

    report = await run_mcp_import(
        [
            _entry("class", "class", transport="stdio", command="npx"),
            _entry("match", "match", transport="stdio", command="npx"),
        ],
        scope=_scope(persist),
    )

    assert [r["status"] for r in report.results] == ["invalid", "created"]
    assert "Python keyword" in report.results[0]["error"]
    assert landed == ["match"]


async def test_a_scope_that_does_not_check_commits_without_dialling():
    """The plugin install checks its servers together before it reaches this
    loop, so its scope carries no checker and nothing here dials again."""
    landed: list[str] = []

    async def persist(conn, server, entry):
        landed.append(server.name)
        return True

    with patch("src.server.services.mcp_validation.check_server") as check:
        report = await run_mcp_import(
            [_entry("srv", "srv", transport="http", url="https://api.example.com/mcp")],
            scope=_scope(persist),
        )

    assert landed == ["srv"]
    check.assert_not_called()
    assert report.created == 1


async def test_one_check_sees_every_entry_as_it_would_be_saved():
    """Each entry is checked with its own literals ahead of the vault, under
    the ref it would be saved with; an entry the import refuses is not dialled,
    and only what passed lands."""
    from unittest.mock import AsyncMock

    from src.server.services.mcp_validation import CheckFailure, Passed

    landed: list[str] = []

    async def persist(conn, server, entry):
        landed.append(server.name)
        return True

    def answer(items):
        return [
            CheckFailure(server.name, "credential_rejected", "bad key")
            if server.name == "srv_b"
            else Passed(server.name)
            for server, _extras in items
        ]

    check = AsyncMock(side_effect=answer)
    report = await run_mcp_import(
        [
            _remote("srv_a", "sk-live-9f8e7d6c5b4a3210"),
            _entry("class", "class", transport="stdio", command="npx"),
            _remote("srv_b", "sk-live-0a1b2c3d4e5f6789"),
        ],
        scope=_scope(persist, check=check),
    )

    check.assert_awaited_once()
    (items,) = check.await_args.args
    assert [(s.name, s.headers, dict(extras)) for s, extras in items] == [
        (
            "srv_a",
            {"X-Api-Key": "${vault:SRV_A_X_API_KEY}"},
            {"SRV_A_X_API_KEY": "sk-live-9f8e7d6c5b4a3210"},
        ),
        (
            "srv_b",
            {"X-Api-Key": "${vault:SRV_B_X_API_KEY}"},
            {"SRV_B_X_API_KEY": "sk-live-0a1b2c3d4e5f6789"},
        ),
    ]
    assert [r["status"] for r in report.results] == ["created", "invalid", "check_failed"]
    assert landed == ["srv_a"]


async def test_only_what_the_account_has_room_for_is_checked():
    """The check dials out, so a blob of thousands must not become thousands
    of probes. An entry the import did not take is never checked and never
    lands, even when one it took fails and leaves its name or room unused."""
    from unittest.mock import AsyncMock

    from src.server.services.mcp_validation import CheckFailure, Passed

    landed: list[str] = []

    async def persist(conn, server, entry):
        landed.append(server.name)
        return True

    def answer(items):
        return [
            CheckFailure(server.name, "unreachable", "down")
            if server.name == "srv_a"
            else Passed(server.name)
            for server, _extras in items
        ]

    check = AsyncMock(side_effect=answer)
    report = await run_mcp_import(
        [
            _remote("srv_a", "sk-live-9f8e7d6c5b4a3210"),
            _remote("srv_a", "sk-live-0a1b2c3d4e5f6789"),
            _remote("srv_b", "sk-live-1a2b3c4d5e6f7089"),
            *(_remote(f"srv_{i}", "sk-live-0f1e2d3c4b5a6978") for i in range(20)),
        ],
        scope=_scope(persist, existing_names=[f"held_{i}" for i in range(48)], check=check),
    )

    (items,) = check.await_args.args
    assert [server.name for server, _extras in items] == ["srv_a", "srv_b"]
    assert [r["status"] for r in report.results[:3]] == [
        "check_failed", "skipped", "created"
    ]
    assert {r["status"] for r in report.results[3:]} == {"error"}
    assert landed == ["srv_b"]


async def test_headers_that_are_not_an_object_are_one_invalid_entry():
    landed: list[str] = []

    async def persist(conn, server, entry):
        landed.append(server.name)
        return True

    report = await run_mcp_import(
        [
            _entry("good", "good", transport="http", url="https://good.example.com/mcp"),
            _entry(
                "bad",
                "bad",
                transport="http",
                url="https://bad.example.com/mcp",
                headers=["Authorization: Bearer x"],
            ),
        ],
        scope=_scope(persist),
    )

    assert [r["status"] for r in report.results] == ["created", "invalid"]
    assert "headers" in report.results[1]["error"]
    assert landed == ["good"]
