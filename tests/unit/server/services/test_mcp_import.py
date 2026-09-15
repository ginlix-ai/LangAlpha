"""The shared import loop: what one entry costs the entries around it.

Two things travel between entries and both are load-bearing: the name an entry
reserves only by landing, and the vault ref a literal earns. A literal that is
really a blank the user has yet to fill must travel nowhere at all.
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


def _scope(persist, *, existing_names=(), create_secret=None):
    async def _drop(conn, secret):
        return None

    return ImportScope(
        reserved_names=set(),
        existing_names=set(existing_names),
        current_count=0,
        cap=50,
        cap_message="cap",
        exists_message="exists",
        existing_secret_names=set(),
        create_secret=create_secret or _drop,
        persist=persist,
    )


def _entry(original: str, name: str, **config) -> ParsedMcpServer:
    return ParsedMcpServer(
        original_name=original,
        name=name,
        renamed=original != name,
        config={"name": name, **config},
    )


async def test_an_entry_that_never_landed_does_not_reserve_its_name():
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
