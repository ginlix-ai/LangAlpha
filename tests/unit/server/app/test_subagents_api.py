"""The Subagents REST contract the pane relies on: status codes, error shapes, and what each write leaves."""

from __future__ import annotations

import copy
from urllib.parse import quote

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.server.services.subagents import writes
from src.server.services.subagents.context import SubagentsUnavailable
from tests.conftest import create_test_app
from tests.unit.server.services.subagents._fakes import install

pytestmark = pytest.mark.asyncio

URL = "/api/v1/subagents"
STALE = "0" * 64


@pytest.fixture
def world(monkeypatch):
    world = install(monkeypatch)
    world.custom("alpha")
    world.override("research", model="model-a")
    # An override whose base is gone.
    world.override("ghost", base_key="builtin:ghost", enabled=False)
    return world


@pytest_asyncio.fixture
async def client(world):
    from src.server.app.subagents import router

    async with AsyncClient(transport=ASGITransport(app=create_test_app(router)), base_url="http://test") as c:
        yield c


async def _views(client) -> dict[str, dict]:
    resp = await client.get(URL)
    assert resp.status_code == 200
    return {v["name"]: v for v in resp.json()["subagents"]}


async def _version(client, name: str, spec: str | None) -> str | None:
    if spec == "read":
        return (await _views(client))[name]["version"]
    return STALE if spec == "stale" else None


async def test_get_lists_the_roster_then_customs_then_orphans(client):
    resp = await client.get(URL)

    assert resp.status_code == 200
    body = resp.json()
    assert [v["name"] for v in body["subagents"]] == ["research", "data-prep", "alpha", "ghost"]
    ghost = body["subagents"][-1]
    assert (ghost["kind"], ghost["base"], [p["code"] for p in ghost["problems"]]) == ("base", None, ["base_removed"])
    assert body["limits"] == {"max_custom": 20, "description_max": 400, "instructions_max_bytes": 16384}
    assert body["brokerages"] == {"connected": True}
    assert body["skills"] == ["dcf", "sec-filings"]
    assert all(len(v["version"]) == 64 for v in body["subagents"])


@pytest.mark.parametrize(
    ("plugin", "installed"),
    [
        # Its plugin is gone (an older build's uninstall left the row behind).
        ("gone-kit", False),
        # Its plugin is still there but no longer ships it.
        ("toolkit", True),
    ],
)
async def test_an_orphaned_plugin_override_says_whether_its_plugin_is_installed(monkeypatch, plugin, installed):
    from src.server.app.subagents import router

    world = install(monkeypatch)
    world.override("ghost", base_key="builtin:ghost")
    world.override("old-reader", base_key=f"plugin:{plugin}/old-reader")
    async with AsyncClient(transport=ASGITransport(app=create_test_app(router)), base_url="http://test") as c:
        views = await _views(c)

    orphan = views["old-reader"]
    assert (orphan["kind"], orphan["source"], [p["code"] for p in orphan["problems"]]) == (
        "base", "plugin", ["base_removed"],
    )
    assert orphan["plugin"] == {"name": plugin, "display_name": plugin, "installed": installed}
    assert views["ghost"]["plugin"] is None


@pytest.mark.parametrize(
    ("name", "version", "start_from", "fields", "status", "field"),
    [
        ("alpha", "stale", None, {"enabled": False}, 409, None),
        ("alpha", None, None, {"description": "Fresh."}, 409, None),
        # No new custom subagent takes a defined base's name, offered or not.
        ("research", None, None, {"description": "Fresh."}, 422, "name"),
        ("dormant", None, None, {"description": "Fresh."}, 422, "name"),
        ("filings-reader", None, None, {"description": "Fresh."}, 422, "name"),
        ("main", None, None, {"description": "Fresh."}, 422, "name"),
        ("ghost", None, None, {"description": "Fresh."}, 422, "name"),
        # An orphaned override takes only DELETE.
        ("ghost", "read", None, {"enabled": True}, 422, "name"),
        ("alpha", "read", "research", {}, 422, "start_from"),
        ("new-one", None, "nobody", {}, 422, "start_from"),
        # A custom subagent needs a description to be handed work by.
        ("new-one", None, None, {}, 422, "description"),
        ("research", "read", None, {"description": "Mine."}, 422, "description"),
    ],
)
async def test_a_refused_put_saves_nothing(client, world, name, version, start_from, fields, status, field):
    body = {"version": await _version(client, name, version), "start_from": start_from, "fields": fields}
    rows = copy.deepcopy(world.rows)

    resp = await client.put(f"{URL}/{name}", json=body)

    assert resp.status_code == status
    if status == 409:
        assert resp.json() == {"detail": {"code": "version_conflict"}}
    else:
        assert [e["field"] for e in resp.json()["detail"]["errors"]] == [field]
    assert world.rows == rows


async def test_a_422_names_each_field_with_its_message_and_options(client):
    version = await _version(client, "alpha", "read")

    resp = await client.put(f"{URL}/alpha", json={"version": version, "fields": {
        "tools": ["telepathy"], "brokerages": "all", "description": "x" * 401,
    }})

    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert set(detail) == {"errors"}
    by_field = {e["field"]: e for e in detail["errors"]}
    assert set(by_field) == {"tools", "brokerages", "description"}
    assert all(isinstance(e["message"], str) and e["message"] for e in detail["errors"])
    assert by_field["tools"]["options"] == ["web", "market_data", "code", "shell"]
    assert by_field["brokerages"]["options"] == ["off", "on"]
    # A limit has nothing to pick from.
    assert "options" not in by_field["description"]


@pytest.mark.parametrize("name", ["alpha", "research"])
async def test_a_put_returns_the_saved_view(client, name):
    version = await _version(client, name, "read")

    resp = await client.put(f"{URL}/{name}", json={"version": version, "fields": {"brokerages": "off"}})

    assert resp.status_code == 200
    assert resp.json()["brokerages"] == "off"
    # The view returned carries the version the next write sends.
    assert resp.json()["version"] == await _version(client, name, "read")


async def test_a_deployment_name_with_a_slash_can_be_tuned_and_reset(client, world):
    """Deployment config keys reach the pane verbatim, so a route must carry the
    whole name the pane encodes, slash included."""
    from tests.unit.server.services.subagents._fakes import research_base

    base = research_base(name="team/research", source="user")
    world.bases["team/research"] = type(base)("deployment:team/research", base.definition)
    world.offered = (*world.offered, "team/research")
    path = f"{URL}/{quote('team/research', safe='')}"

    resp = await client.put(path, json={"version": await _version(client, "team/research", "read"),
                                        "fields": {"brokerages": "off"}})
    assert resp.status_code == 200
    assert resp.json()["name"] == "team/research"
    assert "team/research" in world.rows

    resp = await client.delete(path, params={"version": await _version(client, "team/research", "read")})
    assert resp.status_code == 204
    assert "team/research" not in world.rows


@pytest.mark.parametrize(
    ("name", "version", "status", "row_after"),
    [
        ("alpha", "read", 204, False),
        # A base loses the user's changes: the reset.
        ("research", "read", 204, False),
        ("ghost", "read", 204, False),
        # Never edited: nothing to remove, and not an error.
        ("data-prep", "read", 204, False),
        ("nobody", "stale", 404, False),
        ("alpha", "stale", 409, True),
    ],
)
async def test_delete(client, world, name, version, status, row_after):
    resp = await client.delete(f"{URL}/{name}", params={"version": await _version(client, name, version)})

    assert resp.status_code == status
    assert (name in world.rows) is row_after
    if status == 409:
        assert resp.json() == {"detail": {"code": "version_conflict"}}


@pytest.mark.parametrize(
    ("method", "path", "kwargs"),
    [
        ("GET", URL, {}),
        ("PUT", f"{URL}/alpha", {"json": {"version": STALE, "fields": {}}}),
        ("DELETE", f"{URL}/alpha", {"params": {"version": STALE}}),
    ],
)
async def test_503_with_retry_after_while_the_agent_config_loads(client, monkeypatch, method, path, kwargs):
    async def not_loaded(user_id, *args, **kwargs):
        raise SubagentsUnavailable("agent configuration not loaded")

    monkeypatch.setattr(writes, "load_context", not_loaded)
    monkeypatch.setattr(writes, "roster", not_loaded)

    resp = await client.request(method, path, **kwargs)

    assert resp.status_code == 503
    assert resp.headers["retry-after"] == "2"


@pytest.mark.parametrize(
    ("field", "message_has"),
    [
        ("model", "x\\ud800"),
        ("description", "unpaired surrogate"),
    ],
)
async def test_a_lone_surrogate_is_a_422_not_a_500(client, world, field, message_has):
    version = await _version(client, "alpha", "read")
    rows = copy.deepcopy(world.rows)
    body = ('{"version": "%s", "fields": {"%s": "x\\ud800"}}' % (version, field)).encode()

    resp = await client.put(f"{URL}/alpha", content=body, headers={"content-type": "application/json"})

    assert resp.status_code == 422
    [error] = resp.json()["detail"]["errors"]
    assert error["field"] == field
    assert message_has in error["message"]
    assert world.rows == rows
