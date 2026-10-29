"""A plugin's skill goes with its plugin: the list says so and DELETE holds to it.

Deleting the row by hand leaves the plugin installed and claiming a skill it no
longer delivers, until the next update writes it back.
"""

import pytest

from src.server.app import skills as skills_app


def _row(**over) -> dict:
    return {
        "name": "earnings-recap",
        "description": "Recap a quarter",
        "command": None,
        "enabled": True,
        "confirmed": True,
        "plugin_id": "6f1c1f0e-0000-4000-8000-000000000001",
        "plugin_name": "research-kit",
        "plugin_enabled": True,
        "workspace_id": None,
        **over,
    }


def test_a_plugin_skill_is_listed_as_not_deletable():
    assert skills_app._user_row_to_info(_row()).deletable is False
    assert skills_app._user_row_to_info(_row(plugin_id=None)).deletable is True


@pytest.mark.asyncio
async def test_delete_refuses_a_plugin_skill(skills_client, monkeypatch):
    deleted: list[str] = []

    async def _get(user_id, name, **_):
        return _row()

    async def _delete(user_id, name, **_):
        deleted.append(name)
        return _row()

    monkeypatch.setattr(skills_app, "reserved_skill_names", lambda: frozenset())
    monkeypatch.setattr(skills_app, "get_user_skill", _get)
    monkeypatch.setattr(skills_app, "delete_user_skill", _delete)

    resp = await skills_client.delete("/api/v1/skills/earnings-recap")

    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["code"] == "skill_plugin_owned"
    assert detail["plugin"] == "research-kit"
    assert deleted == []


@pytest.mark.asyncio
async def test_delete_spares_a_plugin_skill_that_took_the_name_after_the_read(
    skills_client, monkeypatch
):
    reads = iter([None, _row()])
    deletes: list[dict] = []

    async def _get(user_id, name, **_):
        return next(reads)

    async def _delete(user_id, name, **kwargs):
        deletes.append(kwargs)
        return None

    monkeypatch.setattr(skills_app, "reserved_skill_names", lambda: frozenset())
    monkeypatch.setattr(skills_app, "get_user_skill", _get)
    monkeypatch.setattr(skills_app, "delete_user_skill", _delete)

    resp = await skills_client.delete("/api/v1/skills/earnings-recap")

    assert resp.status_code == 409
    assert resp.json()["detail"]["code"] == "skill_plugin_owned"
    assert deletes == [{"hand_made": True}]
