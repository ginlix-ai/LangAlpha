"""Which thread titles a page shows its reader: asked of each shared
workspace once, by workspace rather than by its owner."""

from __future__ import annotations

import pytest

from src.server.services.shared_workspaces import access

USER = "user-1"
HOLDER = "holder"


@pytest.mark.asyncio
async def test_thread_titles_ask_reach_once_per_workspace_not_per_owner(monkeypatch):
    """Two shared workspaces with one holder: reaching one says nothing of
    the other, so each is asked, and a page of one workspace asks once. Not
    a holder (another person's own workspace) is never shown."""
    found = {
        "t1": ("In reach", HOLDER, "w-shared"),
        "t2": ("Also in reach", HOLDER, "w-shared"),
        "t3": ("Out of reach", HOLDER, "w-private"),
        "t4": ("Mine", USER, "w-mine"),
        "t5": ("A stranger's", "someone", "w-theirs"),
    }
    asked: list[str] = []

    async def titled(ids, user_id):
        assert user_id == USER
        return {t: found[t] for t in ids}

    async def shared_access(workspace_id, holder_id, caller):
        asked.append(workspace_id)
        assert (holder_id, caller) == (HOLDER, USER)
        if workspace_id != "w-shared":
            raise access.NoAccess(status_code=404, detail="Workspace not found")

    monkeypatch.setattr(access.thread_reach, "titled", titled)
    monkeypatch.setattr(access, "shared_access", shared_access)
    monkeypatch.setattr(access, "is_holder", lambda owner: owner == HOLDER)
    got = await access.thread_titles(USER, ["t1", "t2", "t3", "t4", "t5", None])
    assert got == {"t1": "In reach", "t2": "Also in reach", "t4": "Mine"}
    assert sorted(asked) == ["w-private", "w-shared"]
