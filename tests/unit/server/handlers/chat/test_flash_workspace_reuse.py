"""Coverage for the flash-workspace reuse guard (duplicate-upsert dedup).

The route may upsert the user's shared flash workspace and hand the row to the
workflow so it doesn't repeat the upsert. The workflow trusts the row only when
its id is the caller's canonical (deterministic UUID v5) flash workspace.
"""

from src.server.handlers.chat.flash_run import _reusable_flash_workspace
from src.server.database.workspace import get_flash_workspace_id

USER = "usr-flash-001"


def test_canonical_row_is_reusable():
    canonical_id = get_flash_workspace_id(USER)
    assert _reusable_flash_workspace({"workspace_id": canonical_id}, USER) is True


def test_none_falls_back_to_upsert():
    assert _reusable_flash_workspace(None, USER) is False


def test_non_canonical_id_is_not_reused():
    # A workspace that isn't this user's deterministic flash id must NOT be
    # trusted — the workflow must upsert the canonical one instead.
    assert (
        _reusable_flash_workspace(
            {"workspace_id": "99999999-9999-9999-9999-999999999999"}, USER
        )
        is False
    )


def test_other_users_flash_id_is_not_reused():
    other_id = get_flash_workspace_id("usr-other-002")
    assert _reusable_flash_workspace({"workspace_id": other_id}, USER) is False
