"""``get_viewer``: a public page's lenient owner check.

A visitor with an expired session, or one who arrives while the signing keys
cannot be fetched, must get the public answer rather than an error; the second
also carries the outage, since they may be the owner. Any other refusal is
still a refusal, and a live session still names its owner.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from src.server.utils import api


def _req():
    return SimpleNamespace(headers={})


def _refusing(status):
    return patch.object(
        api,
        "get_optional_user_id",
        AsyncMock(side_effect=HTTPException(status_code=status)),
    )


@pytest.mark.asyncio
async def test_an_expired_session_reads_as_anonymous():
    with _refusing(401):
        assert await api.get_viewer(_req(), None) == api.Viewer(None)


@pytest.mark.asyncio
async def test_a_credential_the_keys_cannot_check_reads_as_anonymous_but_unconfirmed():
    with _refusing(503):
        viewer = await api.get_viewer(_req(), None)
    assert viewer.user_id is None
    assert viewer.unconfirmed is not None
    assert viewer.unconfirmed.status_code == 503


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 403, 429])
async def test_any_other_refusal_still_propagates(status):
    with _refusing(status):
        with pytest.raises(HTTPException) as exc:
            await api.get_viewer(_req(), None)
    assert exc.value.status_code == status


@pytest.mark.asyncio
async def test_a_live_session_names_its_owner():
    with patch.object(
        api, "get_optional_user_id", AsyncMock(return_value="user-owner")
    ) as inner:
        assert await api.get_viewer(_req(), None) == api.Viewer("user-owner")
    inner.assert_awaited_once()
