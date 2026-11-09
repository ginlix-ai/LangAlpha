"""A 404 from the platform means "no such report" only when the platform says
so; any other 404 is a platform without the route, which a withdrawal must
not read as a report already gone."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from src.server.services import reports


def _client(status: int, body: dict) -> AsyncMock:
    client = AsyncMock()
    client.post.return_value = httpx.Response(status, json=body)
    return client


async def _withdraw_against(status: int, body: dict) -> reports.PlatformResult:
    with patch("src.server.utils.http_client.get_http_client", AsyncMock(return_value=_client(status, body))), \
         patch("src.server.dependencies.usage_limits.service_headers", return_value={}):
        return await reports._withdraw("rep1", "u1")


@pytest.mark.asyncio
async def test_a_report_the_platform_never_got_is_already_withdrawn():
    result = await _withdraw_against(404, {"detail": {"error": "report_not_found"}})
    assert result.status == "accepted"


@pytest.mark.asyncio
async def test_a_platform_without_the_route_leaves_the_withdrawal_undone():
    result = await _withdraw_against(404, {"detail": "Not Found"})
    assert result.status == "unavailable"


@pytest.mark.asyncio
async def test_a_call_that_outlives_its_deadline_is_unavailable(monkeypatch):
    # httpx bounds each phase, not the call; the whole call is bounded too.
    import asyncio

    async def slow(*args, **kwargs):
        await asyncio.sleep(1)

    client = AsyncMock()
    client.post.side_effect = slow
    monkeypatch.setattr(reports, "_FORWARD_TIMEOUT_S", 0.05)
    with patch("src.server.utils.http_client.get_http_client", AsyncMock(return_value=client)), \
         patch("src.server.dependencies.usage_limits.service_headers", return_value={}):
        assert (await reports._withdraw("rep1", "u1")).status == "unavailable"


def test_a_capability_in_a_failed_turns_error_does_not_leave():
    from src.server.utils.secret_redactor import redact_capability_paths

    text = "fetch https://api.x/api/v1/wsfiles/g/GRANT1/a.png failed; see https://app.x/s/SHARE9"
    assert "GRANT1" not in redact_capability_paths(text) and "SHARE9" not in redact_capability_paths(text)
    assert redact_capability_paths("/chat/t/abc/assets/s/x") == "/chat/t/abc/assets/s/x"
    assert redact_capability_paths("(see https://app.x/s/SHARE9)") == "(see https://app.x/s/[redacted])"
