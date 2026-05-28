"""Tests for the credit-gate ``is_byok`` logic in ``_handle_send_message``.

The gate reads ``config.credential_source`` (set by ``resolve_llm_config``) to
decide which enforcement path to take:
  - OAUTH or BYOK  → ``enforce_credit_limit(byok=True)``  (negative-balance only)
  - PLATFORM or NONE → ``enforce_credit_limit(byok=False)`` (daily quota check)

The regression guard: even when ``config.llm_client`` is set (non-None), a
PLATFORM credential_source must still produce ``byok=False``.  The old check
``is_byok = config.llm_client is not None`` would have produced ``byok=True``
for a platform-reasoning user whose eager client build set ``llm_client``.
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from ptc_agent.config.agent import CredentialSource
from tests.conftest import create_test_app


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_config(credential_source: CredentialSource, *, llm_client_set: bool = False):
    """Stub ``AgentConfig``-like object returned by ``resolve_llm_config``."""
    cfg = MagicMock()
    cfg.credential_source = credential_source
    # Set llm_client to a non-None object to exercise the regression guard.
    cfg.llm_client = MagicMock(name="platform-client") if llm_client_set else None
    cfg.llm = MagicMock()
    cfg.llm.name = "claude-sonnet-placeholder"
    return cfg


def _empty_async_gen():
    async def _gen():
        if False:
            yield ""

    return _gen()


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def threads_client():
    """Threads router with auth + rate-limit dependencies neutralised."""
    from src.server.app.threads import router
    from src.server.dependencies.usage_limits import ChatAuthResult, enforce_chat_limit

    app = create_test_app(router)
    app.dependency_overrides[enforce_chat_limit] = lambda: ChatAuthResult(
        user_id="usr-placeholder-001", access_tier=0
    )
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


# ---------------------------------------------------------------------------
# Parametrised gate test
# ---------------------------------------------------------------------------


_GATE_CASES: list[tuple[CredentialSource, bool, bool]] = [
    # (credential_source, llm_client_set, expected_byok)
    # PLATFORM with llm_client set — the core regression case:
    # old code would give byok=True; new code must give byok=False.
    (CredentialSource.PLATFORM, True, False),
    # NONE — no credential at all, platform path.
    (CredentialSource.NONE, False, False),
    # BYOK — user provided own key.
    (CredentialSource.BYOK, True, True),
    # OAUTH — user connected via OAuth.
    (CredentialSource.OAUTH, True, True),
]


@pytest.mark.parametrize(
    "credential_source, llm_client_set, expected_byok",
    _GATE_CASES,
    ids=[c[0].value for c in _GATE_CASES],
)
@pytest.mark.asyncio
async def test_credit_gate_byok_arg(
    threads_client,
    credential_source: CredentialSource,
    llm_client_set: bool,
    expected_byok: bool,
):
    """``enforce_credit_limit`` receives the correct ``byok`` kwarg.

    The config stub always carries the given ``credential_source``; when
    ``llm_client_set=True`` the stub also carries a non-None ``llm_client``
    to prove the old ``llm_client is not None`` heuristic would have misfired.
    """
    from src.server.app import setup as setup_module

    stub_config = _make_config(credential_source, llm_client_set=llm_client_set)

    enforce_mock = AsyncMock()
    wm_singleton = MagicMock()
    wm_singleton.has_ready_session.return_value = True

    with (
        patch.object(setup_module, "agent_config", MagicMock()),
        patch(
            "src.server.handlers.chat.resolve_llm_config",
            new=AsyncMock(return_value=stub_config),
        ),
        patch(
            "src.server.dependencies.usage_limits.enforce_credit_limit",
            new=enforce_mock,
        ),
        patch(
            "src.server.database.conversation.get_thread_by_id",
            new=AsyncMock(return_value={"workspace_id": "ws-placeholder"}),
        ),
        patch(
            "src.server.services.workspace_manager.WorkspaceManager.get_instance",
            return_value=wm_singleton,
        ),
        patch(
            "src.server.handlers.chat.astream_ptc_workflow",
            return_value=_empty_async_gen(),
        ),
        patch(
            "src.server.app.threads.observe_chat_stream",
            side_effect=lambda gen, **_: gen,
        ),
    ):
        async with threads_client.stream(
            "POST",
            "/api/v1/threads/tid-placeholder/messages",
            json={
                "workspace_id": "ws-placeholder",
                "messages": [{"role": "user", "content": "test query"}],
                "agent_mode": "ptc",
            },
        ) as resp:
            assert resp.status_code == 200

    # Verify enforce_credit_limit was called with the expected byok value.
    enforce_mock.assert_awaited_once()
    _, kwargs = enforce_mock.await_args
    actual_byok = kwargs.get("byok")
    assert actual_byok is expected_byok, (
        f"credential_source={credential_source!r} llm_client_set={llm_client_set}: "
        f"expected enforce_credit_limit(byok={expected_byok}), got byok={actual_byok!r}"
    )
