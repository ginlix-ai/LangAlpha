"""Which model a send runs, and what the thread keeps, in ``_handle_send_message``.

One thread read authorizes the send and gives the model the thread holds; the
named and held models go to ``thread_model.turn_model`` for one decision, and
only a named model is handed to the run generator to keep on the thread. Retry
and HITL resume reach the same handler.
"""

from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from ptc_agent.config.agent import CredentialSource
from tests.conftest import create_test_app
from src.server.services.llm.thread_model import NamedModel

THREADS_MOD = "src.server.app.threads.messaging"
THREAD_MODEL_MOD = "src.server.services.llm.thread_model"
USER = "usr-caller-001"


def _make_config():
    cfg = MagicMock()
    cfg.credential_source = CredentialSource.PLATFORM
    cfg.llm_client = None
    cfg.llm = MagicMock()
    cfg.llm.name = "claude-sonnet-placeholder"
    return cfg


def _empty_async_gen(*_args, **_kwargs):
    async def _gen():
        if False:
            yield ""

    return _gen()


def _app():
    from src.server.app.threads import router
    from src.server.dependencies.usage_limits import ChatAuthResult, enforce_chat_limit

    app = create_test_app(router)
    app.dependency_overrides[enforce_chat_limit] = lambda: ChatAuthResult(
        user_id=USER, access_tier=0
    )
    return app


@contextmanager
def _send_stubs(*, owner_id, stored_model=None, turn_model=None):
    """Everything past the route stubbed; yields the mocks a test asserts on.

    ``turn_model`` is what ``thread_model.turn_model`` answers.
    """
    from src.server.app import setup as setup_module

    wm_singleton = MagicMock()
    wm_singleton.has_ready_session.return_value = True
    meta = (
        {
            "user_id": owner_id,
            "is_shared": False,
            "msg_type": "ptc",
            "workspace_id": "ws-placeholder",
            "llm_model": stored_model,
        }
        if owner_id is not None
        else None
    )
    mocks = {
        "meta": AsyncMock(return_value=meta),
        "turn_model": AsyncMock(return_value=turn_model),
        "resolve": AsyncMock(return_value=_make_config()),
        "astream": MagicMock(side_effect=_empty_async_gen),
    }

    with (
        patch(f"{THREADS_MOD}.get_thread_auth_meta", new=mocks["meta"]),
        patch(f"{THREAD_MODEL_MOD}.turn_model", new=mocks["turn_model"]),
        patch(
            "src.server.database.workspace.get_workspace",
            new=AsyncMock(return_value={"user_id": USER, "status": "running"}),
        ),
        patch.object(setup_module, "agent_config", MagicMock()),
        patch("src.server.services.llm.config.resolve_llm_config", new=mocks["resolve"]),
        patch("src.server.dependencies.usage_limits.enforce_credit_limit", new=AsyncMock()),
        patch(
            "src.server.services.workspace_manager.WorkspaceManager.get_instance",
            return_value=wm_singleton,
        ),
        patch("src.server.handlers.chat.astream_ptc_workflow", new=mocks["astream"]),
        patch(f"{THREADS_MOD}.observe_chat_stream", side_effect=lambda gen, **_: gen),
        patch("src.server.dependencies.usage_limits.release_burst_slot", new=AsyncMock()),
        patch(f"{THREADS_MOD}._assert_stream_transport_ready", new=AsyncMock()),
    ):
        yield mocks


async def _send(
    path="/api/v1/threads/tid-mine/messages",
    llm_model=None,
    workspace_id="ws-placeholder",
    headers=None,
):
    body = {
        "messages": [{"role": "user", "content": "test query"}],
        "agent_mode": "ptc",
    }
    if workspace_id is not None:
        body["workspace_id"] = workspace_id
    if llm_model is not None:
        body["llm_model"] = llm_model
    async with AsyncClient(transport=ASGITransport(app=_app()), base_url="http://test") as c:
        return await c.post(path, json=body, headers=headers)


def _resolved_model(resolve):
    # resolve_llm_config(agent_config, user_id, llm_model, is_byok, ...)
    return resolve.await_args.args[2]


@pytest.mark.asyncio
async def test_a_turn_naming_no_model_runs_the_threads_own():
    with _send_stubs(owner_id=USER, stored_model="m-pin", turn_model="m-pin") as m:
        resp = await _send()

    assert resp.status_code == 200
    m["meta"].assert_awaited_once_with("tid-mine")
    m["turn_model"].assert_awaited_once_with(USER, "tid-mine", named=None, held="m-pin")
    assert _resolved_model(m["resolve"]) == "m-pin"
    # Nothing new was named, so there is nothing to keep.
    assert m["astream"].call_args.kwargs["named_model"] is None


@pytest.mark.asyncio
async def test_the_default_runs_when_the_thread_has_no_usable_model():
    with _send_stubs(owner_id=USER, stored_model="retired", turn_model=None) as m:
        resp = await _send()

    assert resp.status_code == 200
    assert _resolved_model(m["resolve"]) is None


@pytest.mark.asyncio
async def test_a_named_model_runs_and_is_kept():
    with _send_stubs(owner_id=USER, stored_model="m-pin", turn_model="m-named") as m:
        resp = await _send(llm_model="m-named")

    assert resp.status_code == 200
    m["turn_model"].assert_awaited_once_with(
        USER, "tid-mine", named="m-named", held="m-pin"
    )
    assert _resolved_model(m["resolve"]) == "m-named"
    # Kept only while the thread still holds the model this send read.
    assert m["astream"].call_args.kwargs["named_model"] == NamedModel("m-named", seen="m-pin")


def _refusal(kind):
    from fastapi import HTTPException

    return HTTPException(status_code=400, detail={"message": "refused", "type": kind})


@pytest.mark.asyncio
async def test_a_service_turn_naming_no_model_runs_the_default_when_the_threads_cannot_run():
    """A channel turn on a thread whose account lapsed would otherwise be
    refused and dropped, with nobody there to reconnect it."""
    with (
        _send_stubs(owner_id=USER, stored_model="m-oauth", turn_model="m-oauth") as m,
        patch(f"{THREADS_MOD}._get_service_token", return_value="svc-token"),
    ):
        m["resolve"].side_effect = [_refusal("oauth_required"), _make_config()]
        resp = await _send(headers={"X-Service-Token": "svc-token"})

    assert resp.status_code == 200
    assert [call.args[2] for call in m["resolve"].await_args_list] == ["m-oauth", None]


@pytest.mark.asyncio
async def test_a_persons_send_naming_no_model_is_refused_when_the_threads_cannot_run():
    """A client names no model while it has not read the thread yet; the
    person gets the reconnect refusal, not an answer from another model."""
    with _send_stubs(owner_id=USER, stored_model="m-oauth", turn_model="m-oauth") as m:
        m["resolve"].side_effect = [_refusal("oauth_required"), _make_config()]
        resp = await _send()

    assert resp.status_code == 400
    assert resp.json()["detail"]["type"] == "oauth_required"
    assert m["resolve"].await_count == 1


@pytest.mark.asyncio
async def test_a_named_model_that_cannot_run_is_refused():
    with _send_stubs(owner_id=USER, stored_model="m-pin", turn_model="m-oauth") as m:
        m["resolve"].side_effect = _refusal("oauth_required")
        resp = await _send(llm_model="m-oauth")

    assert resp.status_code == 400
    assert m["resolve"].await_count == 1


@pytest.mark.asyncio
async def test_a_new_thread_holds_no_model():
    with _send_stubs(owner_id=None) as m:
        resp = await _send(path="/api/v1/threads/messages")

    assert resp.status_code == 200
    assert m["turn_model"].await_args.kwargs == {"named": None, "held": None}
    assert _resolved_model(m["resolve"]) is None


@pytest.mark.asyncio
async def test_the_thread_read_also_gives_the_workspace():
    with _send_stubs(owner_id=USER) as m:
        resp = await _send(workspace_id=None)

    assert resp.status_code == 200
    m["meta"].assert_awaited_once_with("tid-mine")
    assert m["resolve"].await_args.kwargs["workspace_id"] == "ws-placeholder"


@pytest.mark.asyncio
async def test_another_users_thread_is_refused_before_any_model_decision():
    with _send_stubs(owner_id="usr-other-002", stored_model="m-pin") as m:
        resp = await _send()

    assert resp.status_code == 403
    m["turn_model"].assert_not_awaited()
    m["resolve"].assert_not_awaited()


# ---------------------------------------------------------------------------
# The thread row only keeps a model once the turn is admitted
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "named_model, remembers", [(NamedModel("m-a", seen="m-pin"), True), (None, False)]
)
@pytest.mark.asyncio
async def test_keep_named_model_keeps_only_a_named_model(named_model, remembers):
    from src.server.handlers.chat import request_prep

    remember = AsyncMock(return_value=True)
    with patch.object(request_prep, "remember_thread_llm_model", new=remember):
        await request_prep.keep_named_model("t-1", named_model)

    if remembers:
        remember.assert_awaited_once_with("t-1", "m-a", seen="m-pin")
    else:
        remember.assert_not_awaited()
