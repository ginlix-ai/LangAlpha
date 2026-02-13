import json

import httpx
import pytest

from ptc_cli.api.client import SSEStreamClient


@pytest.mark.asyncio
async def test_list_conversations_sends_user_header():
    captured = {"headers": None, "url": None}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["headers"] = dict(request.headers)
        captured["url"] = str(request.url)
        return httpx.Response(
            200,
            json={"threads": [{"thread_id": "t1", "workspace_id": "w1"}]},
        )

    transport = httpx.MockTransport(handler)

    client = SSEStreamClient(base_url="http://test", user_id="u1")
    client.client = httpx.AsyncClient(transport=transport, timeout=1.0)

    try:
        data = await client.list_conversations(limit=10)
        assert data["threads"][0]["thread_id"] == "t1"
        assert captured["headers"]["x-user-id"] == "u1"
        assert captured["url"].startswith("http://test/api/v1/threads")
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_replay_thread_parses_sse_events():
    sse = "\n\n".join(
        [
            "\n".join(
                [
                    "id: 1",
                    "event: user_message",
                    "data: " + json.dumps({"thread_id": "t1", "content": "hi"}),
                ]
            ),
            "\n".join(
                [
                    "id: 2",
                    "event: replay_done",
                    "data: " + json.dumps({"thread_id": "t1"}),
                ]
            ),
            "",  # trailing
        ]
    )

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=sse.encode("utf-8"), headers={"content-type": "text/event-stream"})

    transport = httpx.MockTransport(handler)

    client = SSEStreamClient(base_url="http://test", user_id="u1")
    client.client = httpx.AsyncClient(transport=transport, timeout=1.0)

    try:
        events = []
        async for event_type, event_data in client.replay_thread("t1"):
            events.append((event_type, event_data))

        assert events[0][0] == "user_message"
        assert events[0][1]["content"] == "hi"
        assert events[1][0] == "replay_done"
    finally:
        await client.close()
