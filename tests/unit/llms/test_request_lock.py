"""Pin what every catalog model sends: its client, its graph build and its outgoing request.

The lock (``request_lock.json``) records, for each entry in the model manifest,
the chat client a turn with its own API key resolves to, the main agent's graph
build around it (tools, middleware, state schema, system prompt digest), and
the HTTP request its first call sends, with credentials and machine-specific
headers left out. A change to any of them for any model fails here, so a
branch that adds a runtime or a route proves it left every other model alone.
The tests never write the lock. For an intended, reviewed change to what a
catalog model resolves to, builds or sends, regenerate it with
``uv run python -m tests.unit.llms.test_request_lock`` (sockets refused, so
nothing leaves the machine) and commit it with the change.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode
from unittest.mock import MagicMock, patch

import aiohttp
import httpx
import pytest
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.tools import tool

try:
    import httpx2
except ImportError:  # pragma: no cover - openai's transport, absent on older pins
    httpx2 = None

LOCK_PATH = Path(__file__).with_name("request_lock.json")
MANIFEST = Path(__file__).resolve().parents[3] / "src" / "llms" / "manifest" / "models.json"
#: Credentials, and headers that name the machine's Python or SDK build or the decoders it has installed.
_DROPPED_HEADERS = ("authorization", "x-api-key", "api-key", "x-goog-api-key", "x-goog-api-client", "user-agent",
                    "content-length", "accept-encoding", "x-stainless-")
#: Query parameters that carry a credential.
_CREDENTIAL_PARAMS = ("key", "api_key", "apikey", "access_token")
#: Endpoint overrides the provider SDKs and ``src/llms`` read from the environment; a capture
#: runs without them, so a machine pointing a provider at a proxy captures the catalog's request.
_ENDPOINT_ENV = ("ANTHROPIC_API_URL", "ANTHROPIC_BASE_URL", "OPENAI_API_BASE", "OPENAI_BASE_URL", "AZURE_OPENAI_ENDPOINT",
                 "DASHSCOPE_API_BASE", "DEEPSEEK_API_BASE", "GOOGLE_GEMINI_BASE_URL", "GOOGLE_VERTEX_BASE_URL",
                 "OPENROUTER_API_BASE", "ZAI_API_BASE")


@tool
def lock_probe(symbol: str) -> str:
    """Look up a symbol."""
    return symbol


class _Captured(Exception):
    pass


def _request_of(client: Any) -> dict[str, Any] | None:
    """The first HTTP request the client sends, refused so nothing retries.

    The provider SDKs send through httpx, httpx2 (openai) or aiohttp
    (google-genai); each is stopped at its client's send, before a socket.
    """
    seen: list[dict[str, Any]] = []

    def keep(method: str, url: str, headers: Any, content: Any) -> None:
        if isinstance(content, (bytes, bytearray)):
            try:
                body: Any = json.loads(content or b"null")
            except ValueError:
                body = {"sha256": hashlib.sha256(content).hexdigest()}
        else:
            body = content
        base, _, query = url.partition("?")
        params = sorted((k, v) for k, v in parse_qsl(query) if k.lower() not in _CREDENTIAL_PARAMS)
        url = base + ("?" + urlencode(params) if params else "")
        kept = {k.lower(): str(v) for k, v in dict(headers or {}).items() if not k.lower().startswith(_DROPPED_HEADERS)}
        seen.append({"method": method.upper(), "url": url, "headers": dict(sorted(kept.items())), "body": body})

    def httpx_send(module: Any) -> Any:
        async def send(self: Any, request: Any, **kwargs: Any) -> Any:
            keep(request.method, str(request.url), request.headers, request.content)
            return module.Response(400, json={"error": {"type": "invalid_request_error", "message": "lock"}}, request=request)
        return send

    async def aiohttp_request(self: Any, method: str, url: Any, **kwargs: Any) -> Any:
        keep(method, str(url), kwargs.get("headers"), kwargs.get("json", kwargs.get("data")))
        raise _Captured

    patches = [patch.object(httpx.AsyncClient, "send", httpx_send(httpx)), patch.object(aiohttp.ClientSession, "_request", aiohttp_request)]
    if httpx2 is not None:
        patches.append(patch.object(httpx2.AsyncClient, "send", httpx_send(httpx2)))
    messages = [SystemMessage("You are a lock probe."), HumanMessage("Look up AAPL.")]
    with contextlib.ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        try:
            asyncio.run(client.bind_tools([lock_probe]).ainvoke(messages))
        except Exception:  # noqa: BLE001 - the refusal is the point
            pass
    return seen[0] if seen else None


def _graph_build_of(model_name: str, client: Any) -> dict[str, Any]:
    """The main agent's build around this model, as create_agent receives it."""
    from ptc_agent.agent.agent import PTCAgent
    from ptc_agent.config.agent import AgentConfig, LLMConfig
    from ptc_agent.config.core import DaytonaConfig, FilesystemConfig, LoggingConfig, MCPConfig, SandboxConfig, SecurityConfig

    built: list[tuple[Any, dict[str, Any]]] = []

    def capture(model: Any, **kw: Any) -> Any:
        built.append((model, kw))
        graph = MagicMock()
        graph.with_config.return_value = graph
        return graph

    config = AgentConfig(llm=LLMConfig(name=model_name), security=SecurityConfig(), logging=LoggingConfig(),
                         sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="lock")), mcp=MCPConfig(), filesystem=FilesystemConfig())
    compaction_llm = GenericFakeChatModel(messages=iter([]))
    sandbox = MagicMock()
    sandbox.workspace.return_value.workspace = "/home/workspace"
    with (
        patch.object(AgentConfig, "get_llm_client", return_value=client),
        patch("ptc_agent.agent.middleware.compaction.compact.create_llm", return_value=compaction_llm),
        patch("ptc_agent.agent.agent.create_agent", side_effect=capture),
    ):
        PTCAgent(config).create_agent(llm=client, thread_id="lock-thread", sandbox=sandbox, mcp_registry=MagicMock(),
                                      user_id="lock-user", tool_summary="")
    model, kw = built[0]
    prompt = str(kw.get("system_prompt"))
    assert "MagicMock" not in prompt, "a stub leaked into the prompt, so its digest would not be stable"
    return {
        "model_is_the_client": model is client,
        "tools": sorted(t.name for t in kw.get("tools") or []),
        "middleware": [type(m).__name__ for m in kw.get("middleware") or []],
        "state_schema": getattr(kw.get("state_schema"), "__name__", None),
        "system_prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest(),
    }


def capture(model_name: str) -> dict[str, Any]:
    from src.llms.llm import create_llm

    with patch.dict(os.environ):
        for name in _ENDPOINT_ENV:
            os.environ.pop(name, None)
        try:
            client = create_llm(model_name, api_key="sk-request-lock")
        except Exception as e:  # noqa: BLE001 - which error a model raises is part of the pin
            return {"client_error": type(e).__name__}
        return {
            "client": type(client).__name__,
            "metadata": {k: v for k, v in sorted((client.metadata or {}).items()) if k != "lc_versions"},
            "graph_build": _graph_build_of(model_name, client),
            "request": _request_of(client),
        }


def current_pins() -> dict[str, Any]:
    return {name: capture(name) for name in sorted(json.loads(MANIFEST.read_text()))}


@pytest.mark.parametrize("model_name", sorted(json.loads(MANIFEST.read_text())))
def test_every_catalog_model_resolves_builds_and_sends_what_it_did(model_name):
    pins = json.loads(LOCK_PATH.read_text())
    assert model_name in pins, "a new catalog model: run python -m tests.unit.llms.test_request_lock"
    assert capture(model_name) == pins[model_name]


def test_the_lock_names_no_model_the_catalog_dropped():
    assert set(json.loads(LOCK_PATH.read_text())) == set(json.loads(MANIFEST.read_text()))


def _refuse(self: Any, *args: Any) -> None:
    raise OSError("the request lock is captured without the network")


if __name__ == "__main__":
    import socket

    socket.socket.connect = _refuse
    pins = current_pins()
    LOCK_PATH.write_text(json.dumps(pins, indent=1, sort_keys=True, ensure_ascii=False) + "\n")
    print(f"wrote {len(pins)} pins -> {LOCK_PATH}")
