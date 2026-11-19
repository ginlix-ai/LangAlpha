"""Tests for ChatCodexOpenAI's request payload."""

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from src.llms.extension.codex import ChatCodexOpenAI


def _make_llm(**overrides):
    defaults = {
        "model": "gpt-5.6-sol",
        "api_key": "fake",
        "output_version": "responses/v1",
        "store": False,
    }
    defaults.update(overrides)
    return ChatCodexOpenAI(**defaults)


class TestSystemToDeveloper:
    """The system prompt goes out as a developer message where it stood, the
    shape codex-rs sends, and never in ``instructions``, which GPT-6.2 leaves
    out of the cached prefix."""

    def test_system_message_sent_as_leading_developer_message(self):
        payload = _make_llm()._get_request_payload(
            [SystemMessage(content="You are a research agent."), HumanMessage(content="Hello")]
        )

        assert "instructions" not in payload
        first, second = payload["input"]
        assert first["role"] == "developer"
        assert first["content"] == "You are a research agent."
        assert second["role"] == "user"

    def test_multiblock_system_message_keeps_its_blocks(self):
        payload = _make_llm()._get_request_payload(
            [
                SystemMessage(content=[{"type": "text", "text": "Part one."}, {"type": "text", "text": "Part two."}]),
                HumanMessage(content="Hello"),
            ]
        )

        first = payload["input"][0]
        assert first["role"] == "developer"
        assert [b["text"] for b in first["content"]] == ["Part one.", "Part two."]


class TestStatelessIdContract:
    """Pins the langchain-openai store=False behavior we depend on since
    dropping our own reasoning-id stripping: unpersisted msg_ ids are removed
    from replayed assistant items, while encrypted reasoning items are kept
    with their rs_ id. A dependency bump that regresses this breaks codex
    turn replay (400s), so it must fail here first.
    """

    def test_msg_id_dropped_encrypted_reasoning_kept(self):
        llm = _make_llm(
            model_kwargs={}, include=["reasoning.encrypted_content"]
        )
        messages = [
            HumanMessage(content="q1"),
            AIMessage(
                content=[
                    {
                        "type": "reasoning",
                        "id": "rs_abc123",
                        "summary": [],
                        "encrypted_content": "enc-blob",
                    },
                    {"type": "text", "text": "Answer one.", "id": "msg_abc123"},
                ]
            ),
            HumanMessage(content="q2"),
        ]
        payload = llm._get_request_payload(messages)

        items = [i for i in payload["input"] if isinstance(i, dict)]
        reasoning = [i for i in items if i.get("type") == "reasoning"]
        assert reasoning == [
            {
                "type": "reasoning",
                "id": "rs_abc123",
                "summary": [],
                "encrypted_content": "enc-blob",
            }
        ]
        assistant = [i for i in items if i.get("role") == "assistant"]
        assert len(assistant) == 1
        assert "id" not in assistant[0]


class TestNullOutputGuard:
    """chatgpt.com Codex backend ships response.output=null on terminal stream
    frames. langchain_openai iterates it unguarded and raises
    TypeError('NoneType' object is not iterable). Importing the codex extension
    installs a guard that coerces null output to [] before iteration.
    """

    def _null_output_response(self):
        from openai.types.responses import Response

        # Exactly what langchain's _coerce_chunk_response yields for a terminal
        # frame whose output is null (non-validating model_construct).
        return Response.model_construct(
            id="resp_test", created_at=0.0, model="gpt-5.3-codex",
            object="response", status="completed", error=None, usage=None,
            incomplete_details=None, output=None, parallel_tool_calls=False,
            tool_choice="auto", tools=[], metadata={},
        )

    def test_null_output_does_not_crash(self):
        import langchain_openai.chat_models.base as base

        # Without the guard this raises TypeError('NoneType' object is not iterable).
        result = base._construct_lc_result_from_responses_api(
            self._null_output_response()
        )
        assert result.generations[0].message.content in ("", [])

    def test_guard_installed_and_idempotent(self):
        import langchain_openai.chat_models.base as base
        from src.llms.extension.codex import _install_responses_output_guard

        fn = base._construct_lc_result_from_responses_api
        assert getattr(fn, "_codex_output_guarded", False) is True
        _install_responses_output_guard()  # re-running must be a no-op
        assert base._construct_lc_result_from_responses_api is fn
