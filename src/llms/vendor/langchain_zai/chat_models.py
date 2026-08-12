"""Zhipu AI / z.ai (GLM) chat models."""

from __future__ import annotations

import json
from typing import Any, Literal, TypeAlias, cast

import openai
from langchain_core.language_models import (
    LangSmithParams,
    LanguageModelInput,
    ModelProfile,
    ModelProfileRegistry,
)
from langchain_core.messages import (
    AIMessage,
    AIMessageChunk,
    BaseMessage,
    SystemMessage,
)
from langchain_core.messages import content as lc_content
from langchain_core.messages.block_translators import register_translator
from langchain_core.messages.block_translators.openai import (
    translate_content as _openai_translate_content,
)
from langchain_core.messages.block_translators.openai import (
    translate_content_chunk as _openai_translate_content_chunk,
)
from langchain_core.outputs import ChatGenerationChunk, ChatResult
from langchain_core.prompt_values import PromptValue
from langchain_core.runnables import Runnable, RunnableLambda
from langchain_core.utils import from_env, secret_from_env
from langchain_core.utils.function_calling import convert_to_openai_tool
from langchain_openai.chat_models.base import BaseChatOpenAI
from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator
from typing_extensions import Self

from ._version import __version__
from .data._profiles import _PROFILES

DEFAULT_API_BASE = "https://api.z.ai/api/paas/v4"
"""z.ai (international) OpenAI-compatible endpoint for GLM models."""

BIGMODEL_API_BASE = "https://open.bigmodel.cn/api/paas/v4"
"""Zhipu AI (mainland China, bigmodel.cn) OpenAI-compatible endpoint for GLM models."""

_DictOrPydanticClass: TypeAlias = dict[str, Any] | type[BaseModel]
_DictOrPydantic: TypeAlias = dict[str, Any] | BaseModel

_MODEL_PROFILES = cast("ModelProfileRegistry", _PROFILES)


def _get_default_model_profile(model_name: str) -> ModelProfile:
    """Return the built-in capability profile for ``model_name`` (empty if unknown)."""
    default = _MODEL_PROFILES.get(model_name) or {}
    return default.copy()


def _json_mode_format_instructions(schema: _DictOrPydanticClass) -> str:
    """Format instructions appended in JSON mode, carrying the target schema."""
    json_schema = convert_to_openai_tool(schema)["function"]["parameters"]
    return (
        "Respond with a single JSON object that conforms to this JSON Schema:\n"
        f"{json.dumps(json_schema, ensure_ascii=False)}\n"
        "Output only the JSON object — no markdown fences, no commentary."
    )


class ChatZai(BaseChatOpenAI):
    """Zhipu AI / z.ai chat model integration for GLM models.

    Talks to GLM models over the OpenAI-compatible ``/api/paas/v4`` endpoint
    (``api.z.ai`` by default, or ``open.bigmodel.cn`` for mainland China). GLM
    returns chain-of-thought in ``reasoning_content`` (the DeepSeek protocol);
    this class captures it inbound and, unlike a plain OpenAI client, feeds it
    back out across an agent loop so the model keeps its own prior thinking.

    Reasoning preservation:
        GLM's server-side ``clear_thinking`` flag defaults to ``true``, which
        strips prior-turn ``reasoning_content`` before the model reads it (so
        glm-5.2 in particular ignores fed-back reasoning entirely). When
        ``preserve_reasoning`` is ``True`` (the default) and a prior assistant
        turn carries ``reasoning_content``, this class re-attaches it to the
        outbound request and sets ``thinking.clear_thinking=false`` so the model
        actually reads it. Set ``preserve_reasoning=False`` for stateless,
        plain-OpenAI behavior.

    Setup:
        Install ``langchain-zai`` and set the environment variable
        ``ZAI_API_KEY``.

        ```bash
        pip install -U langchain-zai
        export ZAI_API_KEY="your-api-key"
        ```

    Key init args — completion params:
        model:
            Name of the GLM model to use, e.g. ``'glm-5.2'`` or ``'glm-4.6'``.
        temperature:
            Sampling temperature.
        max_tokens:
            Max number of tokens to generate.

    Key init args — client params:
        timeout:
            Timeout for requests.
        max_retries:
            Max number of retries.
        api_key:
            Zhipu AI / z.ai API key. If not passed in, read from env var
            ``ZAI_API_KEY``.
        api_base:
            Endpoint base URL. Defaults to ``api.z.ai``; pass
            ``langchain_zai.BIGMODEL_API_BASE`` for the mainland China endpoint.

    Instantiate:
        ```python
        from langchain_zai import ChatZai

        model = ChatZai(
            model="glm-5.2",
            temperature=0,
            max_tokens=None,
            timeout=None,
            max_retries=2,
            # api_key="...",
            # other params...
        )
        ```

    Invoke:
        ```python
        messages = [
            ("system", "You are a helpful translator. Translate the user sentence to French."),
            ("human", "I love programming."),
        ]
        model.invoke(messages)
        ```

    Stream:
        ```python
        for chunk in model.stream(messages):
            print(chunk.text, end="")
        ```

    Async:
        ```python
        await model.ainvoke(messages)
        ```

    Tool calling:
        ```python
        from pydantic import BaseModel, Field


        class GetWeather(BaseModel):
            '''Get the current weather in a given location'''

            location: str = Field(..., description="The city and state, e.g. San Francisco, CA")


        model_with_tools = model.bind_tools([GetWeather])
        ai_msg = model_with_tools.invoke("What is the weather in LA?")
        ai_msg.tool_calls
        ```

    Reasoning content:
        ```python
        ai_msg = model.invoke("What is 3^3?")
        ai_msg.additional_kwargs["reasoning_content"]
        ```
    """  # noqa: E501

    model_name: str = Field(alias="model")
    """The name of the model, e.g. ``'glm-5.2'``."""
    api_key: SecretStr | None = Field(
        default_factory=secret_from_env("ZAI_API_KEY", default=None),
    )
    """Zhipu AI / z.ai API key."""
    api_base: str = Field(
        alias="base_url",
        default_factory=from_env("ZAI_API_BASE", default=DEFAULT_API_BASE),
    )
    """API base URL.

    Automatically read from env variable ``ZAI_API_BASE`` if not provided.
    Defaults to the z.ai international endpoint; use
    ``langchain_zai.BIGMODEL_API_BASE`` for mainland China.
    """
    preserve_reasoning: bool = Field(default=True)
    """Whether to round-trip GLM ``reasoning_content`` across turns.

    When ``True``, prior-turn reasoning is re-attached to outbound requests and
    ``thinking.clear_thinking=false`` is set so the model reads it. When
    ``False``, the model behaves like a stateless OpenAI-compatible client.
    """

    model_config = ConfigDict(populate_by_name=True)

    @property
    def _llm_type(self) -> str:
        """Return type of chat model."""
        return "chat-zai"

    @property
    def lc_secrets(self) -> dict[str, str]:
        """A map of constructor argument names to secret ids."""
        return {"api_key": "ZAI_API_KEY"}

    def _get_ls_params(
        self,
        stop: list[str] | None = None,
        **kwargs: Any,
    ) -> LangSmithParams:
        """Get standard LangSmith tracing params."""
        ls_params = super()._get_ls_params(stop=stop, **kwargs)
        ls_params["ls_provider"] = "zai"
        return ls_params

    @model_validator(mode="after")
    def _set_zai_version(self) -> Self:
        """Set package version in metadata.

        Named uniquely to avoid shadowing ``BaseChatOpenAI._set_openai_chat_version``;
        Pydantic replaces same-named validators rather than chaining them.
        """
        self._add_version("langchain-zai", __version__)
        return self

    @model_validator(mode="after")
    def validate_environment(self) -> Self:
        """Validate necessary environment vars and build the OpenAI clients."""
        if not (self.api_key and self.api_key.get_secret_value()):
            msg = (
                "Zhipu AI / z.ai API key not found. Set the ZAI_API_KEY "
                "environment variable or pass `api_key`. ChatZai never falls "
                "back to OPENAI_API_KEY, regardless of `api_base`."
            )
            raise ValueError(msg)
        client_params: dict = {
            k: v
            for k, v in {
                "api_key": self.api_key.get_secret_value() if self.api_key else None,
                "base_url": self.api_base,
                "timeout": self.request_timeout,
                "max_retries": self.max_retries,
                "default_headers": self.default_headers,
                "default_query": self.default_query,
            }.items()
            if v is not None
        }

        if not (self.client or None):
            sync_specific: dict = {"http_client": self.http_client}
            self.root_client = openai.OpenAI(**client_params, **sync_specific)
            self.client = self.root_client.chat.completions
        if not (self.async_client or None):
            async_specific: dict = {"http_client": self.http_async_client}
            self.root_async_client = openai.AsyncOpenAI(
                **client_params,
                **async_specific,
            )
            self.async_client = self.root_async_client.chat.completions
        return self

    def _resolve_model_profile(self) -> ModelProfile | None:
        """Return the built-in capability profile for the configured model."""
        return _get_default_model_profile(self.model_name) or None

    def _get_request_payload(
        self,
        input_: LanguageModelInput,
        *,
        stop: list[str] | None = None,
        **kwargs: Any,
    ) -> dict:
        """Build the request payload, preserving GLM reasoning across turns."""
        payload = super()._get_request_payload(input_, stop=stop, **kwargs)
        _normalize_message_content(payload.get("messages", []))
        _coerce_tool_choice(payload)

        if not self.preserve_reasoning:
            return payload

        messages = input_.to_messages() if isinstance(input_, PromptValue) else input_
        reinjected = _reinject_reasoning(messages, payload.get("messages", []))
        if reinjected:
            _disable_clear_thinking(payload)
        return payload

    def _create_chat_result(
        self,
        response: dict | openai.BaseModel,
        generation_info: dict | None = None,
    ) -> ChatResult:
        """Create a chat result, capturing GLM ``reasoning_content``."""
        rtn = super()._create_chat_result(response, generation_info)

        if not isinstance(response, openai.BaseModel):
            return rtn

        for generation in rtn.generations:
            if generation.message.response_metadata is None:
                generation.message.response_metadata = {}
            generation.message.response_metadata["model_provider"] = "zai"

        choices = getattr(response, "choices", None)
        if choices and hasattr(choices[0].message, "reasoning_content"):
            rtn.generations[0].message.additional_kwargs["reasoning_content"] = choices[
                0
            ].message.reasoning_content
        # Handle use via OpenRouter, which exposes reasoning under ``model_extra``.
        elif choices and hasattr(choices[0].message, "model_extra"):
            model_extra = choices[0].message.model_extra
            if isinstance(model_extra, dict) and (
                reasoning := model_extra.get("reasoning")
            ):
                rtn.generations[0].message.additional_kwargs["reasoning_content"] = (
                    reasoning
                )

        return rtn

    def _convert_chunk_to_generation_chunk(
        self,
        chunk: dict,
        default_chunk_class: type,
        base_generation_info: dict | None,
    ) -> ChatGenerationChunk | None:
        """Convert a streaming chunk, capturing GLM ``reasoning_content``."""
        generation_chunk = super()._convert_chunk_to_generation_chunk(
            chunk,
            default_chunk_class,
            base_generation_info,
        )
        if (choices := chunk.get("choices")) and generation_chunk:
            top = choices[0]
            if isinstance(generation_chunk.message, AIMessageChunk):
                generation_chunk.message.response_metadata = {
                    **generation_chunk.message.response_metadata,
                    "model_provider": "zai",
                }
                if (
                    reasoning_content := top.get("delta", {}).get("reasoning_content")
                ) is not None:
                    generation_chunk.message.additional_kwargs["reasoning_content"] = (
                        reasoning_content
                    )
                # Handle use via OpenRouter.
                elif (reasoning := top.get("delta", {}).get("reasoning")) is not None:
                    generation_chunk.message.additional_kwargs["reasoning_content"] = (
                        reasoning
                    )

        return generation_chunk

    def with_structured_output(
        self,
        schema: _DictOrPydanticClass | None = None,
        *,
        method: Literal[
            "function_calling",
            "json_mode",
            "json_schema",
        ] = "json_mode",
        include_raw: bool = False,
        strict: bool | None = None,
        **kwargs: Any,
    ) -> Runnable[LanguageModelInput, _DictOrPydantic]:
        """Return a runnable that produces structured output matching ``schema``.

        Defaults to GLM-native JSON mode, appending a system message that carries
        the schema (JSON mode sends none on the wire). ``json_schema`` and
        ``strict`` request server-side enforcement, which GLM's ``json_schema``
        silently lacks, so both route to ``function_calling``.
        """
        if method == "json_schema" or (method == "json_mode" and strict is not None):
            method = "function_calling"
        structured = super().with_structured_output(
            schema,
            method=method,
            include_raw=include_raw,
            strict=strict,
            **kwargs,
        )
        if method == "json_mode" and schema is not None:
            instructions = _json_mode_format_instructions(schema)

            def _append_format_instructions(
                input_: LanguageModelInput,
            ) -> list[BaseMessage]:
                return [
                    *self._convert_input(input_).to_messages(),
                    SystemMessage(content=instructions),
                ]

            structured = RunnableLambda(_append_format_instructions) | structured
        return structured


def _normalize_message_content(messages: list[dict]) -> None:
    """Coerce list-form ``tool``/``assistant`` content to strings, in place.

    The OpenAI-compatible GLM endpoint expects string content on ``tool`` and
    ``assistant`` messages, not the structured-block list form.
    """
    for message in messages:
        if message.get("role") == "tool" and isinstance(message.get("content"), list):
            parts = [
                block.get("text", "")
                if isinstance(block, dict) and block.get("type") == "text"
                else json.dumps(block, ensure_ascii=False)
                for block in message["content"]
            ]
            message["content"] = "".join(parts)
        elif message.get("role") == "assistant" and isinstance(
            message.get("content"), list
        ):
            text_parts = [
                block.get("text", "")
                for block in message["content"]
                if isinstance(block, dict) and block.get("type") == "text"
            ]
            message["content"] = "".join(text_parts) if text_parts else ""


def _coerce_tool_choice(payload: dict) -> None:
    """Coerce an unsupported named ``tool_choice`` to ``"required"``, in place.

    GLM's ``/api/paas/v4`` accepts only ``"auto"``/``"required"``/``"none"`` for
    ``tool_choice``; the OpenAI named form
    ``{"type": "function", "function": {"name": ...}}`` is rejected (error 1210).
    ``with_structured_output`` and forced ``bind_tools`` emit the named form, so
    map it to ``"required"`` and, to preserve the forcing semantics exactly,
    filter ``tools`` down to the named tool when it is present in the list. If
    the named tool is absent, ``tools`` is left untouched (defensive fallback).
    """
    tool_choice = payload.get("tool_choice")
    if not isinstance(tool_choice, dict):
        return
    payload["tool_choice"] = "required"

    function = tool_choice.get("function")
    name = function.get("name") if isinstance(function, dict) else None
    tools = payload.get("tools")
    if not name or not isinstance(tools, list):
        return
    named = [
        tool
        for tool in tools
        if isinstance(tool, dict)
        and tool.get("type") == "function"
        and isinstance(tool.get("function"), dict)
        and tool["function"].get("name") == name
    ]
    if named:
        payload["tools"] = named


def _reinject_reasoning(
    source_messages: LanguageModelInput,
    serialized_messages: list[dict],
) -> bool:
    """Re-attach captured ``reasoning_content`` onto outbound assistant messages.

    Source messages map 1:1, in order, onto the serialized payload messages.
    Returns ``True`` if any reasoning was re-injected.
    """
    if not isinstance(source_messages, list):
        return False
    reinjected = False
    for source, serialized in zip(source_messages, serialized_messages, strict=False):
        if not isinstance(source, AIMessage):
            continue
        if serialized.get("role") != "assistant":
            continue
        reasoning = source.additional_kwargs.get("reasoning_content")
        if reasoning:
            serialized["reasoning_content"] = reasoning
            reinjected = True
    return reinjected


def _disable_clear_thinking(payload: dict) -> None:
    """Set ``thinking.clear_thinking=false`` (nested) so GLM reads fed-back reasoning.

    The flag must live inside the ``thinking`` object; a top-level
    ``clear_thinking`` is silently ignored by the server. Copy-on-write:
    ``payload["extra_body"]`` may be the caller's own ``extra_body`` dict, so it
    is rebuilt rather than mutated.
    """
    extra_body = payload.get("extra_body")
    if not isinstance(extra_body, dict):
        extra_body = {}
    thinking = extra_body.get("thinking")
    if not isinstance(thinking, dict):
        thinking = {}
    payload["extra_body"] = {
        **extra_body,
        "thinking": {"type": "enabled", **thinking, "clear_thinking": False},
    }


def _zai_reasoning_block(
    message: AIMessage,
    *,
    streaming: bool,
) -> lc_content.ReasoningContentBlock | None:
    """Build a v1 ``reasoning`` block from captured ``reasoning_content``."""
    reasoning = message.additional_kwargs.get("reasoning_content")
    if not isinstance(reasoning, str) or not reasoning:
        return None
    block: dict[str, Any] = {"type": "reasoning", "reasoning": reasoning}
    if streaming:
        # A string index keeps streamed reasoning deltas merging with each other
        # while never colliding with the integer-indexed tool-call blocks. Without
        # this, the core stream reindexer puts both reasoning and the first tool
        # call at index 0, and ``merge_lists`` folds the tool call into the
        # reasoning block — dropping the tool call from ``content_blocks``.
        block["index"] = "lc_reasoning_0"
    return cast("lc_content.ReasoningContentBlock", block)


def _translate_zai_content(message: AIMessage) -> list[lc_content.ContentBlock]:
    """Translate a full GLM ``AIMessage`` to v1 content blocks (with reasoning)."""
    blocks = _openai_translate_content(message)
    if not any(b.get("type") == "reasoning" for b in blocks) and (
        block := _zai_reasoning_block(message, streaming=False)
    ):
        blocks.insert(0, block)
    return blocks


def _translate_zai_content_chunk(
    message: AIMessageChunk,
) -> list[lc_content.ContentBlock]:
    """Translate a streamed GLM ``AIMessageChunk`` to v1 content blocks.

    GLM streams ``reasoning_content`` and tool calls together; delegating tool/text
    handling to the OpenAI translator and adding the reasoning block here (with a
    non-colliding index) is what keeps streamed tool calls from being swallowed.
    """
    blocks = _openai_translate_content_chunk(message)
    if not any(b.get("type") == "reasoning" for b in blocks) and (
        block := _zai_reasoning_block(message, streaming=True)
    ):
        blocks.insert(0, block)
    return blocks


register_translator("zai", _translate_zai_content, _translate_zai_content_chunk)
