"""记忆中间件 - 自动在 Agent 对话中注入/保存记忆。"""

import logging
from typing import Any

from langchain_core.messages import AnyMessage, HumanMessage, RemoveMessage
from langgraph.graph.message import REMOVE_ALL_MESSAGES
from langgraph.runtime import Runtime
from typing_extensions import override

from langchain.agents.middleware.types import AgentMiddleware, AgentState

from .store import ConversationMemoryStore

logger = logging.getLogger(__name__)


class MemoryMiddleware(AgentMiddleware):
    def __init__(self, user_id="default", search_limit=5, min_message_length=10):
        self.store = ConversationMemoryStore(user_id=user_id)
        self.search_limit = search_limit
        self.min_message_length = min_message_length
        self._last_user_message = ""

    @override
    async def abefore_model(self, state: AgentState, runtime: Runtime) -> dict[str, Any] | None:
        messages: list[AnyMessage] = state.get("messages", [])
        if not messages:
            return None
        user_text = self._extract_last_user_text(messages)
        if not user_text or len(user_text) < self.min_message_length:
            return None
        self._last_user_message = user_text
        memories = self.store.search(user_text, limit=self.search_limit)
        if not memories:
            return None
        memory_text = self.store.format_for_prompt(memories)
        logger.info(f"[Memory] Injecting {len(memories)} memories ({len(memory_text)} chars) into context")
        memory_msg = HumanMessage(
            content=(
                "[System Note: The following context was recalled from "
                "earlier parts of this conversation that may have been "
                "summarized. Use this information to maintain continuity.]\n\n"
                f"{memory_text}"
            ),
        )
        return {
            "messages": [
                RemoveMessage(id=REMOVE_ALL_MESSAGES),
                memory_msg,
                *messages,
            ],
        }

    @override
    def before_model(self, state: AgentState, runtime: Runtime) -> dict[str, Any] | None:
        return None

    @override
    async def aafter_model(self, state: AgentState, runtime: Runtime) -> dict[str, Any] | None:
        if not self._last_user_message:
            return None
        messages: list[AnyMessage] = state.get("messages", [])
        if not messages:
            return None
        assistant_text = self._extract_last_ai_text(messages)
        if not assistant_text:
            return None
        user_msg = self._last_user_message
        self._last_user_message = ""
        self.store.save(user_message=user_msg, assistant_message=assistant_text)
        return None

    @override
    def after_model(self, state: AgentState, runtime: Runtime) -> dict[str, Any] | None:
        return None

    def _extract_last_user_text(self, messages):
        for msg in reversed(messages):
            if msg.type == "human":
                return self._get_text_content(msg.content)
        return ""

    def _extract_last_ai_text(self, messages):
        for msg in reversed(messages):
            if msg.type == "ai":
                return self._get_text_content(msg.content)
        return ""

    @staticmethod
    def _get_text_content(content):
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            texts = []
            for item in content:
                if isinstance(item, str):
                    texts.append(item)
                elif isinstance(item, dict) and item.get("type") == "text":
                    texts.append(item.get("text", ""))
            return " ".join(texts)
        return str(content) if content else ""
