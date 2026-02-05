"""Memory Middleware - 在 LLM 调用前注入记忆，调用后保存记忆。"""

import logging
from typing import Any, Optional
from langchain_core.messages import HumanMessage, AIMessage
from langchain.agents.middleware import AgentMiddleware
from ptc_agent.agent.middleware.memory.store import ConversationMemoryStore

logger = logging.getLogger(__name__)


class MemoryMiddleware(AgentMiddleware):
    def __init__(self, user_id: str = "default", search_limit: int = 5):
        self.store = ConversationMemoryStore(user_id=user_id)
        self.search_limit = search_limit

    async def abefore_model(self, state: dict, runtime: Any, **kwargs) -> Optional[dict]:
        user_text = self._extract_last_user_text(state)
        if not user_text:
            return None
        memories = self.store.search(user_text, limit=self.search_limit)
        if not memories:
            return None
        context = self.store.format_for_prompt(memories)
        memory_msg = HumanMessage(content=f"{context}\n\n(The above is recalled context from previous conversations. Now responding to the user's current message.)")
        messages = list(state.get("messages", []))
        messages.insert(0, memory_msg)
        return {"messages": messages}

    async def aafter_model(self, state: dict, runtime: Any, **kwargs) -> Optional[dict]:
        user_text = self._extract_last_user_text(state)
        ai_text = self._extract_last_ai_text(state)
        if user_text and ai_text:
            self.store.save(user_text, ai_text)
        return None

    def _extract_last_user_text(self, state: dict) -> Optional[str]:
        for msg in reversed(state.get("messages", [])):
            if isinstance(msg, HumanMessage):
                return self._get_text_content(msg)
            if isinstance(msg, dict) and msg.get("role") == "user":
                return self._get_text_content(msg)
        return None

    def _extract_last_ai_text(self, state: dict) -> Optional[str]:
        for msg in reversed(state.get("messages", [])):
            if isinstance(msg, AIMessage):
                return self._get_text_content(msg)
            if isinstance(msg, dict) and msg.get("role") == "assistant":
                return self._get_text_content(msg)
        return None

    def _get_text_content(self, msg) -> Optional[str]:
        content = msg.content if hasattr(msg, "content") else msg.get("content", "")
        if isinstance(content, str):
            return content if content.strip() else None
        if isinstance(content, list):
            texts = [p.get("text", "") for p in content if isinstance(p, dict) and p.get("type") == "text"]
            result = " ".join(texts).strip()
            return result if result else None
        return None
