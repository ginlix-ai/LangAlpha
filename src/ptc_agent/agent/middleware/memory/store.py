"""记忆存储模块 - 基于 Mem0 的对话记忆管理。"""

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

# 模块级单例，避免 Qdrant 并发访问冲突
_shared_memory_instance = None
_shared_memory_error: Optional[str] = None


def _get_shared_memory():
    global _shared_memory_instance, _shared_memory_error
    if _shared_memory_instance is not None:
        return _shared_memory_instance
    if _shared_memory_error:
        return None
    try:
        from mem0 import Memory

        config = {
            "version": "v1.1",
            "embedder": {
                "provider": "gemini",
                "config": {
                    "model": "models/text-embedding-004",
                    "embedding_dims": 768,
                    "api_key": os.getenv("GEMINI_API_KEY"),
                },
            },
            "llm": {
                "provider": "gemini",
                "config": {
                    "model": "gemini-2.5-flash",
                    "api_key": os.getenv("GEMINI_API_KEY"),
                },
            },
            "vector_store": {
                "provider": "qdrant",
                "config": {
                    "collection_name": "ptc_memories_768",
                    "embedding_model_dims": 768,
                    "on_disk": True,
                    "path": os.path.expanduser("~/.ptc_agent/qdrant_data"),
                },
            },
        }

        _shared_memory_instance = Memory.from_config(config_dict=config)
        logger.info("[Memory] Mem0 shared instance initialized")
        return _shared_memory_instance
    except ImportError:
        _shared_memory_error = "mem0ai not installed"
        logger.warning(f"[Memory] {_shared_memory_error}")
        return None
    except Exception as e:
        _shared_memory_error = str(e)
        logger.error(f"[Memory] Failed to initialize Mem0: {e}")
        return None


class ConversationMemoryStore:
    def __init__(self, user_id: str = "default", agent_id: str = "stealth-agent"):
        self.user_id = user_id
        self.agent_id = agent_id

    def _get_memory(self):
        return _get_shared_memory()

    def save(self, user_message: str, assistant_message: str, metadata: Optional[dict] = None) -> bool:
        memory = self._get_memory()
        if memory is None:
            return False
        try:
            messages = [
                {"role": "user", "content": user_message},
                {"role": "assistant", "content": assistant_message},
            ]
            result = memory.add(messages, user_id=self.user_id, agent_id=self.agent_id, metadata=metadata or {})
            if result and "results" in result:
                for r in result["results"]:
                    event = r.get("event", "")
                    mem_text = r.get("memory", "")
                    if mem_text:
                        logger.info(f"[Memory] {event}: '{mem_text}'")
            return True
        except Exception as e:
            logger.error(f"[Memory] Save failed (non-critical): {e}")
            return False

    def search(self, query: str, limit: int = 5) -> list[str]:
        memory = self._get_memory()
        if memory is None:
            return []
        try:
            results = memory.search(query=query, user_id=self.user_id, agent_id=self.agent_id, limit=limit)
            memories = []
            for r in results.get("results", []):
                text = r.get("memory", "")
                if text:
                    memories.append(text)
            if memories:
                logger.info(f"[Memory] Search '{query[:40]}...' -> {len(memories)} hits")
            return memories
        except Exception as e:
            logger.error(f"[Memory] Search failed (non-critical): {e}")
            return []

    def format_for_prompt(self, memories: list[str]) -> str:
        if not memories:
            return ""
        lines = ["[Recalled Context from earlier conversation]"]
        for mem in memories:
            lines.append(f"- {mem}")
        lines.append("[/Recalled Context]")
        return "\n".join(lines)
