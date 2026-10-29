"""Tool result normalization middleware.

Coerces arbitrary tool results to strings for LLM compatibility and strips NUL
bytes so the content is safe to persist into Postgres TEXT/JSONB columns
downstream. A result that is already a list of content blocks is left as one.
"""
import json
import logging
from typing import Any

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage

from src.llms.attachment_payload import is_attachment_payload

logger = logging.getLogger(__name__)


def _strip_nuls(value: Any) -> Any:
    """Remove NUL bytes from every string inside a structure.

    Keys as well as values, and tuples as well as lists, because both reach
    ``json.dumps`` and both come back as an escaped NUL that JSONB rejects. A
    dict keyed by sandbox output is the realistic way in.

    Returns the original object when there was nothing to strip, so the caller
    can use identity as the "did we touch this" signal and the clean path costs
    no allocation.
    """
    if isinstance(value, str):
        return value.replace("\x00", "") if "\x00" in value else value
    if isinstance(value, dict):
        cleaned = {_strip_nuls(key): _strip_nuls(item) for key, item in value.items()}
        # The length is checked before the pairing, not as part of it. Cleaning
        # two keys that differed only by a NUL merges them, and `zip` then stops
        # at the shorter mapping and never looks at the key that vanished, so a
        # collision whose values are equal reported "nothing changed" and sent
        # the original back out with its NUL intact.
        unchanged = len(cleaned) == len(value) and all(
            new_key is old_key and new_item is old_item
            for (new_key, new_item), (old_key, old_item) in zip(
                cleaned.items(), value.items()
            )
        )
        return value if unchanged else cleaned
    if isinstance(value, (list, tuple)):
        cleaned = [_strip_nuls(item) for item in value]
        if all(new is old for new, old in zip(cleaned, value)):
            return value
        # A cleaned tuple comes back as a tuple, because it may be a dict key and
        # an unhashable replacement raises out of here, before the serialization
        # fallback can turn an awkward result into a displayable string. Rebuilt
        # as a plain tuple rather than as its own type, which would break on a
        # namedtuple; json.dumps writes any of the three as an array.
        return tuple(cleaned) if isinstance(value, tuple) else cleaned
    return value


class ToolResultNormalizationMiddleware(AgentMiddleware):
    """Normalize tool results to LLM-compatible strings and scrub NUL bytes.

    Two concerns, one chokepoint:

    1. **Type coercion** — some tools return Python objects (lists, dicts, None)
       that LLM APIs reject. OpenAI for example raises BadRequestError on array
       ToolMessage content: "Mismatch type string with value array". We coerce
       to a string here so every downstream consumer sees a uniform shape.
       A list of *content blocks* is exempt and passes through as a list: that
       is the shape the provider wants, and it is how a tool result carries an
       image or a PDF.

    2. **NUL safety** — sandbox stdout, file reads, web-fetch markdown, etc. can
       carry literal `\\x00` bytes. Postgres rejects these in TEXT (`cannot
       contain NUL`) and JSONB (`UntranslatableCharacter` on the `\\u0000`
       escape), making affected threads permanently unresumable. Stripping at
       this single point keeps the rest of the system — agent state, SSE
       events, LangSmith traces, msgpack checkpoints — clean.
    """

    def _normalize_result(self, result: Any) -> str | list:
        """Normalize a tool result to NUL-free content the provider accepts.

        Returns the block list unchanged when the result already is one, and a
        string otherwise.
        """
        # An attachment rides on the tool result, so serializing here would hand
        # the model a wall of base64 as prose instead of an image it can look
        # at. Only the arbitrary-object case below is what the coercion is for.
        if is_attachment_payload(result):
            return _strip_nuls(result)

        if isinstance(result, str):
            # A literal six-char `\u0000` in text is just text, and Postgres
            # TEXT takes it; only the raw byte has to go.
            return self._strip_raw_nuls(result)
        if result is None:
            return "[]"
        if isinstance(result, (list, dict)):
            # Cleaned before serializing, so the raw byte is gone from keys and
            # values alike and `json.dumps` has nothing left to escape. Scanning
            # the dumped string for the escape instead would be wrong: a literal
            # six-char `\u0000` in the data dumps as `\\u0000`, and cutting the
            # escape out of that leaves a dangling backslash and unparseable
            # JSON. The string branch above keeps that same literal for the same
            # reason.
            clean = _strip_nuls(result)
            if clean is not result:
                logger.warning("Stripped NUL bytes from tool result")
            try:
                return json.dumps(clean, ensure_ascii=False)
            except (TypeError, ValueError) as e:
                logger.warning(
                    f"Failed to JSON serialize tool result: {e}, falling back to str()"
                )
                return self._strip_raw_nuls(str(clean))
        return self._strip_raw_nuls(str(result))

    @staticmethod
    def _strip_raw_nuls(text: str) -> str:
        if "\x00" not in text:
            return text
        logger.warning("Stripped NUL bytes from tool result")
        return text.replace("\x00", "")

    def wrap_tool_call(self, request, handler):
        """Synchronous tool result normalizer."""
        result = handler(request)

        # Normalize ToolMessage content
        if isinstance(result, ToolMessage):
            result.content = self._normalize_result(result.content)

        return result

    async def awrap_tool_call(self, request, handler):
        """Asynchronous tool result normalizer."""
        result = await handler(request)

        # Normalize ToolMessage content
        if isinstance(result, ToolMessage):
            result.content = self._normalize_result(result.content)

        return result
