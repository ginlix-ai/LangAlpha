"""
Core Content Extraction Utilities for LLM Messages

This module provides low-level utilities for extracting text and reasoning content
from various LLM message formats. These utilities are provider-agnostic and handle:

- Different reasoning/thinking formats (OpenAI, Anthropic, standardized content_blocks)
- Text extraction from nested structures
- Metadata object filtering
- Message content extraction from LangChain messages
- JSON extraction and repair

This module is designed to be used by:
- LLM API call utilities (src/llms/api_call.py)
- Tracking utilities (src/utils/tracking/core.py)
- SSE streaming layer (src/server/content_normalizer.py)
"""

from typing import Any, Optional, Tuple, Union, List, Dict
import logging
import json
import json_repair

logger = logging.getLogger(__name__)


def extract_content_with_type(content: Any) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract text or reasoning content from LLM message formats.

    This is the core extraction function that handles different content formats
    from various LLM providers. It does NOT handle SSE-specific status signals
    (that's the responsibility of higher-level wrappers).

    Handles different content formats:
    - Plain strings: Returns as ("text", "text")
    - Dicts with text/reasoning/thinking fields: Extracts and categorizes
    - Lists with mixed content: Aggregates text and detects reasoning
    - Summary fields: Extracts from OpenAI reasoning format

    Args:
        content: Raw content from LLM (can be string, dict, list, etc.)

    Returns:
        Tuple of (text_content, content_type):
        - text_content: Plain text string if content is text-like or reasoning
        - content_type: "text" or "reasoning"

    Examples:
        >>> extract_content_with_type("Hello")
        ("Hello", "text")

        >>> extract_content_with_type({"type": "thinking", "thinking": "analysis"})
        ("analysis", "reasoning")

        >>> extract_content_with_type({"type": "reasoning", "summary": [{"text": "thought"}]})
        ("thought", "reasoning")

        >>> extract_content_with_type({"text": "response"})
        ("response", "text")

        >>> extract_content_with_type({"result": "data"})
        (None, None)
    """
    if content is None:
        return None, None

    # Handle plain strings
    if isinstance(content, str):
        return (content, "text") if content else (None, None)

    # Handle dict objects
    if isinstance(content, dict):
        # Filter out metadata-only objects
        if _is_metadata_object(content):
            return None, None

        # Handle reasoning objects with summary field
        if content.get("type") == "reasoning" and "summary" in content:
            text = _extract_text_from_summary(content["summary"])
            return (text, "reasoning") if text else (None, None)

        # Handle reasoning objects with summary field but no type (e.g., from additional_kwargs)
        if "summary" in content and "type" not in content:
            text = _extract_text_from_summary(content["summary"])
            return (text, "reasoning") if text else (None, None)

        # Handle thinking objects with thinking field (unified as "reasoning")
        if content.get("type") == "thinking" and "thinking" in content:
            thinking_text = content["thinking"]
            if thinking_text and str(thinking_text).strip():
                return thinking_text, "reasoning"
            return None, None

        # Handle simple text field
        if "text" in content:
            text = content["text"]
            return (text, "text") if text and str(text).strip() else (None, None)

        return None, None

    # Handle list of items
    if isinstance(content, list):
        text_parts = []
        has_reasoning = False  # Track if any reasoning/thinking was found

        for item in content:
            if isinstance(item, str):
                if item:
                    text_parts.append(item)
            elif isinstance(item, dict):
                # Skip metadata objects
                if _is_metadata_object(item):
                    continue

                # Handle reasoning objects
                if item.get("type") == "reasoning" and "summary" in item:
                    text = _extract_text_from_summary(item["summary"])
                    if text:
                        text_parts.append(text)
                        has_reasoning = True
                    continue

                # Handle reasoning objects with summary but no type
                if "summary" in item and "type" not in item:
                    text = _extract_text_from_summary(item["summary"])
                    if text:
                        text_parts.append(text)
                        has_reasoning = True
                    continue

                # Handle thinking objects
                if item.get("type") == "thinking" and "thinking" in item:
                    thinking = item["thinking"]
                    if thinking and str(thinking).strip():
                        text_parts.append(thinking)
                        has_reasoning = True
                    continue

                # Handle text field
                if "text" in item:
                    text = item["text"]
                    if text:
                        text_parts.append(text)
                    continue

        # Return text if found
        if text_parts:
            combined_text = "".join(text_parts)
            content_type = "reasoning" if has_reasoning else "text"
            return combined_text, content_type

        # No text found
        return None, None

    return None, None


# ============================================================================
# Helper Functions
# ============================================================================

def _is_metadata_object(obj: Any) -> bool:
    """
    Check if object is metadata-only (no content).

    Metadata objects typically have only 'id' and 'index' fields.

    Args:
        obj: Object to check

    Returns:
        True if object contains only metadata fields
    """
    if not isinstance(obj, dict):
        return False

    # Get keys excluding common metadata fields
    content_keys = set(obj.keys()) - {"id", "index"}

    # If no content keys remain, it's metadata-only
    return len(content_keys) == 0


def _extract_text_from_summary(summary: Any) -> Optional[str]:
    """
    Extract text from a summary field in reasoning objects.

    Handles format: [{"type": "summary_text", "text": "actual text"}, ...]
    This is common in OpenAI's reasoning format.

    Args:
        summary: Summary field value (typically a list)

    Returns:
        Extracted text or None if empty
    """
    if not isinstance(summary, list):
        return None

    text_parts = []
    for item in summary:
        if isinstance(item, dict):
            # Check for summary_text type
            if item.get("type") == "summary_text" and "text" in item:
                text_parts.append(item["text"])
            # Fallback to any text field
            elif "text" in item:
                text_parts.append(item["text"])

    combined_text = "\n\n".join(text_parts) if text_parts else ""

    return combined_text if combined_text else None


def extract_reasoning_summary_index(content: Any) -> Optional[int]:
    """Extract the summary_text item index from reasoning content.

    OpenAI Response API streams reasoning chunks where each ``summary_text``
    item carries an ``index`` (0, 1, 2…) identifying its reasoning "thought
    step". When that index changes across chunks (e.g. 0→1) a new reasoning
    section has started, so streaming callers insert a blank-line separator
    before it — otherwise the next section's ``**Title**`` glues onto the end
    of the previous section's prose. Returns None when the content is not
    reasoning or carries no summary index.

    Note: the top-level reasoning dict also has an ``index`` field, but that is
    the position in the content array (always 0) — NOT the thought-step index.
    """
    items = [content] if isinstance(content, dict) else (content if isinstance(content, list) else [])
    for item in items:
        if not isinstance(item, dict) or item.get("type") != "reasoning":
            continue
        summary = item.get("summary")
        if isinstance(summary, list):
            for s in summary:
                if isinstance(s, dict) and "index" in s:
                    return s["index"]
    return None


# ============================================================================
# LangChain Message Content Extraction
# ============================================================================

def get_message_content(message: Any) -> Union[str, List, None]:
    """
    Extract content from a LangChain message using the standardized content_blocks property.

    **Future-proof for langchain 1.0+**:
    LangChain 1.0 introduced .content_blocks as a standardized property that provides
    a unified format across all LLM providers (OpenAI, Anthropic, Google Gemini, etc.).

    This function:
    1. Checks if .content_blocks exists and is not empty (preferred, standardized format)
    2. Falls back to .content for backward compatibility

    Args:
        message: A LangChain message object (AIMessage, AIMessageChunk, etc.)

    Returns:
        Content in either standardized content_blocks format (list) or legacy content format (str/list/None)
    """
    # Check for content_blocks first
    if hasattr(message, 'content_blocks'):
        try:
            content_blocks = message.content_blocks
            if content_blocks and len(content_blocks) > 0:
                logger.debug(f"Using content_blocks (standardized format): {len(content_blocks)} blocks")
                return content_blocks
        except Exception as e:
            # If content_blocks fails, fall back to content
            logger.debug(f"content_blocks access failed: {e}, falling back to .content")

    # Fall back to legacy .content property
    if hasattr(message, 'content'):
        return message.content

    # Final fallback for unexpected message types
    return str(message)


# ============================================================================
# LLM Content Formatting
# ============================================================================

def format_llm_content(content: Union[str, List, None], additional_kwargs: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Normalize LLM response content to a structured dict with separated reasoning and text.

    Handles different response formats from various LLMs:
    - String content: Returns as {"reasoning": None, "text": content}
    - List content: Extracts and separates text/reasoning/thinking blocks
      - Standardized content_blocks format (LangChain 1.0+)
      - Legacy Response API format (OpenAI gpt-5 with reasoning)
      - Anthropic extended thinking format (Claude with thinking)
    - None: Returns {"reasoning": None, "text": ""}

    Supported block types:
    - "text": Main response content
    - "reasoning": OpenAI reasoning blocks and standardized format
    - "thinking": Anthropic extended thinking blocks

    Also extracts reasoning from additional_kwargs if present:
    - additional_kwargs["reasoning_content"]: Primary reasoning field
    - additional_kwargs["reasoning"]: Fallback reasoning field

    Args:
        content: Raw content from LLM response (can be from .content or .content_blocks)
        additional_kwargs: Optional dict containing additional message metadata (e.g., reasoning_content)

    Returns:
        Dict with:
        {
            "reasoning": str | None,  # Combined reasoning/thinking blocks from both content and additional_kwargs, or None
            "text": str               # Main text content
        }
    """
    # Extract reasoning and text from content
    reasoning_from_content = None
    text_content = ""

    if content is None:
        text_content = ""
    elif isinstance(content, str):
        text_content = content
    elif isinstance(content, list):
        formatted_parts = []
        reasoning_parts = []

        for item in content:
            if isinstance(item, dict):
                item_type = item.get("type", "")

                if item_type == "text":
                    # Extract main text content
                    text = item.get("text", "")
                    if text:
                        formatted_parts.append(text)

                elif item_type == "reasoning":
                    # Standardized content_blocks format: {"type": "reasoning", "reasoning": "..."}
                    reasoning_text = item.get("reasoning", "")
                    if reasoning_text:
                        reasoning_parts.append(reasoning_text)

                    # OpenAI Response API format: {"type": "reasoning", "summary": [...]}
                    summary = item.get("summary", [])
                    if summary and isinstance(summary, list) and len(summary) > 0:
                        extracted = _extract_text_from_summary(summary)
                        if extracted:
                            reasoning_parts.append(extracted)

                elif item_type == "thinking":
                    # Anthropic extended thinking format: {"type": "thinking", "thinking": "..."}
                    thinking_text = item.get("thinking", "")
                    if thinking_text:
                        reasoning_parts.append(thinking_text)

            elif isinstance(item, str):
                # Direct string in list
                formatted_parts.append(item)

        text_content = "\n".join(formatted_parts)
        reasoning_from_content = "\n".join(reasoning_parts) if reasoning_parts else None
    else:
        # Fallback for unexpected types
        text_content = str(content)

    # Extract reasoning from additional_kwargs if present
    reasoning_from_kwargs = None
    if additional_kwargs:
        # Check for reasoning_content (primary field)
        reasoning_content_raw = additional_kwargs.get("reasoning_content")
        if reasoning_content_raw:
            reasoning_text, _ = extract_content_with_type(reasoning_content_raw)
            if reasoning_text:
                reasoning_from_kwargs = reasoning_text

        # Fallback to reasoning field
        if not reasoning_from_kwargs:
            reasoning_raw = additional_kwargs.get("reasoning")
            if reasoning_raw:
                reasoning_text, _ = extract_content_with_type(reasoning_raw)
                if reasoning_text:
                    reasoning_from_kwargs = reasoning_text

    # Merge reasoning from both sources
    all_reasoning_parts = []
    if reasoning_from_content:
        all_reasoning_parts.append(reasoning_from_content)
    if reasoning_from_kwargs:
        all_reasoning_parts.append(reasoning_from_kwargs)

    final_reasoning = "\n\n".join(all_reasoning_parts) if all_reasoning_parts else None

    return {
        "reasoning": final_reasoning,
        "text": text_content
    }


# ============================================================================
# JSON Extraction and Repair
# ============================================================================

def extract_json_from_content(content: Union[str, List, None]) -> str:
    """
    Extract JSON text from LLM content, skipping reasoning/thinking blocks.

    This function separates structured output (JSON) from reasoning/thinking content
    in Response API models and standardized content_blocks. It's specifically designed
    for parsing, not display.

    Handles different content formats:
    - Standardized content_blocks (LangChain 1.0+): Extracts only type="text" items,
      ignoring type="reasoning" and type="thinking" blocks
    - Legacy Response API (OpenAI): Extracts only type="text" items,
      ignoring type="reasoning" blocks
    - Anthropic extended thinking: Extracts only type="text" items,
      ignoring type="thinking" blocks
    - Chat completions (simple strings): Returns string directly
    - Lists of mixed content: Aggregates text items only

    Args:
        content: Raw content from LLM response chunk. Can be:
            - str: Plain text response
            - list: Response API or content_blocks format with type annotations
            - dict: Single content item
            - None: Empty response

    Returns:
        str: Extracted JSON text suitable for parsing with json.loads()

    Example:
        >>> # Standardized content_blocks format
        >>> content = [
        ...     {"type": "reasoning", "reasoning": "thinking..."},
        ...     {"type": "text", "text": '{"answer": 42}'}
        ... ]
        >>> extract_json_from_content(content)
        '{"answer": 42}'

        >>> # Legacy Response API format
        >>> content = [
        ...     {"type": "reasoning", "summary": [{"text": "thinking..."}]},
        ...     {"type": "text", "text": '{"answer": 42}'}
        ... ]
        >>> extract_json_from_content(content)
        '{"answer": 42}'

        >>> # Regular chat completion
        >>> content = '{"answer": 42}'
        >>> extract_json_from_content(content)
        '{"answer": 42}'
    """
    if content is None:
        return ""

    # Simple string content (chat completions)
    if isinstance(content, str):
        return content

    # Single dict item
    if isinstance(content, dict):
        # Extract text field if present
        if "text" in content:
            return content["text"]
        # Skip reasoning/thinking-only dicts (all formats)
        if content.get("type") in ("reasoning", "thinking"):
            return ""
        return ""

    # List of content items (Response API or content_blocks format)
    if isinstance(content, list):
        text_parts = []
        for item in content:
            if isinstance(item, str):
                # Direct string in list
                text_parts.append(item)
            elif isinstance(item, dict):
                # Only extract type="text" items, skip type="reasoning" and type="thinking"
                if item.get("type") == "text":
                    text_parts.append(item.get("text", ""))
                # Skip reasoning/thinking blocks entirely (all formats)
                elif item.get("type") in ("reasoning", "thinking"):
                    continue
        return "".join(text_parts)

    # Fallback for unexpected types
    return str(content)


def repair_json_output(content: str) -> str:
    """
    Repair and normalize JSON output from LLM responses.

    Args:
        content (str): String content that may contain JSON

    Returns:
        str: Repaired JSON string, or original content if not JSON
    """
    # Handle non-string inputs first
    if not isinstance(content, str):
        content_dict = format_llm_content(content)
        # Combine for backward compatibility in this function
        content = content_dict["text"]
        if content_dict["reasoning"]:
            content = f"Reasoning: {content_dict['reasoning']}\n\n{content}"

    content = content.strip()

    try:
        # Try to repair and parse JSON
        repaired_content = json_repair.loads(content)
        if not isinstance(repaired_content, dict) and not isinstance(
            repaired_content, list
        ):
            logger.warning("Repaired content is not a valid JSON object or array.")
            return content
        content = json.dumps(repaired_content, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"JSON repair failed: {e}")

    return content
