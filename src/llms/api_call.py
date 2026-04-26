from typing import Any, Dict, Optional, Type, Union, Tuple
from pydantic import BaseModel
import asyncio
import logging
import json
from .token_counter import extract_token_usage
from .content_utils import (
    format_llm_content,
    get_message_content,
)

# Configure logging for token usage
logger = logging.getLogger(__name__)


def create_messages(system_prompt: str, user_prompt: str) -> list:
    """Create a list of messages for LLM invocation.

    Note: This returns messages directly without using template.invoke()
    to avoid creating a separate LangSmith trace for prompt rendering.
    """
    from langchain_core.messages import SystemMessage, HumanMessage
    return [
        SystemMessage(content=system_prompt),
        HumanMessage(content=user_prompt),
    ]


def maybe_disable_streaming(model: object) -> None:
    """Set ``streaming=False`` unless the provider rejects non-streaming.

    The Codex proxy requires ``stream=true`` on every request (it returns
    HTTP 400 ``{'detail': 'Stream must be set to true'}`` otherwise), so
    ``ChatCodexOpenAI`` instances must keep their construction-time
    ``streaming=True`` flag. Mutates ``model`` in place — callers that
    share the instance across requests MUST deep-copy first (see
    ``workflow_handler.py::compact`` and ``fetch.py::_extract_with_llm``
    for the reference pattern).
    """
    from src.llms.extension.codex import ChatCodexOpenAI

    if isinstance(model, ChatCodexOpenAI):
        return
    if hasattr(model, "streaming"):
        model.streaming = False


async def make_api_call(
    llm: object,
    system_prompt: str,
    user_prompt: str,
    response_schema: Optional[Type[BaseModel]] = None,
    return_token_usage: bool = False,
    max_parsing_retries: int = 5,
    disable_tracing: bool = False,
) -> Union[Dict[str, Any], BaseModel, str, Tuple[Any, Dict[str, Any]]]:
    """
    Make an async API call to an LLM with flexible response handling.

    Args:
        llm: The LLM instance
        system_prompt: System prompt
        user_prompt: User prompt
        response_schema: Optional Pydantic schema for structured output
        return_token_usage: If True, returns tuple of (response, token_usage)
        max_parsing_retries: Maximum number of retries for parsing failures (default: 5)
        disable_tracing: If True, disables LangSmith tracing for this call

    Returns:
        Structured response, dict, or string depending on configuration
        If return_token_usage=True, returns tuple of (response, token_usage)

    Note:
        When a model doesn't support native structured output, the system will retry
        parsing up to max_parsing_retries times before failing.
    """
    from langsmith import tracing_context

    # Enable stream_usage for OpenAI models to get token counts
    if hasattr(llm, 'model_name') and hasattr(llm, 'stream_usage'):
        llm.stream_usage = True

    messages = create_messages(system_prompt, user_prompt)

    # Get model name for logging
    model_name = getattr(llm, 'model_name', 'unknown')

    # Use tracing_context to disable LangSmith tracing if requested
    with tracing_context(enabled=not disable_tracing):
        # Case 1: No schema requested, return raw content
        if response_schema is None:
            response = await llm.ainvoke(messages)
            content = get_message_content(response)

            # Extract token usage (silent by default)
            token_info = extract_token_usage(response)

            if return_token_usage:
                return content, token_info
            return content

        # Case 2: Native structured output
        try:
            # Use include_raw=True to get token usage with structured output
            client = llm.with_structured_output(response_schema, include_raw=True)
            response_with_raw = await client.ainvoke(messages)

            token_info = {}

            # Extract the parsed response and raw response
            # Check if response_with_raw is not None before checking if it's a dict
            if response_with_raw is not None and isinstance(response_with_raw, dict) and 'raw' in response_with_raw:
                response = response_with_raw.get('parsed')
                if response is None:
                    # Parsing failed, fall back to manual parsing
                    raise Exception("Structured output parsing returned None")
                raw_response = response_with_raw['raw']
                token_info = extract_token_usage(raw_response)
            else:
                # Fallback to direct response if include_raw didn't work
                client = llm.with_structured_output(response_schema)
                response = await client.ainvoke(messages)

            if return_token_usage:
                return response, token_info
            return response

        except Exception:
            # Fallback: Model doesn't support structured output, try manual parsing with retries
            from pydantic import ValidationError

            last_error = None
            total_token_info = {}
            retry_count = 0

            for attempt in range(max_parsing_retries):
                retry_count = attempt + 1
                try:
                    # Get response from LLM
                    response = await llm.ainvoke(messages)
                    content = get_message_content(response)

                    # Extract and accumulate token usage
                    token_info = extract_token_usage(response)
                    if token_info:
                        for key, value in token_info.items():
                            total_token_info[key] = total_token_info.get(key, 0) + value

                    # Flatten content_blocks (reasoning + text) to text.
                    if isinstance(content, list):
                        content = format_llm_content(content).get("text", "") or ""

                    # Try to extract and parse JSON
                    if isinstance(content, str):
                        # Look for JSON structure in the content
                        # Try multiple extraction strategies
                        json_str = None

                        # Strategy 1: Find complete JSON object
                        start_idx = content.find('{')
                        end_idx = content.rfind('}') + 1
                        if start_idx != -1 and end_idx > start_idx:
                            json_str = content[start_idx:end_idx]

                        # Strategy 2: Look for code blocks with JSON
                        if not json_str:
                            json_pattern = r'```(?:json)?\s*(\{.*?\})\s*```'
                            import re
                            match = re.search(json_pattern, content, re.DOTALL)
                            if match:
                                json_str = match.group(1)

                        if json_str:
                            # Parse JSON and validate with schema
                            data = json.loads(json_str)
                            parsed = response_schema(**data)

                            # Add retry metadata to token info
                            total_token_info['retry_attempts'] = retry_count

                            if return_token_usage:
                                return parsed, total_token_info
                            return parsed
                        else:
                            raise ValueError(f"No JSON structure found in response (attempt {retry_count}/{max_parsing_retries})")
                    else:
                        raise ValueError(f"Response is not a string (attempt {retry_count}/{max_parsing_retries})")

                except (json.JSONDecodeError, ValidationError, ValueError) as parse_error:
                    last_error = parse_error
                    if retry_count < max_parsing_retries:
                        # Add clarification to the prompt for next attempt
                        messages = create_messages(
                            system_prompt + f"\n\nIMPORTANT: Your response MUST be valid JSON that matches this schema: {response_schema.model_json_schema()}",
                            user_prompt + f"\n\n(Retry {retry_count + 1}/{max_parsing_retries} due to parsing error. Please ensure your response is properly formatted JSON.)"
                        )
                        await asyncio.sleep(0.5 * retry_count)  # Exponential backoff
                        continue

            # All retries failed, raise the last error with context
            total_token_info['retry_attempts'] = retry_count
            total_token_info['parsing_failed'] = True

            error_msg = f"Failed to parse response after {retry_count} attempts. Last error: {last_error}"
            logger.debug(error_msg)  # Changed from error to debug to reduce console noise

            # If return_token_usage is True, we still need to return something
            if return_token_usage:
                raise ValueError(error_msg + f" | Token usage: {total_token_info}")
            else:
                raise ValueError(error_msg)
    

async def parse_structured_output(llm: object, text: str, schema_class: Optional[Type[BaseModel]] = None) -> Union[BaseModel, Dict[str, Any]]:
    """
    Asynchronously parse unstructured text into a structured format using an LLM.
    
    Args:
        llm: The LLM instance to use for parsing
        text: The text to parse
        schema_class: The Pydantic schema class to parse into
        
    Returns:
        Parsed structured output as a Pydantic model instance
    """
    if schema_class is None:
        raise ValueError("schema_class parameter is required for parse_structured_output")
    
    system_prompt = f"You are a helpful assistant that understands and translates text to JSON format according to the following schema. {schema_class.model_json_schema()}"
    user_prompt = f"{text}"
    messages = create_messages(system_prompt, user_prompt)

    try:
        client = llm.with_structured_output(schema_class)
        response = await client.ainvoke(messages)
        return response
    except Exception as e:
        # If structured output fails, try to parse manually
        response = await llm.ainvoke(messages)
        content = get_message_content(response)
        # Try to parse JSON manually
        import json
        try:
            data = json.loads(content)
            return schema_class(**data)
        except:
            raise e
    
