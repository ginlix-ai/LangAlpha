from src.llms.patches import _patch_langchain_anthropic_usage_metadata
_patch_langchain_anthropic_usage_metadata()

from .llm import LLM, ModelConfig, create_llm, get_llm_by_type, get_configured_llm_models, get_input_modalities, should_enable_caching
from .api_call import (
    make_api_call,
    parse_structured_output,
    create_messages,
    maybe_disable_streaming,
)
from .content_utils import (
    get_message_content,
    format_llm_content,
    repair_json_output,
    extract_json_from_content
)
from .token_counter import (
    TokenUsageTracker,
    TokenUsageRecord,
    get_global_tracker,
    reset_global_tracker,
    extract_token_usage
)
from .result_logger import (
    ResultLogger,
    get_result_logger
)


__all__ = ['LLM', 'ModelConfig', 'create_llm', 'get_llm_by_type', 'get_configured_llm_models', 'get_input_modalities', 'should_enable_caching',
           'make_api_call', 'parse_structured_output', 'maybe_disable_streaming',
           'create_messages', 'get_message_content', 'format_llm_content', 'repair_json_output', 'extract_json_from_content',
           'extract_token_usage',
           'TokenUsageTracker', 'TokenUsageRecord',
           'get_global_tracker', 'reset_global_tracker',
           'ResultLogger', 'get_result_logger',
           ]