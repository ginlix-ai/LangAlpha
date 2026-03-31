"""Tool-related middlewares for LangChain agents.

This module contains middleware classes that handle tool input/output processing:
- Argument parsing: Converts JSON-encoded string arguments to Python objects
- Error handling: Catches tool execution errors and returns simplified messages
- Result normalization: Ensures all tool results are strings for LLM compatibility
"""

from .argument_parsing import ToolArgumentParsingMiddleware
from .code_validation import CodeValidationMiddleware
from .empty_call_retry import EmptyToolCallRetryMiddleware
from .error_handling import (
    ToolErrorHandlingMiddleware,
    simplify_tool_error,
)
from .leak_detection import LeakDetectionMiddleware
from .protected_path import ProtectedPathMiddleware
from .result_normalization import ToolResultNormalizationMiddleware

__all__ = [
    "CodeValidationMiddleware",
    "EmptyToolCallRetryMiddleware",
    "LeakDetectionMiddleware",
    "ProtectedPathMiddleware",
    "ToolArgumentParsingMiddleware",
    "ToolErrorHandlingMiddleware",
    "ToolResultNormalizationMiddleware",
    "simplify_tool_error",
]
