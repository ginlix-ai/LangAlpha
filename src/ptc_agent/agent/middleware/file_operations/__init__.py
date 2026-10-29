"""File operation middlewares.

This module provides middleware for intercepting file operations:
- FileOperationMiddleware: SSE event emission for Write/Edit
- MultimodalMiddleware: attaches image/PDF bytes to a visual Read's result
- MultimodalStripMiddleware: removes attachment blocks the target model can't read
"""

from ptc_agent.agent.middleware.file_operations.sse_middleware import (
    FileOperationMiddleware,
    FileOperationState,
)
from ptc_agent.agent.middleware.file_operations.multimodal import MultimodalMiddleware
from ptc_agent.agent.middleware.file_operations.multimodal_strip import (
    MultimodalStripMiddleware,
)

__all__ = [
    "FileOperationMiddleware",
    "FileOperationState",
    "MultimodalMiddleware",
    "MultimodalStripMiddleware",
]
