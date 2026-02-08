"""
Core execution tracking utilities.

Provides:
- TrackedExecution: Dataclass for tracked execution data
- ExecutionTracker: Thread-safe tracker for execution context
- ToolCallProcessor: Extract and analyze tool calls from messages
- ExecutionAnalyzer: Analyze execution results for status and metrics
- Message serialization utilities
- Token usage and cost calculation utilities
"""

import logging
import time
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from langchain_core.messages import AIMessage, ToolMessage

logger = logging.getLogger(__name__)


# ============================================================================
# Execution Tracker - Thread-safe context storage
# ============================================================================

@dataclass
class TrackedExecution:
    """Container for tracked execution data."""
    messages: List[Any] = field(default_factory=list)  # Flat list for backward compatibility
    agent_messages: Dict[str, List[Any]] = field(default_factory=dict)  # Messages organized by agent
    tool_calls: List[Dict[str, Any]] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    status: str = "pending"
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    token_usage: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Query-response tracking fields
    query_id: Optional[str] = None
    response_id: Optional[str] = None

    # Agent execution order tracking
    _agent_index_counter: int = 0
    agent_execution_index: Dict[str, int] = field(default_factory=dict)

    @property
    def execution_time(self) -> float:
        """Calculate execution time in seconds."""
        if self.end_time:
            return self.end_time - self.start_time
        return time.time() - self.start_time


class ExecutionTracker:
    """Thread-safe tracker for execution data using contextvars."""

    _current_context: ContextVar[Optional[TrackedExecution]] = ContextVar(
        'execution_context', default=None
    )

    @classmethod
    def start_tracking(cls) -> TrackedExecution:
        """Start a new tracking context."""
        context = TrackedExecution(start_time=time.time())
        cls._current_context.set(context)
        return context

    @classmethod
    def stop_tracking(cls) -> Optional[TrackedExecution]:
        """Stop tracking and return the context."""
        context = cls._current_context.get()
        if context:
            context.end_time = time.time()
        cls._current_context.set(None)
        return context

    @classmethod
    def get_context(cls) -> Optional[TrackedExecution]:
        """Get the current tracking context."""
        return cls._current_context.get()

    @classmethod
    def update_context(cls, agent_name: Optional[str] = None, **kwargs):
        """
        Update the current context with new data.

        Args:
            agent_name: Optional agent name to scope messages to specific agent
            **kwargs: Data to update (messages, tool_calls, metrics, etc.)
        """
        context = cls.get_context()
        if not context:
            context = cls.start_tracking()

        # Handle agent-scoped messages
        if agent_name and 'messages' in kwargs:
            # Initialize agent's message list if needed
            if agent_name not in context.agent_messages:
                context.agent_messages[agent_name] = []
                # Track agent execution order (first message from this agent)
                context.agent_execution_index[agent_name] = context._agent_index_counter
                context._agent_index_counter += 1

            # Add messages to agent's list
            messages = kwargs['messages']
            message_list = messages if isinstance(messages, list) else [messages]

            # Inject agent name into AIMessages that lack one
            for msg in message_list:
                if isinstance(msg, AIMessage) and not msg.name:
                    msg.name = agent_name

            # Inject timestamp for message ordering
            for msg in message_list:
                # Initialize _metadata if it doesn't exist
                if not hasattr(msg, '_metadata'):
                    msg._metadata = {}
                elif msg._metadata is None:
                    msg._metadata = {}

                # Add timestamp (message_index will be added during serialization)
                msg._metadata['timestamp'] = datetime.now().isoformat()

            # Add processed messages to agent's list
            if isinstance(messages, list):
                context.agent_messages[agent_name].extend(message_list)
            else:
                context.agent_messages[agent_name].append(message_list[0])

            # Also continue to add to flat list for backward compatibility
            # (don't remove 'messages' from kwargs, let it fall through)

        for key, value in kwargs.items():
            if hasattr(context, key):
                if key in ('messages', 'tool_calls', 'errors', 'warnings'):
                    # Append to lists
                    current = getattr(context, key)
                    if isinstance(value, list):
                        current.extend(value)
                    else:
                        current.append(value)
                elif key == 'metrics':
                    # Merge dicts
                    context.metrics.update(value)
                else:
                    setattr(context, key, value)

    @classmethod
    def get_messages(cls) -> List[Any]:
        """Get tracked messages."""
        context = cls.get_context()
        return context.messages if context else []

    @classmethod
    def get_tool_calls(cls) -> List[Dict[str, Any]]:
        """Get tracked tool calls."""
        context = cls.get_context()
        return context.tool_calls if context else []

    @classmethod
    def get_metrics(cls) -> Dict[str, Any]:
        """Get tracked metrics."""
        context = cls.get_context()
        return context.metrics if context else {}

    @classmethod
    def get_status(cls) -> str:
        """Get execution status."""
        context = cls.get_context()
        return context.status if context else "unknown"


# Convenience function for accessing tracker
def get_tracker() -> ExecutionTracker:
    """Get the execution tracker instance."""
    return ExecutionTracker


# ============================================================================
# Tool Call Processing
# ============================================================================

class ToolCallProcessor:
    """Processes and analyzes tool calls from agent messages."""

    @staticmethod
    def extract_tool_calls(messages: List[Any]) -> List[Dict[str, Any]]:
        """Extract all tool calls from agent messages with their execution results."""
        tool_calls = []

        for i, message in enumerate(messages):
            if not isinstance(message, AIMessage):
                continue

            if not hasattr(message, 'tool_calls') or not message.tool_calls:
                continue

            for tc in message.tool_calls:
                tool_call_data = ToolCallProcessor._parse_tool_call(tc, i)
                tool_call_data = ToolCallProcessor._find_tool_result(
                    tool_call_data, messages, i
                )
                tool_calls.append(tool_call_data)

        return tool_calls

    @staticmethod
    def _parse_tool_call(tc: Any, message_index: int) -> Dict[str, Any]:
        """Parse a single tool call object."""
        if isinstance(tc, dict):
            return {
                'name': tc.get('name'),
                'args': tc.get('args'),
                'id': tc.get('id'),
                'message_index': message_index,
                'timestamp': datetime.now().isoformat(),
                'status': 'pending',
                'has_error': False
            }
        else:
            return {
                'name': getattr(tc, 'name', None),
                'args': getattr(tc, 'args', None),
                'id': getattr(tc, 'id', None),
                'message_index': message_index,
                'timestamp': datetime.now().isoformat(),
                'status': 'pending',
                'has_error': False
            }

    @staticmethod
    def _find_tool_result(
        tool_call_data: Dict[str, Any],
        messages: List[Any],
        start_index: int
    ) -> Dict[str, Any]:
        """Find the result of a tool call in subsequent messages."""
        tool_id = tool_call_data['id']
        if not tool_id:
            return tool_call_data

        for j in range(start_index + 1, len(messages)):
            msg = messages[j]
            if not isinstance(msg, ToolMessage):
                continue

            msg_tool_id = getattr(msg, 'tool_call_id', None)
            if msg_tool_id != tool_id:
                continue

            content = str(msg.content if hasattr(msg, 'content') else msg)
            is_error, metadata = ToolCallProcessor._detect_error(content, tool_call_data['name'])

            # Store execution metadata if available
            if metadata:
                tool_call_data['execution_metadata'] = metadata

            if is_error:
                tool_call_data['status'] = 'error'
                tool_call_data['has_error'] = True
                tool_call_data['error_message'] = content[:1000]
            else:
                # Check if it's partial success from metadata
                if metadata and metadata.get('is_partial_success'):
                    tool_call_data['status'] = 'partial_success'
                else:
                    tool_call_data['status'] = 'success'
            break

        return tool_call_data

    @staticmethod
    def _detect_error(content: str, tool_name: str) -> tuple[bool, Optional[Dict[str, Any]]]:
        """Detect if tool execution resulted in an error and extract metadata.

        Returns:
            tuple: (is_error, metadata_dict or None)
        """
        import json
        import re

        metadata = None

        # Parse visible indicators from execute_python_code output
        if tool_name == 'execute_python_code':
            # Try to parse as JSON first (most common case for wrapped tool responses)
            try:
                parsed = json.loads(content)
                if isinstance(parsed, dict) and 'status' in parsed:
                    status = parsed.get('status')

                    # Get execution_metrics directly from JSON structure
                    if 'execution_metrics' in parsed:
                        # New structured format
                        metadata = parsed['execution_metrics'].copy()
                        metadata['status'] = status
                        if status == 'partial_success':
                            metadata['is_partial_success'] = True

                        # Add chart files if present
                        if 'chart_files' in parsed:
                            metadata['chart_files'] = parsed['chart_files']

                        # Return based on the status
                        if status == 'error':
                            return True, metadata
                        elif status == 'partial_success':
                            return False, metadata  # Not a complete error
                        else:  # success
                            return False, metadata
                    else:
                        # Fallback for backward compatibility - parse from output string
                        output = parsed.get('output', '')
                        metadata = {'status': status}

                        if status == 'partial_success':
                            metadata['is_partial_success'] = True

                        # Try to extract metrics from output if not in JSON
                        metrics_match = re.search(r'=== EXECUTION_METRICS ===(.*?)(?:===|$)', output, re.DOTALL)
                        if metrics_match:
                            for line in metrics_match.group(1).strip().split('\n'):
                                if ':' in line:
                                    key, value = line.split(':', 1)
                                    key, value = key.strip(), value.strip()
                                    if key in ['charts_generated', 'queries_executed', 'dataframes_created']:
                                        try:
                                            metadata[key] = int(value)
                                        except ValueError:
                                            metadata[key] = 0

                        # Return based on status
                        return status == 'error', metadata
            except (json.JSONDecodeError, ValueError):
                # Not JSON - shouldn't happen with new format
                logger.debug(f"Tool output is not JSON: {tool_name}")
                pass

        # Parse technical_analyze_stock_with_indicators output
        if tool_name == 'technical_analyze_stock_with_indicators':
            try:
                parsed = json.loads(content)
                if isinstance(parsed, dict):
                    metadata = {}

                    # Extract chart files
                    if 'chart_files' in parsed:
                        metadata['chart_files'] = parsed['chart_files']

                    # Extract upload status
                    if 'oss_upload' in parsed:
                        oss_upload = parsed['oss_upload']
                        if isinstance(oss_upload, dict):
                            metadata['charts_generated'] = oss_upload.get('uploaded_count', 0)
                            metadata['upload_status'] = oss_upload.get('status')

                    return False, metadata  # Tool doesn't error
            except (json.JSONDecodeError, ValueError):
                pass

        content_lower = content.lower()

        # Python exceptions
        if 'traceback (most recent call last)' in content_lower:
            return True, metadata

        # Explicit error prefixes
        if content.startswith(('Error:', '错误:', 'Failed to', '无法')):
            return True, metadata

        # Exception patterns
        if 'exception:' in content_lower and 'raise' in content_lower:
            return True, metadata

        # API errors
        if '"error":' in content and ('"message":' in content or '"code":' in content):
            return True, metadata

        # Special handling for search tools
        search_tools = ['WebSearch', 'WebFetch']
        if tool_name in search_tools:
            if 'failed to search' in content_lower or 'search error' in content_lower:
                return True, metadata
            # Catch Exception('Error XXX:') pattern common in search API failures
            if content.startswith("Exception(") and "Error" in content:
                return True, metadata
            if "Exception('Error" in content:
                return True, metadata
            return False, metadata

        # For non-execute_python_code tools, use basic error detection
        return False, metadata


# ============================================================================
# Execution Analysis
# ============================================================================

class ExecutionAnalyzer:
    """Analyzes execution results during runtime to determine status and extract metrics."""

    @staticmethod
    def _extract_text_from_output(output) -> Dict[str, Any]:
        """Extract content from output which may be a string or list.

        Returns:
            Dict with "reasoning" (str | None) and "text" (str) fields
        """
        # Import here to avoid circular dependency
        from src.llms import format_llm_content
        return format_llm_content(output)

    @staticmethod
    def analyze(
        messages: List[Any],
        final_output: str,
        tool_calls: List[Dict[str, Any]],
        task_type: str = None
    ) -> Dict[str, Any]:
        """
        Analyze execution result to determine status and metrics.

        Returns:
            Dictionary with status, errors, warnings, and metrics
        """
        from dataclasses import dataclass, field as dc_field

        @dataclass
        class ExecutionMetrics:
            """Metrics collected during task execution."""
            total_tool_calls: int = 0
            successful_tool_calls: int = 0
            failed_tool_calls: int = 0
            tool_success_rate: float = 0.0
            tools_used: List[str] = dc_field(default_factory=list)
            tool_details: Dict[str, Dict[str, Any]] = dc_field(default_factory=dict)

            def add_tool_call(self, tool_name: str, success: bool):
                """Record a tool call."""
                self.total_tool_calls += 1
                if success:
                    self.successful_tool_calls += 1
                else:
                    self.failed_tool_calls += 1

                if tool_name not in self.tool_details:
                    self.tool_details[tool_name] = {"attempts": 0, "successes": 0}

                self.tool_details[tool_name]["attempts"] += 1
                if success:
                    self.tool_details[tool_name]["successes"] += 1

                # Track unique tools used
                if tool_name not in self.tools_used:
                    self.tools_used.append(tool_name)

            def calculate_rates(self):
                """Calculate success rates."""
                if self.total_tool_calls > 0:
                    self.tool_success_rate = round(
                        (self.successful_tool_calls / self.total_tool_calls) * 100, 2
                    )

                for stats in self.tool_details.values():
                    if stats["attempts"] > 0:
                        stats["success_rate"] = round(
                            (stats["successes"] / stats["attempts"]) * 100, 2
                        )

        metrics = ExecutionMetrics()
        errors = []
        warnings = []

        # Process tool calls with metadata
        for tc in tool_calls:
            tool_name = tc.get('name', '')
            is_error = tc.get('status') == 'error' or tc.get('has_error', False)

            # For execute_python_code, check metadata for partial success
            if tool_name == 'execute_python_code' and tc.get('execution_metadata'):
                metadata = tc['execution_metadata']
                if metadata.get('status') == 'partial_success':
                    # Partial success with achievements counts as success for metrics
                    if any([metadata.get('charts_generated', 0) > 0,
                           metadata.get('queries_executed', 0) > 0,
                           metadata.get('dataframes_created', 0) > 0]):
                        is_error = False

            metrics.add_tool_call(tool_name, not is_error)

            if is_error and tc.get('error_message'):
                errors.append(tc['error_message'][:500])

        metrics.calculate_rates()

        # Collect tool metadata for status determination
        tool_metadata = None
        for tc in tool_calls:
            if tc.get('execution_metadata'):
                tool_metadata = tc['execution_metadata']
                break  # Use first metadata found

        # Determine status with partial success consideration
        status = ExecutionAnalyzer._determine_status(
            metrics, final_output, task_type, tool_metadata
        )

        # Generate warnings
        if metrics.failed_tool_calls > 0:
            if metrics.failed_tool_calls == metrics.total_tool_calls:
                warnings.append(f"All {metrics.total_tool_calls} tool calls failed")
            elif metrics.tool_success_rate < 50:
                warnings.append(f"Low tool success rate: {metrics.tool_success_rate}%")

        # Determine detailed status
        detailed_status = status
        if status == 'success' and metrics.failed_tool_calls == 0:
            detailed_status = 'complete_success'  # Perfect execution
        elif status == 'partial_success':
            detailed_status = 'partial_success'  # Partial success from tool

        return {
            'status': status,
            'detailed_status': detailed_status,
            'errors': errors,
            'warnings': warnings,
            'metrics': metrics.__dict__
        }

    @staticmethod
    def _determine_status(
        metrics,  # ExecutionMetrics instance
        final_output: str,
        task_type: str = None,
        tool_metadata: Dict[str, Any] = None
    ) -> str:
        """Determine task status based on metrics, output quality, and task type."""
        import re

        # TaskStatus value constants (using strings directly to avoid circular import)
        SUCCESS = "success"
        FAILURE = "failure"
        PARTIAL_SUCCESS = "partial_success"

        # Check if tool explicitly reported partial_success
        if tool_metadata and tool_metadata.get('is_partial_success'):
            return PARTIAL_SUCCESS

        # Extract content from output (handles both string and list)
        output_content = ExecutionAnalyzer._extract_text_from_output(final_output)
        output_text = output_content["text"]

        # Check if output is meaningful (at least 50 characters)
        output_length = len(output_text.strip()) if output_text else 0
        has_minimal_output = output_length >= 50
        has_meaningful_output = output_length > 100

        # Special requirement for researcher tasks: must have at least one tool call
        if task_type == "researcher":
            if metrics.total_tool_calls == 0:
                return FAILURE
            if metrics.successful_tool_calls == 0:
                return FAILURE
            if not has_minimal_output:
                return FAILURE
            return SUCCESS

        # Enhanced logic for coder tasks with metadata from tools
        if task_type == "coder" and tool_metadata:
            # Check if this is truly a partial success (had meaningful output before error)
            has_output_before_error = False
            if "OUTPUT_BEFORE_ERROR" in output_text:
                # Extract the output before error to check its length
                match = re.search(r'=== OUTPUT_BEFORE_ERROR ===(.*?)(?:=== ERROR_INFO ===)', output_text, re.DOTALL)
                if match:
                    output_before_error = match.group(1).strip()
                    if len(output_before_error) > 50:  # Meaningful output before error
                        has_output_before_error = True

            # Only apply metadata-based success for partial success or non-errors
            tool_status = tool_metadata.get('status', 'unknown')
            if tool_status == 'error' and not has_output_before_error:
                # Plain error without meaningful output - don't use metadata for success
                pass  # Fall through to standard failure logic
            else:
                # Partial success or success - check metadata
                if tool_metadata.get('charts_generated', 0) > 0:
                    if has_minimal_output:
                        return SUCCESS
                    else:
                        # Charts saved but output too short - still success
                        return SUCCESS

                if tool_metadata.get('dataframes_created', 0) > 0 and output_length > 500:
                    # Significant data processing occurred
                    return SUCCESS

        # Standard logic for other tasks
        if metrics.successful_tool_calls > 0 and has_minimal_output:
            return SUCCESS

        # Check for partial success based on OUTPUT_BEFORE_ERROR
        if metrics.total_tool_calls > 0 and metrics.successful_tool_calls == 0:
            # All tools failed, but check output for evidence of partial success
            if task_type == "coder":
                if "OUTPUT_BEFORE_ERROR" in output_text and output_length > 200:
                    # Has meaningful output before error
                    return PARTIAL_SUCCESS

        # If no tools were called but there's meaningful output, it might be success
        if metrics.total_tool_calls == 0 and has_meaningful_output:
            return SUCCESS

        # Default to failure if none of the success conditions were met
        return FAILURE


# ============================================================================
# Message Serialization
# ============================================================================

def serialize_agent_message(msg: Any) -> Dict[str, Any]:
    """
    Serialize a single agent message including all metadata and reasoning content.

    Args:
        msg: Message object (AIMessage, ToolMessage, HumanMessage, or dict)

    Returns:
        Dictionary with serialized message data including reasoning if present
    """
    from src.llms.content_utils import extract_content_with_type

    serialized = {
        'type': type(msg).__name__,
        'content': None,
        'reasoning': None,  # NEW: Add reasoning field
        'tool_calls': None,
        'tool_call_id': None,
        'name': None,
        'response_metadata': None,
        'id': None,
        'timestamp': None,  # Message timestamp for ordering
        'worker_instance_id': None,  # Worker instance ID for concurrent workers
        'query_id': None  # Query ID associated with worker messages
    }

    # Extract content
    if hasattr(msg, 'content'):
        serialized['content'] = msg.content
    elif isinstance(msg, dict):
        serialized['content'] = msg.get('content', str(msg))
    else:
        serialized['content'] = str(msg)

    # Extract reasoning from additional_kwargs if present (for AIMessage)
    if isinstance(msg, AIMessage) and hasattr(msg, 'additional_kwargs'):
        additional_kwargs = msg.additional_kwargs

        # Check for reasoning_content (primary field)
        reasoning_content_raw = additional_kwargs.get("reasoning_content")
        if reasoning_content_raw:
            reasoning_text, _ = extract_content_with_type(reasoning_content_raw)
            if reasoning_text:
                serialized['reasoning'] = reasoning_text

        # Fallback to reasoning field
        if not serialized['reasoning']:
            reasoning_raw = additional_kwargs.get("reasoning")
            if reasoning_raw:
                reasoning_text, _ = extract_content_with_type(reasoning_raw)
                if reasoning_text:
                    serialized['reasoning'] = reasoning_text

    # Extract tool calls from AIMessage
    if isinstance(msg, AIMessage):
        if hasattr(msg, 'tool_calls') and msg.tool_calls:
            serialized['tool_calls'] = msg.tool_calls
        if hasattr(msg, 'response_metadata'):
            serialized['response_metadata'] = msg.response_metadata

    # Extract tool_call_id from ToolMessage
    if isinstance(msg, ToolMessage):
        if hasattr(msg, 'tool_call_id'):
            serialized['tool_call_id'] = msg.tool_call_id

    # Extract common metadata
    if hasattr(msg, 'name'):
        serialized['name'] = msg.name
    elif isinstance(msg, dict):
        serialized['name'] = msg.get('name')

    if hasattr(msg, 'id'):
        serialized['id'] = msg.id
    elif isinstance(msg, dict):
        serialized['id'] = msg.get('id')

    # Extract timestamp and other metadata from _metadata
    if hasattr(msg, '_metadata') and msg._metadata:
        serialized['timestamp'] = msg._metadata.get('timestamp')
        serialized['worker_instance_id'] = msg._metadata.get('worker_instance_id')
        serialized['query_id'] = msg._metadata.get('query_id')
    elif isinstance(msg, dict) and '_metadata' in msg:
        metadata = msg.get('_metadata', {})
        serialized['timestamp'] = metadata.get('timestamp')
        serialized['worker_instance_id'] = metadata.get('worker_instance_id')
        serialized['query_id'] = metadata.get('query_id')

    return serialized


def renumber_agent_index(agent_execution_index: Dict[str, int]) -> Dict[str, int]:
    """
    Renumber agent_index to be relative to agents that actually executed.

    Converts absolute agent_index (based on global counter) to relative ordering
    (0-indexed based on execution sequence in this specific response).

    Args:
        agent_execution_index: Dict mapping agent names to their absolute execution index

    Returns:
        Dict mapping agent names to relative execution index (0, 1, 2...)

    Example:
        Input:  {"coordinator": 0, "researcher": 2, "reporter": 5}
        Output: {"coordinator": 0, "researcher": 1, "reporter": 2}
    """
    if not agent_execution_index:
        return {}

    # Sort agents by their original execution order
    sorted_agents = sorted(agent_execution_index.items(), key=lambda x: x[1])

    # Assign new sequential order starting from 0
    return {agent_name: idx for idx, (agent_name, _) in enumerate(sorted_agents)}


# ============================================================================
# Cost and Token Utilities
# ============================================================================

def add_cost_to_token_usage(token_usage: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Add cost calculations to token usage data from UsageMetadataCallbackHandler.

    Reuses existing cost calculation utilities from src.llms.pricing_utils.
    Supports model aliases, case-insensitive matching, and automatic provider detection.

    Args:
        token_usage: Token usage dictionary from UsageMetadataCallbackHandler
                    Format: {model_name: {input_tokens, output_tokens, input_token_details, ...}}

    Returns:
        Restructured token usage with cost information:
        {
            "by_model": {model_name: {input_tokens, output_tokens, ...}},
            "total_cost": float,
            "cost_breakdown": {input_cost, output_cost, cached_cost, ...}
        }
    """
    from src.llms.pricing_utils import calculate_total_cost, find_model_pricing, detect_provider_for_model

    if not token_usage:
        return {
            "by_model": {},
            "total_cost": 0.0,
            "cost_breakdown": {}
        }

    total_cost = 0.0
    aggregated_breakdown = {
        "input_cost": 0.0,
        "output_cost": 0.0,
        "cached_cost": 0.0,
        "cache_storage_cost": 0.0,
        "cache_5m_cost": 0.0,
        "cache_1h_cost": 0.0
    }

    # Calculate cost for each model
    for model_name, usage in token_usage.items():
        # Detect provider from llm_config.json for provider-aware pricing lookup
        # This handles cases where the same model is offered by different providers
        # with different pricing (e.g., GPT-4 via OpenAI vs Azure)
        provider = detect_provider_for_model(model_name)

        # Get pricing information using centralized lookup with provider context
        # Supports: case-insensitive matching, aliases, version fallback
        pricing = find_model_pricing(model_name, provider=provider)
        if not pricing:
            logger.debug(f"No pricing found for model: {model_name} (provider: {provider})")
            continue

        # Extract token counts
        input_tokens = usage.get('input_tokens', 0)
        output_tokens = usage.get('output_tokens', 0)

        # Extract cached tokens from input_token_details
        cached_tokens = 0
        cache_5m_tokens = 0
        cache_1h_tokens = 0

        if 'input_token_details' in usage:
            details = usage['input_token_details']
            # Cache hits (reads)
            cached_tokens = details.get('cache_read', 0)

            # Cache creation tokens - handle multiple formats:
            # Format 1: Flat format (ephemeral tokens at top level)
            cache_5m_tokens = details.get('ephemeral_5m_input_tokens', 0)
            cache_1h_tokens = details.get('ephemeral_1h_input_tokens', 0)

            # Format 2 & 3: cache_creation field (dict or int)
            if 'cache_creation' in details:
                cache_creation = details['cache_creation']

                # Dict format: {'ephemeral_5m_input_tokens': X, 'ephemeral_1h_input_tokens': Y}
                if isinstance(cache_creation, dict):
                    cache_5m_tokens = cache_creation.get('ephemeral_5m_input_tokens', 0)
                    cache_1h_tokens = cache_creation.get('ephemeral_1h_input_tokens', 0)

                # Int format: total cache creation tokens (assign to 5m if not already set)
                elif isinstance(cache_creation, int) and cache_creation > 0:
                    if cache_5m_tokens == 0 and cache_1h_tokens == 0:
                        cache_5m_tokens = cache_creation

        # Calculate cost for this model using pricing utilities
        cost_result = calculate_total_cost(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cached_tokens=cached_tokens,
            cache_5m_tokens=cache_5m_tokens,
            cache_1h_tokens=cache_1h_tokens,
            pricing=pricing
        )

        total_cost += cost_result['total_cost']

        # Aggregate breakdown across all models
        breakdown = cost_result.get('breakdown', {})
        if 'input' in breakdown:
            aggregated_breakdown['input_cost'] += breakdown['input']['cost']
        if 'cached_input' in breakdown:
            aggregated_breakdown['cached_cost'] += breakdown['cached_input']['cost']
        if 'output' in breakdown:
            aggregated_breakdown['output_cost'] += breakdown['output']['cost']
        if 'cache_storage' in breakdown:
            aggregated_breakdown['cache_storage_cost'] += breakdown['cache_storage']['cost']
        if 'cache_5m_creation' in breakdown:
            aggregated_breakdown['cache_5m_cost'] += breakdown['cache_5m_creation']['cost']
        if 'cache_1h_creation' in breakdown:
            aggregated_breakdown['cache_1h_cost'] += breakdown['cache_1h_creation']['cost']

    # Clean up zero-value breakdown entries
    cost_breakdown = {k: v for k, v in aggregated_breakdown.items() if v > 0}

    return {
        "by_model": token_usage,
        "total_cost": total_cost,
        "cost_breakdown": cost_breakdown
    }


def calculate_cost_from_per_call_records(
    per_call_records: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Calculate accurate cost from per-call token usage records.

    This function calculates costs by processing each LLM call individually before
    aggregating, enabling accurate pricing for models with tiered pricing, 2D matrix
    pricing, or input-dependent pricing.

    Unlike add_cost_to_token_usage() which receives aggregated token counts,
    this function receives per-call records from PerCallTokenTracker, allowing
    accurate tiered pricing where rates vary based on token thresholds.

    Example:
        With tiered pricing (0-32k: $0.80, 32k-128k: $1.20):

        Incorrect (aggregated tokens): Two 30k calls → 60k total → Tier 2 pricing
        Correct (per-call): Two 30k calls → Each in Tier 1 → Accurate pricing

    Args:
        per_call_records: List of per-call token records from PerCallTokenTracker
                         Each record contains:
                         - model_name: str
                         - usage: UsageMetadata dict
                         - timestamp: str (ISO format)
                         - run_id: str
                         - parent_run_id: Optional[str]

    Returns:
        {
            "by_model": {
                model_name: {
                    input_tokens: int,
                    output_tokens: int,
                    total_tokens: int,
                    cached_tokens: int,
                    call_count: int,
                    total_cost: float,
                }
            },
            "total_cost": float,
            "cost_breakdown": {
                input_cost: float,
                output_cost: float,
                cached_cost: float,
                ...
            },
            "per_call_costs": [
                {
                    model_name: str,
                    input_tokens: int,
                    output_tokens: int,
                    cost: float,
                    breakdown: dict,
                    timestamp: str,
                    run_id: str,
                },
                ...
            ]
        }
    """
    from src.llms.pricing_utils import (
        calculate_total_cost,
        find_model_pricing,
        detect_provider_for_model
    )

    if not per_call_records:
        return {
            "by_model": {},
            "total_cost": 0.0,
            "cost_breakdown": {},
            "per_call_costs": []
        }

    total_cost = 0.0
    aggregated_breakdown = {
        "input_cost": 0.0,
        "output_cost": 0.0,
        "cached_cost": 0.0,
        "cache_storage_cost": 0.0,
        "cache_5m_cost": 0.0,
        "cache_1h_cost": 0.0
    }
    per_call_costs = []
    by_model: Dict[str, Dict[str, Any]] = {}

    # Process each call individually
    for record in per_call_records:
        model_name = record["model_name"]
        usage = record["usage"]

        # Detect provider for this model
        provider = detect_provider_for_model(model_name)

        # Get pricing information
        pricing = find_model_pricing(model_name, provider=provider)
        if not pricing:
            logger.debug(f"No pricing found for model: {model_name} (provider: {provider})")
            continue

        # Extract token counts from this specific call
        input_tokens = usage.get('input_tokens', 0)
        output_tokens = usage.get('output_tokens', 0)

        # Extract cached tokens and cache creation tokens
        # Primary: Use flattened structure (what extract_token_usage() actually returns)
        cached_tokens = usage.get('cached_tokens', 0)
        cache_5m_tokens = 0
        cache_1h_tokens = 0

        # Fallback: Check nested structure (for legacy/edge cases)
        if 'input_token_details' in usage:
            details = usage['input_token_details']

            # Cache hits (reads) - only if not already set from flattened structure
            if cached_tokens == 0:
                cached_tokens = details.get('cache_read', 0)

            # Cache creation tokens - handle multiple formats
            cache_5m_tokens = details.get('ephemeral_5m_input_tokens', 0)
            cache_1h_tokens = details.get('ephemeral_1h_input_tokens', 0)

            if 'cache_creation' in details:
                cache_creation = details['cache_creation']

                if isinstance(cache_creation, dict):
                    cache_5m_tokens = cache_creation.get('ephemeral_5m_input_tokens', 0)
                    cache_1h_tokens = cache_creation.get('ephemeral_1h_input_tokens', 0)
                elif isinstance(cache_creation, int) and cache_creation > 0:
                    if cache_5m_tokens == 0 and cache_1h_tokens == 0:
                        cache_5m_tokens = cache_creation

        # Calculate cost for THIS CALL ONLY (accurate tiered pricing)
        cost_result = calculate_total_cost(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cached_tokens=cached_tokens,
            cache_5m_tokens=cache_5m_tokens,
            cache_1h_tokens=cache_1h_tokens,
            pricing=pricing
        )

        call_cost = cost_result['total_cost']
        call_breakdown = cost_result.get('breakdown', {})

        # Store per-call cost record
        per_call_costs.append({
            "model_name": model_name,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cached_tokens": cached_tokens,
            "cost": call_cost,
            "breakdown": call_breakdown,
            "timestamp": record.get("timestamp"),
            "run_id": record.get("run_id"),
            "parent_run_id": record.get("parent_run_id"),
        })

        # Aggregate total cost
        total_cost += call_cost

        # Aggregate breakdown across all calls
        if 'input' in call_breakdown:
            aggregated_breakdown['input_cost'] += call_breakdown['input']['cost']
        if 'cached_input' in call_breakdown:
            aggregated_breakdown['cached_cost'] += call_breakdown['cached_input']['cost']
        if 'output' in call_breakdown:
            aggregated_breakdown['output_cost'] += call_breakdown['output']['cost']
        if 'cache_storage' in call_breakdown:
            aggregated_breakdown['cache_storage_cost'] += call_breakdown['cache_storage']['cost']
        if 'cache_5m_creation' in call_breakdown:
            aggregated_breakdown['cache_5m_cost'] += call_breakdown['cache_5m_creation']['cost']
        if 'cache_1h_creation' in call_breakdown:
            aggregated_breakdown['cache_1h_cost'] += call_breakdown['cache_1h_creation']['cost']

        # Aggregate usage by model (for reporting)
        if model_name not in by_model:
            by_model[model_name] = {
                'input_tokens': 0,
                'output_tokens': 0,
                'total_tokens': 0,
                'cached_tokens': 0,
                'call_count': 0,
                'total_cost': 0.0,
            }

        by_model[model_name]['input_tokens'] += input_tokens
        by_model[model_name]['output_tokens'] += output_tokens
        by_model[model_name]['total_tokens'] += input_tokens + output_tokens
        by_model[model_name]['cached_tokens'] += cached_tokens
        by_model[model_name]['call_count'] += 1
        by_model[model_name]['total_cost'] += call_cost

    # Clean up zero-value breakdown entries
    cost_breakdown = {k: v for k, v in aggregated_breakdown.items() if v > 0}

    return {
        "by_model": by_model,
        "total_cost": total_cost,
        "cost_breakdown": cost_breakdown,
        "per_call_costs": per_call_costs,
    }


# Public API
__all__ = [
    'TrackedExecution',
    'ExecutionTracker',
    'ToolCallProcessor',
    'ExecutionAnalyzer',
    'get_tracker',
    'serialize_agent_message',
    'add_cost_to_token_usage',
    'calculate_cost_from_per_call_records',
]
