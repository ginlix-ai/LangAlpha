"""
Request and Response models for workflow state retrieval endpoints.

This module provides Pydantic models for retrieving and displaying
historical workflow states from LangGraph checkpoints.
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field
from langchain_core.messages import BaseMessage


def serialize_message(message: Any) -> Dict[str, Any]:
    """
    Serialize a LangChain message object to a JSON-friendly dictionary.

    Args:
        message: LangChain message object (HumanMessage, AIMessage, etc.) or dict

    Returns:
        Dictionary representation of the message
    """
    if isinstance(message, dict):
        # Already a dict, return as-is
        return message

    # Handle LangChain message objects
    if hasattr(message, "type") and hasattr(message, "content"):
        result = {
            "role": message.type,
            "content": message.content,
        }

        # Add optional fields if present
        if hasattr(message, "id"):
            result["id"] = message.id
        if hasattr(message, "name"):
            result["name"] = message.name
        if hasattr(message, "tool_calls") and message.tool_calls:
            result["tool_calls"] = message.tool_calls
        if hasattr(message, "tool_call_id"):
            result["tool_call_id"] = message.tool_call_id
        if hasattr(message, "additional_kwargs") and message.additional_kwargs:
            result["additional_kwargs"] = message.additional_kwargs

        return result

    # Fallback: convert to string
    return {"role": "unknown", "content": str(message)}


def serialize_plan(plan: Any) -> Optional[Dict[str, Any]]:
    """
    Serialize a Plan object to a JSON-friendly dictionary.

    Args:
        plan: Plan object from state

    Returns:
        Dictionary representation of the plan or None
    """
    if not plan:
        return None

    # Handle Plan object (Pydantic model)
    if hasattr(plan, "model_dump"):
        return plan.model_dump()

    # Handle dict format
    if isinstance(plan, dict):
        return plan

    # Fallback
    return str(plan)


def serialize_observations(observations: List[Any]) -> List[Dict[str, Any]]:
    """
    Serialize observations list to JSON-friendly format.

    Args:
        observations: List of observation objects

    Returns:
        List of dictionaries
    """
    if not observations:
        return []

    result = []
    for obs in observations:
        # Handle Observation object
        if hasattr(obs, "model_dump"):
            result.append(obs.model_dump())
        # Handle dict format
        elif isinstance(obs, dict):
            result.append(obs)
        # Fallback
        else:
            result.append({"content": str(obs)})

    return result


def serialize_state_snapshot(state: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Serialize a complete LangGraph state snapshot to JSON-friendly format.

    This function recursively serializes all complex objects in the state including:
    - LangChain messages in the 'messages' field
    - Plan objects in the 'current_plan' field
    - Observation objects in the 'observations' field
    - Any other nested complex objects

    Args:
        state: Raw state dictionary from LangGraph StateSnapshot.values

    Returns:
        Completely JSON-serializable dictionary or None

    Example:
        >>> raw_state = await graph.aget_state(config)
        >>> serialized = serialize_state_snapshot(raw_state.values)
        >>> json.dumps(serialized)  # No serialization errors
    """
    if not state:
        return None

    if not isinstance(state, dict):
        # If state is not a dict, return None
        return None

    # Create a new dict to avoid modifying the original
    serialized_state = {}

    for key, value in state.items():
        # Handle messages array
        if key == "messages" and isinstance(value, list):
            serialized_state[key] = [serialize_message(msg) for msg in value]

        # Handle current_plan object
        elif key == "current_plan":
            serialized_state[key] = serialize_plan(value)

        # Handle observations array
        elif key == "observations" and isinstance(value, list):
            serialized_state[key] = serialize_observations(value)

        # Handle primitive types (str, int, float, bool, None)
        elif value is None or isinstance(value, (str, int, float, bool)):
            serialized_state[key] = value

        # Handle lists (recursively serialize elements)
        elif isinstance(value, list):
            serialized_state[key] = [
                serialize_state_snapshot(item) if isinstance(item, dict) else item
                for item in value
            ]

        # Handle nested dicts (recursively serialize)
        elif isinstance(value, dict):
            serialized_state[key] = serialize_state_snapshot(value)

        # Handle objects with model_dump (Pydantic models)
        elif hasattr(value, "model_dump"):
            serialized_state[key] = value.model_dump()

        # Handle objects with dict() method
        elif hasattr(value, "dict") and callable(value.dict):
            serialized_state[key] = value.dict()

        # Fallback: convert to string representation
        else:
            # For unknown types, convert to string to avoid serialization errors
            serialized_state[key] = str(value)

    return serialized_state


class PlanStepResponse(BaseModel):
    """Represents a single step in the research plan."""
    title: str = Field(..., description="Step title")
    description: str = Field(..., description="Step description")
    step_type: str = Field(..., description="Step type (RESEARCH, TECHNICAL, DATA)")
    execution_res: Optional[str] = Field(None, description="Execution result if completed")
    agent: Optional[str] = Field(None, description="Agent assigned to this step")


class PlanResponse(BaseModel):
    """Represents the research plan."""
    title: str = Field(..., description="Plan title")
    market_type: Optional[str] = Field(None, description="Market type (e.g. US stocks, China A-shares)")
    locale: Optional[str] = Field(None, description="Locale (zh-CN, en-US, etc.)")
    steps: List[PlanStepResponse] = Field(default_factory=list, description="Plan steps")


class ObservationResponse(BaseModel):
    """Represents an agent observation."""
    step_type: str = Field(..., description="Type of step (RESEARCH, TECHNICAL, DATA)")
    content: List[str] = Field(default_factory=list, description="Observation content")


class WorkflowStateResponse(BaseModel):
    """
    Complete workflow state response including all execution data.

    This represents the full state of a workflow thread retrieved from
    LangGraph checkpoints, suitable for displaying historical execution
    in the frontend.
    """
    thread_id: str = Field(..., description="Thread/conversation ID")
    checkpoint_id: Optional[str] = Field(None, description="Current checkpoint ID")

    # Core workflow data
    messages: List[Dict[str, Any]] = Field(default_factory=list, description="All conversation messages")
    plan: Optional[PlanResponse] = Field(None, description="Research plan with steps")
    observations: List[ObservationResponse] = Field(default_factory=list, description="Agent observations")
    final_report: Optional[str] = Field(None, description="Final generated report")

    # State metadata
    research_topic: Optional[str] = Field(None, description="Research topic/query")
    market_type: Optional[str] = Field(None, description="Market type identified")
    locale: Optional[str] = Field(None, description="Locale/language")
    deepthinking: bool = Field(False, description="Deep thinking mode enabled")
    auto_accepted_plan: bool = Field(True, description="Plan auto-accepted")

    # Execution metadata
    plan_iterations: int = Field(0, description="Number of plan iterations")
    completed: bool = Field(False, description="Workflow completed")
    next_nodes: List[str] = Field(default_factory=list, description="Next nodes to execute")

    # Timestamps
    created_at: Optional[datetime] = Field(None, description="Checkpoint creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")

    class Config:
        json_schema_extra = {
            "example": {
                "thread_id": "3d4e5f6g-7h8i-9j0k-1l2m-3n4o5p6q7r8s",
                "checkpoint_id": "1ef1234-...",
                "messages": [
                    {"role": "human", "content": "Analyze AAPL stock price"},
                    {"role": "ai", "content": "I will analyze AAPL stock price for you..."}
                ],
                "plan": {
                    "title": "AAPL Stock Analysis Plan",
                    "market_type": "US",
                    "steps": [
                        {
                            "title": "Get AAPL basic info",
                            "description": "Fetch company fundamentals",
                            "step_type": "DATA",
                            "execution_res": "completed"
                        }
                    ]
                },
                "final_report": "AAPL stock analysis report...",
                "research_topic": "AAPL Stock Analysis",
                "completed": True
            }
        }


class ThreadListItem(BaseModel):
    """Represents a single thread in the thread list."""
    thread_id: str = Field(..., description="Thread ID")
    query: Optional[str] = Field(None, description="Initial query/topic")
    checkpoint_count: int = Field(0, description="Number of checkpoints")
    last_updated: Optional[datetime] = Field(None, description="Last update timestamp")
    created_at: Optional[datetime] = Field(None, description="Creation timestamp")
    status: str = Field("unknown", description="Workflow status (completed, interrupted, failed)")


class ThreadListResponse(BaseModel):
    """Response for listing all workflow threads."""
    threads: List[ThreadListItem] = Field(default_factory=list, description="List of threads")
    total: int = Field(0, description="Total number of threads")
    limit: Optional[int] = Field(None, description="Limit applied")
    offset: Optional[int] = Field(None, description="Offset applied")


class CheckpointMetadata(BaseModel):
    """
    Checkpoint metadata from LangGraph v1 StateSnapshot.

    Contains information about how and when the checkpoint was created.
    """
    source: str = Field(..., description="Source of checkpoint (input, loop, update)")
    step: int = Field(..., description="Execution step number")
    writes: Optional[Dict[str, Any]] = Field(None, description="What nodes wrote to state at this checkpoint")

    class Config:
        json_schema_extra = {
            "example": {
                "source": "loop",
                "step": 2,
                "writes": {
                    "planner": {
                        "current_plan": {"title": "Research Plan", "steps": []}
                    }
                }
            }
        }


class TaskInfo(BaseModel):
    """
    Information about a pending task in a checkpoint.

    Tasks represent nodes that are scheduled to execute next.
    They can contain error information if the node failed or
    interrupt data if the node was paused for human feedback.
    """
    id: str = Field(..., description="Task ID")
    name: str = Field(..., description="Node/task name")
    has_error: bool = Field(False, description="Whether task has an error")
    error_message: Optional[str] = Field(None, description="Error message if task failed")
    has_interrupts: bool = Field(False, description="Whether task has interrupts")
    interrupt_count: int = Field(0, description="Number of interrupts")

    class Config:
        json_schema_extra = {
            "example": {
                "id": "abc-123-def",
                "name": "researcher",
                "has_error": False,
                "error_message": None,
                "has_interrupts": False,
                "interrupt_count": 0
            }
        }


class CheckpointResponse(BaseModel):
    """
    Single checkpoint snapshot from LangGraph v1.

    Represents the state of the workflow at a specific point in time.
    """
    checkpoint_id: str = Field(..., description="Unique checkpoint identifier")
    parent_checkpoint_id: Optional[str] = Field(None, description="Parent checkpoint for lineage tracking")
    created_at: Optional[datetime] = Field(None, description="Checkpoint creation timestamp")
    metadata: CheckpointMetadata = Field(..., description="Checkpoint metadata")
    next_nodes: List[str] = Field(default_factory=list, description="Next nodes to execute")
    pending_tasks: int = Field(0, description="Number of pending tasks")
    tasks: List[TaskInfo] = Field(default_factory=list, description="Detailed task information including errors and interrupts")
    completed: bool = Field(False, description="Whether workflow is completed at this checkpoint")
    state_preview: Dict[str, Any] = Field(
        default_factory=dict,
        description="Preview of important state fields"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "checkpoint_id": "1ef663ba-28fe-6528-8002-5a559208592c",
                "parent_checkpoint_id": "1ef663ba-28f9-6ec4-8001-31981c2c39f8",
                "created_at": "2024-08-29T19:19:38.821749+00:00",
                "metadata": {
                    "source": "loop",
                    "step": 2,
                    "writes": {"planner": {"current_plan": {}}}
                },
                "next_nodes": [],
                "pending_tasks": 0,
                "completed": True,
                "state_preview": {
                    "research_topic": "AAPL Stock Analysis",
                    "plan_iterations": 1,
                    "has_final_report": True,
                    "message_count": 5
                }
            }
        }


class CheckpointHistoryResponse(BaseModel):
    """
    Response for checkpoint history endpoint.

    Contains a list of checkpoint snapshots ordered chronologically (newest first).
    """
    thread_id: str = Field(..., description="Thread ID")
    total_checkpoints: int = Field(..., description="Total number of checkpoints returned")
    checkpoints: List[CheckpointResponse] = Field(
        default_factory=list,
        description="List of checkpoint snapshots (newest first)"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "thread_id": "3d4e5f6g-7h8i-9j0k-1l2m-3n4o5p6q7r8s",
                "total_checkpoints": 4,
                "checkpoints": [
                    {
                        "checkpoint_id": "1ef663ba-28fe-6528-8002-5a559208592c",
                        "parent_checkpoint_id": "1ef663ba-28f9-6ec4-8001-31981c2c39f8",
                        "created_at": "2024-08-29T19:19:38.821749+00:00",
                        "metadata": {"source": "loop", "step": 2, "writes": {}},
                        "next_nodes": [],
                        "pending_tasks": 0,
                        "completed": True,
                        "state_preview": {"research_topic": "AAPL Stock Analysis"}
                    }
                ]
            }
        }
