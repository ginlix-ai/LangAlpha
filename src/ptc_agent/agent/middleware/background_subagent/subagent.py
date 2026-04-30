"""Middleware for providing subagents to an agent via a `Task` tool."""

from collections.abc import Awaitable, Callable, Sequence
from typing import Annotated, Any, NotRequired, TypedDict, cast

import structlog
from langchain.agents import create_agent
from langchain.agents.middleware import HumanInTheLoopMiddleware, InterruptOnConfig
from langchain.agents.middleware.types import (
    AgentMiddleware,
    ModelRequest,
    ModelResponse,
)
from langchain.tools import BaseTool, ToolRuntime
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, ToolMessage
from langchain_core.runnables import Runnable
from langchain_core.tools import StructuredTool
from langgraph.config import get_config
from langgraph.types import Command

from ptc_agent.agent.middleware.background_subagent.middleware import (
    current_background_token_tracker,
    current_background_tool_call_id,
)
from ptc_agent.agent.middleware.background_subagent.registry import BackgroundTaskRegistry
from ptc_agent.agent.middleware._utils import append_to_system_message
from src.llms.llm import narrow_prompt_cache_key

logger = structlog.get_logger(__name__)


class SubAgent(TypedDict):
    """Specification for an agent.

    When specifying custom agents, the `default_middleware` from `SubAgentMiddleware`
    will be applied first, followed by any `middleware` specified in this spec.
    To use only custom middleware without the defaults, pass `default_middleware=[]`
    to `SubAgentMiddleware`.

    Required fields:
        name: Unique identifier for the subagent.

            The main agent uses this name when calling the `Task()` tool.
        description: What this subagent does.

            Be specific and action-oriented. The main agent uses this to decide when to delegate.
        system_prompt: Instructions for the subagent.

            Include tool usage guidance and output format requirements.
        tools: Tools the subagent can use.

            Keep this minimal and include only what's needed.

    Optional fields:
        model: Override the main agent's model.

            Use the format `'provider:model-name'` (e.g., `'openai:gpt-4o'`).
        middleware: Additional middleware for custom behavior, logging, or rate limiting.
        interrupt_on: Configure human-in-the-loop for specific tools.

            Requires a checkpointer.
    """

    name: str
    """Unique identifier for the subagent."""

    description: str
    """What this subagent does. The main agent uses this to decide when to delegate."""

    system_prompt: str
    """Instructions for the subagent."""

    tools: Sequence[BaseTool | Callable | dict[str, Any]]
    """Tools the subagent can use."""

    model: NotRequired[str | BaseChatModel]
    """Override the main agent's model. Use `'provider:model-name'` format."""

    middleware: NotRequired[list[AgentMiddleware]]
    """Additional middleware for custom behavior."""

    interrupt_on: NotRequired[dict[str, bool | InterruptOnConfig]]
    """Configure human-in-the-loop for specific tools."""


class CompiledSubAgent(TypedDict):
    """A pre-compiled agent spec.

    !!! note

        The runnable's state schema must include a 'messages' key.

        This is required for the subagent to communicate results back to the main agent.

    When the subagent completes, the final message in the 'messages' list will be
    extracted and returned as a `ToolMessage` to the parent agent.
    """

    name: str
    """Unique identifier for the subagent."""

    description: str
    """What this subagent does."""

    runnable: Runnable
    """A custom agent implementation.

    Create a custom agent using either:

    1. LangChain's [`create_agent()`](https://docs.langchain.com/oss/python/langchain/quickstart)
    2. A custom graph using [`langgraph`](https://docs.langchain.com/oss/python/langgraph/quickstart)

    If you're creating a custom graph, make sure the state schema includes a 'messages' key.
    This is required for the subagent to communicate results back to the main agent.
    """


DEFAULT_SUBAGENT_PROMPT = "In order to complete the objective that the user asks of you, you have access to a number of standard tools."

# State keys that are excluded when passing state to subagents and when returning
# updates from subagents.
# When returning updates:
# 1. The messages key is handled explicitly to ensure only the final message is included
# 2. The todos and structured_response keys are excluded as they do not have a defined reducer
#    and no clear meaning for returning them from a subagent to the main agent.
_EXCLUDED_STATE_KEYS = {"messages", "todos", "structured_response"}

TASK_TOOL_DESCRIPTION = """Launch a subagent for complex, multi-step tasks.

Args:
    description: Short 1-2 sentence summary of the task (displayed as title)
    prompt: Detailed instructions for the subagent to execute
    subagent_type: Agent type to use
    action: "init" (new task, default), "update" (instruct running task), "resume" (resume completed task)
    task_id: Required for "update" and "resume" actions

Usage:
- Use for: Complex tasks, isolated research, context-heavy operations
- NOT for: Simple 1-2 tool operations (do directly)
- Parallel: Launch multiple agents in single message for concurrent tasks
- Results: Subagent returns final report only (intermediate steps hidden)

The subagent works autonomously. Provide clear, complete instructions in the prompt."""


def _get_subagents(
    *,
    default_model: str | BaseChatModel,
    default_tools: Sequence[BaseTool | Callable | dict[str, Any]],
    default_middleware: list[AgentMiddleware] | None,
    default_interrupt_on: dict[str, bool | InterruptOnConfig] | None,
    subagents: list[SubAgent | CompiledSubAgent],
    general_purpose_agent: bool,
    checkpointer: Any | None = None,
) -> tuple[dict[str, Any], list[str]]:
    """Create subagent instances from specifications.

    Args:
        default_model: Default model for subagents that don't specify one.
        default_tools: Default tools for subagents that don't specify tools.
        default_middleware: Middleware to apply to all subagents. If `None`,
            no default middleware is applied.
        default_interrupt_on: The tool configs to use for the default general-purpose subagent. These
            are also the fallback for any subagents that don't specify their own tool configs.
        subagents: List of agent specifications or pre-compiled agents.
        general_purpose_agent: Whether to include a general-purpose subagent.

    Returns:
        Tuple of (agent_dict, description_list) where agent_dict maps agent names
        to runnable instances and description_list contains formatted descriptions.
    """
    # Use empty list if None (no default middleware)
    default_subagent_middleware = default_middleware or []

    agents: dict[str, Any] = {}
    subagent_descriptions = []

    # Create general-purpose agent if enabled
    if general_purpose_agent:
        general_purpose_middleware = [*default_subagent_middleware]
        if default_interrupt_on:
            general_purpose_middleware.append(
                HumanInTheLoopMiddleware(interrupt_on=default_interrupt_on)
            )
        general_purpose_subagent = create_agent(
            narrow_prompt_cache_key(default_model, "general-purpose"),
            system_prompt=DEFAULT_SUBAGENT_PROMPT,
            tools=default_tools,
            middleware=general_purpose_middleware,
            name="general-purpose",
            checkpointer=checkpointer,
        )
        agents["general-purpose"] = general_purpose_subagent
        subagent_descriptions.append(
            "- general-purpose: General-purpose agent with access to all tools."
        )

    # Process custom subagents
    for agent_ in subagents:
        subagent_descriptions.append(f"- {agent_['name']}: {agent_['description']}")
        if "runnable" in agent_:
            custom_agent = cast("CompiledSubAgent", agent_)
            agents[custom_agent["name"]] = custom_agent["runnable"]
            continue
        _tools = agent_.get("tools", list(default_tools))

        subagent_model = agent_.get("model", default_model)

        _middleware = (
            [*default_subagent_middleware, *agent_["middleware"]]
            if "middleware" in agent_
            else [*default_subagent_middleware]
        )

        interrupt_on = agent_.get("interrupt_on", default_interrupt_on)
        if interrupt_on:
            _middleware.append(HumanInTheLoopMiddleware(interrupt_on=interrupt_on))

        agents[agent_["name"]] = create_agent(
            narrow_prompt_cache_key(subagent_model, agent_["name"]),
            system_prompt=agent_["system_prompt"],
            tools=_tools,
            middleware=_middleware,
            name=agent_["name"],
            checkpointer=checkpointer,
        )
    return agents, subagent_descriptions


def _create_task_tool(
    *,
    default_model: str | BaseChatModel,
    default_tools: Sequence[BaseTool | Callable | dict[str, Any]],
    default_middleware: list[AgentMiddleware] | None,
    default_interrupt_on: dict[str, bool | InterruptOnConfig] | None,
    subagents: list[SubAgent | CompiledSubAgent],
    general_purpose_agent: bool,
    task_description: str = TASK_TOOL_DESCRIPTION,
    registry: BackgroundTaskRegistry | None = None,
    checkpointer: Any | None = None,
) -> BaseTool:
    """Create a Task tool for invoking subagents.

    Args:
        default_model: Default model for subagents.
        default_tools: Default tools for subagents.
        default_middleware: Middleware to apply to all subagents.
        default_interrupt_on: The tool configs to use for the default general-purpose subagent. These
            are also the fallback for any subagents that don't specify their own tool configs.
        subagents: List of subagent specifications.
        general_purpose_agent: Whether to include general-purpose agent.
        task_description: Description for the Task tool.

    Returns:
        A StructuredTool that can invoke subagents by type.
    """
    subagent_graphs, _subagent_descriptions = _get_subagents(
        default_model=default_model,
        default_tools=default_tools,
        default_middleware=default_middleware,
        default_interrupt_on=default_interrupt_on,
        subagents=subagents,
        general_purpose_agent=general_purpose_agent,
        checkpointer=checkpointer,
    )

    def _return_command_with_state_update(result: dict, tool_call_id: str) -> Command:
        # Validate that the result contains a 'messages' key
        if "messages" not in result:
            error_msg = (
                "CompiledSubAgent must return a state containing a 'messages' key. "
                "Custom StateGraphs used with CompiledSubAgent should include 'messages' "
                "in their state schema to communicate results back to the main agent."
            )
            raise ValueError(error_msg)

        state_update = {
            k: v for k, v in result.items() if k not in _EXCLUDED_STATE_KEYS
        }
        # Strip trailing whitespace to prevent API errors with Anthropic
        message_text = (
            result["messages"][-1].text.rstrip() if result["messages"][-1].text else ""
        )
        return Command(
            update={
                **state_update,
                "messages": [ToolMessage(message_text, tool_call_id=tool_call_id)],
            }
        )

    def _validate_and_prepare_state(
        subagent_type: str, prompt: str, runtime: ToolRuntime
    ) -> tuple[Runnable, dict]:
        """Prepare state for invocation."""
        subagent = subagent_graphs[subagent_type]
        # Create a new state dict to avoid mutating the original
        subagent_state = {
            k: v for k, v in runtime.state.items() if k not in _EXCLUDED_STATE_KEYS
        }
        subagent_state["messages"] = [HumanMessage(content=prompt)]
        return subagent, subagent_state

    def _get_background_task_context() -> tuple[str | None, str | None]:
        """Return ``(checkpoint_ns, subagent_type)`` for the BackgroundTask in context, or ``(None, None)`` outside a managed background invocation."""
        if registry is None:
            return None, None
        bg_task_id = current_background_tool_call_id.get()
        if not bg_task_id:
            return None, None
        bg_task = registry.get_by_tool_call_id(bg_task_id)
        if bg_task and bg_task.completed is False:
            return f"task:{bg_task.task_id}", bg_task.subagent_type
        return None, None

    def task(
        description: Annotated[
            str,
            "Short 1-2 sentence summary of the task (displayed as title)",
        ],
        prompt: Annotated[
            str,
            "Detailed instructions for the subagent to execute",
        ],
        subagent_type: Annotated[
            str | None,
            "The type of subagent to use. Required for init action.",
        ] = None,
        action: Annotated[
            str,
            "'init' (default), 'update', or 'resume'",
        ] = "init",
        task_id: Annotated[
            str | None,
            "Task ID. Required for update and resume actions.",
        ] = None,
        runtime: ToolRuntime = None,  # type: ignore[assignment]
    ) -> str | Command:
        # Resolve subagent_type based on action
        effective_type = subagent_type
        if action == "update" or action == "resume":
            # For resume/follow-up, type is inferred; validate if explicitly provided
            _bg_checkpoint_ns, resume_type = _get_background_task_context()
            effective_type = effective_type or resume_type or "general-purpose"
            if effective_type not in subagent_graphs:
                allowed_types = ", ".join([f"`{k}`" for k in subagent_graphs])
                return f"We cannot invoke subagent {effective_type} because it does not exist, the only allowed types are {allowed_types}"
        else:
            # action == "init" (default)
            if effective_type is None:
                return "Error: subagent_type is required for new tasks."
            if effective_type not in subagent_graphs:
                allowed_types = ", ".join([f"`{k}`" for k in subagent_graphs])
                return f"We cannot invoke subagent {effective_type} because it does not exist, the only allowed types are {allowed_types}"

        subagent, subagent_state = _validate_and_prepare_state(
            effective_type, prompt, runtime
        )

        # Build config: use parent's thread_id + checkpoint_ns for isolation.
        # Drop parent callbacks entirely — the parent runtime registers its
        # own PerCallTokenTracker on the workflow run, and inheriting it
        # would double-bill every subagent LLM call (parent's tracker AND
        # bg_tracker both record on_llm_end). LangSmith tracing rides on
        # the SDK's ambient auto-tracer (ContextVar-propagated), so dropping
        # the explicit callbacks list does not affect trace coverage.
        raw_parent_config = get_config()
        parent_config = {k: v for k, v in raw_parent_config.items() if k != "callbacks"}
        parent_configurable = parent_config.get("configurable", {})

        if checkpointer:
            # Get task_id from BackgroundTask via ContextVar
            bg_tool_call_id = current_background_tool_call_id.get()
            bg_task = (
                registry.get_by_tool_call_id(bg_tool_call_id)
                if bg_tool_call_id and registry
                else None
            )
            checkpoint_ns = f"task:{bg_task.task_id}" if bg_task else ""
            config = {
                **parent_config,
                "configurable": {
                    **parent_configurable,
                    "thread_id": parent_configurable.get("thread_id", ""),
                    "checkpoint_ns": checkpoint_ns,
                },
                "metadata": {
                    "subagent_type": effective_type,
                    "description": prompt[:200],
                },
            }
        else:
            config = {}

        bg_tracker = current_background_token_tracker.get(None)
        if bg_tracker is not None:
            if not config:
                config = {}
            config["callbacks"] = [bg_tracker]

        result = subagent.invoke(subagent_state, config)
        if not runtime.tool_call_id:
            value_error_msg = "Tool call ID is required for subagent invocation"
            raise ValueError(value_error_msg)
        return _return_command_with_state_update(result, runtime.tool_call_id)

    async def atask(
        description: Annotated[
            str,
            "Short 1-2 sentence summary of the task (displayed as title)",
        ],
        prompt: Annotated[
            str,
            "Detailed instructions for the subagent to execute",
        ],
        subagent_type: Annotated[
            str | None,
            "The type of subagent to use. Required for init action.",
        ] = None,
        action: Annotated[
            str,
            "'init' (default), 'update', or 'resume'",
        ] = "init",
        task_id: Annotated[
            str | None,
            "Task ID. Required for update and resume actions.",
        ] = None,
        runtime: ToolRuntime = None,  # type: ignore[assignment]
    ) -> str | Command:
        # Resolve subagent_type based on action
        effective_type = subagent_type

        # Set for both init and resume of managed background tasks.
        bg_checkpoint_ns, resume_type = _get_background_task_context()
        has_bg_task_context = bg_checkpoint_ns is not None

        if action == "update" or action == "resume" or has_bg_task_context:
            # For resume/follow-up, type is inferred; validate if explicitly provided
            effective_type = effective_type or resume_type or "general-purpose"
            if effective_type not in subagent_graphs:
                allowed_types = ", ".join([f"`{k}`" for k in subagent_graphs])
                return f"We cannot invoke subagent {effective_type} because it does not exist, the only allowed types are {allowed_types}"
        else:
            # action == "init" (default)
            if effective_type is None:
                return "Error: subagent_type is required for new tasks."
            if effective_type not in subagent_graphs:
                allowed_types = ", ".join([f"`{k}`" for k in subagent_graphs])
                return f"We cannot invoke subagent {effective_type} because it does not exist, the only allowed types are {allowed_types}"

        subagent = subagent_graphs[effective_type]

        # Get parent config to preserve streaming namespace.
        # Drop parent callbacks entirely — the parent runtime registers its
        # own PerCallTokenTracker on the workflow run, and inheriting it
        # would double-bill every subagent LLM call. LangSmith tracing
        # rides on the SDK's ambient auto-tracer (ContextVar-propagated),
        # so dropping the explicit callbacks list does not affect coverage.
        raw_parent_config: dict[str, Any] = dict(get_config())
        parent_config: dict[str, Any] = {
            k: v for k, v in raw_parent_config.items() if k != "callbacks"
        }
        parent_configurable: dict[str, Any] = parent_config.get("configurable", {})

        def _compose_callbacks() -> list[Any]:
            bg_tracker = current_background_token_tracker.get(None)
            return [bg_tracker] if bg_tracker is not None else []

        if has_bg_task_context and checkpointer:
            # Per-task checkpoint_ns; LangGraph hydrates from prior checkpoint if present.
            invoke_state = {
                k: v for k, v in runtime.state.items() if k not in _EXCLUDED_STATE_KEYS
            }
            invoke_state["messages"] = [HumanMessage(content=prompt)]
            config = {
                **parent_config,
                "configurable": {
                    **parent_configurable,
                    "thread_id": parent_configurable.get("thread_id", ""),
                    "checkpoint_ns": bg_checkpoint_ns,
                },
                "metadata": {
                    "subagent_type": effective_type,
                    "description": prompt[:200],
                },
            }
            callbacks = _compose_callbacks()
            if callbacks:
                config["callbacks"] = callbacks

            logger.info(
                "Invoking subagent with task checkpoint_ns",
                checkpoint_ns=bg_checkpoint_ns,
                parent_thread_id=parent_configurable.get("thread_id", ""),
                subagent_type=effective_type,
            )
            result = await subagent.ainvoke(invoke_state, config)
        else:
            # New task: use parent's thread_id + checkpoint_ns for isolation
            _subagent, subagent_state = _validate_and_prepare_state(
                effective_type, prompt, runtime
            )
            if checkpointer:
                # Get task_id from BackgroundTask via ContextVar
                bg_tool_call_id = current_background_tool_call_id.get()
                bg_task = (
                    registry.get_by_tool_call_id(bg_tool_call_id)
                    if bg_tool_call_id and registry
                    else None
                )
                checkpoint_ns = f"task:{bg_task.task_id}" if bg_task else ""
                config = {
                    **parent_config,
                    "configurable": {
                        **parent_configurable,
                        "thread_id": parent_configurable.get("thread_id", ""),
                        "checkpoint_ns": checkpoint_ns,
                    },
                    "metadata": {
                        "subagent_type": effective_type,
                        "description": prompt[:200],
                    },
                }
            else:
                config = {}

            callbacks = _compose_callbacks()
            if callbacks:
                if not config:
                    config = {}
                config["callbacks"] = callbacks

            result = await subagent.ainvoke(subagent_state, config)

        if not runtime.tool_call_id:
            value_error_msg = "Tool call ID is required for subagent invocation"
            raise ValueError(value_error_msg)
        return _return_command_with_state_update(result, runtime.tool_call_id)

    return StructuredTool.from_function(
        name="Task",
        func=task,
        coroutine=atask,
        description=task_description,
    )


class SubAgentMiddleware(AgentMiddleware):
    """Middleware for providing subagents to an agent via a `Task` tool.

    This  middleware adds a `Task` tool to the agent that can be used to invoke subagents.
    Subagents are useful for handling complex tasks that require multiple steps, or tasks
    that require a lot of context to resolve.

    A chief benefit of subagents is that they can handle multi-step tasks, and then return
    a clean, concise response to the main agent.

    Subagents are also great for different domains of expertise that require a narrower
    subset of tools and focus.

    This middleware comes with a default general-purpose subagent that can be used to
    handle the same tasks as the main agent, but with isolated context.

    Args:
        default_model: The model to use for subagents.

            Can be a `LanguageModelLike` or a dict for `init_chat_model`.
        default_tools: The tools to use for the default general-purpose subagent.
        default_middleware: Default middleware to apply to all subagents.

            If `None`, no default middleware is applied.

            Pass a list to specify custom middleware.
        default_interrupt_on: The tool configs to use for the default general-purpose subagent.

            These are also the fallback for any subagents that don't specify their own tool configs.
        subagents: A list of additional subagents to provide to the agent.
        system_prompt: Additional system prompt to append. When provided, appended to
            the agent's system message via middleware.
        general_purpose_agent: Whether to include the general-purpose agent.
        task_description: Description for the Task tool.

    Example:
        ```python
        from ptc_agent.agent.middleware.background_subagent.subagent_middleware import SubAgentMiddleware
        from langchain.agents import create_agent

        # Basic usage with defaults (no default middleware)
        agent = create_agent(
            "openai:gpt-4o",
            middleware=[
                SubAgentMiddleware(
                    default_model="openai:gpt-4o",
                    subagents=[],
                )
            ],
        )

        # Add custom middleware to subagents
        agent = create_agent(
            "openai:gpt-4o",
            middleware=[
                SubAgentMiddleware(
                    default_model="openai:gpt-4o",
                    default_middleware=[TodoListMiddleware()],
                    subagents=[],
                )
            ],
        )
        ```
    """

    def __init__(
        self,
        *,
        default_model: str | BaseChatModel,
        default_tools: Sequence[BaseTool | Callable | dict[str, Any]] | None = None,
        default_middleware: list[AgentMiddleware] | None = None,
        default_interrupt_on: dict[str, bool | InterruptOnConfig] | None = None,
        subagents: list[SubAgent | CompiledSubAgent] | None = None,
        system_prompt: str | None = None,
        general_purpose_agent: bool = True,
        task_description: str = TASK_TOOL_DESCRIPTION,
        registry: BackgroundTaskRegistry | None = None,
        checkpointer: Any | None = None,
    ) -> None:
        """Initialize the `SubAgentMiddleware`."""
        super().__init__()
        self.system_prompt = system_prompt
        task_tool = _create_task_tool(
            default_model=default_model,
            default_tools=default_tools or [],
            default_middleware=default_middleware,
            default_interrupt_on=default_interrupt_on,
            subagents=subagents or [],
            general_purpose_agent=general_purpose_agent,
            task_description=task_description,
            registry=registry,
            checkpointer=checkpointer,
        )
        self.tools = [task_tool]

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelResponse:
        """Update the system message to include instructions on using subagents."""
        if self.system_prompt is not None:
            new_system_message = append_to_system_message(
                request.system_message, self.system_prompt
            )
            return handler(request.override(system_message=new_system_message))
        return handler(request)

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        """(async) Update the system message to include instructions on using subagents."""
        if self.system_prompt is not None:
            new_system_message = append_to_system_message(
                request.system_message, self.system_prompt
            )
            return await handler(request.override(system_message=new_system_message))
        return await handler(request)
