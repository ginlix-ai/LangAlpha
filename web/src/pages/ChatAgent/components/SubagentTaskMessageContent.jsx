import React, { useState } from 'react';
import { Bot, Loader2, ChevronDown, ChevronUp } from 'lucide-react';

/**
 * SubagentTaskMessageContent Component
 *
 * Renders a compact, clickable chunk in the main chat view to indicate that
 * a background subagent task was launched (via the `task` tool).
 *
 * - Appears in chronological order like tool call chunks.
 * - Clicking it will open a subagent floating card with more details.
 *
 * @param {Object} props
 * @param {string} props.subagentId - Logical identifier for the subagent task (usually tool_call_id)
 * @param {string} props.description - Task description (from tool args)
 * @param {string} props.type - Subagent type (e.g., "general-purpose")
 * @param {string} props.status - Task status ("running" | "completed" | "unknown")
 * @param {Function} props.onOpen - Callback when user clicks to open the subagent card
 */
function SubagentTaskMessageContent({
  subagentId,
  description,
  type = 'general-purpose',
  status = 'unknown',
  onOpen,
}) {
  const [isExpanded, setIsExpanded] = useState(false);

  // If we somehow have no basic info, don't render
  if (!subagentId && !description) {
    return null;
  }

  const handleToggle = () => {
    setIsExpanded((prev) => !prev);
  };

  const handleOpen = () => {
    console.log('[SubagentTaskMessageContent] handleOpen called, onOpen:', onOpen);
    if (onOpen) {
      console.log('[SubagentTaskMessageContent] Calling onOpen with:', { subagentId, description, type, status });
      onOpen({
        subagentId,
        description,
        type,
        status,
      });
    } else {
      console.warn('[SubagentTaskMessageContent] onOpen is not defined!');
    }
  };

  const isRunning = status === 'running';
  const isCompleted = status === 'completed';

  return (
    <div className="mt-2">
      {/* Subagent indicator button */}
      <button
        onClick={handleToggle}
        className="flex items-center gap-2 px-3 py-1.5 rounded-md transition-colors hover:bg-white/10 w-full text-left"
        style={{
          backgroundColor: isRunning
            ? 'rgba(97, 85, 245, 0.15)'
            : 'rgba(255, 255, 255, 0.05)',
          border: '1px solid rgba(255, 255, 255, 0.1)',
        }}
        title={isRunning ? 'Subagent task running...' : 'View subagent task details'}
      >
        {/* Icon: Bot with loading spinner when running */}
        <div className="relative">
          <Bot className="h-4 w-4" style={{ color: '#6155F5' }} />
          {isRunning && (
            <Loader2
              className="h-3 w-3 absolute -top-0.5 -right-0.5 animate-spin"
              style={{ color: '#6155F5' }}
            />
          )}
        </div>

        {/* Label */}
        <div className="flex flex-col gap-0.5 min-w-0">
          <span className="text-xs font-medium" style={{ color: '#FFFFFF', opacity: 0.9 }}>
            Subagent Task ({type})
          </span>
          {description && (
            <span
              className="text-xs truncate"
              style={{ color: '#FFFFFF', opacity: 0.7 }}
            >
              {description}
            </span>
          )}
        </div>

        {/* Status */}
        {isCompleted && !isRunning && (
          <span className="ml-auto text-xs" style={{ color: '#0FEDBE', opacity: 0.8 }}>
            completed
          </span>
        )}

        {/* Expand / collapse */}
        {isExpanded ? (
          <ChevronUp className="h-3 w-3 ml-2" style={{ color: '#FFFFFF', opacity: 0.6 }} />
        ) : (
          <ChevronDown className="h-3 w-3 ml-2" style={{ color: '#FFFFFF', opacity: 0.6 }} />
        )}
      </button>

      {/* Expanded details + Open button */}
      {isExpanded && (
        <div
          className="mt-2 px-3 py-2 rounded-md text-xs space-y-2"
          style={{
            backgroundColor: 'rgba(97, 85, 245, 0.08)',
            border: '1px solid rgba(97, 85, 245, 0.2)',
            color: '#FFFFFF',
            opacity: 0.9,
          }}
        >
          {description && (
            <p>
              <span className="font-semibold">Description: </span>
              {description}
            </p>
          )}
          <p>
            <span className="font-semibold">Type: </span>
            {type}
          </p>
          <p>
            <span className="font-semibold">Status: </span>
            {status}
          </p>

          <button
            onClick={handleOpen}
            className="mt-1 inline-flex items-center justify-center px-3 py-1.5 rounded-md text-xs font-medium transition-colors"
            style={{
              backgroundColor: '#6155F5',
              color: '#FFFFFF',
            }}
          >
            Open subagent details
          </button>
        </div>
      )}
    </div>
  );
}

export default SubagentTaskMessageContent;

