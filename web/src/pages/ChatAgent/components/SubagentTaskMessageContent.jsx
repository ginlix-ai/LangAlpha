import React from 'react';
import { Bot, Loader2 } from 'lucide-react';

/**
 * SubagentTaskMessageContent Component
 *
 * Renders a compact, clickable chunk in the main chat view to indicate that
 * a background subagent task was launched (via the `task` tool).
 *
 * - Appears in chronological order like tool call chunks.
 * - Clicking it will open the subagent panel with details.
 *
 * @param {Object} props
 * @param {string} props.subagentId - Logical identifier for the subagent task (usually tool_call_id)
 * @param {string} props.description - Task description (from tool args)
 * @param {string} props.type - Subagent type (e.g., "general-purpose")
 * @param {string} props.status - Task status ("running" | "completed" | "unknown")
 * @param {Function} props.onOpen - Callback when user clicks to open the subagent panel
 */
function SubagentTaskMessageContent({
  subagentId,
  description,
  type = 'general-purpose',
  status = 'unknown',
  onOpen,
}) {
  // If we somehow have no basic info, don't render
  if (!subagentId && !description) {
    return null;
  }

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
      {/* Subagent indicator button - click to open panel */}
      <button
        onClick={handleOpen}
        className="flex items-start gap-3 px-3 py-2.5 rounded-md transition-colors hover:bg-white/10 w-full text-left"
        style={{
          backgroundColor: isRunning
            ? 'rgba(97, 85, 245, 0.15)'
            : 'rgba(255, 255, 255, 0.05)',
          border: '1px solid rgba(255, 255, 255, 0.1)',
        }}
        title={isRunning ? 'Click to view running subagent' : 'Click to view subagent details'}
      >
        {/* Icon: Bot with loading spinner when running */}
        <div className="relative flex-shrink-0 mt-0.5">
          <Bot className="h-5 w-5" style={{ color: '#6155F5' }} />
          {isRunning && (
            <Loader2
              className="h-3.5 w-3.5 absolute -top-0.5 -right-0.5 animate-spin"
              style={{ color: '#6155F5' }}
            />
          )}
        </div>

        {/* Label */}
        <div className="flex flex-col gap-1 min-w-0 flex-1">
          <span className="text-sm font-medium leading-relaxed" style={{ color: '#FFFFFF', opacity: 0.9 }}>
            Subagent Task ({type})
          </span>
          {description && (
            <span
              className="text-sm leading-relaxed line-clamp-2 font-normal"
              style={{ 
                color: '#FFFFFF', 
                opacity: 0.7,
                fontWeight: 400,
                display: '-webkit-box',
                WebkitLineClamp: 2,
                WebkitBoxOrient: 'vertical',
                overflow: 'hidden',
              }}
            >
              {description}
            </span>
          )}
        </div>

        {/* Status */}
        {isCompleted && !isRunning && (
          <span className="ml-3 text-sm flex-shrink-0" style={{ color: '#0FEDBE', opacity: 0.8 }}>
            completed
          </span>
        )}
      </button>
    </div>
  );
}

export default SubagentTaskMessageContent;

