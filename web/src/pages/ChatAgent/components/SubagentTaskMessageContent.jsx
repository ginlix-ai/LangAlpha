import React from 'react';
import { Check, Loader2, ArrowRight } from 'lucide-react';
import iconRoboSing from '../../../assets/img/icon-robo-sing.svg';
import './AgentSidebar.css';

/**
 * SubagentTaskMessageContent Component
 *
 * Renders a prominent, clickable card in the main chat view to indicate that
 * a background subagent task was launched (via the `task` tool).
 *
 * Two click targets:
 * - Entire card → opens subagent tab (via onOpen)
 * - "View output" link → opens detail panel (via onDetailOpen)
 *
 * @param {Object} props
 * @param {string} props.subagentId - Logical identifier for the subagent task (usually tool_call_id)
 * @param {string} props.description - Task description (from tool args)
 * @param {string} props.type - Subagent type (e.g., "general-purpose")
 * @param {string} props.status - Task status ("running" | "completed" | "unknown")
 * @param {Function} props.onOpen - Callback when user clicks to open the subagent tab
 * @param {Function} props.onDetailOpen - Callback to open the result in DetailPanel
 * @param {Object} props.toolCallProcess - The tool_call_process object for this Task tool call
 */
function SubagentTaskMessageContent({
  subagentId,
  description,
  type = 'general-purpose',
  status = 'unknown',
  onOpen,
  onDetailOpen,
  toolCallProcess,
}) {
  if (!subagentId && !description) {
    return null;
  }

  const isRunning = status === 'running';
  const isCompleted = status === 'completed';
  const hasResult = isCompleted && toolCallProcess?.toolCallResult?.content;

  const handleCardClick = () => {
    if (onOpen) {
      onOpen({ subagentId, description, type, status });
    }
  };

  const handleViewOutput = (e) => {
    e.stopPropagation();
    if (onDetailOpen && toolCallProcess) {
      onDetailOpen(toolCallProcess);
    }
  };

  return (
    <div className="my-2">
      <button
        onClick={handleCardClick}
        className="flex items-start gap-3 px-4 py-3 rounded-lg transition-colors hover:brightness-110 w-full text-left"
        style={{
          backgroundColor: isRunning
            ? 'rgba(97, 85, 245, 0.15)'
            : 'rgba(97, 85, 245, 0.08)',
          borderLeft: '4px solid #6155F5',
          borderTop: '1px solid rgba(97, 85, 245, 0.2)',
          borderRight: '1px solid rgba(97, 85, 245, 0.2)',
          borderBottom: '1px solid rgba(97, 85, 245, 0.2)',
        }}
        title={isRunning ? 'Click to view running subagent' : 'Click to view subagent details'}
      >
        {/* Icon with pulse animation when running */}
        <div className="relative flex-shrink-0 mt-0.5">
          <img
            src={iconRoboSing}
            alt="Subagent"
            className={`w-6 h-6 ${isRunning ? 'agent-tab-active-pulse' : ''}`}
          />
          {isRunning && (
            <Loader2
              className="h-3 w-3 absolute -bottom-0.5 -right-0.5 animate-spin"
              style={{ color: '#6155F5' }}
            />
          )}
        </div>

        {/* Content */}
        <div className="flex flex-col gap-1.5 min-w-0 flex-1">
          {/* Title + status */}
          <div className="flex items-center justify-between gap-2">
            <span className="text-sm font-medium" style={{ color: '#FFFFFF', opacity: 0.9 }}>
              Subagent Task
              <span className="font-normal ml-1.5" style={{ opacity: 0.5 }}>({type})</span>
            </span>
            <span className="flex items-center gap-1 text-xs flex-shrink-0" style={{
              color: isRunning ? '#6155F5' : isCompleted ? '#0FEDBE' : 'rgba(255, 255, 255, 0.4)',
            }}>
              {isRunning && <Loader2 className="h-3 w-3 animate-spin" />}
              {isCompleted && <Check className="h-3 w-3" />}
              {isRunning ? 'Running' : isCompleted ? 'Completed' : status}
            </span>
          </div>

          {/* Description */}
          {description && (
            <span
              className="text-sm leading-relaxed font-normal"
              style={{
                color: '#FFFFFF',
                opacity: 0.65,
                display: '-webkit-box',
                WebkitLineClamp: 3,
                WebkitBoxOrient: 'vertical',
                overflow: 'hidden',
              }}
            >
              {description}
            </span>
          )}

          {/* View output link - only when completed with result */}
          {hasResult && (
            <div className="pt-1" style={{ borderTop: '1px solid rgba(255, 255, 255, 0.08)' }}>
              <span
                onClick={handleViewOutput}
                className="inline-flex items-center gap-1 text-xs cursor-pointer hover:underline"
                style={{ color: '#6155F5' }}
                role="button"
                tabIndex={0}
                onKeyDown={(e) => { if (e.key === 'Enter') handleViewOutput(e); }}
              >
                View output
                <ArrowRight className="h-3 w-3" />
              </span>
            </div>
          )}
        </div>
      </button>
    </div>
  );
}

export default SubagentTaskMessageContent;
