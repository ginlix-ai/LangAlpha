import React from 'react';
import { Check, Loader2, ArrowRight } from 'lucide-react';
import iconRoboSing from '../../../assets/img/icon-robo-sing.svg';
import './AgentSidebar.css';

/**
 * Extract a short one-line summary from a full task description.
 * Takes the first sentence or first line, truncated to maxLen chars.
 */
function summarize(text, maxLen = 100) {
  if (!text || typeof text !== 'string') return '';
  // Take first line only
  const firstLine = text.split(/\n/)[0].trim();
  // Remove trailing colon (often "Research X comprehensively. Cover:")
  const cleaned = firstLine.replace(/:$/, '');
  if (cleaned.length <= maxLen) return cleaned;
  return cleaned.slice(0, maxLen).replace(/\s+\S*$/, '') + '…';
}

/**
 * SubagentTaskMessageContent Component
 *
 * Renders a compact, clickable card in the main chat view to indicate that
 * a background subagent task was launched (via the `task` tool).
 * Shows only a short summary — click to open the subagent tab for full details.
 *
 * @param {Object} props
 * @param {string} props.subagentId - Logical identifier for the subagent task
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
  const summary = summarize(description);

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
        className="flex items-center gap-2.5 px-3.5 py-2.5 w-full text-left rounded-lg transition-colors hover:bg-white/5"
        style={{
          backgroundColor: isRunning
            ? 'rgba(97, 85, 245, 0.12)'
            : 'rgba(97, 85, 245, 0.06)',
          border: '1px solid rgba(97, 85, 245, 0.2)',
        }}
        title={isRunning ? 'Click to view running subagent' : 'Click to view subagent details'}
      >
        {/* Icon */}
        <div className="relative flex-shrink-0">
          <img
            src={iconRoboSing}
            alt="Subagent"
            className={`w-5 h-5 ${isRunning ? 'agent-tab-active-pulse' : ''}`}
          />
          {isRunning && (
            <Loader2
              className="h-2.5 w-2.5 absolute -bottom-0.5 -right-0.5 animate-spin"
              style={{ color: '#6155F5' }}
            />
          )}
        </div>

        {/* Summary text — single line */}
        <span
          className="text-xs flex-1 min-w-0 truncate"
          style={{ color: 'rgba(255, 255, 255, 0.7)' }}
        >
          {summary || 'Subagent Task'}
        </span>

        {/* Type badge */}
        <span className="text-xs flex-shrink-0" style={{ color: 'rgba(255, 255, 255, 0.3)' }}>
          {type}
        </span>

        {/* Status */}
        <span className="flex items-center gap-1 text-xs flex-shrink-0" style={{
          color: isRunning ? '#6155F5' : isCompleted ? '#0FEDBE' : 'rgba(255, 255, 255, 0.4)',
        }}>
          {isRunning && <Loader2 className="h-3 w-3 animate-spin" />}
          {isCompleted && <Check className="h-3 w-3" />}
          {isRunning ? 'Running' : isCompleted ? 'Completed' : status}
        </span>

        {/* View output arrow (only when completed) */}
        {hasResult && (
          <ArrowRight
            className="h-3.5 w-3.5 flex-shrink-0"
            style={{ color: '#6155F5' }}
            onClick={handleViewOutput}
          />
        )}
      </button>
    </div>
  );
}

export default SubagentTaskMessageContent;
