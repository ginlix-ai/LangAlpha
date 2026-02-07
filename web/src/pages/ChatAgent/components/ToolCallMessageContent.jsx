import React, { useState } from 'react';
import { Wrench, Loader2, ChevronDown, ChevronUp } from 'lucide-react';

/**
 * ToolCallMessageContent Component
 * 
 * Renders tool call information from tool_calls and tool_call_result events.
 * 
 * Features:
 * - Shows an icon indicating tool call status (loading when in progress, finished when complete)
 * - Displays tool name (e.g., "write_file")
 * - Clickable icon to toggle visibility of tool call details
 * - Tool call details are folded by default, can be expanded on click
 * - Displays tool_calls and tool_call_result with different visual styles
 * 
 * @param {Object} props
 * @param {string} props.toolCallId - Unique identifier for this tool call
 * @param {string} props.toolName - Name of the tool (e.g., "write_file")
 * @param {Object} props.toolCall - Complete tool_calls event data
 * @param {Object} props.toolCallResult - tool_call_result event data
 * @param {boolean} props.isInProgress - Whether tool call is currently in progress
 * @param {boolean} props.isComplete - Whether tool call has completed
 * @param {boolean} props.isFailed - Whether tool call failed
 */
function ToolCallMessageContent({ 
  toolCallId, 
  toolName, 
  toolCall, 
  toolCallResult, 
  isInProgress, 
  isComplete,
  isFailed = false
}) {
  const [isExpanded, setIsExpanded] = useState(false);

  // Don't render if there's no tool call data
  if (!toolName && !toolCall) {
    return null;
  }

  const handleToggle = () => {
    setIsExpanded(!isExpanded);
  };

  // Determine display name
  const displayName = toolName || toolCall?.name || 'Tool Call';

  return (
    <div className="mt-2">
      {/* Tool call indicator button */}
      <button
        onClick={handleToggle}
        className="flex items-center gap-2 px-3 py-1.5 rounded-md transition-colors hover:bg-white/10"
        style={{
          backgroundColor: isInProgress 
            ? 'rgba(97, 85, 245, 0.15)' 
            : 'rgba(255, 255, 255, 0.05)',
          border: '1px solid rgba(255, 255, 255, 0.1)',
        }}
        title={isInProgress ? 'Tool call in progress...' : 'View tool call details'}
      >
        {/* Icon: Wrench with loading spinner when active, static wrench when complete/failed */}
        <div className="relative">
          <Wrench 
            className="h-4 w-4" 
            style={{ color: isFailed ? '#FF383C' : '#6155F5' }} 
          />
          {isInProgress && (
            <Loader2 
              className="h-3 w-3 absolute -top-0.5 -right-0.5 animate-spin" 
              style={{ color: '#6155F5' }} 
            />
          )}
        </div>
        
        {/* Tool name label */}
        <span className="text-xs font-medium" style={{ color: '#FFFFFF', opacity: 0.9 }}>
          {displayName}
        </span>
        
        {/* Status indicator */}
        {isComplete && !isInProgress && (
          <span 
            className="text-xs" 
            style={{ 
              color: isFailed ? '#FF383C' : '#FFFFFF', 
              opacity: isFailed ? 0.9 : 0.6 
            }}
          >
            {isFailed ? '(failed)' : '(complete)'}
          </span>
        )}
        
        {/* Expand/collapse icon */}
        {isExpanded ? (
          <ChevronUp className="h-3 w-3 ml-auto" style={{ color: '#FFFFFF', opacity: 0.6 }} />
        ) : (
          <ChevronDown className="h-3 w-3 ml-auto" style={{ color: '#FFFFFF', opacity: 0.6 }} />
        )}
      </button>

      {/* Tool call details (shown when expanded) */}
      {isExpanded && (
        <div
          className="mt-2 space-y-3"
          style={{
            backgroundColor: 'rgba(97, 85, 245, 0.1)',
            border: '1px solid rgba(97, 85, 245, 0.2)',
            borderRadius: '6px',
            padding: '12px',
          }}
        >
          {/* Tool Call (complete call data) */}
          {toolCall && (
            <div>
              <p className="text-xs font-semibold mb-2" style={{ color: '#FFFFFF', opacity: 0.8 }}>
                Tool Call:
              </p>
              <div
                className="px-3 py-2 rounded text-xs"
                style={{
                  backgroundColor: 'rgba(0, 0, 0, 0.2)',
                  color: '#FFFFFF',
                  opacity: 0.9,
                }}
              >
                <div className="mb-1">
                  <span className="font-semibold">Name:</span> {toolCall.name}
                </div>
                {toolCall.args && (
                  <div className="mt-2">
                    <span className="font-semibold">Arguments:</span>
                    <pre className="mt-1 font-mono text-xs whitespace-pre-wrap break-words">
                      {JSON.stringify(toolCall.args, null, 2)}
                    </pre>
                  </div>
                )}
                {toolCall.id && (
                  <div className="mt-2 text-xs" style={{ opacity: 0.7 }}>
                    <span className="font-semibold">ID:</span> {toolCall.id}
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Tool Call Result */}
          {toolCallResult && (
            <div>
              <p className="text-xs font-semibold mb-2" style={{ color: '#FFFFFF', opacity: 0.8 }}>
                Result:
              </p>
              <div
                className="px-3 py-2 rounded text-xs whitespace-pre-wrap break-words"
                style={{
                  backgroundColor: toolCallResult.content?.includes('ERROR') 
                    ? 'rgba(255, 56, 60, 0.15)' 
                    : 'rgba(15, 237, 190, 0.15)',
                  border: `1px solid ${toolCallResult.content?.includes('ERROR') 
                    ? 'rgba(255, 56, 60, 0.3)' 
                    : 'rgba(15, 237, 190, 0.3)'}`,
                  color: '#FFFFFF',
                  opacity: 0.9,
                }}
              >
                {toolCallResult.content || 'No result content'}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default ToolCallMessageContent;
