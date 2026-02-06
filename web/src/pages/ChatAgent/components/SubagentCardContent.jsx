import React from 'react';
import { Bot, Loader2, CheckCircle2, Circle } from 'lucide-react';
import TextMessageContent from './TextMessageContent';
import ReasoningMessageContent from './ReasoningMessageContent';
import ToolCallMessageContent from './ToolCallMessageContent';

/**
 * SubagentCardContent Component
 * 
 * Renders subagent work content for the floating card.
 * Displays subagent messages, reasoning, tool calls, and status.
 * 
 * @param {Object} props
 * @param {string} props.taskId - Task ID (e.g., "Task-1")
 * @param {string} props.description - Task description
 * @param {string} props.type - Subagent type (e.g., "general-purpose")
 * @param {number} props.toolCalls - Number of tool calls made
 * @param {string} props.currentTool - Current tool being used
 * @param {Object} props.messages - Subagent messages state (similar to main chat messages)
 * @param {boolean} props.isHistory - Whether this card is shown from history replay (hides status/header)
 */
function SubagentCardContent({ 
  taskId, 
  description, 
  type, 
  toolCalls = 0, 
  currentTool = '', 
  messages = [],
  isHistory = false,
}) {
  /**
   * Get status icon based on current state
   */
  const getStatusIcon = () => {
    if (currentTool) {
      return <Loader2 className="h-4 w-4 animate-spin" style={{ color: '#6155F5' }} />;
    }
    return <Circle className="h-4 w-4" style={{ color: '#FFFFFF', opacity: 0.5 }} />;
  };

  /**
   * Get status text
   */
  const getStatusText = () => {
    if (currentTool) {
      return `Running: ${currentTool}`;
    }
    if (toolCalls > 0) {
      return `Completed (${toolCalls} tool calls)`;
    }
    return 'Initializing';
  };

  return (
    <div className="space-y-3">
      {/* Header with task info (hidden for history cards to avoid showing internal IDs) */}
      {!isHistory && (
        <div className="flex items-center gap-2 pb-2" style={{ borderBottom: '1px solid rgba(255, 255, 255, 0.1)' }}>
          <Bot className="h-4 w-4" style={{ color: '#6155F5' }} />
          <span className="text-sm font-semibold" style={{ color: '#FFFFFF' }}>
            {taskId}
          </span>
          <span className="text-xs ml-auto" style={{ color: '#FFFFFF', opacity: 0.6 }}>
            {type}
          </span>
        </div>
      )}

      {/* Task description */}
      {description && (
        <div className="text-xs" style={{ color: '#FFFFFF', opacity: 0.8 }}>
          {description}
        </div>
      )}

      {/* Status indicator (hidden for history cards where status is less relevant) */}
      {!isHistory && (
        <div className="flex items-center gap-2 text-xs" style={{ color: '#FFFFFF', opacity: 0.7 }}>
          {getStatusIcon()}
          <span>{getStatusText()}</span>
        </div>
      )}

      {/* Messages content */}
      {messages.length > 0 && (
        <div className="space-y-2 max-h-64 overflow-y-auto" style={{ borderTop: '1px solid rgba(255, 255, 255, 0.1)', paddingTop: '8px' }}>
          {messages.map((msg) => {
            // Render message based on type
            if (msg.role === 'assistant') {
              // Render content segments in order
              const segments = msg.contentSegments || [];
              return (
                <div key={msg.id} className="space-y-1">
                  {segments
                    .sort((a, b) => (a.order || 0) - (b.order || 0))
                    .map((segment, idx) => {
                      if (segment.type === 'reasoning') {
                        const reasoning = msg.reasoningProcesses?.[segment.reasoningId];
                        if (reasoning) {
                          // Debug: Log reasoning content to help identify issues
                          if (process.env.NODE_ENV === 'development' && !reasoning.content) {
                            console.warn('[SubagentCardContent] Reasoning process exists but content is empty:', {
                              reasoningId: segment.reasoningId,
                              reasoning,
                              messageId: msg.id,
                            });
                          }
                          return (
                            <ReasoningMessageContent
                              key={`${msg.id}-reasoning-${idx}`}
                              reasoningContent={reasoning.content || ''}
                              isReasoning={reasoning.isReasoning || false}
                              reasoningComplete={reasoning.reasoningComplete || false}
                            />
                          );
                        } else if (process.env.NODE_ENV === 'development') {
                          console.warn('[SubagentCardContent] Reasoning segment found but no reasoning process:', {
                            reasoningId: segment.reasoningId,
                            availableReasoningIds: Object.keys(msg.reasoningProcesses || {}),
                            messageId: msg.id,
                          });
                        }
                      } else if (segment.type === 'tool_call') {
                        const toolCall = msg.toolCallProcesses?.[segment.toolCallId];
                        if (toolCall) {
                          return (
                            <ToolCallMessageContent
                              key={`${msg.id}-tool-${idx}`}
                              toolName={toolCall.toolName || 'Unknown Tool'}
                              toolCall={toolCall.toolCall}
                              toolCallResult={toolCall.toolCallResult}
                              isInProgress={toolCall.isInProgress || false}
                              isComplete={toolCall.isComplete || false}
                            />
                          );
                        }
                      } else if (segment.type === 'text') {
                        return (
                          <TextMessageContent
                            key={`${msg.id}-text-${idx}`}
                            content={segment.content || ''}
                          />
                        );
                      }
                      return null;
                    })}
                </div>
              );
            }
            return null;
          })}
        </div>
      )}
    </div>
  );
}

export default SubagentCardContent;
