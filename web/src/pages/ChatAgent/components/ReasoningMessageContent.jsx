import React, { useState } from 'react';
import { Brain, Loader2, ChevronDown, ChevronUp } from 'lucide-react';

/**
 * ReasoningMessageContent Component
 * 
 * Renders reasoning content from message_chunk events with content_type: reasoning.
 * 
 * Features:
 * - Shows an icon indicating reasoning status (loading when active, finished when complete)
 * - Clickable icon to toggle visibility of reasoning content
 * - Reasoning content is folded by default, can be expanded on click
 * 
 * @param {Object} props
 * @param {string} props.reasoningContent - The accumulated reasoning content
 * @param {boolean} props.isReasoning - Whether reasoning is currently in progress
 * @param {boolean} props.reasoningComplete - Whether reasoning process has completed
 */
function ReasoningMessageContent({ reasoningContent, isReasoning, reasoningComplete }) {
  const [isExpanded, setIsExpanded] = useState(false);

  // Don't render if there's no reasoning content, reasoning hasn't started, and reasoning isn't complete
  if (!reasoningContent && !isReasoning && !reasoningComplete) {
    return null;
  }

  const handleToggle = () => {
    setIsExpanded(!isExpanded);
  };

  return (
    <div className="mt-2">
      {/* Reasoning indicator button */}
      <button
        onClick={handleToggle}
        className="transition-colors hover:bg-white/10"
        style={{
          boxSizing: 'border-box',
          display: 'flex',
          alignItems: 'center',
          gap: '8px',
          fontSize: '14px',
          lineHeight: '20px',
          color: 'var(--Labels-Secondary)',
          padding: '4px 12px',
          borderRadius: '6px',
          backgroundColor: isReasoning 
            ? 'rgba(97, 85, 245, 0.15)' 
            : 'transparent',
          border: isReasoning 
            ? '1px solid rgba(255, 255, 255, 0.1)' 
            : 'none',
          width: '100%',
        }}
        title={isReasoning ? 'Reasoning in progress...' : 'View reasoning process'}
      >
        {/* Icon: Brain with loading spinner when active, static brain when complete */}
        <div className="relative flex-shrink-0">
          <Brain className="h-4 w-4" style={{ color: 'var(--Labels-Secondary)' }} />
          {isReasoning && (
            <Loader2 
              className="h-3 w-3 absolute -top-0.5 -right-0.5 animate-spin" 
              style={{ color: 'var(--Labels-Secondary)' }} 
            />
          )}
        </div>
        
        {/* Label */}
        <span className="font-medium" style={{ color: 'inherit' }}>
          {isReasoning ? 'Reasoning...' : 'Reasoning'}
        </span>
        
        {/* Expand/collapse icon */}
        <div
          style={{
            flexShrink: 0,
            color: 'var(--Labels-Quaternary)',
            display: 'flex',
            alignItems: 'center',
            gap: '4px',
          }}
        >
          {isExpanded ? (
            <ChevronUp className="h-4 w-4" />
          ) : (
            <ChevronDown className="h-4 w-4" />
          )}
        </div>
      </button>

      {/* Reasoning content (shown when expanded) */}
      {isExpanded && reasoningContent && (
        <div
          className="mt-2 px-3 py-2 rounded-md text-xs"
          style={{
            backgroundColor: 'rgba(97, 85, 245, 0.1)',
            border: '1px solid rgba(97, 85, 245, 0.2)',
            color: '#FFFFFF',
            opacity: 0.9,
          }}
        >
          <p className="whitespace-pre-wrap break-words">{reasoningContent}</p>
        </div>
      )}
    </div>
  );
}

export default ReasoningMessageContent;
