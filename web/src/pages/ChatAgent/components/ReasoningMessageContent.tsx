import React, { useState } from 'react';
import { Brain, ChevronDown, ChevronUp } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import Markdown from './Markdown';

/** Module scope, not a literal: a fresh object would defeat Markdown's memo on every tick. */
const REASONING_BODY_STYLE = { borderLeft: '2px solid var(--color-border-elevated)' };

interface ReasoningMessageContentProps {
  reasoningContent: string;
  isReasoning: boolean;
  reasoningComplete: boolean;
  reasoningTitle?: string | null;
}

/**
 * ReasoningMessageContent Component
 *
 * Renders reasoning content from message_chunk events with content_type: reasoning.
 *
 * Features:
 * - Shows an icon indicating reasoning status (loading when active, finished when complete)
 * - Clickable icon to toggle visibility of reasoning content
 * - Reasoning content is folded by default, can be expanded on click
 */
function ReasoningMessageContent({ reasoningContent, isReasoning, reasoningComplete, reasoningTitle }: ReasoningMessageContentProps): React.ReactElement | null {
  const [isExpanded, setIsExpanded] = useState(isReasoning);

  // Don't render if there's no reasoning content, reasoning hasn't started, and reasoning isn't complete
  if (!reasoningContent && !isReasoning && !reasoningComplete) {
    return null;
  }

  const handleToggle = (): void => {
    setIsExpanded(!isExpanded);
  };

  return (
    <div className="mt-2">
      {/* Reasoning indicator button */}
      <button
        onClick={handleToggle}
        className="transition-colors hover:bg-foreground/10"
        style={{
          boxSizing: 'border-box',
          display: 'flex',
          alignItems: 'center',
          gap: '8px',
          fontSize: '0.875rem',
          lineHeight: '20px',
          color: 'var(--Labels-Secondary)',
          padding: '4px 12px',
          borderRadius: '6px',
          backgroundColor: isReasoning
            ? 'var(--color-bg-hover)'
            : 'transparent',
          border: isReasoning
            ? '1px solid var(--color-border-muted)'
            : 'none',
          width: '100%',
        }}
        title={isReasoning ? 'Reasoning in progress...' : 'View reasoning process'}
      >
        {/* Icon: Brain with loading spinner when active, static brain when complete */}
        <div className="relative flex-shrink-0">
          <Brain className="h-4 w-4" style={{ color: 'var(--Labels-Secondary)' }} />
          {isReasoning && (
            <span aria-hidden="true" className="absolute -top-0.5 -right-0.5">
              <Loader size={12} className="text-[color:var(--Labels-Secondary)]" />
            </span>
          )}
        </div>

        {/* Label: when complete show "Reasoning"; when streaming and title present show "Reasoning: Title"; else "Reasoning..." or "Reasoning" */}
        <span style={{ color: 'inherit' }} className="truncate min-w-0">
          {reasoningComplete
            ? 'Reasoning'
            : reasoningTitle
              ? `Reasoning: ${reasoningTitle}`
              : isReasoning
                ? 'Reasoning...'
                : 'Reasoning'}
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

      {/* Reasoning content (shown when expanded) - vertical line on left, no box */}
      {isExpanded && reasoningContent && (
        <Markdown
          variant="compact"
          content={reasoningContent}
          className="mt-2 pl-3 pr-0 py-1 text-xs"
          style={REASONING_BODY_STYLE}
        />
      )}
    </div>
  );
}

// memo'd: reasoning streams token-by-token alongside other render blocks, so
// MessageContentSegments re-renders each token. All four props are primitives
// and default shallow compare skips Markdown's AST parse when the reasoning
// content for this block is unchanged.
export default React.memo(ReasoningMessageContent);
