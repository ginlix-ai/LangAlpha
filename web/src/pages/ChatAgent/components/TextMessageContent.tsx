import React from 'react';
import { AlertTriangle } from 'lucide-react';
import Markdown from './Markdown';
import { useAnimatedText } from '@/components/ui/animated-text';
import { parseErrorMessage, type ParsedError } from '../utils/parseErrorMessage';

interface TextMessageContentProps {
  content: string;
  isStreaming: boolean;
  hasError: boolean;
  onOpenFile?: (path: string, workspaceId?: string) => void;
}

/**
 * TextMessageContent Component
 *
 * Renders text content from message_chunk events with content_type: text.
 * Supports markdown formatting including bold, italic, lists, code blocks, etc.
 */
function TextMessageContent({ content, isStreaming, hasError, onOpenFile }: TextMessageContentProps): React.ReactElement | null {
  const displayText = useAnimatedText(content || '', { enabled: isStreaming });

  if (!content) {
    return null;
  }

  if (hasError) {
    const parsed = parseErrorMessage(content);
    return <ErrorDisplay parsed={parsed} />;
  }

  return (
    <Markdown variant="chat" content={displayText} className="text-base" onOpenFile={onOpenFile} />
  );
}

interface ErrorDisplayProps {
  parsed: ParsedError;
}

/**
 * ErrorDisplay Component
 *
 * Renders a parsed error message in a clean, structured format.
 */
function ErrorDisplay({ parsed }: ErrorDisplayProps): React.ReactElement {
  return (
    <div
      className="flex gap-3 px-4 py-3 rounded-lg text-sm"
      style={{
        backgroundColor: 'var(--color-loss-soft)',
        border: '1px solid var(--color-border-loss)',
      }}
    >
      <AlertTriangle
        className="h-5 w-5 flex-shrink-0 mt-0.5"
        style={{ color: 'var(--color-loss)' }}
      />
      <div className="min-w-0 space-y-1">
        <div className="font-medium" style={{ color: 'var(--color-loss)' }}>
          {parsed.title}
        </div>
        {parsed.detail && (
          <div style={{ color: 'var(--color-text-tertiary)' }}>
            {parsed.detail}
          </div>
        )}
        {parsed.model && (
          <div
            className="inline-block px-2 py-0.5 rounded text-xs mt-1"
            style={{
              backgroundColor: 'var(--color-border-muted)',
              color: 'var(--color-text-tertiary)',
            }}
          >
            {parsed.model}
            {parsed.statusCode ? ` · ${parsed.statusCode}` : ''}
          </div>
        )}
      </div>
    </div>
  );
}

export default TextMessageContent;
// eslint-disable-next-line react-refresh/only-export-components
export { parseErrorMessage, ErrorDisplay };
