import React from 'react';
import { AlertTriangle } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import Markdown from './Markdown';
import { useAnimatedText } from '@/components/ui/animated-text';
import { parseErrorMessage, type ParsedError } from '../utils/parseErrorMessage';
import { UPSTREAM_HINT_I18N_KEY, type StructuredError } from '@/utils/rateLimitError';

interface TextMessageContentProps {
  content: string;
  isStreaming: boolean;
  hasError: boolean;
  /** When the backend classified the failure (currently ``upstream`` only —
   *  internal errors render at the chat-input banner, not inline), use the
   *  structured fields instead of re-parsing the raw message text. */
  structuredError?: StructuredError;
  onOpenFile?: (path: string, workspaceId?: string) => void;
}

/**
 * TextMessageContent Component
 *
 * Renders text content from message_chunk events with content_type: text.
 * Supports markdown formatting including bold, italic, lists, code blocks, etc.
 */
function TextMessageContent({ content, isStreaming, hasError, structuredError, onOpenFile }: TextMessageContentProps): React.ReactElement | null {
  const displayText = useAnimatedText(content || '', { enabled: isStreaming });

  if (!content) {
    return null;
  }

  if (hasError) {
    if (structuredError?.kind === 'upstream') {
      return <StructuredErrorDisplay err={structuredError} fallbackText={content} />;
    }
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

interface StructuredErrorDisplayProps {
  err: StructuredError;
  /** Raw error text to fall back to when ``err.message`` is empty. */
  fallbackText: string;
}

/**
 * StructuredErrorDisplay
 *
 * Inline error card for classified upstream-provider failures. Renders the
 * kind-aware headline (with status code when known), the raw error message,
 * and the hint list ("check your API key", etc.). Used instead of the
 * regex-based ``ErrorDisplay`` when the backend has already classified the
 * error source — ``parseErrorMessage`` can't tell "Authentication error" from
 * the provider apart from the kind-routed message our own service returns.
 */
function StructuredErrorDisplay({ err, fallbackText }: StructuredErrorDisplayProps): React.ReactElement {
  const { t } = useTranslation();
  const headline = err.statusCode
    ? t('chat.errorUpstreamHeadlineStatus', { status: err.statusCode })
    : t('chat.errorUpstreamHeadline');
  const body = err.message || fallbackText;
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
          {headline}
        </div>
        <div className="break-words" style={{ color: 'var(--color-text-tertiary)' }}>
          {body}
        </div>
        {err.hints && err.hints.length > 0 && (
          <ul className="mt-1 list-disc pl-4 flex flex-col gap-0.5 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            {err.hints.map((h) => (
              <li key={h}>{t(UPSTREAM_HINT_I18N_KEY[h] ?? h)}</li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

// memo'd: every streaming token re-renders MessageContentSegments, which maps
// over earlier text blocks with unchanged content. Default shallow compare on
// primitive props + stable onOpenFile (useCallback in ChatView/SharedChatView)
// skips Markdown's AST parse for those stable blocks. Non-primitive props added
// later must be referentially stable or memoization becomes a no-op.
export default React.memo(TextMessageContent);
// eslint-disable-next-line react-refresh/only-export-components
export { parseErrorMessage, ErrorDisplay, StructuredErrorDisplay };
