import React, { useLayoutEffect, useState } from 'react';
import { AlertTriangle } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import Markdown from './Markdown';
import { useModelName } from '@/hooks/useModelName';
import { useAnimatedText } from '@/components/ui/animated-text';
import { visibleParagraphPrefix } from '@/lib/paragraphGate';
import { useTranscriptDisplay } from '@/lib/transcriptDisplay';
import { parseErrorMessage, type ParsedError } from '../utils/parseErrorMessage';
import type { OpenFileHandler } from '../utils/fileLocation';
import { UPSTREAM_HINT_I18N_KEY, type StructuredError } from '@/utils/rateLimitError';
import { ErrorLink, NoticeLine } from '@/components/ui/error-banner';

interface TextMessageContentProps {
  content: string;
  isStreaming: boolean;
  onOpenFile?: OpenFileHandler;
  /** Whether everything that has arrived is on screen. The typewriter trails
   *  the stream, so what follows this block can wait for it to catch up.
   *  Called from a layout effect, so a reader deciding in a layout effect of
   *  its own (a turn settling, see `ProseTyping`) has this commit's answer. */
  onRevealed?: (done: boolean) => void;
}

/**
 * TextMessageContent Component
 *
 * Renders text content from message_chunk events with content_type: text.
 * Supports markdown formatting including bold, italic, lists, code blocks, etc.
 * A failed turn's text renders here as text too: its failure is not part of
 * the content, and the bubble shows it once as a `TurnErrorCard`.
 */
function TextMessageContent({ content, isStreaming, onOpenFile, onRevealed }: TextMessageContentProps): React.ReactElement | null {
  const { streamingMode } = useTranscriptDisplay();
  // Paragraph mode holds the sentence being written, so the typewriter has
  // nothing left to animate: a paragraph lands whole or not at all.
  const gated = streamingMode === 'paragraph' && isStreaming;
  const text = content || '';
  // Whatever this bubble has already painted stays painted. The gate is a
  // floor on what to show, never a reason to take words back: switching the
  // delivery preference mid-reply used to drop the text between the last
  // blank line and the caret, and a reply with no blank line yet vanished
  // whole. The mark is per stream, so a regenerate starts from nothing. It is
  // state set during render, not a ref, so the compiler sees it as an input.
  const [shownLen, setShownLen] = useState(() => (isStreaming ? 0 : text.length));
  const target = gated ? visibleParagraphPrefix(text) : text;
  const visibleLen = Math.max(target.length, isStreaming ? Math.min(shownLen, text.length) : 0);
  if (visibleLen !== shownLen) setShownLen(visibleLen);
  const visible = text.slice(0, visibleLen);
  const displayText = useAnimatedText(visible, { enabled: isStreaming && !gated });
  // Measured against everything that has ARRIVED, not against what the gate
  // chose to release. What follows this block waits on this flag, so comparing
  // against the released prefix called a held-back paragraph "shown" and let an
  // activity block overtake the prose it belongs under.
  const revealed = !content || displayText === text;
  useLayoutEffect(() => { onRevealed?.(revealed); }, [onRevealed, revealed]);

  if (!content) {
    return null;
  }

  // Nothing has crossed a paragraph boundary yet. Render nothing rather than an
  // empty Markdown root, which would take the block's vertical rhythm with it.
  if (gated && !visible) {
    return null;
  }

  return (
    // The typewriter still writes after the stream ends, until it catches up.
    <Markdown variant="chat" content={displayText} className="text-base" onOpenFile={onOpenFile} streaming={isStreaming || displayText !== text} />
  );
}

/**
 * Why a turn failed, drawn once in its bubble after whatever it streamed. The
 * kind is the server's classification, so a classified failure gets its
 * headline from the kind rather than from keywords in its text; only a
 * failure nobody classified (a client-side send failure, an older server's
 * frame) is still read by `parseErrorMessage`.
 */
function TurnErrorCard({ err }: { err: StructuredError }): React.ReactElement {
  const { t } = useTranslation();
  if (err.kind === 'notice') return <NoticeLine message={err.message} className="py-1.5" />;
  if (err.kind === 'upstream') return <StructuredErrorDisplay err={err} />;
  if (err.kind === 'internal') {
    return <ErrorDisplay parsed={{ title: t('chat.errorInternalHeadline'), detail: err.message || null, model: null, statusCode: null }} />;
  }
  return <ErrorDisplay parsed={parseErrorMessage(err.message)} />;
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
  const name = useModelName();
  return (
    <div
      className="flex gap-3 px-4 py-3 rounded-lg text-sm"
      style={{
        backgroundColor: 'var(--color-loss-soft)',
        border: '1px solid var(--color-border-loss)',
      }}
    >
      <AlertTriangle
        className="h-5 w-5 shrink-0 mt-0.5"
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
            className="inline-block px-2 py-0.5 rounded text-xs"
            style={{
              backgroundColor: 'var(--color-border-muted)',
              color: 'var(--color-text-tertiary)',
            }}
          >
            {name(parsed.model)}
            {parsed.statusCode ? ` · ${parsed.statusCode}` : ''}
          </div>
        )}
      </div>
    </div>
  );
}

interface StructuredErrorDisplayProps {
  err: StructuredError;
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
function StructuredErrorDisplay({ err }: StructuredErrorDisplayProps): React.ReactElement {
  const { t } = useTranslation();
  const name = useModelName();
  // When the failure is attributable to a specific model, name it in the
  // headline; otherwise fall back to the generic "model provider" copy.
  const headline = err.model
    ? (err.statusCode
        ? t('chat.errorUpstreamHeadlineModelStatus', { model: name(err.model), status: err.statusCode })
        : t('chat.errorUpstreamHeadlineModel', { model: name(err.model) }))
    : (err.statusCode
        ? t('chat.errorUpstreamHeadlineStatus', { status: err.statusCode })
        : t('chat.errorUpstreamHeadline'));
  // Models other than the primary that were also tried before giving up. Only
  // shown when the middleware actually attempted more than one model.
  const alsoTried = (err.attemptedModels && err.attemptedModels.length > 1)
    ? err.attemptedModels.filter((m) => m.model !== err.model)
    : [];
  return (
    <div
      className="flex gap-3 px-4 py-3 rounded-lg text-sm"
      style={{
        backgroundColor: 'var(--color-loss-soft)',
        border: '1px solid var(--color-border-loss)',
      }}
    >
      <AlertTriangle
        className="h-5 w-5 shrink-0 mt-0.5"
        style={{ color: 'var(--color-loss)' }}
      />
      <div className="min-w-0 space-y-1">
        <div className="font-medium" style={{ color: 'var(--color-loss)' }}>
          {headline}
        </div>
        {err.message && (
          <div className="wrap-break-word" style={{ color: 'var(--color-text-tertiary)' }}>
            {err.message}
          </div>
        )}
        {err.links && err.links.length > 0 && (
          <div className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
            {err.links.map((l, i) => (
              <React.Fragment key={`${l.url}|${l.label}`}>
                {i > 0 && <span aria-hidden="true"> · </span>}
                <ErrorLink {...l} />
              </React.Fragment>
            ))}
          </div>
        )}
        {err.hints && err.hints.length > 0 && (
          <ul className="mt-1 list-disc pl-4 flex flex-col gap-0.5 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            {err.hints.map((h) => (
              <li key={h}>{t(UPSTREAM_HINT_I18N_KEY[h] ?? h)}</li>
            ))}
          </ul>
        )}
        {alsoTried.length > 0 && (
          <div className="mt-1 text-xs" style={{ color: 'var(--color-text-quaternary)' }}>
            {t('chat.errorAttemptedModels')}{' '}
            {alsoTried.map((m, i) => (
              <React.Fragment key={m.model}>
                {i > 0 && ', '}
                {/* Per-model error text lives in the tooltip, not inline. */}
                <span title={m.error || undefined}>{name(m.model)}</span>
              </React.Fragment>
            ))}
          </div>
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
export { parseErrorMessage, ErrorDisplay, StructuredErrorDisplay, TurnErrorCard };
