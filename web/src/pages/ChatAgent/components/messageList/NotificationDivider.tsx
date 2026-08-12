import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { MessageRecord } from './types';

// --- NotificationDivider ---

interface NotificationDividerProps {
  message?: MessageRecord;
  content?: string;
  detail?: string;
  /** Picks the expander toggle label — 'summary' (default) or 'error'. */
  detailKind?: 'summary' | 'error';
}

/**
 * NotificationDivider -- centered inline divider for system events
 * (e.g. compaction, offload, model fallback). Renders as a muted horizontal
 * rule with text, similar to date dividers in chat apps. When ``detail`` is
 * set, a "View summary"/"View error" toggle reveals the full text in a muted
 * panel below the divider.
 */
export function NotificationDivider({ message, content, detail, detailKind }: NotificationDividerProps): React.ReactElement {
  const { t } = useTranslation();
  const text = content ?? (message?.content as string | undefined);
  const effectiveDetail =
    detail ?? ((message as { detail?: string } | undefined)?.detail);
  const hasDetail = typeof effectiveDetail === 'string' && effectiveDetail.trim().length > 0;
  const [expanded, setExpanded] = useState(false);
  const isError = detailKind === 'error';

  return (
    <div className="my-1">
      <div className="flex items-center gap-3 py-2">
        <div className="flex-1" style={{ borderTop: '1px solid var(--color-border-muted)' }} />
        <span
          className="text-xs whitespace-nowrap"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {text}
        </span>
        {hasDetail && (
          <button
            type="button"
            onClick={() => setExpanded((v) => !v)}
            className="text-xs underline-offset-2 hover:underline whitespace-nowrap"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {expanded
              ? t(isError ? 'chat.hideErrorDetail' : 'chat.hideSummary')
              : t(isError ? 'chat.viewErrorDetail' : 'chat.viewSummary')}
          </button>
        )}
        <div className="flex-1" style={{ borderTop: '1px solid var(--color-border-muted)' }} />
      </div>
      {hasDetail && expanded && (
        <div
          className="mt-1 mb-2 mx-auto max-w-3xl rounded-md px-3 py-2 text-sm whitespace-pre-wrap break-words"
          style={{
            color: 'var(--color-text-secondary)',
            background: 'var(--color-bg-subtle)',
            border: '1px solid var(--color-border-muted)',
          }}
        >
          {effectiveDetail}
        </div>
      )}
    </div>
  );
}
