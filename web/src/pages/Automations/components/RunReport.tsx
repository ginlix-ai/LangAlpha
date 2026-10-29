import React from 'react';
import { useTranslation } from 'react-i18next';
import { ArrowUpRight } from 'lucide-react';
import { HeaderButton } from '@/components/mcp/McpPrimitives';
import { ErrorLink } from '@/components/ui/error-banner';
import type { AutomationExecution } from '@/types/automation';
import type { ErrorLinkSpec } from '@/utils/rateLimitError';
import type { RunView } from '../utils/status';
import { StatusGlyph } from './StatusMark';
import './RunReport.css';

/** A feed entry already shows its status glyph beside the automation's name,
 *  so its meta line goes without; the inspector's report opens on the meta
 *  line, so the glyph leads it there. */
const LAYOUT = {
  entry: { excerpt: 'automation-run-excerpt', glyph: false },
  report: { excerpt: 'automation-report-excerpt', glyph: true },
} as const;

interface RunReportProps {
  run: AutomationExecution;
  view: RunView;
  meta: string[];
  layout: keyof typeof LAYOUT;
  busy: boolean;
  onSkip: () => void;
  onOpenThread: () => void;
  /** Passed only where the caller allows the failed run another go. */
  onRetry?: () => void;
  onResume?: () => void;
  /** Where the reader acts on the failure's cause, such as the plan a usage
   *  limit sends them to. */
  links?: ErrorLinkSpec[];
}

/** What one run left behind: the answer's start, the error, a note on why it
 *  waits or was skipped, and what can be done about it. */
export function RunReport({ run, view, meta, layout, busy, onSkip, onOpenThread, onRetry, onResume, links = [] }: RunReportProps) {
  const { t } = useTranslation();
  const classes = LAYOUT[layout];
  const hasThread = !!run.conversation_thread_id;
  return (
    <>
      <div className="automation-mono automation-run-meta">
        {classes.glyph && <StatusGlyph ui={view.ui} size={12} />}
        <span>{meta.join(' · ')}</span>
      </div>
      {run.excerpt && <p className={classes.excerpt}>{run.excerpt}</p>}
      {view.ui.danger && run.error_message && <p className="automation-mono automation-run-error">{run.error_message}</p>}
      {view.noteKey && <p className="automation-run-note">{t(view.noteKey)}</p>}
      {(view.waiting || onRetry || onResume || links.length > 0 || hasThread) && (
        <div className="automation-run-actions">
          {view.waiting && (
            <HeaderButton variant="secondary" disabled={busy} onClick={onSkip}>
              {t('automation.skipRun')}
            </HeaderButton>
          )}
          {onRetry && (
            <HeaderButton variant="secondary" disabled={busy} onClick={onRetry}>
              {t('common.retry')}
            </HeaderButton>
          )}
          {onResume && (
            <HeaderButton variant="secondary" disabled={busy} onClick={onResume}>
              {t('automation.resume')}
            </HeaderButton>
          )}
          {links.map((l) => (
            <span key={`${l.url}|${l.label}`} className="automation-run-cause-link">
              <ErrorLink {...l} />
            </span>
          ))}
          {hasThread && (
            <button type="button" className="automation-link" onClick={onOpenThread}>
              {t(view.ui.live ? 'automation.watchLive' : 'automation.openThread')}
              <ArrowUpRight className="h-3 w-3" />
            </button>
          )}
        </div>
      )}
    </>
  );
}
