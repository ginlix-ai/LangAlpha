import React, { useState } from 'react';
import { useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { AlertTriangle, Info } from 'lucide-react';
import type { ErrorLinkSpec, StructuredError } from '@/utils/rateLimitError';
import { parseErrorMessage } from '@/pages/ChatAgent/utils/parseErrorMessage';
import { usePlanClock } from '@/hooks/useClaudeCodePlan';
import { cn } from '@/lib/utils';
import { useModelName } from '@/hooks/useModelName';
import {
  isReportable, planStopCopy, planStopOf, UPSTREAM_HINT_I18N_KEY, upstreamFailureCopy, type PlanStop,
} from '@/lib/upstreamFailure';
import { ReportDialog } from '@/components/report/ReportDialog';
import { useReportsEnabled } from '@/hooks/useReportsEnabled';

interface ErrorBannerProps {
  error: string | StructuredError | null | undefined;
  className?: string;
  style?: React.CSSProperties;
  /** The thread a failed turn belongs to. With it, a failure the server
   *  classified offers "Report this", which sends the failure to the team. */
  reportThreadId?: string;
}

export function ErrorLink({ url, label, labelKey, external }: ErrorLinkSpec): React.ReactElement {
  const navigate = useNavigate();
  const { t } = useTranslation();
  // A leading slash is not enough to claim a URL as ours: with a path-shaped
  // VITE_PLATFORM_URL the account portal answers on /account/*, same origin but
  // a different app, and routing to it lands on our catch-all with no network
  // request at all. So the call site declares `external` and we believe it.
  const isRouterPath = !external && url.startsWith('/') && !url.startsWith('//');
  // Cross-origin URLs (an upstream provider's status page) open in a new tab so
  // the chat survives the detour; `external` asks for the same treatment on our
  // own origin, for banners whose state would not survive a navigation.
  const opensNewTab = external || (!isRouterPath && (() => {
    try {
      return new URL(url, window.location.href).origin !== window.location.origin;
    } catch {
      return false;
    }
  })());
  return (
      <a
        href={url}
        {...(opensNewTab && { target: '_blank', rel: 'noopener noreferrer' })}
        onClick={(e) => {
          if (isRouterPath) {
            e.preventDefault();
            navigate(url);
          }
        }}
        style={{ textDecoration: 'underline', fontWeight: 500 }}
      >
        {labelKey ? t(labelKey) : label}
      </a>
  );
}

/** The quiet line a notice reads as: nothing failed, so no error colors. */
export function NoticeLine({ message, className, style }: { message: string; className?: string; style?: React.CSSProperties }): React.ReactElement {
  return (
    <div
      className={`flex items-center gap-2 text-xs ${className ?? ''}`}
      role="status"
      aria-live="polite"
      style={{ backgroundColor: 'var(--color-bg-page)', color: 'var(--color-text-tertiary)', ...style }}
    >
      <Info aria-hidden="true" className="h-3.5 w-3.5 shrink-0" />
      <span className="min-w-0 wrap-break-word">{message}</span>
    </div>
  );
}

/** A plan's limit, worded in the reader's zone. A leaf of its own, so the
 *  plan clock's per-minute tick redraws only this. */
function PlanStopBanner({ stop, className, style }: {
  stop: PlanStop; className: string; style: React.CSSProperties;
}): React.ReactElement {
  const { t } = useTranslation();
  const { title, text } = planStopCopy(t, stop, usePlanClock());
  return (
    <div className={className} style={style}>
      <AlertTriangle className="h-4 w-4 shrink-0 mt-0.5" style={{ color: 'var(--color-loss)' }} />
      <div className="flex flex-col gap-1 min-w-0">
        <span className="font-medium">{title}</span>
        <span className="wrap-break-word">{text}</span>
      </div>
    </div>
  );
}

/** "Report this" and its dialog, for a turn that failed. */
export function ReportFailure({ error, threadId }: { error: StructuredError; threadId: string }): React.ReactElement | null {
  const { t } = useTranslation();
  const enabled = useReportsEnabled();
  const [open, setOpen] = useState(false);
  if (!enabled) return null;
  // The payer and reason ride on the type so whoever reads the report sees at
  // once whose account failed and what the provider said of it.
  const type = error.kind === 'upstream'
    ? [`upstream_error${error.statusCode ? ` ${error.statusCode}` : ''}`, error.payer, error.reason].filter(Boolean).join(' · ')
    : 'internal_error';
  return (
    <>
      <button
        type="button"
        onClick={() => setOpen(true)}
        className="tap-target self-start text-xs underline underline-offset-2 opacity-90 hover:opacity-100"
      >
        {t('report.bannerAction')}
      </button>
      <ReportDialog
        open={open}
        onOpenChange={setOpen}
        subject={{
          event: 'error_report',
          source: 'error_banner',
          threadId,
          runId: error.runId,
          error: { type, message: error.message },
        }}
      />
    </>
  );
}

export function ErrorBanner({ error, className, style, reportThreadId }: ErrorBannerProps): React.ReactElement | null {
  const { t } = useTranslation();
  const name = useModelName();
  if (!error) return null;

  // Nothing failed: the same quiet row as the thread's other notices.
  if (typeof error === 'object' && error.kind === 'notice') {
    return <NoticeLine message={error.message} className={`px-3 py-1.5 ${className ?? ''}`} style={style} />;
  }

  // In the composer stack the corner and the text's inset are the stack's
  // (ChatView); anywhere else, these.
  const baseClass = cn('flex items-start gap-2 py-2 pr-3 pl-[var(--stack-row-pad,0.75rem)] rounded-[var(--stack-row-radius,var(--radius-md))] text-sm', className);
  // Opaque: the tint is layered over the page surface, so the answer's text
  // scrolling under the banner above the composer does not read through it.
  const baseStyle: React.CSSProperties = {
    backgroundColor: 'var(--color-bg-page)',
    backgroundImage: 'linear-gradient(var(--color-loss-soft), var(--color-loss-soft))',
    color: 'var(--color-loss)',
    ...style,
  };

  if (typeof error === 'object' && 'message' in error) {
    const err = error as StructuredError;
    const stop = planStopOf(err);
    if (stop) return <PlanStopBanner stop={stop} className={baseClass} style={baseStyle} />;
    // Worded by who pays the provider: see `upstreamFailureCopy`.
    const upstream = err.kind === 'upstream' ? upstreamFailureCopy(err, t, name) : null;
    const headline = err.kind === 'internal' ? t('chat.errorInternalHeadline') : upstream?.headline;
    const message = upstream ? (upstream.detail ?? '') : err.message;
    const hasHints = err.kind === 'upstream' && err.hints && err.hints.length > 0;
    return (
      <div className={baseClass} style={baseStyle}>
        <AlertTriangle className="h-4 w-4 shrink-0 mt-0.5" style={{ color: 'var(--color-loss)' }} />
        <div className="flex flex-col gap-1 min-w-0">
          {headline && <span className="font-medium">{headline}</span>}
          {upstream?.source && <span>{upstream.source}</span>}
          {/* A failure the server reports by its outcome alone has no text,
              and its headline is the whole message. */}
          {(message || (err.links && err.links.length > 0)) && (
            <span className="wrap-break-word">
              {message}
              {err.links?.map((l, i) => (
                <React.Fragment key={`${l.url}|${l.label}`}>
                  {i === 0 ? ' ' : <span aria-hidden="true"> · </span>}
                  <ErrorLink {...l} />
                </React.Fragment>
              ))}
            </span>
          )}
          {hasHints && (
            <ul className="mt-1 list-disc pl-4 flex flex-col gap-0.5 text-xs opacity-90">
              {err.hints!.map((h) => (
                <li key={h}>{t(UPSTREAM_HINT_I18N_KEY[h] ?? h)}</li>
              ))}
            </ul>
          )}
          {reportThreadId && isReportable(err) && (
            <ReportFailure error={err} threadId={reportThreadId} />
          )}
        </div>
      </div>
    );
  }

  const parsed = parseErrorMessage(error as string);
  return (
    <div className={baseClass} style={baseStyle}>
      <AlertTriangle className="h-4 w-4 shrink-0 mt-0.5" style={{ color: 'var(--color-loss)' }} />
      <span className="wrap-break-word">
        {parsed.detail ? `${parsed.title}: ${parsed.detail}` : parsed.title}
      </span>
    </div>
  );
}

export default ErrorBanner;
