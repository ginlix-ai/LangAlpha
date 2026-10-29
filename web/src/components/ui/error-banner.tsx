import React from 'react';
import { useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { AlertTriangle, Info } from 'lucide-react';
import { UPSTREAM_HINT_I18N_KEY, type ErrorLinkSpec, type StructuredError } from '@/utils/rateLimitError';
import { parseErrorMessage } from '@/pages/ChatAgent/utils/parseErrorMessage';

interface ErrorBannerProps {
  error: string | StructuredError | null | undefined;
  className?: string;
  style?: React.CSSProperties;
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

export function ErrorBanner({ error, className, style }: ErrorBannerProps): React.ReactElement | null {
  const { t } = useTranslation();
  if (!error) return null;

  // Nothing failed: the same quiet row as the thread's other notices.
  if (typeof error === 'object' && error.kind === 'notice') {
    return <NoticeLine message={error.message} className={`px-3 py-1.5 ${className ?? ''}`} style={style} />;
  }

  const baseClass = `flex items-start gap-2 px-3 py-2 rounded-md text-sm ${className ?? ''}`;
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
    const isUpstream = err.kind === 'upstream';
    const isInternal = err.kind === 'internal';
    const headline = isUpstream
      ? (err.statusCode
          ? t('chat.errorUpstreamHeadlineStatus', { status: err.statusCode })
          : t('chat.errorUpstreamHeadline'))
      : isInternal
        ? t('chat.errorInternalHeadline')
        : null;
    const hasHints = isUpstream && err.hints && err.hints.length > 0;
    return (
      <div className={baseClass} style={baseStyle}>
        <AlertTriangle className="h-4 w-4 shrink-0 mt-0.5" style={{ color: 'var(--color-loss)' }} />
        <div className="flex flex-col gap-1 min-w-0">
          {headline && <span className="font-medium">{headline}</span>}
          {/* A failure the server reports by its outcome alone has no text,
              and its headline is the whole message. */}
          {(err.message || (err.links && err.links.length > 0)) && (
            <span className="wrap-break-word">
              {err.message}
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
