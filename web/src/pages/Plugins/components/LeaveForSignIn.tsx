import { useId } from 'react';
import { useTranslation } from 'react-i18next';
import { ModalShell } from '@/components/ui/ModalShell';
import { isDesktopShell } from '@/lib/desktop';
import { pendingIntent } from '@/pages/ChatAgent/utils/api/mcpOauth';
import type { ResolvedConnect } from '../hooks/useMcpOauthActions';

/**
 * Where the vendor's sign-in will happen, as the user will see it in the
 * address bar: the registrable part of the host, so `mcp.aiera.com` reads as
 * `aiera.com`. A guess for the rare two-part suffix (`co.uk`), which only
 * costs a slightly longer name.
 */
function siteOf(request: ResolvedConnect): string | null {
  const raw = request.vendor?.site ?? request.url;
  if (!raw) return null;
  let host: string;
  try {
    host = new URL(raw.includes('://') ? raw : `https://${raw}`).hostname;
  } catch {
    return null;
  }
  if (!host.includes('.') || /^[\d.]+$/.test(host) || host.startsWith('[')) return host;
  const labels = host.replace(/^www\./, '').split('.');
  const keep = labels.length > 2 && labels.at(-2)!.length <= 3 && labels.at(-1)!.length === 2 ? 3 : 2;
  return labels.slice(-keep).join('.');
}

/**
 * The step before an install or a save leaves this app for the vendor's
 * sign-in page.
 *
 * Without it the page simply went away: the user pressed Install, and the next
 * thing on screen was somebody else's site asking for a password, with nothing
 * here saying it was expected, that it would come back, or that nothing had
 * happened yet. Asked by the connect lifecycle, like a brokerage's consent, so
 * every surface that can start one shows the same step.
 */
export function LeaveForSignIn({
  request,
  pending,
  onConfirm,
  onCancel,
}: {
  request: ResolvedConnect;
  /** The sign-in is starting and the page is about to leave. */
  pending: boolean;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  const titleId = useId();
  const label = request.label ?? request.vendor?.label ?? request.name;
  const site = siteOf(request);
  const desktop = isDesktopShell();
  const approve = site
    ? t(desktop ? 'plugins.signin.leave.approveOnDesktop' : 'plugins.signin.leave.approveOn', { site })
    : t(desktop ? 'plugins.signin.leave.approveDesktop' : 'plugins.signin.leave.approve');
  // What waits on the sign-in, which is what the user is told nothing happens to.
  const what = pendingIntent(request.pending?.kind);

  const footer = (
    <div className="flex items-center justify-end gap-2">
      {/* Live while the sign-in starts: backing out of it costs nothing, and
          the lifecycle stops the start rather than the page leaving. */}
      <button
        type="button"
        onClick={onCancel}
        className="px-3 py-1.5 text-xs rounded-md transition-colors hover:bg-foreground/10"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        {t('common.cancel')}
      </button>
      <button
        type="button"
        onClick={onConfirm}
        disabled={pending}
        className="px-3 py-1.5 text-xs rounded-md transition-colors disabled:opacity-50"
        style={{
          color: 'var(--color-btn-primary-text)',
          backgroundColor: 'var(--color-btn-primary-bg)',
        }}
      >
        {pending
          ? t('plugins.signin.leave.opening')
          : site
            ? t('plugins.signin.leave.continueTo', { site })
            : t('plugins.signin.leave.continue')}
      </button>
    </div>
  );

  return (
    <ModalShell
      labelId={titleId}
      title={t('plugins.signin.leave.title', { label })}
      onClose={onCancel}
      dismissable={!pending}
      width="narrow"
      // Above the install dialogs (1015), which stay open underneath so that
      // Cancel lands the user back on the step they were on.
      zIndex={1020}
      footer={footer}
    >
      <p className="text-sm leading-relaxed" style={{ color: 'var(--color-text-secondary)' }}>
        {/* Joined by the catalog rather than here: English puts a space
            between sentences and Chinese does not. */}
        {t('plugins.signin.leave.body', {
          needs: t('plugins.signin.leave.needs', { label }),
          approve,
          until: t(`plugins.signin.leave.until.${what}`),
        })}
      </p>
    </ModalShell>
  );
}
