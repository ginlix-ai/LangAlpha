import React from 'react';
import { useTranslation } from 'react-i18next';
import { AppWindow } from 'lucide-react';
import { buttonSlot } from '@/components/ui/button-class';
import { Loader } from '@/components/ui/loader';
import type { SavedDashboard } from '@/lib/dashboards';
import type { Away } from '../hooks/useDashboardSource';

interface DashboardAwayNoticeProps {
  dashboard: SavedDashboard;
  /**
   * Why it is not on screen. `waiting` is the user's own choice not to wait
   * for it to start; `ready` says it has come up since.
   */
  away: Away | { kind: 'waiting'; ready: boolean };
  /** The action is under way. */
  busy: boolean;
  /** Show it, or ask for it again. */
  onOpen: () => void;
  /** Take one that no longer exists off the list. */
  onRemove: () => void;
}

/**
 * Why the built-in dashboard is showing in place of the one the user chose.
 * Theirs stays chosen: a machine that was asleep is back next visit, so a
 * failure here never undoes the choice.
 */
export function DashboardAwayNotice({ dashboard, away, busy, onOpen, onRemove }: DashboardAwayNoticeProps): React.ReactElement {
  const { t } = useTranslation();
  const name = dashboard.title || t('customDashboard.untitled');
  const page = dashboard.kind === 'page';

  let title: string;
  let detail: string | null = null;
  let action: string;
  let act = onOpen;
  switch (away.kind) {
    case 'waiting':
      title = away.ready
        ? t('customDashboard.away.ready', { name })
        : t(page ? 'customDashboard.away.waitingPage' : 'customDashboard.away.waiting', { name });
      action = t('customDashboard.away.open', { name });
      break;
    case 'gone':
      title = t('customDashboard.away.gone', { name });
      detail = t(page ? 'customDashboard.away.goneDetailPage' : 'customDashboard.away.goneDetail');
      action = t('customDashboard.away.remove');
      act = onRemove;
      break;
    case 'noVersion':
      title = t('customDashboard.away.failed', { name });
      detail = t('customDashboard.away.noVersion');
      action = t('customDashboard.away.keepNow');
      break;
    case 'failed':
      title = t('customDashboard.away.failed', { name });
      detail = away.detail ?? t(page ? 'customDashboard.away.failedDetailPage' : 'customDashboard.away.failedDetail');
      action = t('customDashboard.away.retry');
      break;
  }

  return (
    <div
      role="status"
      className="mb-6 flex items-center gap-3 rounded-lg border border-(--color-border-muted) bg-(--color-bg-card) px-4 py-3"
    >
      <AppWindow size={16} className="shrink-0 text-(--color-text-tertiary)" aria-hidden />
      <div className="min-w-0 flex-1">
        <p className="text-sm font-medium text-(--color-text-primary)">{title}</p>
        {detail && <p className="mt-0.5 text-xs text-(--color-text-secondary)">{detail}</p>}
      </div>
      <button
        type="button"
        {...buttonSlot('outline')}
        // Kept focusable while busy, so focus is not dropped mid-action.
        onClick={busy ? undefined : act}
        aria-disabled={busy || undefined}
        aria-busy={busy || undefined}
        className="flex shrink-0 items-center gap-1.5 rounded-md border border-(--color-border-muted) px-3 py-1.5 text-xs font-medium text-(--color-text-primary) transition-colors hover:bg-(--color-bg-hover)"
      >
        {busy && <Loader size={12} />}
        {action}
      </button>
    </div>
  );
}
