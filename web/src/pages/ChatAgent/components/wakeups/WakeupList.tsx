import { useCallback, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { useLocale } from '@/hooks/useLocale';
import type { Wakeup } from '@/types/wakeups';
import { useCancelWakeups } from '../../hooks/useWakeups';
import { stateLabel, triggerLabel, wakeupTitle } from '../wakeupLabels';

type RowError = 'cancelFailed' | 'cancelTooLate';

/**
 * A cancel's progress per wakeup, shared by the chip's list and the Status
 * panel. A `too_late` wakeup has already reached the agent, and stays listed
 * with the reason; any other outcome is settled by the listing the answer
 * carries, so it needs no word here.
 */
function useWakeupCancel(threadId: string | null) {
  const { mutateAsync } = useCancelWakeups(threadId);
  const [busy, setBusy] = useState<ReadonlySet<string>>(() => new Set());
  const [errors, setErrors] = useState<Readonly<Record<string, RowError>>>({});

  const cancel = useCallback((which: readonly string[] | 'all', ids: readonly string[]) => {
    if (ids.length === 0) return;
    setBusy((prev) => new Set([...prev, ...ids]));
    setErrors((prev) => Object.fromEntries(Object.entries(prev).filter(([id]) => !ids.includes(id))));
    mutateAsync(which).then(
      ({ results }) => {
        const late = results.filter((r) => r.outcome === 'too_late').map((r) => r.wakeup_id);
        if (late.length > 0) {
          setErrors((prev) => ({ ...prev, ...Object.fromEntries(late.map((id) => [id, 'cancelTooLate' as const])) }));
        }
      },
      () => setErrors((prev) => ({ ...prev, ...Object.fromEntries(ids.map((id) => [id, 'cancelFailed' as const])) })),
    ).finally(() => setBusy((prev) => new Set([...prev].filter((id) => !ids.includes(id)))));
  }, [mutateAsync]);

  return { busy, errors, cancel };
}

interface WakeupListItemProps {
  wakeup: Wakeup;
  now: number;
  userTimezone: string | null;
  busy: boolean;
  error: RowError | undefined;
  onCancel: () => void;
}

/** One wakeup: its name, what sets it off when the agent named it, where it
 *  stands, and a Cancel while the server will take one. */
function WakeupListItem({ wakeup, now, userTimezone, busy, error, onCancel }: WakeupListItemProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const title = wakeupTitle(wakeup, t, locale, now, userTimezone);
  const trigger = wakeup.label ? triggerLabel(wakeup, t, locale, now, userTimezone) : null;
  return (
    <li className="flex items-start gap-3 px-2.5 py-2" data-wakeup-state={wakeup.state}>
      <div className="min-w-0 flex-1">
        <div className="wrap-break-word text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {title}
        </div>
        {trigger && (
          <div className="mt-0.5 wrap-break-word text-xs" style={{ color: 'var(--color-text-secondary)' }}>
            {trigger}
          </div>
        )}
        <div className="mt-0.5 text-xs tabular-nums" style={{ color: 'var(--color-text-tertiary)' }}>
          {stateLabel(wakeup, t, locale, now, userTimezone)}
        </div>
        {error && (
          <div className="mt-0.5 text-xs" style={{ color: 'var(--color-loss)' }} role="alert">
            {t(`chat.wakeups.${error}`)}
          </div>
        )}
      </div>
      {wakeup.cancellable && (
        <Button
          variant="ghost"
          size="sm"
          className="h-7 shrink-0 px-2 text-xs"
          style={{ color: 'var(--color-text-secondary)' }}
          disabled={busy}
          onClick={onCancel}
          aria-label={t('chat.wakeups.cancelLabel', { label: title })}
        >
          {busy ? t('chat.wakeups.cancelling') : t('chat.wakeups.cancel')}
        </Button>
      )}
    </li>
  );
}

interface WakeupListRowsProps {
  /** The thread the wakeups are in, which a Cancel goes to. */
  threadId: string | null;
  wakeups: readonly Wakeup[];
  now: number;
  userTimezone: string | null;
  /** Offer Cancel all above this many cancellable rows. */
  cancelAllFrom?: number;
}

/** The rows of a list of wakeups, each with its own Cancel, then Cancel all. */
export function WakeupListRows({ threadId, wakeups, now, userTimezone, cancelAllFrom = 2 }: WakeupListRowsProps) {
  const { t } = useTranslation();
  const { busy, errors, cancel } = useWakeupCancel(threadId);
  const cancellable = wakeups.filter((w) => w.cancellable).map((w) => w.wakeup_id);
  return (
    <>
      {wakeups.map((wakeup) => (
        <WakeupListItem
          key={wakeup.wakeup_id}
          wakeup={wakeup}
          now={now}
          userTimezone={userTimezone}
          busy={busy.has(wakeup.wakeup_id)}
          error={errors[wakeup.wakeup_id]}
          onCancel={() => cancel([wakeup.wakeup_id], [wakeup.wakeup_id])}
        />
      ))}
      {cancellable.length >= cancelAllFrom && (
        <li className="flex justify-end px-2.5 pt-1 pb-0.5">
          <Button
            variant="ghost"
            size="sm"
            className="h-7 px-2 text-xs"
            style={{ color: 'var(--color-text-secondary)' }}
            disabled={cancellable.every((id) => busy.has(id))}
            onClick={() => cancel('all', cancellable)}
          >
            {t('chat.wakeups.cancelAll')}
          </Button>
        </li>
      )}
    </>
  );
}
