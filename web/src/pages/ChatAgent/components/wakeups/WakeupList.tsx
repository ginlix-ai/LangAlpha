import { useCallback, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { useLocale } from '@/hooks/useLocale';
import type { Wakeup, WakeupPrice } from '@/types/wakeups';
import { useCancelWakeups } from '../../hooks/useWakeups';
import { armedPrice, distanceLabel, priceDistance, stateLabel, triggerLabel, wakeupTitle } from '../wakeupLabels';
import { lastTrade, useThreadQuotes } from '../chatView/threadQuotesContext';

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
  const armed = armedPrice(wakeup);
  return (
    <li className="flex items-start gap-2.5 px-2 py-1.5" data-wakeup-state={wakeup.state}>
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
        {armed && <PriceDistanceLine price={armed} />}
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

/**
 * How far an armed price wakeup is from going off: the words, then a bar
 * filling from the price it was set at toward its level. Read at the last
 * trade in the chat's quote of its ticker, which follows it closer than the
 * listing does, else at the monitor's last reading. A move names the level it
 * works out to, which its own words do not.
 */
function PriceDistanceLine({ price }: { price: WakeupPrice }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const live = lastTrade(useThreadQuotes().bySymbol[price.symbol]);
  const distance = priceDistance(price, live);
  if (!distance) return null;
  const isMove = price.kind === 'pct_up' || price.kind === 'pct_down';
  return (
    <>
      <div className="mt-0.5 text-xs tabular-nums" style={{ color: 'var(--color-text-secondary)' }}>
        {distanceLabel(distance, isMove, t, locale)}
      </div>
      {distance.progress != null && (
        <div aria-hidden="true" className="relative mt-1.5 mr-1 h-1 rounded-full" style={{ background: 'var(--color-bg-hover)' }}>
          <div
            className="absolute inset-y-0 left-0 rounded-full transition-[width] duration-enter ease-out"
            style={{ width: `${Math.round(distance.progress * 100)}%`, background: 'var(--color-text-quaternary)' }}
          />
          <div className="absolute -top-0.5 right-0 h-2 w-0.5 rounded-full" style={{ background: 'var(--color-text-tertiary)' }} />
        </div>
      )}
    </>
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
        <li className="flex justify-end px-2 pt-1 pb-0.5">
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
