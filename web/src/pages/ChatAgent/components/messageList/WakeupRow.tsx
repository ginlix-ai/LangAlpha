import type { ReactElement } from 'react';
import { useTranslation } from 'react-i18next';
import { AlarmClock, BellRing } from 'lucide-react';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { useLoadedHomeTimezone } from '@/hooks/useHomeTimezone';
import type { WakeupArrival } from '@/types/chat';
import { isPriceArrival, priceAlertText, wakeupRowText } from './wakeupRowLabels';

interface WakeupRowProps {
  wakeup: WakeupArrival;
}

/**
 * A wakeup in the transcript, in the divider vocabulary the other system
 * events use: above the turn it started, or where it landed inside a running
 * turn. Never a user bubble, since nobody typed it. A price's reads as a
 * price alert always has.
 */
export function WakeupRow({ wakeup }: WakeupRowProps): ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const zone = useLoadedHomeTimezone();
  // Only which day is today moves the labels.
  const now = useNow(60 * 60_000);
  const price = isPriceArrival(wakeup);
  const Icon = price ? BellRing : AlarmClock;
  const marker = price ? { 'data-market-alert': wakeup.source } : { 'data-wakeup': wakeup.source };
  return (
    <div className="my-1" {...marker}>
      <div className="flex items-center gap-3 py-2">
        <div className="min-w-4 flex-1" style={{ borderTop: '1px solid var(--color-border-muted)' }} />
        <span className="inline-flex min-w-0 items-center gap-1.5 text-xs tabular-nums" style={{ color: 'var(--color-text-tertiary)' }}>
          <Icon aria-hidden="true" className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
          <span>{price ? priceAlertText(wakeup, t, locale) : wakeupRowText(wakeup, t, locale, now, zone)}</span>
        </span>
        <div className="min-w-4 flex-1" style={{ borderTop: '1px solid var(--color-border-muted)' }} />
      </div>
    </div>
  );
}
