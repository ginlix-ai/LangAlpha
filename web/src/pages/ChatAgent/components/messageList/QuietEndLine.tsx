import { useContext, useLayoutEffect, useRef, useState, type ReactElement } from 'react';
import { useTranslation } from 'react-i18next';
import { AlarmClock } from 'lucide-react';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { useLoadedHomeTimezone } from '@/hooks/useHomeTimezone';
import type { QuietEnd } from '../../session/wakeupEvents';
import { wakeupTitle } from '../wakeupLabels';
import { CollapseClockContext, playEntrance, turnEndDelays } from './transcriptMotion';

interface QuietEndLineProps {
  end: QuietEnd;
  /** The turn was drawn running here, so the line arrives with its ending,
   *  as the Sources pill does. */
  entering: boolean;
}

/**
 * The close of a wake turn that ended quietly but stays in the transcript:
 * one that changed its wakeups, or one the quiet checks row cannot fold
 * because another row sits among its replies. Without it the turn stops on
 * its last tool row and reads as cut off; with it the turn has an answer, so
 * it folds like any reply. Quiet ink, not prose: the agent chose not to write
 * to the user, and the line is its note on what it checked, not a reply.
 */
export function QuietEndLine({ end, entering }: QuietEndLineProps): ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const zone = useLoadedHomeTimezone();
  // Only which day is today moves the labels.
  const now = useNow(60 * 60_000);
  const ref = useRef<HTMLDivElement>(null);
  const collapseClock = useContext(CollapseClockContext);
  const [enter] = useState(entering);
  useLayoutEffect(() => (
    enter && ref.current ? playEntrance(ref.current, 0, turnEndDelays(collapseClock).sources) : undefined
  ), [enter, collapseClock]);

  const changes = [
    ...end.set.map((w) => ({ id: `set-${w.wakeup_id}`, text: t('chat.wakeups.quietEndSet', { name: wakeupTitle(w, t, locale, now, zone) }) })),
    ...end.cancelled.map((w) => ({ id: `cancelled-${w.wakeup_id}`, text: t('chat.wakeups.quietEndCancelled', { name: wakeupTitle(w, t, locale, now, zone) }) })),
  ];

  return (
    <div ref={ref} className="mt-1" data-quiet-end>
      <div className="flex items-baseline gap-2 text-sm">
        <span className="inline-flex shrink-0 items-center gap-1 self-center text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
          <AlarmClock aria-hidden="true" className="h-3.5 w-3.5 shrink-0" />
          {t('chat.wakeups.quietEnd')}
        </span>
        <span className="min-w-0 wrap-break-word" style={{ color: 'var(--color-text-secondary)' }}>{end.status}</span>
      </div>
      {changes.length > 0 && (
        <ul className="mt-2 flex flex-wrap gap-1.5">
          {changes.map((change) => (
            <li
              key={change.id}
              className="rounded-full px-2 py-0.5 text-xs tabular-nums"
              style={{ border: '1px solid var(--color-border-muted)', color: 'var(--color-text-secondary)' }}
            >
              {change.text}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
