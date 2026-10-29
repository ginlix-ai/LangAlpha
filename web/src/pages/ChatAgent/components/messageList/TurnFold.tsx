import React, { memo, useLayoutEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { ChevronDown } from 'lucide-react';
import { SwapCell } from '@/components/ui/crossfade';
import { motion } from '@/lib/framer';
import { SPRING_FOLD } from '@/lib/motion';
import { formatWorkedFor } from '@/lib/elapsed';
import { announceAnchoredToggle } from '../../utils/anchoredToggle';
import { TURN_END_DELAY, isFreshSend, playEntrance } from './transcriptMotion';
import type { FoldState } from './types';

export interface TurnFoldProps {
  /** The turn's fold. `live` while the turn runs; afterwards the header is
   *  the fold's switch. `process` is a turn with no answer, open to its cards
   *  with the accordions shut: it already looks open, so the chevron points
   *  down. `unfolded` is a turn that ran here with nothing to fold: the row it
   *  worked under stays, as a label, rather than leaving from above the
   *  answer. */
  state: FoldState;
  /** Epoch ms of the user message that opened the turn. */
  startedAt: number;
  /** Epoch ms the turn settled. Absent on a turn whose end was never recorded. */
  completedAt?: number;
  /** The message that opened the turn was sent from this view, so the row
   *  arrives with it (see `isFreshSend`). */
  sentHere?: boolean;
  /** The turn, which `onToggle` is called with. */
  turnIndex: number;
  onToggle: (turnIndex: number) => void;
}

/**
 * One box for both states, so a turn settling changes the words inside the row
 * and nothing else: same hairline, same ink, same height. A row that grew a
 * border or a chevron's worth of height at the moment a turn finished would
 * shift the whole answer under the reader's eye. The two labels crossfade in
 * one cell: the clock leaves first, and the settled label arrives once it has
 * gone.
 */
const ROW_STYLE: React.CSSProperties = {
  paddingTop: '5px',
  paddingBottom: '5px',
  marginBottom: '0.5rem',
  borderBottom: '1px solid var(--color-border-muted)',
  fontSize: '0.8125rem',
  color: 'var(--Labels-Tertiary)',
};

const CONTROL_STYLE: React.CSSProperties = { color: 'inherit', font: 'inherit' };

/**
 * The header that stands in for a turn's process: a running clock while the
 * turn works, then the switch that unfolds what it did. Memoized on plain
 * props, since the list re-renders on every streamed frame and a long thread
 * has a row for each of its settled turns.
 */
export const TurnFold = memo(function TurnFold({ state, startedAt, completedAt, sentHere, turnIndex, onToggle }: TurnFoldProps): React.ReactElement {
  const { t } = useTranslation();
  const rowRef = useRef<HTMLDivElement>(null);
  // The row is in place from the send on, so nothing is ever inserted above a
  // reply already typing. It fades in with the message that opened it.
  const [enter] = useState(() => isFreshSend(sentHere === true, startedAt));
  useLayoutEffect(() => (enter && rowRef.current ? playEntrance(rowRef.current) : undefined), [enter]);

  const workedFor = completedAt === undefined
    ? t('chat.worked')
    : t('chat.workedFor', { duration: formatWorkedFor(completedAt - startedAt, t) });

  return (
    <div ref={rowRef} style={ROW_STYLE} data-turn-fold={state}>
      <SwapCell
        swapKey={state === 'live' ? 'live' : 'settled'}
        layerClassName="inline-flex"
        enterDelay={state === 'live' ? 0 : TURN_END_DELAY.label}
      >
        {state === 'live' ? (
          <WorkingLabel startedAt={startedAt} />
        ) : state === 'unfolded' ? (
          <span className="tabular-nums">{workedFor}</span>
        ) : (
          <button
            type="button"
            aria-expanded={state !== 'collapsed'}
            onClick={(e) => { announceAnchoredToggle(e.currentTarget); onToggle(turnIndex); }}
            className="inline-flex items-center gap-1 text-left bg-transparent border-0 p-0 cursor-pointer transition-colors hover:text-foreground"
            style={CONTROL_STYLE}
          >
            <span className="tabular-nums">{workedFor}</span>
            <motion.span
              animate={{ rotate: state === 'collapsed' ? 0 : 90 }}
              transition={SPRING_FOLD}
              className="inline-flex shrink-0"
              style={{ opacity: 0.6 }}
            >
              <ChevronDown className="h-3.5 w-3.5 -rotate-90" />
            </motion.span>
          </button>
        )}
      </SwapCell>
    </div>
  );
});

/**
 * The elapsed label writes its own text node once a second. A turn runs for
 * minutes beside a stream that already commits on every chunk; putting the
 * clock through React state would add a commit per second to that, for one
 * word that no other part of the tree reads. The first write is a layout
 * effect, so the first painted frame already carries the text while render
 * stays free of the clock.
 */
function WorkingLabel({ startedAt }: { startedAt: number }): React.ReactElement {
  const { t } = useTranslation();
  const ref = useRef<HTMLSpanElement>(null);

  useLayoutEffect(() => {
    const write = () => {
      const el = ref.current;
      if (el) el.textContent = t('chat.workingFor', { duration: formatWorkedFor(Date.now() - startedAt, t) });
    };
    write();
    const id = setInterval(write, 1000);
    return () => clearInterval(id);
  }, [startedAt, t]);

  return <span ref={ref} className="tabular-nums" />;
}

export default TurnFold;
