/**
 * The small drawings inside the tray's chip faces (`trayFaces`), as
 * components of their own so that module holds no component.
 */
import { MoveMark } from '@/components/ui/move-mark';
import { changeDirection, directionTone } from '@/lib/priceMove';
import { cn } from '@/lib/utils';
import type { PlanStep } from '@/types/chat';
import { DispatchStatusGlyph } from '../dispatchStatusUi';
import type { RosterTask } from './delegatedWork';
import { taskDispatchStatus } from './statusRows';
import type { ThreadQuote } from './threadQuotesContext';

/**
 * How far along the plan is, a bar per step: done, running in the accent,
 * still to come. Narrower as the plan grows, so twelve steps stay short.
 * With `clip`, a row with no room left drops whole bars off the end (they
 * wrap onto a line the height hides) rather than run past the chip; the step
 * count beside them still says where the plan is.
 */
export function PlanTicks({ steps, clip = false }: { steps: PlanStep[]; clip?: boolean }) {
  const width = steps.length <= 5 ? 'w-2' : steps.length <= 8 ? 'w-1.5' : 'w-1';
  return (
    <span
      aria-hidden="true"
      className={cn('inline-flex items-center gap-0.5', clip ? 'h-[3px] min-w-0 flex-wrap overflow-hidden' : 'shrink-0')}
    >
      {steps.map((step, i) => (
        <span
          key={i}
          className={cn(
            'h-[3px] rounded-full',
            width,
            step.status === 'in_progress' ? 'bg-(--color-accent-primary)'
              : step.status === 'completed' ? 'bg-(--color-text-quaternary)'
                : 'bg-(--color-border-default)',
          )}
        />
      ))}
    </span>
  );
}

/**
 * A ticker and the way its day moved, drawn: the arrow carries the direction
 * without the color, and its color carries it at a glance. A move under the
 * one place the tight chip would print reads as flat; a ticker with no quote
 * yet stands alone.
 */
export function MarkedTicker({ symbol, quote }: { symbol: string; quote: ThreadQuote | undefined }) {
  const dir = quote?.pct != null ? changeDirection(quote.pct, 1) : null;
  return (
    <span aria-hidden="true" className="flex items-center gap-0.5">
      <span className="font-medium" style={{ color: 'var(--color-text-secondary)' }}>{symbol}</span>
      {dir === 'up' || dir === 'down' ? (
        <span className="flex" style={{ color: directionTone(dir) }}><MoveMark dir={dir} /></span>
      ) : dir === 'flat' ? (
        <span style={{ color: 'var(--color-text-tertiary)' }}>·</span>
      ) : null}
    </span>
  );
}

/** A roster task's glyph: the spinner only while it runs, its ending once settled. */
export function TaskGlyph({ row }: { row: RosterTask }) {
  const status = taskDispatchStatus(row);
  return status && <DispatchStatusGlyph status={status} />;
}
