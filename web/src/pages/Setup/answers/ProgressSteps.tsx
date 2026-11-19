import type { ReactNode } from 'react';
import { AlertTriangle, Check } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { ROW_CARD } from '@/components/modelAccess/parts';
import { cn } from '@/lib/utils';

/** Where a step stands, in the words a sign-in's own steps use, with
 *  `failed` for the one that did not go through. */
export type ProgressState = 'done' | 'active' | 'todo' | 'failed';

/** One step of a few that setup ticks off: its mark, its name, and under
 *  the step in hand (or the one that failed) what it needs. */
export function ProgressStep({ n, state, title, children }: { n: number; state: ProgressState; title: string; children?: ReactNode }) {
  const mark = state === 'done'
    ? <Check className="h-3.5 w-3.5" style={{ color: 'var(--color-success)' }} />
    : state === 'active'
      ? <Loader size={12} style={{ color: 'var(--color-accent-primary)' }} />
      : state === 'failed'
        ? <AlertTriangle className="h-3.5 w-3.5" style={{ color: 'var(--color-icon-danger)' }} />
        : <span className="text-sm">{n}</span>;
  return (
    <li className="flex items-start gap-3 px-4 py-3.5" data-step-state={state}>
      <span
        aria-hidden="true"
        className="flex h-8 w-8 shrink-0 items-center justify-center rounded-md border"
        style={{ borderColor: state === 'active' ? 'var(--color-text-primary)' : 'var(--color-border-muted)', color: 'var(--color-text-tertiary)' }}
      >
        {mark}
      </span>
      <div className="flex min-w-0 flex-1 flex-col gap-2 pt-1.5">
        <span className="text-sm font-medium" style={{ color: state === 'todo' ? 'var(--color-text-tertiary)' : 'var(--color-text-primary)' }}>
          {title}
        </span>
        {(state === 'active' || state === 'failed') && children}
      </div>
    </li>
  );
}

/** The card the steps sit in, a card of rows. */
export function ProgressList({ children, testId }: { children: ReactNode; testId?: string }) {
  return (
    <ol data-testid={testId} className={cn(ROW_CARD, 'overflow-hidden')}>
      {children}
    </ol>
  );
}
