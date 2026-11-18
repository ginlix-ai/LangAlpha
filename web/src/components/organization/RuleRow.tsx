import type { ReactNode } from 'react';
import { Building2, Lock, type LucideIcon } from 'lucide-react';
import { cn } from '@/lib/utils';

/**
 * Something a rule keeps the user from doing, said as a rule rather than a
 * failure: a lock on a neutral surface, what the rule is, what to do instead,
 * and at most one action, the one that fixes it. Nothing broke and trying
 * again gets the same answer, so it never takes the error colors or offers a
 * report.
 */
export function RuleRow({ title, body, action, className, icon: Icon = Lock }: {
  title: string;
  /** A limit reached rather than a rule met reads with its own mark. */
  icon?: LucideIcon;
  body?: ReactNode;
  action?: { label: string; onPress: () => void };
  className?: string;
}) {
  return (
    <div
      role="status"
      className={cn('flex items-start gap-2.5 rounded-lg border px-3.5 py-3 text-sm', className)}
      style={{
        backgroundColor: 'var(--color-bg-card)',
        borderColor: 'var(--color-border-muted)',
        color: 'var(--color-text-primary)',
      }}
    >
      <Icon aria-hidden="true" className="h-4 w-4 shrink-0 mt-0.5" style={{ color: 'var(--color-text-tertiary)' }} />
      <div className="min-w-0 flex flex-col gap-0.5">
        <span className="font-medium wrap-break-word">{title}</span>
        {(body || action) && (
          <span className="wrap-break-word" style={{ color: 'var(--color-text-secondary)' }}>
            {body}
            {action && (
              <>
                {body ? ' ' : null}
                <button
                  type="button"
                  onClick={action.onPress}
                  className="tap-target underline underline-offset-2 font-medium"
                  style={{ color: 'var(--color-text-primary)' }}
                >
                  {action.label}
                </button>
              </>
            )}
          </span>
        )}
      </div>
    </div>
  );
}

/**
 * One line saying what a rule does to the section it sits in, where the
 * controls it takes away used to be: an organization's mark, then the
 * sentence. Information, not an action.
 */
export function RuleNote({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <p
      className={cn('flex items-start gap-2 text-[0.8125rem] leading-snug', className)}
      style={{ color: 'var(--color-text-secondary)' }}
    >
      <Building2 aria-hidden="true" className="h-3.5 w-3.5 shrink-0 mt-0.5" style={{ color: 'var(--color-text-tertiary)' }} />
      <span className="min-w-0 wrap-break-word">{children}</span>
    </p>
  );
}
