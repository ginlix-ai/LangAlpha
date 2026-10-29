/**
 * The small rounded label set beside a title: a computer row's Primary and
 * This Mac, and a setup's Preview in the warning tint a preview carries
 * elsewhere.
 */
import type { ReactNode } from 'react';
import type { LucideIcon } from 'lucide-react';

const TONES = {
  muted: { backgroundColor: 'var(--color-border-muted)', color: 'var(--color-text-secondary)' },
  warning: { backgroundColor: 'var(--color-warning-soft)', color: 'var(--color-warning)' },
} as const;

export function Pill({
  tone = 'muted',
  icon: Icon,
  title,
  children,
}: {
  tone?: keyof typeof TONES;
  icon?: LucideIcon;
  title?: string;
  children: ReactNode;
}) {
  return (
    <span
      className="inline-flex shrink-0 items-center gap-1 rounded-full px-2 py-0.5 text-[0.625rem] font-medium"
      style={TONES[tone]}
      title={title}
    >
      {Icon && <Icon className="h-3 w-3" />}
      {children}
    </span>
  );
}
