import type React from 'react';
import { cn } from '@/lib/utils';

/**
 * The small secondary button the connector form uses for its in-place
 * actions: check this address, add a header, keep this key in Secrets. These
 * used to be amber text links, and amber is the annotation colour here, so an
 * action dressed in it read as a remark beside the field rather than something
 * to press. A neutral border and the secondary text colour make it a button
 * without competing with the footer's primary one.
 */
export function McpQuietButton({
  icon,
  tone = 'neutral',
  size = 'sm',
  className,
  children,
  type = 'button',
  ...rest
}: React.ButtonHTMLAttributes<HTMLButtonElement> & {
  icon?: React.ReactNode;
  /** `primary` is for the one next step a panel is asking for. */
  tone?: 'neutral' | 'primary';
  size?: 'sm' | 'md';
}) {
  return (
    <button
      type={type}
      className={cn(
        'inline-flex items-center justify-center gap-1 rounded-md whitespace-nowrap transition-colors disabled:opacity-50 disabled:pointer-events-none',
        size === 'sm' ? 'h-6 px-2 text-[0.6875rem]' : 'h-7 px-2.5 text-xs',
        tone === 'primary'
          ? 'border border-transparent bg-(--color-btn-primary-bg) text-(--color-btn-primary-text) hover:opacity-90'
          : 'border border-(--color-border-default) text-(--color-text-secondary) hover:bg-(--color-bg-hover) hover:text-(--color-text-primary)',
        className,
      )}
      {...rest}
    >
      {icon}
      {children}
    </button>
  );
}
