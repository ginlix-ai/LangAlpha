import type React from 'react';
import { cn } from '@/lib/utils';

/**
 * A bar standing in for a run of text while it loads. Inline and centred on
 * the line, so dropped into the element the text will fill it takes that
 * element's line box, and the skeleton is as tall as what replaces it. The
 * caller gives the width; the pulse is the loop a loading skeleton may keep
 * (DESIGN.md § Motion).
 */
export function SkeletonBar({ className, style }: { className?: string; style?: React.CSSProperties }) {
  return (
    <span
      aria-hidden="true"
      className={cn('inline-block h-[0.75em] max-w-full rounded align-middle animate-pulse', className)}
      style={{ backgroundColor: 'var(--color-border-default)', ...style }}
    />
  );
}

export default SkeletonBar;
