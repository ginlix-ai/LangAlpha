import React, { useMemo } from 'react';
import { cn } from '@/lib/utils';

interface TextShimmerProps {
  children: string;
  as?: React.ElementType;
  className?: string;
  /** Seconds per sweep. */
  duration?: number;
  spread?: number;
}

/**
 * A running label (DESIGN.md § Status & liveness). The sweep is the
 * `.shimmer-sweep` keyframe in styles/animations.css, which the reduced-motion
 * rule stops; it was a framer loop on `background-position`, which ran on the
 * JS frame loop and ignored that setting.
 */
export function TextShimmer({
  children,
  as: Component = 'p',
  className,
  duration = 2,
  spread = 2,
}: TextShimmerProps) {
  const dynamicSpread = useMemo(() => {
    return children.length * spread;
  }, [children, spread]);

  return (
    <Component
      className={cn(
        'shimmer-sweep relative inline-block bg-size-[250%_100%,auto] bg-clip-text',
        'text-transparent [--base-color:#a1a1aa] [--base-gradient-color:#000]',
        '[--bg:linear-gradient(90deg,#0000_calc(50%-var(--spread)),var(--base-gradient-color),#0000_calc(50%+var(--spread)))] [background-repeat:no-repeat,padding-box]',
        'dark:[--base-color:#71717a] dark:[--base-gradient-color:#ffffff] dark:[--bg:linear-gradient(90deg,#0000_calc(50%-var(--spread)),var(--base-gradient-color),#0000_calc(50%+var(--spread)))]',
        className
      )}
      style={{
        '--spread': `${dynamicSpread}px`,
        '--shimmer-period': `${duration}s`,
        backgroundImage: `var(--bg), linear-gradient(var(--base-color), var(--base-color))`,
      } as React.CSSProperties}
    >
      {children}
    </Component>
  );
}
