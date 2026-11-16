import React, { useLayoutEffect, useRef } from 'react';
import { writeRolling } from '@/lib/rollingDigits';

type RollingTextProps = Omit<React.HTMLAttributes<HTMLSpanElement>, 'children'> & { children: string };

/**
 * A span whose digits roll as its text ticks (`lib/rollingDigits`). The text
 * is written from a layout effect, never rendered, since React would otherwise
 * reconcile against the spans a roll puts there. Takes its text as children so
 * it can stand in for a plain span, as `TextShimmer`'s `as`.
 */
export function RollingText({ children, ...rest }: RollingTextProps): React.ReactElement {
  const ref = useRef<HTMLSpanElement>(null);
  useLayoutEffect(() => {
    if (ref.current) writeRolling(ref.current, children);
  }, [children]);
  return <span ref={ref} {...rest} />;
}
