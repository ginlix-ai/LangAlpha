import { useLayoutEffect, useRef, type RefObject } from 'react';

/**
 * A ref holding the value of the last commit, for handlers, observers and
 * effects that must read the current value without re-subscribing on it.
 * Assigning `ref.current = value` in render instead makes the React Compiler
 * skip the whole component, and a render that never commits would leak its
 * value. Written in a layout effect, so this component's later effects and
 * every passive effect see it; a child's layout effect would not, so it is
 * not for values a child reads during its own commit.
 */
export function useLatestRef<T>(value: T): RefObject<T> {
  const ref = useRef(value);
  useLayoutEffect(() => {
    ref.current = value;
  });
  return ref;
}
