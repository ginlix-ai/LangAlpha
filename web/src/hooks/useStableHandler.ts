import { useCallback } from 'react';
import { useLatestRef } from './useLatestRef';

/**
 * Identity-stable wrapper that always invokes the latest committed render's
 * closure (the useEvent pattern). For event handlers only: the ref is
 * re-pointed in a layout effect, so every passive effect of the commit, a
 * child's or one declared earlier included, already reaches the new closure,
 * but a call *during* render or from a child's layout effect could reach a
 * one-commit-old one. Use it to pass handlers into memoized children without
 * letting upstream dependency churn (e.g. per-chunk hook re-renders) break
 * the memo.
 */
export function useStableHandler<A extends unknown[], R>(fn: (...args: A) => R): (...args: A) => R {
  const ref = useLatestRef(fn);
  return useCallback((...args: A) => ref.current(...args), [ref]);
}
