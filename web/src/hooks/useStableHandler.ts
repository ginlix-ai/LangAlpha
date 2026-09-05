import { useCallback, useEffect, useRef } from 'react';

/**
 * Identity-stable wrapper that always invokes the latest render's closure
 * (the useEvent pattern). For event handlers only: the ref is re-pointed
 * after commit, so a call *during* render could reach a one-commit-old
 * closure. Use it to pass handlers into memoized children without letting
 * upstream dependency churn (e.g. per-chunk hook re-renders) break the memo.
 */
export function useStableHandler<A extends unknown[], R>(fn: (...args: A) => R): (...args: A) => R {
  const ref = useRef(fn);
  useEffect(() => {
    ref.current = fn;
  });
  return useCallback((...args: A) => ref.current(...args), []);
}
