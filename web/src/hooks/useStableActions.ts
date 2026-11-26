import { useMemo } from 'react';
import { useLatestRef } from './useLatestRef';

type AnyAction = (...args: never[]) => unknown;

/**
 * `useStableHandler` for a record of handlers: each one present gets an
 * identity that holds across renders and calls the latest committed closure.
 * The record's identity changes only when which handlers are present does,
 * so a host can hand a memoized tree a bundle whose members it rebuilds on
 * every render. Same caveat as `useStableHandler`: for event handlers, not
 * for calls made during render.
 */
export function useStableActions<T extends { [K in keyof T]?: AnyAction }>(actions: T): T {
  const latest = useLatestRef(actions);
  const names = (Object.keys(actions) as (keyof T & string)[])
    .filter((name) => typeof actions[name] === 'function')
    .join('\n');
  return useMemo(() => {
    const stable: Partial<Record<keyof T, unknown>> = {};
    for (const name of names ? (names.split('\n') as (keyof T & string)[]) : []) {
      stable[name] = (...args: unknown[]) => (latest.current[name] as ((...a: unknown[]) => unknown) | undefined)?.(...args);
    }
    return stable as T;
  }, [names, latest]);
}
