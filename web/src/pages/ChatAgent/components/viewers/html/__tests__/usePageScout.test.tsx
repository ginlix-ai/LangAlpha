/**
 * The scout runs the page the review reads, as it is now, whether or not a
 * frame shows it. It starts once the list's first read names the page's code,
 * so it never runs code older than that; once a later read names other code,
 * it loads the page again and holds the review until the new one reports.
 */
import { act, renderHook } from '@testing-library/react';
import { isValidElement, type ReactElement } from 'react';
import { describe, expect, it, vi } from 'vitest';

vi.mock('../PageScout', () => ({ PageScout: () => null }));

import { usePageScout } from '../usePageScout';

type ScoutProps = { onSettled: () => void; onFailed: () => void };
const scoutOf = (element: unknown) => {
  if (!isValidElement(element)) throw new Error('no scout');
  return { key: element.key, props: (element as ReactElement<ScoutProps>).props };
};
const following = (path: string) => renderHook(({ code }: { code?: string }) => {
  const scout = usePageScout('ws', path);
  scout.follow(code);
  return scout;
}, { initialProps: { code: undefined as string | undefined } });

describe('usePageScout', () => {
  it('runs the page after the first read names its code, and again when that code moves', () => {
    const { result, rerender } = following('a.html');
    // Held, and read first, before anything runs.
    expect(result.current.holding).toBe(true);
    expect(result.current.reads).toBe(true);
    expect(result.current.element).toBeFalsy();

    rerender({ code: 'one' });
    const first = scoutOf(result.current.element);
    // No read while it runs: its reports would each read the list again.
    expect(result.current.reads).toBe(false);
    expect(result.current.holding).toBe(true);
    act(() => first.props.onSettled());
    expect(result.current.holding).toBe(false);
    expect(result.current.reads).toBe(true);

    // The read after it names the same code: what the scout ran.
    rerender({ code: 'one' });
    expect(result.current.holding).toBe(false);
    expect(scoutOf(result.current.element).key).toBe(first.key);

    // The agent changed the page, even before the scout reported: loaded
    // again, a new mount and grant, and held.
    rerender({ code: 'two' });
    expect(result.current.holding).toBe(true);
    expect(result.current.reads).toBe(false);
    const again = scoutOf(result.current.element);
    expect(again.key).not.toBe(first.key);
    act(() => again.props.onSettled());
    expect(result.current.holding).toBe(false);
  });

  it('holds the review when the page could not run, until a retry runs it again', () => {
    const { result, rerender } = following('a.html');
    rerender({ code: 'one' });
    const first = scoutOf(result.current.element);
    act(() => first.props.onFailed());
    expect(result.current.failed).toBe(true);
    expect(result.current.holding).toBe(true);
    // The start cap letting it go later still holds it.
    act(() => first.props.onSettled());
    expect(result.current.holding).toBe(true);

    act(() => result.current.retry());
    const again = scoutOf(result.current.element);
    expect(again.key).not.toBe(first.key);
    expect(result.current.failed).toBe(false);
    expect(result.current.holding).toBe(true);
    act(() => again.props.onSettled());
    expect(result.current.holding).toBe(false);
  });

  it('runs nothing for a file that is no page', () => {
    const { result, rerender } = following('a.csv');
    rerender({ code: 'one' });
    rerender({ code: 'two' });
    expect(result.current.holding).toBe(false);
    expect(result.current.reads).toBe(true);
    expect(result.current.element).toBeFalsy();
  });
});
