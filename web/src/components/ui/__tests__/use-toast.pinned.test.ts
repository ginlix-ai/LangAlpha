import { describe, it, expect } from 'vitest';
import { reducer, type ToastProps } from '../use-toast';

// The cap is a policy for transient notices: keep the newest few, drop the
// oldest. That is wrong for a notice carrying the only control out of a broken
// state — a stale build's Reload — because by the time anything else arrives
// it is the oldest thing on screen, so it is always the one that goes.

const add = (state: { toasts: ToastProps[] }, id: string, pinned?: boolean) =>
  reducer(state, { type: 'ADD_TOAST', toast: { id, pinned } });

const ids = (state: { toasts: ToastProps[] }) => state.toasts.map((t) => t.id);

describe('the toast cap', () => {
  it('drops the oldest ordinary notice', () => {
    let state = { toasts: [] as ToastProps[] };
    for (const id of ['a', 'b', 'c', 'd']) state = add(state, id);

    expect(ids(state)).toEqual(['d', 'c', 'b']);
  });

  it('keeps a pinned notice and drops the oldest unpinned one instead', () => {
    let state = add({ toasts: [] as ToastProps[] }, 'stale', true);
    for (const id of ['a', 'b', 'c']) state = add(state, id);

    expect(ids(state)).toContain('stale');
    expect(ids(state)).toHaveLength(3);
    expect(ids(state)).toEqual(['c', 'b', 'stale']);
  });

  it('goes over the limit rather than dropping a pinned notice', () => {
    // Pinning is a claim that losing the toast loses something, and nothing
    // above this would notice it went. Exceeding the cap is the safer miss.
    let state = { toasts: [] as ToastProps[] };
    for (const id of ['p1', 'p2', 'p3', 'p4']) state = add(state, id, true);

    expect(ids(state)).toHaveLength(4);
  });
});
