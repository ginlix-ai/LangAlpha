import { useEffect, useEffectEvent, useRef } from 'react';
import { focusIsLost, lastInputWasPointer } from '@/lib/inputModality';

const FOCUSABLE = 'button:not([disabled]), a[href], input:not([disabled]), [tabindex]:not([tabindex="-1"])';

/**
 * Marks a scope whose landing is still to come. What remounts the scope's
 * rows meanwhile (the nav tree's kit swap) waits on it, since the remount
 * would take the pending landing with the hook that holds it.
 */
export const LANDING_ATTR = 'data-focus-landing';

/**
 * Focus for a "show more" row pressed from the keyboard. The row leaves while
 * its page loads, and for good once nothing is left, which drops the focus on
 * the page; it goes to the first item the page added instead, or back to the
 * row when the page brought none, or to the last item when the row is gone too.
 * A click leaves focus where clicking leaves it.
 *
 * Items are told apart by id rather than by count, since a list can already
 * hold an item from past the page it shows (the current workspace), which a
 * position would skip over. Focus lands once `busy` is false and the ids or
 * `busy` changed since the press. `target(scope, id)` finds the item with
 * `id` inside `scope`, the element the press named, which outlives the row;
 * with `null` it finds the row. Returns the press, to call from its click.
 */
export function useLoadMoreFocus(
  ids: readonly string[],
  busy: boolean,
  target: (scope: Element, id: string | null) => Element | null | undefined,
): (scope: Element | null) => void {
  const pending = useRef<{ scope: Element; seen: Set<string> } | null>(null);
  const settle = (next: { scope: Element; seen: Set<string> } | null) => {
    pending.current?.scope.removeAttribute(LANDING_ATTR);
    pending.current = next;
    next?.scope.setAttribute(LANDING_ATTR, '');
  };
  const land = useEffectEvent(() => {
    const press = pending.current;
    if (!press || busy) return;
    settle(null);
    if (!press.scope.isConnected || !focusIsLost()) return;
    const added = ids.find((id) => !press.seen.has(id));
    const last = ids.at(-1);
    const el = target(press.scope, added ?? null) ?? (last !== undefined ? target(press.scope, last) : null);
    const focusable = el?.matches(FOCUSABLE) ? el : el?.querySelector(FOCUSABLE);
    if (focusable instanceof HTMLElement) focusable.focus();
  });
  // Keyed on the ids' content, not the array, which is rebuilt every render:
  // a render between the press and the page's arrival would land it early.
  const listed = ids.join('\n');
  useEffect(() => land(), [listed, busy]);
  useEffect(() => () => pending.current?.scope.removeAttribute(LANDING_ATTR), []);
  return (scope) => {
    settle(scope && !lastInputWasPointer() ? { scope, seen: new Set(ids) } : null);
  };
}
