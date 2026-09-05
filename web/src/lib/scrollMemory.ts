import { useLayoutEffect } from 'react';
import type React from 'react';
import { registerAuthReset } from '@/lib/authResets';

/**
 * Session-scoped scroll positions keyed by a stable id (route path, thread id).
 * Module-level so positions survive route unmounts — the whole point: coming
 * back to a tab or thread lands where the user left, not at top/bottom.
 * `'bottom'` is a sticky sentinel: "the user was at the bottom", which for
 * growing content (chat) means re-pin to the new bottom, not a pixel offset.
 */
const positions = new Map<string, number | 'bottom'>();

// LRU bound for long-lived sessions (dashboards left open for days): route/page
// keys are finite, but thread keys accrue per thread visited. Entries are tiny;
// the cap is hygiene, not memory pressure. Deleted threads/workspaces also
// forget their keys eagerly at the delete sites.
const MAX_ENTRIES = 300;

export const scrollMemory = {
  get(key: string): number | 'bottom' | undefined {
    return positions.get(key);
  },
  set(key: string, value: number | 'bottom'): void {
    // Delete-then-set refreshes insertion order, making eviction least-recent.
    positions.delete(key);
    positions.set(key, value);
    if (positions.size > MAX_ENTRIES) {
      const oldest = positions.keys().next().value;
      if (oldest !== undefined) positions.delete(oldest);
    }
  },
  forget(key: string): void {
    positions.delete(key);
  },
  clear(): void {
    positions.clear();
  },
};

// Thread/route keys are per-account state — wipe on sign-out/account switch.
registerAuthReset(() => scrollMemory.clear());

/**
 * Keyed scroll persistence for a container the caller owns. Saves scrollTop
 * per key while the user scrolls; on key change restores the saved offset
 * (0 for never-visited keys, so positions don't bleed between routes sharing
 * the container).
 */
export function useScrollMemory(ref: React.RefObject<HTMLElement | null>, key: string): void {
  // Restore before paint so the incoming route never flashes at the stale offset.
  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    const saved = scrollMemory.get(key);
    const target = typeof saved === 'number' ? saved : 0;
    el.scrollTop = target;
    if (target === 0) return;
    // Already tall enough, which is the common case and needs nothing further.
    // The write clamps to what the port can hold, so reading it back is the test.
    if (el.scrollTop >= target - 4) return;

    // The offset this hook last wrote. Any other value in the port means
    // something else moved it -- a wheel, a key, a drag on the scrollbar -- and
    // where the user is now outranks where they were, so the restore stands
    // down. The check belongs here, immediately before the write, and not in a
    // `scroll` listener: a mutation callback is a microtask and the scroll event
    // it would race is dispatched a frame later, so the write would land first
    // and the listener would then compare the user's position against the value
    // that had just overwritten it and find them equal. Read back rather than
    // assumed to be `target`, since the write clamps to what the port can hold.
    let written = el.scrollTop;
    let stopped = false;
    const stop = () => {
      if (stopped) return;
      stopped = true;
      observer.disconnect();
      el.removeEventListener('load', attempt, true);
      el.removeEventListener('wheel', stop);
      el.removeEventListener('touchstart', stop);
    };
    const attempt = () => {
      if (stopped) return;
      if (el.scrollTop !== written) return stop(); // the port moved, and not by us
      if (el.scrollTop >= target - 4) return stop(); // reached
      el.scrollTop = target;
      written = el.scrollTop;
      if (written >= target - 4) stop();
    };
    // `subtree`, because the growth is inside the page the port wraps, not in
    // the port itself -- whose own box is pinned by the column and so never
    // resizes. `characterData`, because a route that renders its shell first and
    // fills the text in later grows without adding a node.
    const observer = new MutationObserver(attempt);
    observer.observe(el, { childList: true, subtree: true, characterData: true });
    // An image or an iframe finishing its load grows the page without touching
    // the DOM, so the observer above never hears about it. `load` does not
    // bubble; capture is how a listener on the port sees a descendant's.
    el.addEventListener('load', attempt, true);
    // The two gestures that mean "move this port" even when it cannot move yet:
    // a wheel or a swipe against a loading state too short to scroll leaves the
    // offset untouched, so the guard in `attempt` has nothing to notice, and the
    // article arriving a second later would snap the page out from under someone
    // who had already started reading it. Keys are deliberately not here -- a
    // keypress that scrolls is caught by the guard like any other movement, and
    // one that does not is usually someone typing in a field on the page.
    el.addEventListener('wheel', stop, { passive: true });
    el.addEventListener('touchstart', stop, { passive: true });
    requestAnimationFrame(attempt);
    return stop;
  }, [ref, key]);

  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    const onScroll = () => {
      scrollMemory.set(key, el.scrollTop);
    };
    el.addEventListener('scroll', onScroll, { passive: true });
    return () => el.removeEventListener('scroll', onScroll);
  }, [ref, key]);
}
