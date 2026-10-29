import { useSyncExternalStore } from 'react';

/**
 * The reader's OS motion setting, for the motion CSS cannot reach. The global
 * block in `styles/animations.css` already cuts every CSS transition and
 * keyframe, and `MotionConfig reducedMotion="user"` (lib/lazyWithMotion) cuts
 * framer's transforms and layout. Neither touches a framer opacity or
 * background loop, an imperative `animate()` call, or a `behavior: 'smooth'`
 * scroll, which the browser honours whatever the CSS says; those ask here.
 */

const QUERY = '(prefers-reduced-motion: reduce)';

// One query for the page. A matchMedia call parses the query and makes a
// MediaQueryList, and every fold, live row and running icon in a long thread
// reads this, some on every streamed frame. Remade only when matchMedia itself
// is replaced, as a test does.
let mql: MediaQueryList | null = null;
let madeBy: typeof window.matchMedia | undefined;
function query(): MediaQueryList | null {
  if (typeof window === 'undefined' || !window.matchMedia) return null;
  if (madeBy !== window.matchMedia) {
    madeBy = window.matchMedia;
    mql = window.matchMedia(QUERY);
  }
  return mql;
}

/** The reader asked the OS for less motion. */
export function prefersReducedMotion(): boolean {
  return !!query()?.matches;
}

/** How a programmatic scroll should travel: instant for a reader who asked for
 *  less motion, smooth otherwise. */
export function scrollMotion(): ScrollBehavior {
  return prefersReducedMotion() ? 'auto' : 'smooth';
}

// Every subscriber hears the query's changes through one listener on it.
const subscribers = new Set<() => void>();
const notify = () => {
  for (const onChange of subscribers) onChange();
};

function subscribe(onChange: () => void): () => void {
  const q = query();
  if (subscribers.size === 0) q?.addEventListener?.('change', notify);
  subscribers.add(onChange);
  return () => {
    subscribers.delete(onChange);
    if (subscribers.size === 0) q?.removeEventListener?.('change', notify);
  };
}

/**
 * True while motion is allowed, and it follows a change to the OS setting
 * without a reload. A store snapshot rather than a read in render, which the
 * React Compiler would freeze at its first value.
 */
export function useMotionSafe(): boolean {
  return !useSyncExternalStore(subscribe, prefersReducedMotion, () => false);
}
