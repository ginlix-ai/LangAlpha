import { useEffect, useState } from 'react';
import * as z from 'zod/mini';
import { useStorageUser, userLocalStorage } from './userStorage';

// Per user: one account's shapes say what it holds (how many workspaces, how
// long its watchlist is), and the session can change hands while this app is
// closed (another app on the origin shares it), so clearing at sign-out here
// would not be enough.
const PREFIX = 'lastShape.';

// What this module writes: a row count or a height in px, whole. Anything on
// the origin can write the slot, so a value outside that (a hand edit, a
// corrupt write) reads as no shape, not as a skeleton thousands of px tall.
const shapeSchema = z.int().check(z.gte(0), z.lte(10_000));

function readShape(key: string): number | null {
  const stored = userLocalStorage.getItem(PREFIX + key);
  // `Number('')` is 0, a shape this module could have written, so an empty
  // slot is turned away before it gets there.
  const parsed = shapeSchema.safeParse(stored ? Number(stored) : undefined);
  return parsed.success ? parsed.data : null;
}

/**
 * What a region held the last time it loaded on this device (a row count, a
 * height in px), or null if it never has. A skeleton drawn to it takes the
 * shape the data most likely will, so nothing around the region moves when
 * the data lands. `current` is the loaded value, null while loading, and is
 * kept for next time whenever it changes.
 */
export function useLastShape(key: string, current: number | null): number | null {
  // Read once per key and user, so the skeleton keeps one shape for its whole
  // load even as `current` is written for the next one.
  const user = useStorageUser();
  const [last, setLast] = useState(() => ({ key, user, value: readShape(key) }));
  let value = last.value;
  if (last.key !== key || last.user !== user) {
    value = readShape(key);
    setLast({ key, user, value });
  }
  useEffect(() => {
    if (current !== null) userLocalStorage.setItem(PREFIX + key, String(Math.round(current)));
  }, [key, current]);
  return value;
}

/**
 * The height a region's loaded content took last time here, for its skeleton
 * to hold. `ref` goes on the loaded content; `record` false stops keeping it
 * for a state a visit does not open in (a drawer the reader opened).
 */
export function useLastHeight(key: string, record = true): { ref: (el: HTMLElement | null) => void; height: number | null } {
  const [el, setEl] = useState<HTMLElement | null>(null);
  // Tagged with the key and the `record` it was measured under, so a region
  // that changes key (a tab) never files one state's height under the next
  // one's, and a height taken while not recording (the drawer open) is not
  // kept the moment recording resumes, before the layout it returns to is
  // measured. Observing again on either change measures that layout.
  const [measured, setMeasured] = useState<{ key: string; record: boolean; height: number } | null>(null);
  useEffect(() => {
    if (!el || typeof ResizeObserver === 'undefined') return;
    const ro = new ResizeObserver(() => {
      // 0 is a page taken out of layout, not a height to keep.
      if (el.offsetHeight > 0) setMeasured({ key, record, height: el.offsetHeight });
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, [el, key, record]);
  const height = useLastShape(key, record && measured?.key === key && measured.record ? measured.height : null);
  return { ref: setEl, height };
}
