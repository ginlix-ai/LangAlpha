import { registerAuthReset } from '@/lib/authResets';
import { storageUser, userLocalStorage } from '@/lib/userStorage';
import type { WidgetStorageWrite } from './widgetProtocol';

/**
 * What a widget kept in its localStorage, kept for it in the app's.
 *
 * A widget frame has an opaque origin, which the browser refuses storage, so
 * the frame's stand-in reports each write and the next render of the same
 * widget is seeded with the result. Scoped to the signed-in user, since a
 * widget's state can hold what its user typed. Bounded twice: per widget, so
 * one widget cannot take the rest, and in all, so widgets never fill the
 * quota the app's own settings are written into.
 *
 * A widget can write on every keystroke or animation frame, and a record runs
 * to 256 KB, so the records are read once into memory, writes land there, and
 * each burst of them reaches localStorage once, a microtask later.
 */

const RECORD_PREFIX = 'widgetStorage:';
const INDEX_KEY = 'widgetStorage.index';
/** Per widget, as the serialized record's length. */
export const WIDGET_STORAGE_MAX = 256 * 1024;
/** All widgets together; the least recently written go first. */
const TOTAL_MAX = 1024 * 1024;

type Entry = { id: string; size: number };

/** A widget's items, and the sum of their `pairSize`s. */
interface Held {
  items: Map<string, string>;
  pairs: number;
}

interface Mirror {
  /** Whose storage this mirrors: an account switch reads afresh. */
  user: string | null;
  index: Entry[] | null;
  records: Map<string, Held>;
  /** Written since the last flush, least recently written first. */
  dirty: Set<string>;
}

let mirror: Mirror | null = null;
let flushQueued = false;

/** Takes a write another frame of the widget made. */
export type WidgetStorageListener = (write: WidgetStorageWrite) => void;

/**
 * The frames of each widget open now. A widget can be open twice at once (the
 * inline frame and its fullscreen copy), and each frame holds its own copy of
 * the items, read when it loaded: unless the other's writes reach it, its next
 * read-modify-write puts back what the other just changed.
 */
const frames = new Map<string, Set<WidgetStorageListener>>();

/**
 * The account each frame was opened under. An in-place account switch leaves
 * the old account's frames mounted for a moment, holding that account's
 * items; a write from one then would land in the new account's storage, and
 * reach the new account's frames of the same widget.
 */
const openedBy = new WeakMap<WidgetStorageListener, string | null>();

/** Hands `listener` every write kept for `id` that another frame made. */
export function subscribeWidgetStorage(id: string, listener: WidgetStorageListener): () => void {
  let set = frames.get(id);
  if (!set) frames.set(id, (set = new Set()));
  set.add(listener);
  openedBy.set(listener, storageUser());
  return () => {
    set.delete(listener);
    if (set.size === 0 && frames.get(id) === set) frames.delete(id);
  };
}

/** Drops what is held in memory; the next read starts from localStorage. */
export function resetWidgetStorage(): void {
  mirror = null;
}
registerAuthReset(resetWidgetStorage);

// Another tab of the app writing the same records: read them again rather
// than write a stale copy over them. A burst's flush always runs first, being
// a microtask, so nothing of this tab's is pending when the event arrives.
if (typeof window !== 'undefined') {
  window.addEventListener('storage', (e) => {
    if (e.key === null || e.key.includes(RECORD_PREFIX) || e.key.includes(INDEX_KEY)) mirror = null;
  });
}

function current(): Mirror {
  const user = storageUser();
  if (!mirror || mirror.user !== user) mirror = { user, index: null, records: new Map(), dirty: new Set() };
  return mirror;
}

function readIndex(): Entry[] {
  try {
    const parsed: unknown = JSON.parse(userLocalStorage.getItem(INDEX_KEY) ?? '[]');
    if (!Array.isArray(parsed)) return [];
    return parsed.filter(
      (e): e is Entry => !!e && typeof e.id === 'string' && typeof e.size === 'number' && Number.isFinite(e.size),
    );
  } catch {
    return [];
  }
}

/** One item's share of the serialized record, its comma aside. */
function pairSize(key: string, value: string): number {
  return JSON.stringify(key).length + 1 + JSON.stringify(value).length;
}

/** The serialized record's length: `{}` is 2, each item adds its pair, and each after the first a comma. */
function recordSize(pairs: number, count: number): number {
  return 2 + pairs + Math.max(0, count - 1);
}

/** A record that does not parse is no record. */
function load(m: Mirror, id: string): Held {
  let held = m.records.get(id);
  if (held) return held;
  const items = new Map<string, string>();
  try {
    const parsed: unknown = JSON.parse(userLocalStorage.getItem(RECORD_PREFIX + id) ?? '{}');
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      for (const [k, v] of Object.entries(parsed)) if (typeof v === 'string') items.set(k, v);
    }
  } catch {
    /* none */
  }
  let pairs = 0;
  for (const [k, v] of items) pairs += pairSize(k, v);
  held = { items, pairs };
  m.records.set(id, held);
  return held;
}

/** The widget's stored items, or `{}`. */
export function readWidgetStorage(id: string): Record<string, string> {
  return Object.fromEntries(load(current(), id).items);
}

/**
 * Apply one write, and pass it to the widget's other frames (all but `from`,
 * the frame that made it). A write that would take the widget past its cap is
 * dropped, as a full Storage refuses it, and reaches no frame.
 */
export function writeWidgetStorage(id: string, write: WidgetStorageWrite, from?: WidgetStorageListener): void {
  const user = storageUser();
  if (from && openedBy.has(from) && openedBy.get(from) !== user) return;
  const m = current();
  const held = load(m, id);
  // Even a write that changes nothing counts as the widget's latest, for which
  // records go first past the total.
  if ('clear' in write) {
    held.items.clear();
    held.pairs = 0;
  } else if (write.value === null) {
    const was = held.items.get(write.key);
    if (was !== undefined) held.pairs -= pairSize(write.key, was);
    held.items.delete(write.key);
  } else {
    const was = held.items.get(write.key);
    const pairs = held.pairs - (was === undefined ? 0 : pairSize(write.key, was)) + pairSize(write.key, write.value);
    if (recordSize(pairs, held.items.size + (was === undefined ? 1 : 0)) > WIDGET_STORAGE_MAX) {
      console.warn(`[widget] localStorage write dropped: over ${WIDGET_STORAGE_MAX / 1024} KB for this widget`);
      return;
    }
    held.items.set(write.key, write.value);
    held.pairs = pairs;
  }
  m.dirty.delete(id);
  m.dirty.add(id);
  if (!flushQueued) {
    flushQueued = true;
    queueMicrotask(flush);
  }
  for (const listener of frames.get(id) ?? []) if (listener !== from && openedBy.get(listener) === user) listener(write);
}

function flush(): void {
  flushQueued = false;
  const m = mirror;
  // Signed out or switched since the writes: they were the last account's,
  // and the storage now reached is someone else's.
  if (!m || m.user !== storageUser()) {
    mirror = null;
    return;
  }
  if (m.dirty.size === 0) return;
  const index = (m.index ??= readIndex());
  let changed = false;
  for (const id of m.dirty) {
    const held = m.records.get(id);
    if (!held) continue;
    const at = index.findIndex((e) => e.id === id);
    if (held.items.size === 0) {
      userLocalStorage.removeItem(RECORD_PREFIX + id);
      if (at >= 0) index.splice(at, 1);
      changed = true;
      continue;
    }
    // Refused (the quota, most likely): what is stored stays as it was, and
    // so does every other widget's record and the index that names them.
    if (!userLocalStorage.setItem(RECORD_PREFIX + id, JSON.stringify(Object.fromEntries(held.items)))) {
      console.warn('[widget] localStorage write not kept: the browser refused it');
      continue;
    }
    if (at >= 0) index.splice(at, 1);
    index.unshift({ id, size: recordSize(held.pairs, held.items.size) });
    changed = true;
  }
  m.dirty.clear();
  if (!changed) return;

  let total = 0;
  const kept: Entry[] = [];
  for (const e of index) {
    if (total + e.size > TOTAL_MAX) {
      userLocalStorage.removeItem(RECORD_PREFIX + e.id);
      m.records.delete(e.id);
      continue;
    }
    total += e.size;
    kept.push(e);
  }
  m.index = kept;
  userLocalStorage.setItem(INDEX_KEY, JSON.stringify(kept));
}
