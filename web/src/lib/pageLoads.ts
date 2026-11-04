import { useSyncExternalStore } from 'react';
import { registerAuthReset } from '@/lib/authResets';

/**
 * What a file's page loaded, as the publish review sends it: `now` from the
 * documents frames show now, and `earlier` from documents of it this tab
 * showed before, less what `now` holds. While the review's own load of the
 * page is open (`scout`), it alone is `now`: it ran the code as it is, and a
 * frame left open since may hold code the agent has changed, so what that
 * frame loaded is `earlier`. A file only `earlier` names goes out unless the
 * owner leaves it out, since a click may be all that loads it; one the page
 * stopped loading after the agent changed it is theirs to take off.
 *
 * A workspace file's path never starts with "/". One that does is an address
 * the page loaded from the root of its host (`bridge/loads.js`), which no view
 * of a workspace file serves: the server warns of it, never takes it for a file.
 */
export interface PageLoads {
  now: readonly string[];
  earlier: readonly string[];
}

export const NO_PAGE_LOADS: PageLoads = { now: [], earlier: [] };
/** What the server takes in one report, and where the bridge stops counting (`bridge/loads.js`). */
export const MAX_PAGE_LOADS = 500;
/** The longest path the server takes; one longer is no file it could publish. */
const MAX_PATH_LENGTH = 1024;

interface Frame {
  loads: readonly string[];
  scout: boolean;
}
/** Each frame showing a file, by its bridge, and what that document loaded. */
const open = new Map<string, Map<object, Frame>>();
/** What documents of a file since closed loaded in this tab. */
const closed = new Map<string, readonly string[]>();
const views = new Map<string, PageLoads>();
const listeners = new Set<() => void>();

const keyOf = (workspaceId: string, filePath: string) => `${workspaceId}:${filePath}`;
const fit = (paths: Iterable<string>): readonly string[] =>
  [...new Set(paths)].filter((path) => path.length <= MAX_PATH_LENGTH).sort().slice(0, MAX_PAGE_LOADS);
const same = (a: readonly string[], b: readonly string[]) => a.length === b.length && a.every((path, i) => path === b[i]);
const scouting = (frames: Map<object, Frame>) => [...frames.values()].some((frame) => frame.scout);
/** Whether a frame besides a scout loaded anything, which leaves `now` as the first scout comes and returns as the last goes. */
const othersLoaded = (frames: Map<object, Frame>) => [...frames.values()].some((frame) => !frame.scout && frame.loads.length > 0);

function changed(key: string): void {
  views.delete(key);
  listeners.forEach((listener) => listener());
}

function close(key: string, loads: readonly string[]): void {
  const before = closed.get(key) ?? [];
  const after = fit([...before, ...loads]);
  if (same(after, before)) return;
  closed.set(key, after);
  changed(key);
}

/**
 * What the document `frame` shows has loaded, its whole list each time
 * (`bridge/loads.js`), so a reload starts it over. Kept here rather than with
 * the frame, as the dialog and the crumb that read it outlive the frame. A
 * frame not counted open (gone before the effect counting it ran) adds to
 * what closed documents loaded.
 */
export function notePageLoads(workspaceId: string, filePath: string, paths: readonly string[], frame: object): void {
  const key = keyOf(workspaceId, filePath);
  const shown = open.get(key)?.get(frame);
  if (!shown) {
    close(key, paths);
    return;
  }
  const loads = fit(paths);
  if (same(loads, shown.loads)) return;
  // A document's list only grows, so one missing a path is a new document.
  const kept = new Set(loads);
  const dropped = shown.loads.filter((path) => !kept.has(path));
  if (dropped.length) closed.set(key, fit([...(closed.get(key) ?? []), ...dropped]));
  shown.loads = loads;
  changed(key);
}

/**
 * Counts a frame showing the file while its bridge is up, which reports what
 * the page loads as it changes. `frame` is that bridge; `scout` marks the
 * review's own load of the page (`PageScout`). Returns the release, which
 * keeps what the document loaded as `earlier`.
 */
export function notePageOpen(
  workspaceId: string, filePath: string, frame: object, { scout = false }: { scout?: boolean } = {},
): () => void {
  const key = keyOf(workspaceId, filePath);
  const frames = open.get(key) ?? new Map<object, Frame>();
  const moves = scout && !scouting(frames) && othersLoaded(frames);
  frames.set(frame, { loads: [], scout });
  open.set(key, frames);
  if (moves) changed(key);
  return () => {
    const gone = frames.get(frame);
    frames.delete(frame);
    if (frames.size === 0 && open.get(key) === frames) open.delete(key);
    // Out of `now` whether or not `earlier` already held it.
    if (gone?.loads.length) closed.set(key, fit([...(closed.get(key) ?? []), ...gone.loads]));
    if (gone?.loads.length || (gone?.scout && !scouting(frames) && othersLoaded(frames))) changed(key);
  };
}

/** The report as it stands, for a send outside render. The same object until it changes. */
export function pageLoadsOf(workspaceId: string, filePath: string): PageLoads {
  const key = keyOf(workspaceId, filePath);
  let view = views.get(key);
  if (!view) {
    const frames = [...(open.get(key)?.values() ?? [])];
    const scouts = frames.some((frame) => frame.scout);
    const now = fit(frames.filter((frame) => frame.scout || !scouts).flatMap((frame) => frame.loads));
    const shown = new Set(now);
    const stale = frames.filter((frame) => !frame.scout && scouts).flatMap((frame) => frame.loads);
    const earlier = fit([...(closed.get(key) ?? []), ...stale].filter((path) => !shown.has(path)));
    view = now.length || earlier.length ? { now, earlier } : NO_PAGE_LOADS;
    views.set(key, view);
  }
  return view;
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function usePageLoads(workspaceId: string, filePath: string): PageLoads {
  return useSyncExternalStore(subscribe, () => pageLoadsOf(workspaceId, filePath));
}

registerAuthReset(() => {
  closed.clear();
  views.clear();
  for (const frames of open.values()) for (const frame of frames.values()) frame.loads = [];
  listeners.forEach((listener) => listener());
});
