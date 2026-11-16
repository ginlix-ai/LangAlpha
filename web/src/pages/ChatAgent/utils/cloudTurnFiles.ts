/**
 * Where the files of a Home turn that ran on the cloud stand-in are now.
 *
 * The turn names them by their path in the stand-in's folder. Once the
 * machine Home lives on is back, a hand-back copies them into a folder of
 * their own in Home there (the folder the server names, dated), so the path
 * alone leads nowhere in Home, or to another file Home already had under
 * that name. The first hand-back after the turn that carried the path holds
 * its copy, or, when that hand-back found the file unchanged, the folder it
 * names for the earlier copy does; until one does, the only copy is in the
 * stand-in, a workspace of its own.
 *
 * A mapping, not a lookup by name: only the hand-back knows its folder and
 * which version of a file went into which one.
 */
import type { HomeFallback } from '@/types/sse';

import type { HomeHandbacks } from './api/workspaces';
import type { TurnFile } from './turnFiles';

/**
 * Microseconds since the epoch of a server timestamp, NaN when it is not one.
 * `Date.parse` keeps milliseconds only, and a hand-back's watermark and the
 * turn placed just after it can fall in the same millisecond.
 */
export function serverMicros(iso: string | undefined | null): number {
  if (!iso) return Number.NaN;
  const fraction = /\.(\d+)/.exec(iso)?.[1] ?? '';
  const seconds = Date.parse(iso.replace(/\.\d+/, ''));
  return seconds * 1000 + Number((fraction + '000000').slice(0, 6));
}

export function placeCloudTurnFiles(
  files: readonly TurnFile[],
  fallback: HomeFallback,
  handbacks: HomeHandbacks,
): TurnFile[] {
  const at = serverMicros(fallback.at);
  const list = handbacks.handbacks;
  const first = list.findIndex((h) => Number.isNaN(at) || serverMicros(h.through) >= at);
  return files.map((file) => {
    // A path into another workspace names that workspace, wherever the turn ran.
    if (file.workspaceId) return file;
    const path = file.path.replace(/^\.\//, '');
    const folder = first < 0 ? null : heldIn(list, first, path);
    if (folder) return { ...file, path: `${folder.replace(/\/+$/, '')}/${path}` };
    return { ...file, workspaceId: handbacks.stand_in_workspace_id };
  });
}

/**
 * The folder holding `path` as hand-backs from `from` on first found it. One
 * that neither carried nor kept it (it waited for room, or stayed on the
 * cloud) leaves the question to the next.
 */
function heldIn(list: HomeHandbacks['handbacks'], from: number, path: string): string | null {
  for (let i = from; i < list.length; i++) {
    const { files, folder, kept } = list[i];
    if (folder && files.includes(path)) return folder;
    if (Object.hasOwn(kept, path)) return kept[path];
  }
  return null;
}

/**
 * The earliest placement among `cloudTurns`, which the hand-backs are read
 * from; the epoch when none says, so every one is read.
 */
export function cloudTurnsSince(cloudTurns: Iterable<HomeFallback>): string {
  let earliest: string | null = null;
  let earliestMicros = Number.POSITIVE_INFINITY;
  for (const { at } of cloudTurns) {
    const t = serverMicros(at);
    if (!Number.isNaN(t) && t < earliestMicros) [earliest, earliestMicros] = [at, t];
  }
  // As the server wrote it, so its microseconds reach the server again.
  return earliest ?? new Date(0).toISOString();
}

/**
 * Each turn's files with those of its cloud turns placed where they are now,
 * and whether one of them is still only on the stand-in (so the hand-backs
 * are worth reading again).
 */
export function placeCloudTurns(
  files: ReadonlyMap<number, TurnFile[]>,
  cloudTurns: ReadonlyMap<number, HomeFallback>,
  handbacks: HomeHandbacks,
): { placed: Map<number, TurnFile[]>; unplaced: boolean } {
  let unplaced = false;
  const placed = new Map(files);
  for (const [turnIndex, fallback] of cloudTurns) {
    const own = files.get(turnIndex);
    if (!own) continue;
    const moved = placeCloudTurnFiles(own, fallback, handbacks);
    if (moved.some((f, i) => !own[i].workspaceId && f.workspaceId === handbacks.stand_in_workspace_id)) unplaced = true;
    placed.set(turnIndex, moved);
  }
  return { placed, unplaced };
}
