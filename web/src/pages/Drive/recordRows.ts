import type { DriveRecordEvent, DriveRecordItem, DriveRecordVersion } from '@/types/drive';

/**
 * One row of a file's record: a version, an event, or a change, which is two
 * or more versions an agent saved under one check-out with one reason.
 */
export type RecordRow =
  | { kind: 'version'; version: DriveRecordVersion }
  | { kind: 'event'; event: DriveRecordEvent }
  | {
    kind: 'change';
    changeId: string;
    /** Newest first, as the record lists them. */
    versions: DriveRecordVersion[];
  };

/**
 * The record's items as rows, newest first. Versions of one change join only
 * while they sit next to each other, so a version by someone else or an event
 * between two of them splits the change into two rows. Call it on every page
 * loaded so far, flattened: a change that straddles a page boundary is one row.
 * A change of one version is a version row.
 */
export function recordRows(items: readonly DriveRecordItem[]): RecordRow[] {
  const rows: RecordRow[] = [];
  for (const item of items) {
    if (item.type === 'event') {
      rows.push({ kind: 'event', event: item });
      continue;
    }
    const changeId = item.change_id;
    const last = rows.at(-1);
    if (changeId && last && last.kind !== 'event') {
      if (last.kind === 'change' && last.changeId === changeId) {
        last.versions.push(item);
        continue;
      }
      if (last.kind === 'version' && last.version.change_id === changeId) {
        rows[rows.length - 1] = { kind: 'change', changeId, versions: [last.version, item] };
        continue;
      }
    }
    rows.push({ kind: 'version', version: item });
  }
  return rows;
}

/** A change's lines added and removed, summed, or null when any of its versions has no counts. */
export function changeStats(versions: readonly DriveRecordVersion[]): { added: number | null; removed: number | null } {
  let added = 0;
  let removed = 0;
  for (const v of versions) {
    if (v.added === null || v.removed === null) return { added: null, removed: null };
    added += v.added;
    removed += v.removed;
  }
  return { added, removed };
}
