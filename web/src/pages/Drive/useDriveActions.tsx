import { useTranslation } from 'react-i18next';
import { downloadFile, driveErrorType } from '@/api/drive';
import { ToastAction } from '@/components/ui/toast';
import { toast } from '@/components/ui/use-toast';
import { useDriveMutations } from '@/hooks/useDrive';
import { useLocale } from '@/hooks/useLocale';
import { formatBytes } from '@/lib/format';
import type { MemoChange } from '@/api/drive';
import type { DriveClearShown, DriveEntry, DriveTrashItem, DriveUsage } from '@/types/drive';
import { driveRefusal, type Refusal } from './driveRefusal';
import { joinDrivePath, nameOf, parentOf } from './drivePaths';
import { memoState } from './memoMarks';

/**
 * How a change asked from a name field or a row came out: where the entry is
 * now, or the reason it was refused. `field` is set only when the name was the
 * problem and the caller has a field to show it by; anything else has had its
 * toast already.
 */
export type NameResult = { ok: true; path: string } | { ok: false; field?: string };

/**
 * The changes a person makes to an entry, each saying how it went. Moving to
 * the trash asks nothing: it is undone from its toast, the way it would be
 * from the trash. The raw mutations stay in here.
 */
export function useDriveActions(space: string | null, usage: DriveUsage | null, onMoved: (from: string, to: string) => void) {
  const { t } = useTranslation();
  const locale = useLocale();
  const m = useDriveMutations(space);

  const refused = (err: unknown, subject: string, failed: string, size?: number | null): Refusal =>
    driveRefusal(err, t, { subject, failed, locale, usage, size });
  const say = ({ toast: body }: Refusal) => toast({ variant: 'destructive', ...body });

  const trash = async (entry: DriveEntry): Promise<boolean> => {
    try {
      const trashId = await m.trash.mutateAsync(entry.path);
      toast({
        description: t('drive:trash.moved', { name: entry.name }),
        action: (
          <ToastAction altText={t('drive:trash.undo')} onClick={() => void undo(trashId, entry)}>
            {t('drive:trash.undo')}
          </ToastAction>
        ),
      });
      return true;
    } catch (err) {
      say(refused(err, entry.name, t('drive:errors.trashFailed')));
      return false;
    }
  };

  const undo = async (trashId: string, entry: DriveEntry) => {
    try {
      await m.restore.mutateAsync(trashId);
    } catch (err) {
      say(refused(err, entry.name, t('drive:errors.restoreFailed', { name: entry.name }), entry.size));
    }
  };


  /** Moves or renames. Asked from a name field (`inline`), a refused name comes back for the field. */
  const move = async (entry: DriveEntry, to: string, inline = false): Promise<NameResult> => {
    if (to === entry.path) return { ok: true, path: to };
    try {
      const moved = await m.move.mutateAsync({ from: entry.path, to });
      onMoved(entry.path, moved.path);
      return { ok: true, path: moved.path };
    } catch (err) {
      const why = refused(err, nameOf(to), t('drive:errors.moveFailed'));
      if (inline && why.field) return { ok: false, field: why.field };
      say(why);
      return { ok: false };
    }
  };

  const rename = (entry: DriveEntry, name: string) => move(entry, joinDrivePath(parentOf(entry.path), name), true);

  const moveInto = (entry: DriveEntry, folder: string) => move(entry, joinDrivePath(folder, entry.name));

  const mkdir = async (dir: string, name: string): Promise<NameResult> => {
    const path = joinDrivePath(dir, name);
    try {
      await m.mkdir.mutateAsync(path);
      return { ok: true, path };
    } catch (err) {
      const why = refused(err, name, t('drive:errors.folderFailed'));
      if (why.field) return { ok: false, field: why.field };
      say(why);
      return { ok: false };
    }
  };

  const download = async (entry: DriveEntry, seq?: number | null) => {
    if (!space) return;
    try {
      await downloadFile(space, entry, seq);
    } catch {
      toast({ variant: 'destructive', title: t('drive:errors.downloadFailed') });
    }
  };

  /** Restores a trash item; answers its top entry where it came back, or null. */
  const restoreFromTrash = async (item: DriveTrashItem): Promise<DriveEntry | null> => {
    const { entry } = item;
    try {
      const restored = await m.restore.mutateAsync(item.trash_id);
      const top = restored.find((e) => e.id === entry.id) ?? restored[0] ?? null;
      // A taken name restores beside it, suffixed or shortened: say so, or
      // the person looks for the old name and finds the one in the way.
      const renamed = top && top.name !== entry.name;
      toast({
        description: renamed
          ? t('drive:trash.restoredAs', { name: entry.name, as: top.name })
          : item.entries > 1 && entry.kind === 'file'
            ? t('drive:trash.restoredMore', { name: entry.name, count: item.entries - 1 })
            : t('drive:trash.restored', { name: top?.name ?? entry.name }),
      });
      return top;
    } catch (err) {
      say(refused(err, entry.name, t('drive:errors.restoreFailed', { name: entry.name }), entry.size));
      return null;
    }
  };

  /** Deletes one trash item for good, or with none empties the trash. */
  const purge = async (item?: DriveTrashItem): Promise<boolean> => {
    try {
      await m.purge.mutateAsync(item?.trash_id);
      if (!item) toast({ description: t('drive:trash.emptied') });
      return true;
    } catch {
      toast({ variant: 'destructive', title: t('drive:errors.purgeFailed') });
      return false;
    }
  };

  /** Saves an old version's content as a new version; answers the entry, or null. */
  const restoreVersion = async (entry: DriveEntry, seq: number): Promise<DriveEntry | null> => {
    try {
      const restored = await m.restoreTo.mutateAsync({ entryId: entry.id, seq });
      toast({ description: t('drive:versions.restored', { seq, next: restored.version ?? '' }) });
      return restored;
    } catch (err) {
      say(refused(err, entry.name, t('drive:versions.restoreFailed')));
      return null;
    }
  };

  /**
   * Marks, un-marks or changes a memo's status and tickers; answers the
   * entry, or null. A field left out of `change` keeps what it was.
   */
  const setMemo = async (entry: DriveEntry, change: MemoChange): Promise<DriveEntry | null> => {
    try {
      const updated = await m.setMemo.mutateAsync({ entryId: entry.id, change });
      if (change.memo === true && memoState(entry).kind !== 'memo') toast({ description: t('drive:memo.marked', { name: entry.name }) });
      else if (change.memo === false) toast({ description: t('drive:memo.unmarked', { name: entry.name }) });
      return updated;
    } catch (err) {
      say(refused(err, entry.name, t('drive:memo.markFailed')));
      return null;
    }
  };

  /** Puts a file back as it was before its last save; answers the entry, or null. */
  const undoSave = async (entry: DriveEntry): Promise<DriveEntry | null> => {
    try {
      const undone = await m.undo.mutateAsync({ entryId: entry.id, ifMatch: entry.sha256 });
      toast({ description: t('drive:undo.done', { name: entry.name }) });
      return undone;
    } catch (err) {
      // Nothing below the head: the earlier copy went (its days ran out, or it was cleared).
      if (driveErrorType(err) === 'not_found') toast({ variant: 'destructive', title: t('drive:undo.nothing') });
      else say(refused(err, entry.name, t('drive:undo.failed')));
      return null;
    }
  };

  /** Drops a file's earlier versions for good, as the person was `shown` them; answers whether they went. */
  const clearHistory = async (entry: DriveEntry, shown: DriveClearShown): Promise<boolean> => {
    try {
      const { dropped, freed_bytes } = await m.clear.mutateAsync({ entryId: entry.id, shown });
      toast({ description: t('drive:record.clearedToast', { count: dropped, name: entry.name, size: formatBytes(freed_bytes, locale) }) });
      return true;
    } catch (err) {
      say(refused(err, entry.name, t('drive:record.clearFailed')));
      return false;
    }
  };

  /** Says why a version was made, where its maker had not yet; answers whether it was kept. */
  const explain = async (entry: DriveEntry, seq: number, note: string): Promise<boolean> => {
    try {
      await m.explain.mutateAsync({ entryId: entry.id, seq, note });
      return true;
    } catch (err) {
      say(refused(err, entry.name, t('drive:record.whyFailed')));
      return false;
    }
  };

  const pending = {
    /** The trash item being restored. */
    restore: m.restore.isPending ? (m.restore.variables ?? null) : null,
    /** The trash item being deleted for good; `all` while the trash empties. */
    purge: m.purge.isPending ? (m.purge.variables ?? 'all') : null,
    restoreVersion: m.restoreTo.isPending,
    setMemo: m.setMemo.isPending,
    undoSave: m.undo.isPending,
    explain: m.explain.isPending,
    clearHistory: m.clear.isPending,
  };

  return { trash, move, rename, moveInto, mkdir, download, restoreFromTrash, purge, restoreVersion, setMemo, undoSave, clearHistory, explain, pending };
}

export type DriveActions = ReturnType<typeof useDriveActions>;
