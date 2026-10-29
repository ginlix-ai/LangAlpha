import { useCallback, useEffect, useRef, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import type { BackupStatusResponse, Workspace } from '@/types/api';
import { backupWorkspaceFiles, getBackupStatus } from '../../utils/api';
import { useComputers } from '../../hooks/useComputers';
import type { BackupResult } from './types';

const sameMembers = (set: ReadonlySet<string>, list: readonly string[]) =>
  set.size === list.length && list.every((path) => set.has(path));

/** COS backup status + manual backup trigger (skipped entirely in readOnly). */
export function useFileBackup({ workspaceId, files, readOnly, computerId, restoreOffered = false }: {
  workspaceId: string;
  files: string[];
  readOnly?: boolean;
  /** The computer the workspace is on, whose switch says whether it backs up. */
  computerId?: string | null;
  /** A restore offer stands, which holds the backup until it is answered. */
  restoreOffered?: boolean;
}) {
  const queryClient = useQueryClient();
  // The computer's switch from its row, which a toggle patches at once; the
  // status read stands in until the list has it.
  const { data: computerData } = useComputers({ enabled: !!computerId && !readOnly });
  const rowSwitch = computerData?.computers?.find((c) => c.computer_id === computerId)?.files_backup;
  const [statusSwitch, setStatusSwitch] = useState<boolean | undefined>(undefined);
  // Backup state
  const [backedUpSet, setBackedUpSet] = useState<Set<string>>(new Set());
  const [modifiedSet, setModifiedSet] = useState<Set<string>>(new Set());
  const [backingUp, setBackingUp] = useState(false);
  const [backupResult, setBackupResult] = useState<BackupResult | null>(null);
  // An earlier result's dismissal must not clear a later one, which may be
  // a list of unsaved files meant to stay until the reader dismisses it.
  const dismissTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const dismissAfter = useCallback((ms: number) => {
    dismissTimer.current = setTimeout(() => setBackupResult(null), ms);
  }, []);
  useEffect(() => () => clearTimeout(dismissTimer.current), []);

  // Asked again after every file list, and usually unchanged: an equal answer
  // keeps the old sets so the tree is not re-rendered for nothing.
  const updateBackupStatus = useCallback((data: { backed_up?: string[]; modified?: string[] }) => {
    const backedUp = data.backed_up || [];
    const modified = data.modified || [];
    setBackedUpSet((prev) => (sameMembers(prev, backedUp) ? prev : new Set(backedUp)));
    setModifiedSet((prev) => (sameMembers(prev, modified) ? prev : new Set(modified)));
  }, []);

  // The status can learn of an offer before the workspace row does: the
  // listing it follows may be what found the folder gone. The row is what
  // shows the offer, so it is read again when the two disagree.
  const noticeStatus = useCallback((status: BackupStatusResponse) => {
    if (typeof status.files_backup === 'boolean') setStatusSwitch(status.files_backup);
    if (status.files_restore_offered_at === undefined) return;
    const key = queryKeys.workspaces.detail(workspaceId);
    const row = queryClient.getQueryData<Workspace>(key);
    // The two endpoints spell UTC differently ('+00:00' and 'Z'), so the
    // instants are compared, not the strings.
    const instant = (at: string | null | undefined) => (at ? Date.parse(at) : null);
    if (row && instant(row.files_restore_offered_at) !== instant(status.files_restore_offered_at)) {
      void queryClient.invalidateQueries({ queryKey: key, exact: true });
    }
  }, [queryClient, workspaceId]);

  // Fetch backup status on mount and when files change (skip in readOnly mode)
  useEffect(() => {
    if (!workspaceId || readOnly) return;
    getBackupStatus(workspaceId)
      .then((status) => {
        updateBackupStatus(status);
        noticeStatus(status);
      })
      .catch(() => {});
  }, [workspaceId, files, updateBackupStatus, noticeStatus, readOnly]);

  const handleBackup = useCallback(async () => {
    if (!workspaceId || backingUp) return;
    setBackingUp(true);
    clearTimeout(dismissTimer.current);
    setBackupResult(null);
    try {
      const result = await backupWorkspaceFiles(workspaceId);
      setBackupResult(result);
      const status = await getBackupStatus(workspaceId);
      updateBackupStatus(status);
      // A clean backup confirms and gets out of the way. One that skipped
      // files stays until dismissed: it is the only place the reader learns
      // which files are not safe.
      if (!result.unsaved_count && !result.unsaved?.length) {
        dismissAfter(3000);
      }
    } catch (err: unknown) {
      const e = err as { response?: { data?: { detail?: string } }; message?: string };
      const msg = e?.response?.data?.detail || e?.message || 'Backup failed';
      setBackupResult({ error: msg });
      dismissAfter(4000);
    }
    // After the try rather than in a `finally`, which the compiler cannot
    // build: the catch above cannot throw, so both paths reach it.
    setBackingUp(false);
  }, [workspaceId, backingUp, updateBackupStatus, dismissAfter]);

  const backupOff = (rowSwitch ?? statusSwitch) === false;

  return {
    backedUpSet,
    modifiedSet,
    backingUp,
    backupResult,
    setBackupResult,
    handleBackup,
    /** The computer does not back up: there is nothing to back up now, and no report to show. */
    backupOff,
    /** Backing up now would only be refused: the switch is off, or an offer holds the backup. */
    canBackUp: !backupOff && !restoreOffered,
  };
}

export type FileBackup = ReturnType<typeof useFileBackup>;
