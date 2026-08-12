import { useCallback, useEffect, useState } from 'react';
import { backupWorkspaceFiles, getBackupStatus } from '../../utils/api';
import type { BackupResult } from './types';

/** COS backup status + manual backup trigger (skipped entirely in readOnly). */
export function useFileBackup({ workspaceId, files, readOnly }: {
  workspaceId: string;
  files: string[];
  readOnly?: boolean;
}) {
  // Backup state
  const [backedUpSet, setBackedUpSet] = useState<Set<string>>(new Set());
  const [modifiedSet, setModifiedSet] = useState<Set<string>>(new Set());
  const [backingUp, setBackingUp] = useState(false);
  const [backupResult, setBackupResult] = useState<BackupResult | null>(null);

  const updateBackupStatus = useCallback((data: { backed_up?: string[]; modified?: string[] }) => {
    setBackedUpSet(new Set(data.backed_up || []));
    setModifiedSet(new Set(data.modified || []));
  }, []);

  // Fetch backup status on mount and when files change (skip in readOnly mode)
  useEffect(() => {
    if (!workspaceId || readOnly) return;
    getBackupStatus(workspaceId)
      .then(updateBackupStatus)
      .catch(() => {});
  }, [workspaceId, files, updateBackupStatus, readOnly]);

  const handleBackup = useCallback(async () => {
    if (!workspaceId || backingUp) return;
    setBackingUp(true);
    setBackupResult(null);
    try {
      const result = await backupWorkspaceFiles(workspaceId);
      setBackupResult(result as BackupResult);
      const status = await getBackupStatus(workspaceId);
      updateBackupStatus(status);
      setTimeout(() => setBackupResult(null), 3000);
    } catch (err: unknown) {
      const e = err as { response?: { data?: { detail?: string } }; message?: string };
      const msg = e?.response?.data?.detail || e?.message || 'Backup failed';
      setBackupResult({ error: msg });
      setTimeout(() => setBackupResult(null), 4000);
    } finally {
      setBackingUp(false);
    }
  }, [workspaceId, backingUp, updateBackupStatus]);

  return {
    backedUpSet,
    modifiedSet,
    backingUp,
    backupResult,
    setBackupResult,
    handleBackup,
  };
}
