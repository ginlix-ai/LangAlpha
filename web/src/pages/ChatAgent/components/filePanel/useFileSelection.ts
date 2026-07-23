import { useCallback, useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { deleteWorkspaceFiles } from '../../utils/api';

/** Multi-select mode + two-step delete for FilePanel (authenticated mode only —
 * delete has no ApiAdapter override; readOnly panels hide the affordances). */
export function useFileSelection({ workspaceId, filteredSortedFiles, targetDirectory, onRefreshFiles }: {
  workspaceId: string;
  filteredSortedFiles: string[];
  targetDirectory?: string | null;
  onRefreshFiles?: () => void;
}) {
  const { t } = useTranslation();
  // Selection / delete state
  const [selectMode, setSelectMode] = useState(false);
  const [selectedPaths, setSelectedPaths] = useState<Set<string>>(new Set());
  const [deleteLoading, setDeleteLoading] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [deleteConfirm, setDeleteConfirm] = useState(false);

  const toggleSelect = useCallback((path: string) => {
    setSelectedPaths((prev) => {
      const next = new Set(prev);
      next.has(path) ? next.delete(path) : next.add(path);
      return next;
    });
  }, []);

  const toggleSelectAll = useCallback(() => {
    setSelectedPaths((prev) => {
      if (prev.size === filteredSortedFiles.length) return new Set<string>();
      return new Set(filteredSortedFiles);
    });
  }, [filteredSortedFiles]);

  const toggleDirSelect = useCallback((dirFiles: string[]) => {
    setSelectedPaths((prev) => {
      const next = new Set(prev);
      const allSelected = dirFiles.every((f) => next.has(f));
      dirFiles.forEach((f) => (allSelected ? next.delete(f) : next.add(f)));
      return next;
    });
  }, []);

  const exitSelectMode = useCallback(() => {
    setSelectMode(false);
    setSelectedPaths(new Set());
    setDeleteError(null);
    setDeleteConfirm(false);
  }, []);

  const handleDelete = useCallback(() => {
    if (selectedPaths.size === 0) return;
    if (!deleteConfirm) {
      setDeleteConfirm(true);
      return;
    }
    const paths = Array.from(selectedPaths);
    exitSelectMode();
    setDeleteLoading(true);
    setDeleteError(null);
    deleteWorkspaceFiles(workspaceId, paths)
      .then((result: { errors?: unknown[] }) => {
        if (result.errors?.length && result.errors.length > 0) {
          setDeleteError(t('filePanel.deletePartialFail', { count: result.errors.length }));
        }
      })
      .catch((err: unknown) => {
        const e = err as { response?: { data?: { detail?: string } }; message?: string };
        setDeleteError(e?.response?.data?.detail || e?.message || t('filePanel.deleteFailed'));
      })
      .finally(() => {
        setDeleteLoading(false);
        onRefreshFiles?.();
      });
  }, [selectedPaths, workspaceId, deleteConfirm, exitSelectMode, onRefreshFiles, t]);

  useEffect(() => {
    if (!deleteConfirm) return;
    const timer = setTimeout(() => setDeleteConfirm(false), 4000);
    return () => clearTimeout(timer);
  }, [deleteConfirm]);

  useEffect(() => { exitSelectMode(); }, [targetDirectory]); // eslint-disable-line react-hooks/exhaustive-deps

  return {
    selectMode,
    setSelectMode,
    selectedPaths,
    deleteLoading,
    deleteError,
    setDeleteError,
    deleteConfirm,
    toggleSelect,
    toggleSelectAll,
    toggleDirSelect,
    exitSelectMode,
    handleDelete,
  };
}
