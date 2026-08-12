import React, { useCallback, useRef, useState } from 'react';
import { uploadWorkspaceFile } from '../../utils/api';

/** Upload + drag-and-drop state for FilePanel (authenticated mode only —
 * upload has no ApiAdapter override; readOnly panels hide the affordances). */
export function useFileUpload({ workspaceId, onRefreshFiles }: {
  workspaceId: string;
  onRefreshFiles?: () => void;
}) {
  // Upload state
  const [uploadProgress, setUploadProgress] = useState<number | null>(null);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Drag-and-drop state
  const [isDragOver, setIsDragOver] = useState(false);
  const dragCounterRef = useRef(0);

  const handleUpload = useCallback(async (file: globalThis.File) => {
    if (!file || !workspaceId) return;
    setUploadError(null);
    setUploadProgress(0);
    try {
      await uploadWorkspaceFile(workspaceId, file, null, (pct: number) => setUploadProgress(pct));
      setUploadProgress(null);
      onRefreshFiles?.();
    } catch (err: unknown) {
      const e = err as { response?: { status?: number; data?: { detail?: string } }; message?: string };
      console.error('[FilePanel] Upload failed:', err);
      let msg = e?.response?.data?.detail || e?.message || 'Upload failed';
      if (e?.response?.status === 413 && !e?.response?.data?.detail) {
        const sizeMB = (file.size / (1024 * 1024)).toFixed(1);
        msg = `File is too large (${sizeMB} MB). Maximum upload size is 250 MB.`;
      }
      setUploadError(msg);
      setUploadProgress(null);
    }
  }, [workspaceId, onRefreshFiles]);

  const handleFileInputChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) handleUpload(file);
    if (fileInputRef.current) fileInputRef.current.value = '';
  }, [handleUpload]);

  const handleDragEnter = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragCounterRef.current++;
    if (e.dataTransfer.types.includes('Files')) {
      setIsDragOver(true);
    }
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragCounterRef.current--;
    if (dragCounterRef.current === 0) {
      setIsDragOver(false);
    }
  }, []);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
  }, []);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragCounterRef.current = 0;
    setIsDragOver(false);
    const file = e.dataTransfer.files?.[0];
    if (file) handleUpload(file);
  }, [handleUpload]);

  return {
    uploadProgress,
    uploadError,
    setUploadError,
    fileInputRef,
    isDragOver,
    handleUpload,
    handleFileInputChange,
    handleDragEnter,
    handleDragLeave,
    handleDragOver,
    handleDrop,
  };
}
