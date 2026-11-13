import React, { useCallback, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { TFunction } from 'i18next';
import { holdReload } from '@/lib/reloadHolds';
import { classifyUploadError } from '../../utils/partUpload';
import { uploadToWorkspace } from '../../utils/workspaceUpload';

function uploadErrorMessage(err: unknown, file: globalThis.File, t: TFunction): string {
  const failure = classifyUploadError(err);
  switch (failure.kind) {
    case 'send':
      return t('filePanel.uploadSendFailed');
    // The server's 413 names its own limit; one without a detail came from
    // something in front of it, whose limit this page cannot know.
    case 'tooLarge':
      return `File is too large to upload (${(file.size / (1024 * 1024)).toFixed(1)} MB).`;
    case 'refused':
      if (failure.message) return failure.message;
      break;
  }
  return (err instanceof Error && err.message) || 'Upload failed';
}

/** Upload + drag-and-drop state for FilePanel (authenticated mode only —
 * upload has no ApiAdapter override; readOnly panels hide the affordances). */
export function useFileUpload({ workspaceId, onRefreshFiles }: {
  workspaceId: string;
  onRefreshFiles?: () => void;
}) {
  const { t } = useTranslation();
  // Upload state: a percentage while the bytes go up, then `uploadSaving`
  // while the workspace takes the file.
  const [uploadProgress, setUploadProgress] = useState<number | null>(null);
  const [uploadSaving, setUploadSaving] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  // One upload at a time: a second would share the one progress bar.
  const uploadRef = useRef<AbortController | null>(null);

  // Drag-and-drop state
  const [isDragOver, setIsDragOver] = useState(false);
  const dragCounterRef = useRef(0);

  const handleUpload = useCallback(async (file: globalThis.File) => {
    if (!file || !workspaceId || uploadRef.current) return;
    const ctrl = new AbortController();
    uploadRef.current = ctrl;
    setUploadError(null);
    setUploadSaving(false);
    setUploadProgress(0);
    const release = holdReload();
    try {
      await uploadToWorkspace(workspaceId, file, {
        signal: ctrl.signal,
        onProgress: (progress) => {
          if (progress.phase === 'saving') {
            setUploadProgress(null);
            setUploadSaving(true);
          } else {
            // Parts the store did not get go again, and can be cancelled again.
            setUploadSaving(false);
            setUploadProgress(progress.percent);
          }
        },
      });
      onRefreshFiles?.();
    } catch (err: unknown) {
      if (!ctrl.signal.aborted) {
        console.error('[FilePanel] Upload failed:', err);
        setUploadError(uploadErrorMessage(err, file, t));
      }
    } finally {
      release();
      uploadRef.current = null;
      setUploadProgress(null);
      setUploadSaving(false);
    }
  }, [workspaceId, onRefreshFiles, t]);

  /** Ends an upload still sending; once the workspace is taking the file it runs to the end. */
  const cancelUpload = useCallback(() => uploadRef.current?.abort(), []);

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
    uploadSaving,
    cancelUpload,
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
