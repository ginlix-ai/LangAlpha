import { useCallback, useRef, useState } from 'react';
import { useEffect } from 'react';
import type { Dispatch, SetStateAction } from 'react';
import type { editor } from 'monaco-editor';
import { useTranslation } from 'react-i18next';

/** Edit-mode state for FilePanel: full-content load, Monaco editor wiring,
 * diff view, save/cancel, and the unsaved-changes guards. The read/write fns
 * are the component's adapter-resolved versions — never direct api imports. */
export function useFileEdit({ workspaceId, selectedFile, fileContent, setFileContent, readFileFullFn, writeFileFn }: {
  workspaceId: string;
  selectedFile: string | null;
  fileContent: string | null;
  setFileContent: Dispatch<SetStateAction<string | null>>;
  readFileFullFn: (workspaceId: string, path: string) => Promise<{ content?: string }>;
  writeFileFn: (workspaceId: string, path: string, content: string) => Promise<unknown>;
}) {
  const { t } = useTranslation();
  // Edit mode state
  const [isEditing, setIsEditing] = useState(false);
  const [editContent, setEditContent] = useState<string | null>(null);
  const [isSaving, setIsSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [showDiff, setShowDiff] = useState(false);
  const [originalContent, setOriginalContent] = useState<string | null>(null);
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null);
  const [canUndo, setCanUndo] = useState(false);
  const [canRedo, setCanRedo] = useState(false);

  const handleUndoRedoChange = useCallback(({ canUndo: u, canRedo: r }: { canUndo: boolean; canRedo: boolean }) => {
    setCanUndo(u);
    setCanRedo(r);
  }, []);

  const hasUnsavedChanges = isEditing && editContent !== null && editContent !== fileContent;

  const handleStartEdit = useCallback(async () => {
    if (!selectedFile || !workspaceId) return;
    setSaveError(null);
    try {
      const data = await readFileFullFn(workspaceId, selectedFile);
      const fullContent = data.content || '';
      if (fullContent.length > 500 * 1024) {
        setSaveError(t('filePanel.fileTooLarge'));
        return;
      }
      setEditContent(fullContent);
      setOriginalContent(fullContent);
      setFileContent(fullContent);
      setIsEditing(true);
    } catch (err: unknown) {
      const e = err as { response?: { data?: { detail?: string } }; message?: string };
      console.error('[FilePanel] Failed to fetch full file for editing:', err);
      setSaveError(e?.response?.data?.detail || e?.message || t('filePanel.loadEditFailed'));
    }
  }, [selectedFile, workspaceId, readFileFullFn, setFileContent, t]);

  const handleEditorChange = useCallback((value: string) => {
    setEditContent(value);
  }, []);

  const handleSave = useCallback(async () => {
    if (!selectedFile || !workspaceId || editContent === null) return;
    if (!window.confirm(t('filePanel.confirmSave'))) return;
    setIsSaving(true);
    setSaveError(null);
    try {
      await writeFileFn(workspaceId, selectedFile, editContent);
      setFileContent(editContent);
      setIsEditing(false);
      setEditContent(null);
      setShowDiff(false);
      setOriginalContent(null);
    } catch (err: unknown) {
      const e = err as { response?: { data?: { detail?: string } }; message?: string };
      console.error('[FilePanel] Save failed:', err);
      setSaveError(e?.response?.data?.detail || e?.message || t('filePanel.saveFailed'));
    } finally {
      setIsSaving(false);
    }
  }, [selectedFile, workspaceId, editContent, writeFileFn, setFileContent, t]);

  const handleCancelEdit = useCallback(() => {
    if (hasUnsavedChanges) {
      if (!window.confirm(t('filePanel.discardChanges'))) return;
    }
    setIsEditing(false);
    setEditContent(null);
    setShowDiff(false);
    setOriginalContent(null);
    setSaveError(null);
  }, [hasUnsavedChanges, t]);

  useEffect(() => {
    if (!isEditing) return;
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 's') {
        e.preventDefault();
        if (editContent !== null && editContent !== fileContent) {
          handleSave();
        }
      }
    };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [isEditing, editContent, fileContent, handleSave]);

  useEffect(() => {
    if (!hasUnsavedChanges) return;
    const handler = (e: BeforeUnloadEvent) => {
      e.preventDefault();
      e.returnValue = '';
    };
    window.addEventListener('beforeunload', handler);
    return () => window.removeEventListener('beforeunload', handler);
  }, [hasUnsavedChanges]);

  return {
    isEditing,
    setIsEditing,
    editContent,
    setEditContent,
    isSaving,
    saveError,
    setSaveError,
    showDiff,
    setShowDiff,
    originalContent,
    setOriginalContent,
    editorRef,
    canUndo,
    setCanUndo,
    canRedo,
    setCanRedo,
    handleUndoRedoChange,
    hasUnsavedChanges,
    handleStartEdit,
    handleEditorChange,
    handleSave,
    handleCancelEdit,
  };
}
