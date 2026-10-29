import { useCallback, useRef, useState } from 'react';
import { useEffect } from 'react';
import type { Dispatch, SetStateAction } from 'react';
import type { editor } from 'monaco-editor';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { useLatestRef } from '@/hooks/useLatestRef';
import { holdReload } from '@/lib/reloadHolds';
import { disposeEditorModel, newEditorModelPath } from '../viewers/editorModels';
import type { AskConfirm } from './usePanelConfirm';

/** One tab's editor, parked while another tab is on screen. */
interface EditDraft {
  isEditing: boolean;
  editContent: string | null;
  originalContent: string | null;
  modelPath: string | null;
}

/** Whether a draft differs from the text its edit started from. The one
 * baseline for every tab: the viewer's copy is a different read (the paged one
 * drops a final newline and turns CRLF into LF) and refetches while the editor
 * is open, so measured against it an untouched file read as edited. */
const isDirty = (d: Pick<EditDraft, 'isEditing' | 'editContent' | 'originalContent'>) =>
  d.isEditing && d.editContent !== null && d.editContent !== d.originalContent;

/** Edit-mode state for FilePanel: full-content load, Monaco editor wiring,
 * diff view, save/cancel, and the unsaved-changes guards. The read/write fns
 * are the component's adapter-resolved versions — never direct api imports.
 *
 * The draft belongs to the tab, not to the panel: switching tabs parks the
 * editor and switching back hands it straight back. Without that a tab strip
 * would silently throw away an edit for the price of looking at another file,
 * which is worse than the single-file panel it replaced. */
export function useFileEdit({ tabId, workspaceId, selectedFile, setFileContent, readFileFullFn, writeFileFn, ask, onSaveSettled }: {
  /** Which tab the editor currently belongs to. */
  tabId: string;
  workspaceId: string;
  selectedFile: string | null;
  setFileContent: Dispatch<SetStateAction<string | null>>;
  readFileFullFn: (workspaceId: string, path: string) => Promise<{ content?: string }>;
  writeFileFn: (workspaceId: string, path: string, content: string) => Promise<unknown>;
  /** The panel's confirmation, asked before a save or a discard. */
  ask: AskConfirm;
  /** A write to `path` has answered, landed or not. */
  onSaveSettled: (path: string) => void;
}) {
  const { t } = useTranslation();
  // Edit mode state
  const [isEditing, setIsEditing] = useState(false);
  const [editContent, setEditContent] = useState<string | null>(null);
  // Which file is being written, not whether one is: a save outlives the file
  // it belongs to, and nothing outside this hook can clear a plain flag, so a
  // write that hung left Save dead on every file opened after it. The sequence
  // is the same idea one step finer, for two writes to the same file.
  const [savingFile, setSavingFile] = useState<string | null>(null);
  const saveSeqRef = useRef(0);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [showDiff, setShowDiff] = useState(false);
  const [originalContent, setOriginalContent] = useState<string | null>(null);
  // The Monaco model this edit session types into, one per session.
  const [modelPath, setModelPath] = useState<string | null>(null);
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null);
  const [canUndo, setCanUndo] = useState(false);
  const [canRedo, setCanRedo] = useState(false);

  const handleUndoRedoChange = useCallback(({ canUndo: u, canRedo: r }: { canUndo: boolean; canRedo: boolean }) => {
    setCanUndo(u);
    setCanRedo(r);
  }, []);

  // Parked drafts, keyed by tab. Swapped during render rather than in an
  // effect so the incoming tab never paints one frame holding the outgoing
  // tab's editor.
  const drafts = useRef(new Map<string, EditDraft>());
  // The active tab whose close was confirmed: the swap below must not park its
  // draft under an id nothing will ever ask for again.
  const dropped = useRef<string | null>(null);
  const [ownerTab, setOwnerTab] = useState(tabId);
  if (ownerTab !== tabId) {
    if (dropped.current === ownerTab) dropped.current = null;
    else drafts.current.set(ownerTab, { isEditing, editContent, originalContent, modelPath });
    const parked = drafts.current.get(tabId);
    setOwnerTab(tabId);
    setIsEditing(parked?.isEditing ?? false);
    setEditContent(parked?.editContent ?? null);
    setOriginalContent(parked?.originalContent ?? null);
    setModelPath(parked?.modelPath ?? null);
    setShowDiff(false);
    setSaveError(null);
  }

  useEffect(() => {
    // The draft is the editor's again; a copy left parked would keep reading
    // as unsaved after the editor saved or cancelled it. Dropped once the
    // swap has committed: a render React throws away and retries would
    // otherwise find nothing parked the second time.
    drafts.current.delete(tabId);
  }, [tabId]);

  // A session's model is disposed once neither the editor nor a parked draft
  // can come back to it: after a save, a cancel, a closed tab or a deleted file.
  const models = useRef(new Set<string>());
  useEffect(() => {
    const live = new Set<string>();
    if (isEditing && modelPath) live.add(modelPath);
    for (const d of drafts.current.values()) if (d.isEditing && d.modelPath) live.add(d.modelPath);
    for (const path of models.current) {
      if (live.has(path)) continue;
      disposeEditorModel(path);
      models.current.delete(path);
    }
  });
  useEffect(() => {
    const owned = models.current;
    return () => owned.forEach(disposeEditorModel);
  }, []);

  const isSaving = savingFile !== null && savingFile === selectedFile;
  const hasUnsavedChanges = isDirty({ isEditing, editContent, originalContent });

  // Monaco reports a change outside any discrete event (Chromium's
  // EditContext), so `editContent` can trail the model by a render: a Cmd+S in
  // that gap saw nothing to save and was swallowed, and a save or a discard
  // check could miss the last keystrokes. The change listener records the text
  // here, stamped with its edit session, and every check a keypress or a click
  // makes reads it; renders keep `editContent`.
  const typedRef = useRef<{ session: string | null; text: string } | null>(null);
  const sessionRef = useLatestRef(modelPath);

  /** The active tab's draft as typed, ahead of the render that will show it. */
  const typedDraft = useCallback(() => {
    const typed = typedRef.current;
    return typed && typed.session === modelPath ? typed.text : editContent;
  }, [modelPath, editContent]);

  const unsavedNow = useCallback(
    () => isDirty({ isEditing, editContent: typedDraft(), originalContent }),
    [isEditing, typedDraft, originalContent],
  );

  /** Whether a tab, this one or a parked one, would lose an edit if closed. */
  const tabHasUnsavedChanges = useCallback((id: string) => {
    if (id === tabId) return unsavedNow();
    const parked = drafts.current.get(id);
    return !!parked && isDirty(parked);
  }, [tabId, unsavedNow]);

  // What leaving the page would lose: the active tab's edit or any parked one.
  // `hasUnsavedChanges` stays the active tab's, which is what the header shows.
  const hasAnyUnsavedChanges = hasUnsavedChanges || [...drafts.current.values()].some(isDirty);
  const anyUnsavedNow = useCallback(
    () => unsavedNow() || [...drafts.current.values()].some(isDirty),
    [unsavedNow],
  );

  const forgetTab = useCallback((id: string) => {
    drafts.current.delete(id);
    if (id === tabId) dropped.current = id;
  }, [tabId]);

  const selectedFileRef = useRef(selectedFile);
  selectedFileRef.current = selectedFile;

  const handleStartEdit = useCallback(async () => {
    if (!selectedFile || !workspaceId) return;
    setSaveError(null);
    try {
      const data = await readFileFullFn(workspaceId, selectedFile);
      // Another file opened while the full read was in flight.
      if (selectedFileRef.current !== selectedFile) return;
      const fullContent = data.content || '';
      if (fullContent.length > 500 * 1024) {
        setSaveError(t('filePanel.fileTooLarge'));
        return;
      }
      const path = newEditorModelPath(selectedFile);
      models.current.add(path);
      setModelPath(path);
      setEditContent(fullContent);
      setOriginalContent(fullContent);
      setFileContent(fullContent);
      setIsEditing(true);
    } catch (err: unknown) {
      const e = err as { response?: { data?: { detail?: string } }; message?: string };
      console.error('[FilePanel] Failed to fetch full file for editing:', err);
      // The read that failed is for a file the panel has already left, so its
      // error would otherwise appear under the name now on screen.
      if (selectedFileRef.current !== selectedFile) return;
      setSaveError(e?.response?.data?.detail || e?.message || t('filePanel.loadEditFailed'));
    }
  }, [selectedFile, workspaceId, readFileFullFn, setFileContent, t]);

  const handleEditorChange = useCallback((value: string) => {
    typedRef.current = { session: sessionRef.current, text: value };
    setEditContent(value);
  }, [sessionRef]);

  // Takes the file and text as they were when the question was asked: the
  // answer arrives on a later render.
  const save = useCallback(async (tab: string, file: string, content: string) => {
    const seq = ++saveSeqRef.current;
    setSavingFile(file);
    setSaveError(null);
    // A confirmed save finishes after the panel is gone, so the stale-build
    // reload a section switch takes waits for it.
    const release = holdReload();
    try {
      await writeFileFn(workspaceId, file, content);
      // Another file opened while the write was in flight, the same guard the
      // full read takes above. Without it this file's text lands in the panel
      // under the other one's name, and feeds its line and heading lookup.
      if (selectedFileRef.current !== file) {
        // The draft went into the parked set still measured against the text
        // this write replaced, so it would ask about discarding an edit the
        // file already holds. Keystrokes typed after the save are kept.
        const parked = drafts.current.get(tab);
        if (parked?.editContent === content) drafts.current.delete(tab);
        else if (parked) drafts.current.set(tab, { ...parked, originalContent: content });
        return;
      }
      setFileContent(content);
      // Keystrokes typed while the write was out are not in it, so the editor
      // stays open on them, measured against what landed, as a parked draft is.
      const typed = typedRef.current;
      if (typed && typed.session === sessionRef.current && typed.text !== content) {
        setOriginalContent(content);
        return;
      }
      setIsEditing(false);
      setEditContent(null);
      setShowDiff(false);
      setOriginalContent(null);
    } catch (err: unknown) {
      const e = err as { response?: { data?: { detail?: string } }; message?: string };
      console.error('[FilePanel] Save failed:', err);
      if (selectedFileRef.current !== file) {
        // The panel has moved on, so there is no header left to hang an inline
        // error under. Staying silent here is what let a reader answer "discard
        // unsaved changes" believing the save had landed and lose the edit with
        // nothing on screen to say otherwise, so the toast names the file.
        toast({
          description: t('filePanel.saveFailedFile', { name: file.split('/').pop() }),
          variant: 'destructive',
        });
        return;
      }
      setSaveError(e?.response?.data?.detail || e?.message || t('filePanel.saveFailed'));
    } finally {
      release();
      // A save that is no longer the current one must not report the panel idle:
      // it would re-enable Save under a newer write and let an older body land last.
      if (seq === saveSeqRef.current) setSavingFile(null);
      onSaveSettled(file);
    }
  }, [workspaceId, writeFileFn, setFileContent, onSaveSettled, sessionRef, t]);

  const handleSave = useCallback(() => {
    const draft = typedDraft();
    if (!selectedFile || !workspaceId || draft === null) return;
    // The button carries the only other re-entry guard and the key handler
    // below does not read it, so this one belongs where every caller passes.
    if (savingFile === selectedFile) return;
    ask(
      { title: t('filePanel.saveTitle'), message: t('filePanel.confirmSave'), confirmLabel: t('common.save') },
      () => { void save(tabId, selectedFile, draft); },
    );
  }, [tabId, selectedFile, savingFile, workspaceId, typedDraft, ask, save, t]);

  const discardEdit = useCallback(() => {
    setIsEditing(false);
    setEditContent(null);
    setShowDiff(false);
    setOriginalContent(null);
    setSaveError(null);
  }, []);

  const handleCancelEdit = useCallback(() => {
    if (!unsavedNow()) {
      discardEdit();
      return;
    }
    const file = selectedFile;
    ask(
      { title: t('filePanel.discardTitle'), message: t('filePanel.discardChanges'), confirmLabel: t('filePanel.discard') },
      // The editor state is the tab in front's, so a panel that has moved on
      // since the question would discard a draft nobody asked about.
      () => { if (selectedFileRef.current === file) discardEdit(); },
    );
  }, [unsavedNow, selectedFile, discardEdit, ask, t]);

  useEffect(() => {
    if (!isEditing) return;
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 's') {
        e.preventDefault();
        if (unsavedNow()) handleSave();
      }
    };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [isEditing, unsavedNow, handleSave]);

  useEffect(() => {
    if (!isEditing && !hasAnyUnsavedChanges) return;
    const handler = (e: BeforeUnloadEvent) => {
      if (!anyUnsavedNow()) return;
      e.preventDefault();
      e.returnValue = '';
    };
    window.addEventListener('beforeunload', handler);
    return () => window.removeEventListener('beforeunload', handler);
  }, [isEditing, hasAnyUnsavedChanges, anyUnsavedNow]);

  return {
    isEditing,
    editContent,
    isSaving,
    saveError,
    showDiff,
    setShowDiff,
    originalContent,
    editorRef,
    modelPath,
    canUndo,
    canRedo,
    handleUndoRedoChange,
    hasUnsavedChanges,
    anyUnsavedNow,
    tabHasUnsavedChanges,
    forgetTab,
    handleStartEdit,
    handleEditorChange,
    handleSave,
    handleCancelEdit,
  };
}
