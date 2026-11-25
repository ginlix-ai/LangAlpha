import { useEffect, useEffectEvent, useReducer, useRef } from 'react';
import type { UseQueryResult } from '@tanstack/react-query';
import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import type { editor } from 'monaco-editor';
import { getEntry, isChangedRefusal } from '@/api/drive';
import { toast } from '@/components/ui/use-toast';
import { useSaveDriveFile } from '@/hooks/useDrive';
import { useLocale } from '@/hooks/useLocale';
import { queryKeys } from '@/lib/queryKeys';
import type { FileBody } from '@/pages/ChatAgent/components/filePanel/fileBody';
import { disposeEditorModel, newEditorModelPath } from '@/pages/ChatAgent/components/viewers/editorModels';
import type { DriveEntry } from '@/types/drive';
import { editReducer, isDirty, isPast, newSitting, VIEWING, type Content, type EditSession, type SaveRequest } from './driveEdit';
import { draftOf, dropDraft, keepDraft } from './driveDrafts';
import { driveRefusal } from './driveRefusal';

/** The head body as content an edit can be written over, or null while it has no hash. */
function asContent(body: FileBody | undefined): Content | null {
  return body?.content != null && body.version ? { sha: body.version, content: body.content } : null;
}

/**
 * The editor for the open file: one session, its saves and its conflicts.
 * "Theirs" is always the file's head as the page reads it (keyed by hash and
 * refreshed by the change feed), so a refused save or a review only has to
 * wait for the head to arrive, never fetch it a second time.
 */
export function useDriveEditor(
  space: string | null,
  path: string | null,
  entry: DriveEntry | undefined,
  head: UseQueryResult<FileBody>,
  /** The past version in view, if any: it reads as it was and never opens a kept draft. */
  version: number | null = null,
) {
  const { t } = useTranslation();
  const locale = useLocale();
  const queryClient = useQueryClient();
  const saveFile = useSaveDriveFile(space ?? '');
  const [raw, dispatch] = useReducer(editReducer, VIEWING);
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null);

  // A session belongs to one file: another file in view is not being edited.
  const session: EditSession | null = raw.mode === 'editing' && raw.path === path ? raw : null;
  const state = session ?? VIEWING;
  const dirty = isDirty(state);
  const headContent = asContent(head.data);
  const theirs = session && headContent && !isPast(session, headContent.sha) ? headContent : null;
  /** The file moved on under an open editor. */
  const behind = !!session && !!entry?.sha256 && !isPast(session, entry.sha256) && session.save === 'idle' && !session.conflict;

  // The editor's model outlives its editor by design; it goes with the session.
  const modelPath = raw.mode === 'editing' ? raw.modelPath : null;
  useEffect(() => () => {
    if (modelPath) disposeEditorModel(modelPath);
  }, [modelPath]);
  useEffect(() => {
    if (raw.mode === 'editing' && raw.path !== path) dispatch({ type: 'close' });
  }, [raw, path]);

  /**
   * Writes the model too, since a kept model only takes its first value from
   * props. Called after the action that moves the session: the model's write
   * reports itself as a change, and a change first would make the draft look
   * edited to the action that follows it.
   */
  const replaceDraft = (content: string) => {
    const model = editorRef.current?.getModel();
    if (model && model.getValue() !== content) model.setValue(content);
  };

  // Unsaved text is kept by entry while it stays unsaved, so the page left
  // and come back to opens on it. A clean session (saved, theirs taken)
  // keeps nothing.
  const entryId = entry && entry.path === path ? entry.id : null;
  useEffect(() => {
    if (!session || !entryId) return;
    if (dirty) keepDraft(entryId, { base: session.base, draft: session.draft });
    else dropDraft(entryId);
  }, [session, dirty, entryId]);

  // A kept draft reopens on its own base, so a file that moved on meanwhile
  // answers its save with the conflict, never a silent rebase.
  const resume = useEffectEvent((id: string) => {
    const kept = draftOf(id);
    if (!kept || !path) return;
    dispatch({ type: 'open', path, sitting: newSitting(), modelPath: newEditorModelPath(path), base: kept.base });
    dispatch({ type: 'change', draft: kept.draft });
  });
  const resumable = !session && version === null && entryId;
  useEffect(() => {
    if (resumable) resume(resumable);
  }, [resumable]);

  // With nothing unsaved the editor follows the file; with unsaved text the
  // pane says so, and the person decides when to look.
  const followSha = behind && !dirty ? theirs?.sha ?? null : null;
  const follow = useEffectEvent(() => {
    if (!theirs) return;
    dispatch({ type: 'rebase', base: theirs });
    replaceDraft(theirs.content);
  });
  useEffect(() => {
    if (followSha) follow();
  }, [followSha]);

  const start = () => {
    if (!path || !headContent) return;
    dispatch({ type: 'open', path, sitting: newSitting(), modelPath: newEditorModelPath(path), base: headContent });
  };

  /** Ends the session as the person asked, unsaved text and all. */
  const close = () => {
    if (entryId) dropDraft(entryId);
    dispatch({ type: 'close' });
  };

  /**
   * The text in the editor. The model holds what was typed: a save pressed
   * straight after typing can arrive before the render that brings the last
   * keystrokes to state.
   */
  const typed = (s: EditSession) => editorRef.current?.getModel()?.getValue() ?? s.draft;

  /** Sends one request and reports each step under it, so only its own session hears. */
  const send = async (req: SaveRequest) => {
    if (!entry || !space) return;
    try {
      const saved = await saveFile.mutateAsync({ ...req, entry, onBusy: () => dispatch({ type: 'retrying', req }) });
      dispatch({ type: 'saved', req, sha: saved.sha256 ?? req.ifMatch, at: Date.now() });
    } catch (err) {
      if (isChangedRefusal(err)) {
        // The head moves on to the version that refused the save; a file
        // gone altogether has no version to compare with.
        const fresh = await queryClient
          .fetchQuery({ queryKey: queryKeys.drive.entry(space, req.path), queryFn: () => getEntry(space, req.path), staleTime: 0 })
          .catch(() => null);
        if (fresh?.is_text) {
          dispatch({ type: 'conflict', req });
          return;
        }
        dispatch({ type: 'save-failed', req });
        toast({ variant: 'destructive', title: t('drive:edit.gone') });
        return;
      }
      dispatch({ type: 'save-failed', req });
      const why = driveRefusal(err, t, {
        subject: entry.name,
        failed: t('drive:edit.saveFailed'),
        locale,
        // The editor holds no usage: a save refused for room says the Drive is full.
        usage: null,
      });
      toast({ variant: 'destructive', ...why.toast });
    }
  };

  /** Saves the draft over the content it was written on. */
  const save = async () => {
    if (!session || session.save !== 'idle' || session.conflict) return;
    const text = typed(session);
    if (text === session.base.content) return;
    const req: SaveRequest = { sitting: session.sitting, path: session.path, text, ifMatch: session.base.sha };
    dispatch({ type: 'saving', req });
    await send(req);
  };

  /** Saves the draft over their content instead, matched on their hash. */
  const keepMine = () => {
    if (!session || !theirs) return;
    const req: SaveRequest = { sitting: session.sitting, path: session.path, text: typed(session), ifMatch: theirs.sha };
    dispatch({ type: 'keep-mine', theirs, req });
    void send(req);
  };

  const takeTheirs = () => {
    if (!theirs) return;
    dispatch({ type: 'take-theirs', theirs });
    replaceDraft(theirs.content);
  };

  const review = () => dispatch({ type: 'review' });

  const change = (draft: string) => dispatch({ type: 'change', draft });

  return {
    state,
    session,
    dirty,
    /** The head the session conflicts with or fell behind, once it has arrived. */
    theirs,
    behind,
    editorRef,
    start,
    change,
    save,
    keepMine,
    takeTheirs,
    close,
    review,
  };
}

export type DriveEditor = ReturnType<typeof useDriveEditor>;
