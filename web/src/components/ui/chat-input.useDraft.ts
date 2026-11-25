import { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react';
import {
  flushDrafts,
  holdDraftTiles,
  moveDraft,
  openDraft,
  readDraft,
  releaseDraftFiles,
  scheduleDraftSave,
  type DraftFileMeta,
} from '@/lib/composerDrafts';
import { keepDraftFiles, loadDraftFile } from '@/lib/composerDrafts/files';
import { useStorageUser } from '@/lib/userStorage';
import type { FileAttachment, HeldFile, MentionedFile, SlashCommand } from './chat-input.types';

type Words = { text: string; mentions: MentionedFile[]; commands: SlashCommand[] };

const EMPTY: Words = { text: '', mentions: [], commands: [] };

const metaOf = ({ id, file, type, upload }: FileAttachment): DraftFileMeta => ({
  id,
  name: file.name,
  type,
  mime: file.type,
  size: file.size,
  lastModified: file.lastModified,
  ...(upload.status === 'ready' ? { attachmentId: upload.attachmentId, readyAt: upload.readyAt } : {}),
});

/**
 * Keeps the composer's draft under `draftKey` (`lib/composerDrafts`): the box
 * shows it while the key is set, and saves it as it changes.
 *
 * A key set opens the draft, filling the box; one cleared closes it, emptying
 * the box (a host keeping a composer mounted out of sight clears it, so the
 * draft is the visible composer's alone). A key that changes while open is the
 * same message whose place has a new name, a new thread once the server names
 * it, so the draft moves with it. A change of account closes one account's
 * draft and opens the other's, so one's words are never saved as another's.
 *
 * Returns what a send calls as it takes the box, `dropDraft`: the draft goes
 * then, before the host acts on the message (navigating to a new chat, say,
 * whose composer would otherwise read the words just sent as a draft). And
 * `restoringFiles`, while a draft's files are on their way back, which holds
 * the send: a message sent meanwhile would leave them out, and they would
 * come back to join the next one.
 */
export function useComposerDraft({
  draftKey,
  text,
  mentions,
  commands,
  files,
  fill,
  restoreFiles,
  forgetFiles,
}: {
  draftKey: string | null;
  text: string;
  mentions: MentionedFile[];
  commands: SlashCommand[];
  files: FileAttachment[];
  /** Replaces the box's text and pills. */
  fill: (words: Words) => void;
  restoreFiles: (files: HeldFile[]) => void;
  /** Drops every tile and leaves the server's copies, which a draft still names. */
  forgetFiles: () => void;
}): { dropDraft: () => void; restoringFiles: boolean } {
  const user = useStorageUser();
  const [owner] = useState(() => Symbol('composer'));
  // The draft in the box, and whose it is. Nothing is saved but under this:
  // before a draft is read, a save of the empty box would remove it.
  const [shown, setShown] = useState<{ key: string; user: string } | null>(null);
  // Saved files still on their way back, which a save meanwhile keeps.
  const [awaiting, setAwaiting] = useState<DraftFileMeta[]>([]);
  // The read under way; a later one makes its files stale.
  const loadRef = useRef(0);
  const mountedRef = useRef(false);

  // In a layout effect, so the box is filled before paint.
  useLayoutEffect(() => {
    if (shown && draftKey && shown.user === user) {
      if (shown.key === draftKey) return;
      moveDraft(shown.key, draftKey);
      setShown({ key: draftKey, user });
      return;
    }
    const load = ++loadRef.current;
    if (shown) {
      setAwaiting([]);
      fill(EMPTY);
      forgetFiles();
      releaseDraftFiles(owner);
    }
    if (!draftKey || !user) {
      setShown(null);
      return;
    }
    setShown({ key: draftKey, user });
    const saved = readDraft(draftKey);
    if (!saved) return;
    fill(saved);
    if (saved.files.length === 0) return;
    setAwaiting(saved.files);
    void Promise.all(saved.files.map(async (meta) => ({ meta, file: await loadDraftFile(meta) }))).then((found) => {
      if (!mountedRef.current || loadRef.current !== load) return;
      setAwaiting([]);
      const back: HeldFile[] = [];
      // A file whose bytes are gone leaves the draft. Its server copy stays,
      // to expire: another tab may hold the bytes and send it.
      for (const { meta, file } of found) {
        if (file) back.push({ id: meta.id, file, type: meta.type, attachmentId: meta.attachmentId, readyAt: meta.readyAt });
      }
      restoreFiles(back);
    });
  }, [draftKey, user, shown, owner, fill, restoreFiles, forgetFiles]);

  const openKey = shown?.key ?? null;
  useLayoutEffect(() => (openKey ? openDraft(openKey) : undefined), [openKey]);

  useEffect(() => {
    if (!shown || shown.key !== draftKey || shown.user !== user) return;
    holdDraftTiles(owner, files.map((f) => f.id));
    keepDraftFiles(files);
    const metas = files.map(metaOf);
    for (const meta of awaiting) if (!metas.some((f) => f.id === meta.id)) metas.push(meta);
    scheduleDraftSave(shown.key, { text, mentions, commands, files: metas });
  }, [shown, draftKey, user, owner, text, mentions, commands, files, awaiting]);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      flushDrafts();
      releaseDraftFiles(owner);
    };
  }, [owner]);

  const dropDraft = useCallback(() => {
    if (shown) scheduleDraftSave(shown.key, { ...EMPTY, files: [] });
  }, [shown]);
  return { dropDraft, restoringFiles: awaiting.length > 0 };
}
