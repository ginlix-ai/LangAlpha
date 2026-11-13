import { useCallback, useEffect, useRef, useState } from 'react';
import {
  discardAttachment,
  RESTAGE_AFTER_MS,
  RESTAGE_RETRY_MS,
  SERVER_COPY_MAX_BYTES,
  stageAttachment,
  STAGED_FOR_MS,
} from '@/pages/ChatAgent/utils/attachmentUpload';
import { checkAttachment } from '@/pages/ChatAgent/utils/api/attachments';
import { apiErrorStatus } from '@/api/errors';
import { isImageFile } from '@/pages/ChatAgent/utils/fileUpload';
import { classifyUploadError } from '@/pages/ChatAgent/utils/partUpload';
import type { FileAttachment, ReadyAttachment, TileUpload } from './chat-input.types';
import { MAX_FILES } from './chat-input.helpers';

const newId = () => Math.random().toString(36).slice(2, 11);

// A tile still waiting stages its file again before the server lets it go, unseen.
export { RESTAGE_AFTER_MS };

/** Attachment handling for the chat composer: each file uploads the moment it
 * is added, and a send names the uploaded ones by id. Also drag & drop and
 * paste capture. A `disabled` composer takes no files, however they come: its
 * drag handlers refuse instead of attach, and a pick or paste that still
 * arrives is dropped. */
export function useFileAttachments({
  disabled = false,
  workspaceIdRef,
  workspaceId = null,
}: {
  disabled?: boolean;
  /** Where the message is going as it stands when a file starts uploading. */
  workspaceIdRef?: { readonly current: string | null };
  /** The same, for a render: a large file is checked again when it changes. */
  workspaceId?: string | null;
} = {}) {
  const [attachedFiles, setAttachedFiles] = useState<FileAttachment[]>([]);
  const [isDragging, setIsDragging] = useState(false);

  // The list as it stands, for the uploads that settle between renders.
  const filesRef = useRef<FileAttachment[]>([]);
  const commit = useCallback((next: (prev: FileAttachment[]) => FileAttachment[]) => {
    filesRef.current = next(filesRef.current);
    setAttachedFiles(filesRef.current);
  }, []);
  const setUpload = useCallback((id: string, upload: TileUpload) => {
    commit((prev) => prev.map((f) => (f.id === id ? { ...f, upload } : f)));
  }, [commit]);

  // Each tile's upload under way, by tile id. Aborting one ends it and
  // discards what it sent.
  const uploads = useRef(new Map<string, AbortController>());

  // The workspace each staged file was checked for, by attachment id: the
  // one its upload began with, then each it was checked against since.
  const checkedFor = useRef(new Map<string, string | null>());

  // Each ready tile's next staging, by tile id, and when its file became
  // ready, which the server's expiry counts from.
  const restages = useRef(new Map<string, { timer: ReturnType<typeof setTimeout>; readyAt: number }>());
  const cancelRestage = useCallback((id: string) => {
    const held = restages.current.get(id);
    if (held) clearTimeout(held.timer);
    restages.current.delete(id);
  }, []);

  /** Stages a ready tile's file again before the server lets it go, then
   *  forgets the old copy; the tile shows ready throughout. */
  const keepStaged = useCallback(function keep(id: string, readyAt: number, wait = readyAt + RESTAGE_AFTER_MS - Date.now()) {
    const prior = restages.current.get(id);
    if (prior) clearTimeout(prior.timer);
    const timer = setTimeout(async () => {
      const tile = filesRef.current.find((f) => f.id === id);
      if (tile?.upload.status !== 'ready') return;
      const old = tile.upload.attachmentId;
      const ctrl = new AbortController();
      uploads.current.set(id, ctrl);
      const target = workspaceIdRef?.current ?? null;
      try {
        const fresh = await stageAttachment(tile.file, { signal: ctrl.signal, workspaceId: target });
        checkedFor.current.set(fresh, target);
        const now = filesRef.current.find((f) => f.id === id);
        // Sent or removed while it went: the copy it made is not needed.
        if (now?.upload.status !== 'ready' || now.upload.attachmentId !== old) {
          discardAttachment(fresh);
          return;
        }
        setUpload(id, { status: 'ready', attachmentId: fresh });
        discardAttachment(old);
        keep(id, Date.now());
      } catch (err) {
        if (ctrl.signal.aborted) return;
        if (Date.now() + RESTAGE_RETRY_MS < readyAt + STAGED_FOR_MS) {
          keep(id, readyAt, RESTAGE_RETRY_MS);
        } else {
          // Out of time: the file shows failed, for the person to retry or remove.
          restages.current.delete(id);
          discardAttachment(old);
          setUpload(id, { status: 'failed', failure: classifyUploadError(err) });
        }
      } finally {
        if (uploads.current.get(id) === ctrl) uploads.current.delete(id);
      }
    }, Math.max(0, wait));
    restages.current.set(id, { timer, readyAt });
  }, [setUpload, workspaceIdRef]);

  const upload = useCallback(async (id: string, file: File) => {
    const ctrl = new AbortController();
    uploads.current.set(id, ctrl);
    const target = workspaceIdRef?.current ?? null;
    try {
      const attachmentId = await stageAttachment(file, {
        signal: ctrl.signal,
        workspaceId: target,
        onProgress: (p) => setUpload(id, { status: 'uploading', percent: p.phase === 'saving' ? null : p.percent }),
      });
      checkedFor.current.set(attachmentId, target);
      setUpload(id, { status: 'ready', attachmentId });
      keepStaged(id, Date.now());
    } catch (err) {
      if (!ctrl.signal.aborted) setUpload(id, { status: 'failed', failure: classifyUploadError(err) });
    } finally {
      uploads.current.delete(id);
    }
  }, [setUpload, keepStaged, workspaceIdRef]);

  // Gone with the composer: what is still uploading ends and is discarded,
  // since nothing could send it, and no file is staged again.
  useEffect(() => {
    const live = uploads.current;
    const timers = restages.current;
    return () => {
      for (const ctrl of live.values()) ctrl.abort();
      for (const { timer } of timers.values()) clearTimeout(timer);
      timers.clear();
    };
  }, []);

  const handleFiles = useCallback((newFilesList: FileList | File[]) => {
    if (disabled) return;
    const room = Math.max(0, MAX_FILES - filesRef.current.length);
    const taken = Array.from(newFilesList).slice(0, room);
    if (taken.length === 0) return;
    const tiles: FileAttachment[] = taken.map((file) => ({
      id: newId(),
      file,
      type: file.type || (isImageFile(file) ? 'image/png' : 'application/octet-stream'),
      upload: { status: 'uploading', percent: 0 },
    }));
    commit((prev) => [...prev, ...tiles]);
    for (const tile of tiles) void upload(tile.id, tile.file);
  }, [disabled, commit, upload]);

  /** Sends a failed file again. */
  const retryFile = useCallback((id: string) => {
    const tile = filesRef.current.find((f) => f.id === id);
    if (tile?.upload.status !== 'failed') return;
    setUpload(id, { status: 'uploading', percent: 0 });
    void upload(id, tile.file);
  }, [setUpload, upload]);

  /** Files a send took that come back to the composer, already on the server.
   *  What no longer fits is not coming back, so the server forgets it. */
  const restoreFiles = useCallback((files: ReadyAttachment[]) => {
    const room = Math.max(0, MAX_FILES - filesRef.current.length);
    for (const f of files.slice(room)) discardAttachment(f.attachmentId);
    const kept = files.slice(0, room);
    const restored = kept.map((f): FileAttachment => ({
      id: newId(),
      file: f.file,
      type: f.type,
      upload: { status: 'ready', attachmentId: f.attachmentId },
    }));
    if (restored.length > 0) commit((prev) => [...prev, ...restored]);
    restored.forEach((tile, i) => keepStaged(tile.id, kept[i].readyAt ?? Date.now()));
  }, [commit, keepStaged]);

  const removeFile = useCallback((id: string) => {
    const tile = filesRef.current.find((f) => f.id === id);
    if (!tile) return;
    uploads.current.get(id)?.abort();
    cancelRestage(id);
    if (tile.upload.status === 'ready') discardAttachment(tile.upload.attachmentId);
    commit((prev) => prev.filter((f) => f.id !== id));
  }, [commit, cancelRestage]);

  /** Every file, as for a draft thrown away. */
  const discardFiles = useCallback(() => {
    for (const f of filesRef.current) removeFile(f.id);
  }, [removeFile]);

  /** A ready tile past the point it is staged again: a tab asleep through its
   *  timer still holds an id the server may already have let go. */
  const isStale = useCallback((f: FileAttachment) => {
    if (f.upload.status !== 'ready') return false;
    const held = restages.current.get(f.id);
    return !!held && Date.now() - held.readyAt >= RESTAGE_AFTER_MS;
  }, []);

  /** Whether a send now would be held by a stale file (`takeFiles`). */
  const holdsStale = useCallback(() => filesRef.current.some(isStale), [isStale]);

  /**
   * The uploaded files, for a send, which takes them out of the composer. The
   * message names them, so they stay on the server, and a staging again
   * under way for one ends, its copy discarded.
   *
   * A stale file is not sent: it uploads again now, showing as uploading, and
   * the send is held (null, nothing taken) until it is ready. So is a send
   * with any file uploading or failed, read here rather than from a render,
   * which a change from a timer can be ahead of.
   */
  const takeFiles = useCallback((): ReadyAttachment[] | null => {
    const stale = filesRef.current.filter(isStale);
    if (stale.length > 0) {
      for (const f of stale) {
        if (f.upload.status !== 'ready') continue;
        uploads.current.get(f.id)?.abort();
        cancelRestage(f.id);
        discardAttachment(f.upload.attachmentId);
        setUpload(f.id, { status: 'uploading', percent: 0 });
        void upload(f.id, f.file);
      }
      return null;
    }
    if (filesRef.current.some((f) => f.upload.status !== 'ready')) return null;
    const taken: ReadyAttachment[] = [];
    for (const f of filesRef.current) {
      if (f.upload.status !== 'ready') continue;
      const readyAt = restages.current.get(f.id)?.readyAt;
      taken.push({ file: f.file, type: f.type, attachmentId: f.upload.attachmentId, ...(readyAt ? { readyAt } : {}) });
      uploads.current.get(f.id)?.abort();
      cancelRestage(f.id);
    }
    commit((prev) => prev.filter((f) => f.upload.status !== 'ready'));
    return taken;
  }, [commit, cancelRestage, isStale, setUpload, upload]);

  // A file larger than the server copies was checked only against where the
  // message was going when it began, if anywhere: one the new target's
  // computer cannot receive fails here, before Send, rather than in the turn.
  useEffect(() => {
    if (!workspaceId) return;
    for (const f of attachedFiles) {
      if (f.upload.status !== 'ready' || f.file.size <= SERVER_COPY_MAX_BYTES) continue;
      const { attachmentId } = f.upload;
      if (checkedFor.current.get(attachmentId) === workspaceId) continue;
      checkedFor.current.set(attachmentId, workspaceId);
      void checkAttachment(attachmentId, workspaceId).catch((err) => {
        // Not answered: the turn checks it again.
        if (apiErrorStatus(err) !== 413) return;
        // Checked since for another target, or no longer this tile's file.
        if (checkedFor.current.get(attachmentId) !== workspaceId) return;
        const now = filesRef.current.find((t) => t.id === f.id);
        if (now?.upload.status !== 'ready' || now.upload.attachmentId !== attachmentId) return;
        cancelRestage(f.id);
        discardAttachment(attachmentId);
        setUpload(f.id, { status: 'failed', failure: classifyUploadError(err) });
      });
    }
  }, [workspaceId, attachedFiles, cancelRestage, setUpload]);

  const uploadingCount = attachedFiles.filter((f) => f.upload.status === 'uploading').length;
  const failedCount = attachedFiles.filter((f) => f.upload.status === 'failed').length;

  // Drag & Drop. A disabled composer refuses a drag over it (shown as such,
  // and kept from reaching the page, which would open the file in its place)
  // and a drop on it is not attached to a message that cannot be sent.
  //
  // The drag is over the composer while some element in it has been entered
  // and not left. Crossing from one child into another fires the child's
  // dragenter before the last one's dragleave, and both bubble here, so a
  // dragleave alone would hide the overlay at every inner edge and the next
  // dragover would show it again. An element removed under the pointer (the
  // send cell's Stop as the turn ends) is never left, so it stops counting
  // once it is out of the document.
  const dragEntered = useRef(new Set<Node>());
  const endDrag = useCallback(() => {
    dragEntered.current.clear();
    setIsDragging(false);
  }, []);
  const onDragEnter = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    if (e.target instanceof Node) dragEntered.current.add(e.target);
    if (!disabled) setIsDragging(true);
  }, [disabled]);
  const onDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    if (disabled) e.dataTransfer.dropEffect = 'none';
  }, [disabled]);
  const onDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    const entered = dragEntered.current;
    if (e.target instanceof Node) entered.delete(e.target);
    for (const node of entered) if (!node.isConnected) entered.delete(node);
    if (entered.size === 0) setIsDragging(false);
  }, []);
  const onDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    endDrag();
    if (e.dataTransfer.files) handleFiles(e.dataTransfer.files);
  }, [endDrag, handleFiles]);

  // A drag that ends without leaving the composer the way it came in (dropped
  // elsewhere on the page, or cancelled) still ends here.
  useEffect(() => {
    if (!isDragging) return;
    window.addEventListener('dragend', endDrag);
    window.addEventListener('drop', endDrag);
    return () => {
      window.removeEventListener('dragend', endDrag);
      window.removeEventListener('drop', endDrag);
    };
  }, [isDragging, endDrag]);

  // Paste Handling
  const handlePaste = useCallback((e: React.ClipboardEvent) => {
    const items = e.clipboardData.items;
    const pastedFiles = [];
    for (let i = 0; i < items.length; i++) {
      if (items[i].kind === 'file') {
        const file = items[i].getAsFile();
        if (file) pastedFiles.push(file);
      }
    }
    if (pastedFiles.length > 0) {
      e.preventDefault();
      handleFiles(pastedFiles);
    }
  }, [handleFiles]);

  return {
    attachedFiles, uploadingCount, failedCount, isDragging,
    handleFiles, retryFile, restoreFiles, removeFile, discardFiles, takeFiles, holdsStale,
    onDragEnter, onDragOver, onDragLeave, onDrop, handlePaste,
  };
}
