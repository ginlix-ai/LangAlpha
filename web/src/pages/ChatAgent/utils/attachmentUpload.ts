/**
 * Stages one chat attachment on the server, by whichever path the backend
 * picks, and resolves with the id a message names it by. Staging needs no
 * computer, so a file attaches the same way whether or not the thread's
 * computer exists yet.
 */
import { holdReload } from '@/lib/reloadHolds';
import {
  beginAttachment,
  completeAttachment,
  deleteAttachment,
  deleteAttachmentOnUnload,
  sendAttachmentContent,
  signAttachmentParts,
} from './api/attachments';
import {
  beginUnlessAborted,
  canceled,
  relayProgress,
  runDirectUpload,
  UploadRefusedError,
  whileOpen,
  type UploadProgress,
} from './partUpload';
import { stagedFilesOf, type AttachmentMeta } from './fileUpload';

export interface AttachmentUploadOptions {
  onProgress?: (progress: UploadProgress) => void;
  /** Aborting ends the upload and discards what was sent. */
  signal?: AbortSignal;
  /** The workspace the message is going to, when the composer knows it. */
  workspaceId?: string | null;
}

const HOUR = 60 * 60 * 1000;
/** The server lets a staged file nothing has named go this long after it is ready. */
export const STAGED_FOR_MS = 24 * HOUR;
/** So a file held unsent stages again well before that, and a send treats an id this old as gone. */
export const RESTAGE_AFTER_MS = 20 * HOUR;
/** How soon a staging again that failed tries once more, while there is time. */
export const RESTAGE_RETRY_MS = HOUR / 4;
/** The largest file the server copies to a computer itself. One larger
 *  reaches it only from the store, which not every computer fetches from, so
 *  the server checks it against where the message is going. */
export const SERVER_COPY_MAX_BYTES = 256 * 1024 * 1024;

/** The server forgets a staged file nothing will name; best effort, since it also expires. */
export function discardAttachment(attachmentId: string): void {
  void deleteAttachment(attachmentId).catch(() => {});
}

export async function uploadAttachment(
  file: File,
  { onProgress, signal, workspaceId }: AttachmentUploadOptions = {},
): Promise<string> {
  // A start the server finished holds one of the user's open slots until it ends.
  const begun = await beginUnlessAborted(
    beginAttachment({
      name: file.name,
      size: file.size,
      mime_type: file.type || 'application/octet-stream',
      ...(workspaceId ? { workspace_id: workspaceId } : {}),
    }),
    signal,
    (b) => deleteAttachment(b.attachment_id),
  );
  const id = begun.attachment_id;
  const routes = {
    cancel: () => deleteAttachment(id),
    cancelOnUnload: (headers: Record<string, string>) => deleteAttachmentOnUnload(id, headers),
  };
  const status = begun.mode === 'direct'
    ? await runDirectUpload(
      file,
      begun.part_count,
      {
        ...routes,
        sign: (numbers, partSignal) => signAttachmentParts(id, numbers, partSignal),
        complete: (s) => completeAttachment(id, s),
      },
      { onProgress, signal },
      (accepted) => accepted,
    )
    : await whileOpen(routes, signal, (ctrl) => sendAttachmentContent(id, file, relayProgress(onProgress), ctrl.signal));
  if (status.state === 'ready') return id;
  // Taken but not kept: what the server still holds of it goes now.
  discardAttachment(id);
  throw new UploadRefusedError(status.error);
}

/**
 * One file staged for a message that has not been sent yet, as a composer
 * tile or a welcome screenshot holds it. A reload waits while it goes, since
 * the bytes would be lost, and one aborted as it finished is discarded, since
 * nothing will name it.
 */
export async function stageAttachment(file: File, options: AttachmentUploadOptions = {}): Promise<string> {
  const release = holdReload();
  try {
    const id = await uploadAttachment(file, options);
    if (options.signal?.aborted) {
      discardAttachment(id);
      throw canceled();
    }
    return id;
  } finally {
    release();
  }
}

/** A send's staged files under the ids a send can still name. */
export interface FreshParts {
  contexts: Record<string, unknown>[] | null;
  meta: AttachmentMeta[] | null;
  /** The new id of each file staged again, by its old one. */
  restaged: Map<string, { attachmentId: string; readyAt: number }>;
}

/**
 * Stages again each of a send's files that is past `RESTAGE_AFTER_MS`, for a
 * send that waited while the server's clock ran: one held behind a running
 * turn. A file that cannot be staged again goes under its old id while the
 * server still keeps it; past that, null, nothing sent.
 */
export async function freshenStaged(
  contexts: Record<string, unknown>[] | null,
  meta: AttachmentMeta[] | null,
  workspaceId?: string | null,
): Promise<FreshParts | null> {
  const restaged: FreshParts['restaged'] = new Map();
  const now = Date.now();
  const stale = stagedFilesOf(contexts, meta).filter((f) => f.readyAt !== undefined && now - f.readyAt >= RESTAGE_AFTER_MS);
  for (const f of stale) {
    try {
      const attachmentId = await stageAttachment(f.file, { workspaceId });
      restaged.set(f.attachmentId, { attachmentId, readyAt: Date.now() });
    } catch {
      if (Date.now() - (f.readyAt ?? now) < STAGED_FOR_MS) continue;
      for (const r of restaged.values()) discardAttachment(r.attachmentId);
      return null;
    }
  }
  if (restaged.size === 0) return { contexts, meta, restaged };
  for (const old of restaged.keys()) discardAttachment(old);
  const ids = (contexts ?? []).flatMap((c) => (typeof c.attachment_id === 'string' ? [c.attachment_id] : []));
  let k = 0;
  return {
    contexts: contexts?.map((c) => {
      const fresh = typeof c.attachment_id === 'string' ? restaged.get(c.attachment_id) : undefined;
      return fresh ? { ...c, attachment_id: fresh.attachmentId } : c;
    }) ?? null,
    // The records hold the files in the order the items name them.
    meta: meta?.map((m) => {
      if (!m.file) return m;
      const fresh = restaged.get(ids[k++]);
      return fresh ? { ...m, readyAt: fresh.readyAt } : m;
    }) ?? null,
    restaged,
  };
}
