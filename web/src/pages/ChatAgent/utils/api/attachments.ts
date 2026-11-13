/**
 * Chat attachment staging: a file goes up when it is added to the composer,
 * and the message then names it by id.
 */
import { api } from '@/api/client';
import { PART_IDLE_MS, SIGN_DEADLINE_MS, UploadSendError, type SignedUploadPart } from '../partUpload';

// Once the last byte is out, the server writes the file to the store before it
// answers, which on a large file takes a while with nothing moving.
export const RELAY_ANSWER_MS = 120_000;

export type AttachmentBegin =
  | { attachment_id: string; mode: 'relay'; max_bytes: number }
  | {
    attachment_id: string;
    mode: 'direct';
    max_bytes: number;
    part_size: number;
    part_count: number;
    expires_at: string;
  };

export interface AttachmentStatus {
  attachment_id: string;
  state: 'uploading' | 'ready' | 'aborted';
  name: string;
  size: number;
  mime_type: string;
  error: string | null;
}

const attachmentsPath = (attachmentId?: string) =>
  `/api/v1/attachments${attachmentId ? `/${attachmentId}` : ''}`;

/** `workspace_id` is where the message is going, when known: a file its computer cannot receive is refused before it is sent. */
export async function beginAttachment(body: { name: string; size: number; mime_type: string; workspace_id?: string }): Promise<AttachmentBegin> {
  // Bounded, as a stalled request would leave its tile uploading for good.
  const { data } = await api.post(attachmentsPath(), body, { timeout: SIGN_DEADLINE_MS });
  return data as AttachmentBegin;
}

/**
 * Sends the whole file through the backend; `beginAttachment` says when this is
 * the path. A request that stops moving (no bytes for `PART_IDLE_MS`, or no
 * answer `RELAY_ANSWER_MS` after the last one) is abandoned as a failed send,
 * which the person can retry, rather than left to hang.
 */
export async function sendAttachmentContent(
  attachmentId: string,
  file: File,
  onProgress: (percent: number) => void,
  signal?: AbortSignal,
): Promise<AttachmentStatus> {
  const form = new FormData();
  form.append('file', file);
  const watch = new AbortController();
  const stop = () => watch.abort();
  if (signal?.aborted) stop();
  else signal?.addEventListener('abort', stop, { once: true });
  let stalled = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const allow = (ms: number) => {
    clearTimeout(timer);
    timer = setTimeout(() => {
      stalled = true;
      watch.abort();
    }, ms);
  };
  allow(PART_IDLE_MS);
  try {
    const { data } = await api.post(`${attachmentsPath(attachmentId)}/content`, form, {
      signal: watch.signal,
      onUploadProgress: (e) => {
        const total = e.total || file.size || 1;
        allow(e.loaded >= total ? RELAY_ANSWER_MS : PART_IDLE_MS);
        onProgress(Math.round((e.loaded * 100) / total));
      },
    });
    return data as AttachmentStatus;
  } catch (e) {
    if (stalled && !signal?.aborted) throw new UploadSendError();
    throw e;
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener('abort', stop);
  }
}

export async function signAttachmentParts(
  attachmentId: string,
  partNumbers: number[],
  signal?: AbortSignal,
): Promise<{ parts: SignedUploadPart[]; expires_in: number }> {
  const { data } = await api.post(`${attachmentsPath(attachmentId)}/parts`, { part_numbers: partNumbers }, { signal });
  return data;
}

/** An answer that does not come in `RELAY_ANSWER_MS` is asked for again, which is safe: completing again answers where the upload stands. */
export async function completeAttachment(attachmentId: string, signal?: AbortSignal): Promise<AttachmentStatus> {
  const { data } = await api.post(`${attachmentsPath(attachmentId)}/complete`, undefined, { signal, timeout: RELAY_ANSWER_MS });
  return data as AttachmentStatus;
}

/** Whether a staged file can go to `workspaceId`: refused (413) when that workspace's computer cannot receive one this large. */
export async function checkAttachment(attachmentId: string, workspaceId: string): Promise<AttachmentStatus> {
  const { data } = await api.post(`${attachmentsPath(attachmentId)}/check`, { workspace_id: workspaceId }, { timeout: SIGN_DEADLINE_MS });
  return data as AttachmentStatus;
}

export async function deleteAttachment(attachmentId: string): Promise<void> {
  await api.delete(attachmentsPath(attachmentId));
}

/**
 * Ends an upload as the page goes away. A keepalive request outlives the page
 * that sent it, where the API client's would be cut off with the page; the
 * caller hands over the auth headers, since there is no time left to fetch them.
 */
export function deleteAttachmentOnUnload(attachmentId: string, headers: Record<string, string>): void {
  try {
    void fetch(api.getUri({ url: attachmentsPath(attachmentId) }), {
      method: 'DELETE',
      keepalive: true,
      headers,
    }).catch(() => {});
  } catch {
    /* best effort: the server expires it */
  }
}
