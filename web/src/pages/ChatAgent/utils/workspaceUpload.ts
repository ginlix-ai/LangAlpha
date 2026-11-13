/**
 * Puts one file into a workspace folder, by whichever path the backend picks.
 *
 * A small file, or a computer that cannot pull from object storage, goes in
 * one request through the backend. A larger one goes from here to the store
 * in parts on URLs the backend signs, and the sandbox then pulls the
 * assembled object into place. Starting the upload answers which; this never
 * guesses, since only the backend knows what the computer can do.
 */
import { ApiError } from '@/api/client';
import {
  beginWorkspaceUpload,
  cancelWorkspaceUpload,
  cancelWorkspaceUploadOnUnload,
  completeWorkspaceUpload,
  getWorkspaceUpload,
  signWorkspaceUploadParts,
  uploadWorkspaceFile,
  type UploadBegin,
  type UploadStatus,
} from './api/files';
import {
  beginUnlessAborted,
  delay,
  relayProgress,
  retryable,
  runDirectUpload,
  UploadRefusedError,
  type UploadProgress,
} from './partUpload';

export interface WorkspaceUploadOptions {
  /** Where in the workspace; the file's own name at the folder's root when absent. */
  path?: string | null;
  onProgress?: (progress: UploadProgress) => void;
  /** Aborting while sending ends the upload and discards what was sent. */
  signal?: AbortSignal;
}

const POLL_MS = 1500;
// About a minute of polls that got no answer.
const POLL_FAILURES_MAX = 40;
const TERMINAL = new Set<UploadStatus['state']>(['done', 'failed', 'aborted']);

/**
 * The router's own answer for a route it lacks. A 404 the route gave (no such
 * workspace, a destination outside it) names what it did not find, and the
 * one-request path would answer the same after the whole file.
 */
function routeMissing(e: unknown): boolean {
  if (!(e instanceof ApiError)) return false;
  const detail = (e.response?.data as { detail?: unknown } | undefined)?.detail;
  return e.status === 405 || (e.status === 404 && detail === 'Not Found');
}

async function waitForEnd(
  workspaceId: string,
  uploadId: string,
  first: UploadStatus,
  signal: AbortSignal | undefined,
  completeAgain: () => Promise<UploadStatus>,
): Promise<UploadStatus> {
  let status = first;
  let failures = 0;
  while (!TERMINAL.has(status.state)) {
    await delay(POLL_MS, signal);
    try {
      // Back to uploading: assembling found the store silent, or short of
      // parts, which completing again sends.
      status = status.state === 'uploading'
        ? await completeAgain()
        : await getWorkspaceUpload(workspaceId, uploadId, signal);
      failures = 0;
    } catch (e) {
      if (signal?.aborted || !retryable(e) || ++failures >= POLL_FAILURES_MAX) throw e;
    }
  }
  return status;
}

/** Upload `file` into the workspace; resolves once it is in the folder. */
export async function uploadToWorkspace(
  workspaceId: string,
  file: File,
  { path = null, onProgress, signal }: WorkspaceUploadOptions = {},
): Promise<{ path: string }> {
  let begun: UploadBegin;
  try {
    // A start the server finished holds room on the computer's disk until it ends.
    begun = await beginUnlessAborted(
      beginWorkspaceUpload(workspaceId, { path: path || file.name, size: file.size }),
      signal,
      (b) => (b.mode === 'direct' ? cancelWorkspaceUpload(workspaceId, b.upload_id) : undefined),
    );
  } catch (e) {
    // A backend older than this page has no such route, and its one-request
    // path still takes the file.
    if (signal?.aborted || !routeMissing(e)) throw e;
    begun = { mode: 'relay' };
  }

  if (begun.mode === 'relay') {
    // Once the body is all sent, what remains is the backend writing it.
    const data = await uploadWorkspaceFile(workspaceId, file, path, relayProgress(onProgress), signal);
    return { path: data.path };
  }

  const uploadId = begun.upload_id;
  const status = await runDirectUpload(
    file,
    begun.part_count,
    {
      sign: (numbers, partSignal) => signWorkspaceUploadParts(workspaceId, uploadId, numbers, partSignal),
      complete: (s) => completeWorkspaceUpload(workspaceId, uploadId, s),
      cancel: () => cancelWorkspaceUpload(workspaceId, uploadId),
      // A page closed or reloaded mid-send takes the upload with it, so it
      // ends the row then rather than when the row would expire.
      cancelOnUnload: (headers) => cancelWorkspaceUploadOnUnload(workspaceId, uploadId, headers),
    },
    { onProgress, signal },
    // The sandbox pulls the assembled object into place after the completion
    // is accepted, and may hand the upload back to complete again.
    (accepted, again) => waitForEnd(workspaceId, uploadId, accepted, signal, again),
  );
  if (status.state !== 'done') throw new UploadRefusedError(status.error);
  return { path: status.path };
}
