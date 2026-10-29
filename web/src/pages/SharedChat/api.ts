/**
 * Public API functions for `/s/` and `/a/` links.
 *
 * Only the metadata read carries a bearer, and only when the app holds one:
 * the owner of a private link is told apart from a visitor by it. Everything
 * else is unauthenticated, keyed by the code or by a serve prefix.
 */

import { getAuthHeaders } from '@/lib/authToken';
import { buildServeUrl } from '../ChatAgent/components/viewers/html/wsfilesUrl';
import type { BodyReaders } from '../ChatAgent/components/filePanel/fileBody';
import type { FileRefResolution } from '../ChatAgent/components/filePanel/types';

const baseURL: string = import.meta.env.VITE_API_BASE_URL ?? '';

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

export interface SharedThreadMetadata {
  kind: 'thread';
  thread_id: string;
  title: string;
  msg_type: string;
  created_at: string;
  updated_at: string;
  workspace_name: string;
  /** The workspace's folder and the root of the computer it is on, which the
   *  agent's absolute paths are spelled with. Absent from an older server. */
  root_dir?: string | null;
  dir_name?: string | null;
  /** Roots of the computers the workspace moved off, which a turn from before
   *  the move spells its paths under. */
  previous_root_dirs?: string[] | null;
  permissions: Record<string, unknown>;
}

interface SharedFileFields {
  kind: 'file';
  name: string;
  /** Workspace-relative entry path. */
  path: string;
  /** The serve prefix the page loads the file (and what it references) under. */
  frame_base: string;
}

/** What everyone but a private link's owner is answered: a share's prefix, which does not lapse. */
export interface SharedPublicFileMetadata extends SharedFileFields {
  access: 'public';
}

/** The owner's answer, whose prefix is a grant. */
export interface SharedOwnerFileMetadata extends SharedFileFields {
  access: 'owner';
  /** Seconds the grant behind `frame_base` has left when it is answered. */
  expires_in: number;
}

export type SharedFileMetadata = SharedPublicFileMetadata | SharedOwnerFileMetadata;

/** An app link, answered for its owner only. */
export interface SharedAppMetadata {
  kind: 'app';
  title: string;
  url: string;
  /** Seconds the signed `url` has left when it is answered. */
  expires_in: number;
}

export type ShareMetadata = SharedThreadMetadata | SharedFileMetadata | SharedAppMetadata;

export interface SharedFileListResponse {
  path: string;
  // The backend returns a flat array of workspace-relative file paths.
  files: string[];
  source: string;
}

export interface SharedFileReadResponse {
  path: string;
  content: string;
  mime: string;
  offset: number;
  limit: number;
  truncated: boolean;
}

// ---------------------------------------------------------------------------
// SSE event type
// ---------------------------------------------------------------------------

/** A single parsed SSE event from the replay stream. */
export type SSEEvent = Record<string, unknown>;

// ---------------------------------------------------------------------------
// API functions
// ---------------------------------------------------------------------------

/**
 * A failure the file panel can classify.
 *
 * `categorizeFileError` reads the status off `response.status`, which is what
 * the API client attaches: the owner path reaches the panel through the shared
 * client and this one does not, so a bare Error classified every shared
 * failure as `unknown`. That gave a permanent 403 a Retry button, and made the
 * panel read a real 404 as a successful landing, so the reference never fell
 * through to the resolve the owner path takes.
 */
function sharedFileError(status: number, message: string): Error {
  return Object.assign(new Error(message), { response: { status } });
}

/**
 * What a code resolves to. A 404 is the one uniform answer for an unknown
 * code, a private link asked for by anyone but its owner, and a link whose
 * sharing was stopped; the page reads it off `response.status`.
 */
export async function getSharedMetadata(
  code: string,
  { asVisitor = false, path = null }: { asVisitor?: boolean; path?: string | null } = {},
): Promise<ShareMetadata> {
  const headers = await getAuthHeaders();
  const params = new URLSearchParams();
  if (asVisitor) params.set('as', 'visitor');
  if (path) params.set('path', path);
  const query = params.toString() ? `?${params}` : '';
  const res = await fetch(`${baseURL}/api/v1/public/shared/${encodeURIComponent(code)}${query}`, { headers });
  if (!res.ok) throw sharedFileError(res.status, `Shared link not found (${res.status})`);
  const data = (await res.json()) as Partial<ShareMetadata> & Record<string, unknown>;
  // An answer with no kind predates file links and can only be a chat.
  return (data.kind ? data : { ...data, kind: 'thread' }) as ShareMetadata;
}

/**
 * A file's response, or the error the file panel classifies. A chat share's
 * download endpoint is gated on `allow_download` rather than `allow_files`,
 * so its 403 names that.
 */
async function served(url: string, verb: 'load' | 'read' | 'download' = 'load'): Promise<Response> {
  const res = await fetch(url);
  if (res.ok) return res;
  if (res.status === 403) {
    throw sharedFileError(403, verb === 'download' ? 'File download not permitted' : 'File access not permitted');
  }
  throw sharedFileError(res.status, `Failed to ${verb} shared file (${res.status})`);
}

function saveBlob(blob: Blob, fileName: string): void {
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = fileName;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(a.href);
}

/**
 * The serve prefix of a chat share's files. Serving is gated on `allow_files`,
 * the same as the rendered report, so previews (images, PDFs, a document's
 * embeds) read here. Routing them through `/files/download` instead would 403
 * on the common copy-link share, which grants `allow_files` alone.
 */
export function sharedServePrefix(shareToken: string): string {
  return `/api/v1/public/shared/${encodeURIComponent(shareToken)}/files/serve/`;
}

/** The bytes of a file under a serve prefix (a share's base or the owner's grant). */
export async function servedBytes(prefix: string, path: string): Promise<ArrayBuffer> {
  return (await served(buildServeUrl(prefix, path))).arrayBuffer();
}

/** A file under a serve prefix as an object URL, for an image a document embeds. */
export async function servedObjectUrl(prefix: string, path: string): Promise<string> {
  return URL.createObjectURL(await (await served(buildServeUrl(prefix, path))).blob());
}

/**
 * The file panel's body readers over a serve prefix. The workspace id they are
 * handed goes unused: the prefix is the credential.
 */
export function servedReaders(prefix: string): BodyReaders {
  const text = async (_workspaceId: string, path: string) => ({
    content: await (await served(buildServeUrl(prefix, path))).text(),
  });
  return {
    readFile: text,
    readFileFull: text,
    downloadFileAsArrayBuffer: (_workspaceId, path) => servedBytes(prefix, path),
  };
}

/** Save a served file under its own name, whichever origin serves it. */
export async function downloadServedFile(url: string, fileName: string): Promise<void> {
  saveBlob(await (await served(url)).blob(), fileName);
}

/**
 * Replay a shared thread's conversation as SSE events.
 */
export async function replaySharedThread(
  shareToken: string,
  onEvent: (event: SSEEvent) => void = () => {},
): Promise<void> {
  const res = await fetch(`${baseURL}/api/v1/public/shared/${shareToken}/replay`);
  if (!res.ok) throw new Error(`Failed to replay shared thread (${res.status})`);
  if (!res.body) throw new Error('Replay stream returned no body');

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let ev: { id?: string; event?: string } = {};

  const processLine = (line: string): void => {
    if (line.startsWith('id: ')) {
      ev.id = line.slice(4).trim();
    } else if (line.startsWith('event: ')) {
      ev.event = line.slice(7).trim();
    } else if (line.startsWith('data: ')) {
      try {
        const d: SSEEvent = JSON.parse(line.slice(6));
        if (ev.event) d.event = ev.event;
        if (ev.id != null) d._eventId = parseInt(ev.id, 10) || ev.id;
        onEvent(d);
      } catch (e) {
        console.warn('[shared-api] SSE parse error', e, line);
      }
      ev = {};
    } else if (line.trim() === '') {
      ev = {};
    }
  };

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop() || '';
    lines.forEach(processLine);
  }
  buffer.split('\n').forEach(processLine);
}

/**
 * List files in a shared thread's workspace.
 */
export async function getSharedFiles(
  shareToken: string,
  path: string = '.',
): Promise<SharedFileListResponse> {
  const params = new URLSearchParams({ path });
  const res = await fetch(`${baseURL}/api/v1/public/shared/${shareToken}/files?${params}`);
  if (!res.ok) {
    if (res.status === 403) throw sharedFileError(res.status, 'File access not permitted');
    throw sharedFileError(res.status, `Failed to list shared files (${res.status})`);
  }
  return res.json() as Promise<SharedFileListResponse>;
}

/**
 * Resolve a file reference against the shared thread's file listing.
 */
export async function resolveSharedFile(
  shareToken: string,
  candidates: string[],
  recentWrites: string[] = [],
): Promise<FileRefResolution> {
  const res = await fetch(`${baseURL}/api/v1/public/shared/${shareToken}/files/resolve`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ candidates, recent_writes: recentWrites }),
  });
  if (!res.ok) {
    if (res.status === 403) throw sharedFileError(res.status, 'File access not permitted');
    throw sharedFileError(res.status, `Failed to resolve shared file (${res.status})`);
  }
  return res.json() as Promise<FileRefResolution>;
}

/**
 * Read a text file from a shared thread's workspace.
 */
export async function readSharedFile(
  shareToken: string,
  path: string,
): Promise<SharedFileReadResponse> {
  const params = new URLSearchParams({ path });
  const res = await served(`${baseURL}/api/v1/public/shared/${shareToken}/files/read?${params}`, 'read');
  return res.json() as Promise<SharedFileReadResponse>;
}

/**
 * Save a file from a chat share. This is the one read gated on
 * `allow_download`; previews go through {@link sharedServePrefix}.
 */
export async function downloadSharedFile(shareToken: string, path: string): Promise<void> {
  const params = new URLSearchParams({ path });
  const res = await served(`${baseURL}/api/v1/public/shared/${shareToken}/files/download?${params}`, 'download');
  saveBlob(await res.blob(), path.split('/').pop() || 'download');
}
