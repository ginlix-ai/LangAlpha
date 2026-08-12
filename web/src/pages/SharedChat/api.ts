/**
 * Public API functions for shared thread access.
 * All requests are unauthenticated — no Bearer token needed.
 */

import { buildSharedServeUrl } from '../ChatAgent/components/viewers/html/wsfilesUrl';

const baseURL: string = import.meta.env.VITE_API_BASE_URL ?? '';

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

export interface SharedThreadMetadata {
  thread_id: string;
  title: string;
  msg_type: string;
  created_at: string;
  updated_at: string;
  workspace_name: string;
  permissions: Record<string, unknown>;
}

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
// Download mode
// ---------------------------------------------------------------------------

export type DownloadMode = 'download' | 'blob' | 'arraybuffer';

// ---------------------------------------------------------------------------
// API functions
// ---------------------------------------------------------------------------

/**
 * Fetch metadata for a shared thread.
 */
export async function getSharedThread(shareToken: string): Promise<SharedThreadMetadata> {
  const res = await fetch(`${baseURL}/api/v1/public/shared/${shareToken}`);
  if (!res.ok) throw new Error(`Shared thread not found (${res.status})`);
  return res.json() as Promise<SharedThreadMetadata>;
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
    if (res.status === 403) throw new Error('File access not permitted');
    throw new Error(`Failed to list shared files (${res.status})`);
  }
  return res.json() as Promise<SharedFileListResponse>;
}

/**
 * Read a text file from a shared thread's workspace.
 */
export async function readSharedFile(
  shareToken: string,
  path: string,
): Promise<SharedFileReadResponse> {
  const params = new URLSearchParams({ path });
  const res = await fetch(`${baseURL}/api/v1/public/shared/${shareToken}/files/read?${params}`);
  if (!res.ok) {
    if (res.status === 403) throw new Error('File access not permitted');
    throw new Error(`Failed to read shared file (${res.status})`);
  }
  return res.json() as Promise<SharedFileReadResponse>;
}

/**
 * Download a raw file from a shared thread's workspace (browser download).
 */
export async function downloadSharedFile(
  shareToken: string,
  path: string,
): Promise<void> {
  const params = new URLSearchParams({ path });
  const res = await fetch(`${baseURL}/api/v1/public/shared/${shareToken}/files/download?${params}`);
  if (!res.ok) {
    if (res.status === 403) throw new Error('File download not permitted');
    throw new Error(`Failed to download shared file (${res.status})`);
  }
  const blob = await res.blob();
  const blobUrl = URL.createObjectURL(blob);
  const fileName = path.split('/').pop() || 'download';
  const a = document.createElement('a');
  a.href = blobUrl;
  a.download = fileName;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(blobUrl);
}

/**
 * Download a shared file in different modes (needed for FilePanel's rich viewers).
 *
 * - `'blob'`        returns an object URL string
 * - `'arraybuffer'` returns an ArrayBuffer
 * - `'download'`    triggers a browser save dialog (returns void)
 */
export async function downloadSharedFileAs(shareToken: string, path: string, mode: 'blob'): Promise<string>;
export async function downloadSharedFileAs(shareToken: string, path: string, mode: 'arraybuffer'): Promise<ArrayBuffer>;
export async function downloadSharedFileAs(shareToken: string, path: string, mode?: 'download'): Promise<void>;
export async function downloadSharedFileAs(
  shareToken: string,
  path: string,
  mode: DownloadMode = 'download',
): Promise<string | ArrayBuffer | void> {
  const params = new URLSearchParams({ path });
  const res = await fetch(`${baseURL}/api/v1/public/shared/${shareToken}/files/download?${params}`);
  if (!res.ok) {
    if (res.status === 403) throw new Error('File download not permitted');
    throw new Error(`Failed to download shared file (${res.status})`);
  }

  if (mode === 'blob') {
    const blob = await res.blob();
    return URL.createObjectURL(blob);
  }
  if (mode === 'arraybuffer') {
    return await res.arrayBuffer();
  }
  // mode === 'download' — trigger browser save
  const blob = await res.blob();
  const fileName = path.split('/').pop() || 'download';
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = fileName;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(a.href);
}

/**
 * Fetch a shared file's bytes as an object URL via the serve endpoint.
 *
 * Serving is gated on `allow_files` (same as the rendered report), so this is
 * the right path for byte-access *previews* — inline markdown images,
 * file-panel image preview. The explicit "save a copy" affordance stays on
 * `downloadSharedFileAs(..., 'download')` (gated on `allow_download`). Routing
 * previews through `/files/download` instead would 403 on the common
 * copy-link share, which grants only `allow_files`.
 */
export async function fetchSharedServeObjectUrl(shareToken: string, path: string): Promise<string> {
  const res = await fetch(buildSharedServeUrl(shareToken, path));
  if (!res.ok) {
    if (res.status === 403) throw new Error('File access not permitted');
    throw new Error(`Failed to load shared file (${res.status})`);
  }
  return URL.createObjectURL(await res.blob());
}

/** Like {@link fetchSharedServeObjectUrl} but returns the raw bytes (binary preview). */
export async function fetchSharedServeArrayBuffer(shareToken: string, path: string): Promise<ArrayBuffer> {
  const res = await fetch(buildSharedServeUrl(shareToken, path));
  if (!res.ok) {
    if (res.status === 403) throw new Error('File access not permitted');
    throw new Error(`Failed to load shared file (${res.status})`);
  }
  return res.arrayBuffer();
}
