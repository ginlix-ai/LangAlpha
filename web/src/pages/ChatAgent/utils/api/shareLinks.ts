/**
 * Share links and file grants (owner side).
 *
 * A leaf on purpose: consumers import this module directly rather than through
 * `utils/api.ts`, so the whole-module mock factories on that barrel keep
 * working without naming these.
 */
import { api } from '@/api/client';
import { apiErrorObject, apiErrorStatus } from '@/api/errors';
import type {
  FileGrant,
  ShareLink,
  ShareLinkFiles,
  ShareLinkTarget,
  SharedLinksResponse,
} from '@/types/api';

export async function createShareLink(workspaceId: string, target: ShareLinkTarget): Promise<ShareLink> {
  const { data } = await api.post<ShareLink>(`/api/v1/workspaces/${workspaceId}/share-links`, target);
  return data;
}

/** One of the owner's links in any of their workspaces, private ones included: 404 when it is not theirs. */
export async function getShareLink(code: string): Promise<ShareLink> {
  const { data } = await api.get<ShareLink>(`/api/v1/share-links/${code}`);
  return data;
}

export async function getShareLinkFiles(workspaceId: string, code: string): Promise<ShareLinkFiles> {
  const { data } = await api.get<ShareLinkFiles>(`/api/v1/workspaces/${workspaceId}/share-links/${code}/files`);
  return data;
}

export type ShareLinkPatch = { shared: true; files: string[] } | { shared: false };

export async function patchShareLink(workspaceId: string, code: string, patch: ShareLinkPatch): Promise<ShareLink> {
  const { data } = await api.patch<ShareLink>(`/api/v1/workspaces/${workspaceId}/share-links/${code}`, patch);
  return data;
}

export async function listSharedLinks(workspaceId: string): Promise<SharedLinksResponse> {
  const { data } = await api.get<SharedLinksResponse>(`/api/v1/workspaces/${workspaceId}/share-links`);
  return data;
}

export async function createFileGrant(workspaceId: string): Promise<FileGrant> {
  const { data } = await api.post<FileGrant>(`/api/v1/workspaces/${workspaceId}/file-grant`);
  return data;
}

/**
 * The absolute link a user pastes somewhere: the app origin plus `/a/<code>`.
 * An app's `path` opens it at that page, the root as `/`, rather than the
 * link's own entry, which moves whenever the agent publishes another page on
 * the same port; only a link with no `path` follows the entry.
 */
export function shareLinkHref(code: string, path?: string | null): string {
  const href = `${window.location.origin}/a/${code}`;
  if (path == null) return href;
  return `${href}?${new URLSearchParams({ path: path.replace(/^\/+/, '') || '/' })}`;
}

/**
 * The code and page of an `/a/` link on this origin, the inverse of
 * `shareLinkHref`, or null for any other href. The page comes back without its
 * leading slash, so the root (`?path=/`) is `''`, and a link naming no page is
 * null: only that one opens the link's entry, which a later call on the port moves.
 * A visitor-mode link (`?as=visitor`) is no owner's link and comes back null.
 */
export function parseShareLinkHref(
  href: string | undefined,
  origin = window.location.origin,
): { code: string; path: string | null } | null {
  if (!href) return null;
  let url: URL;
  try {
    url = new URL(href, origin);
  } catch {
    return null;
  }
  const code = url.origin === origin ? /^\/a\/([0-9A-Za-z]+)\/?$/.exec(url.pathname)?.[1] : undefined;
  if (!code || url.searchParams.has('as')) return null;
  return { code, path: url.searchParams.get('path')?.replace(/^\/+/, '') ?? null };
}

/** Which 409 a share PATCH answered: the reviewed list no longer matches the file, or the link changed during the request. */
export type ShareConflict = 'files_changed' | 'link_changed';

export function shareConflictIn(err: unknown): ShareConflict | null {
  if (apiErrorStatus(err) !== 409) return null;
  const code = apiErrorObject(err)?.code;
  return code === 'files_changed' || code === 'link_changed' ? code : null;
}

/** Which cap a file's list went over: a count of files, or their total bytes. */
export interface ShareCap {
  code: 'too_many_files' | 'too_many_bytes';
  limit: number;
}

/** The cap a 422 names, or null for any other error. */
export function shareCapIn(err: unknown): ShareCap | null {
  if (apiErrorStatus(err) !== 422) return null;
  const { code, limit } = apiErrorObject(err) ?? {};
  if (code !== 'too_many_files' && code !== 'too_many_bytes') return null;
  return { code, limit: typeof limit === 'number' ? limit : 0 };
}
