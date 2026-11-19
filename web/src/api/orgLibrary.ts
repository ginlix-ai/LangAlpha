import { api } from '@/api/client';
import type { LibraryItem, LibraryKind, LibraryPage, LibraryTarget, PublishedLookup } from '@/types/orgLibrary';

const BASE = '/api/v1/org-library';

export async function getLibrary(kind: LibraryKind | null, cursor?: string | null, limit = 50): Promise<LibraryPage> {
  const params: Record<string, string | number> = { limit };
  if (kind) params.kind = kind;
  if (cursor) params.cursor = cursor;
  const { data } = await api.get<LibraryPage>(BASE, { params });
  return data;
}

function targetParams(target: LibraryTarget): Record<string, string> {
  if (target.kind === 'thread') return { thread_id: target.threadId };
  if (target.kind === 'file') return { artifact_id: target.artifactId };
  return { skill_name: target.skillName };
}

export async function getPublished(target: LibraryTarget): Promise<PublishedLookup> {
  const params = targetParams(target);
  const { data } = await api.get<PublishedLookup>(`${BASE}/published`, { params });
  return data;
}

/** Publish, or publish again. With no title the server names the item as
 *  the app names the content, and an item published before keeps its own. */
export async function publishToLibrary(target: LibraryTarget, title?: string): Promise<LibraryItem> {
  const { data } = await api.post<LibraryItem>(BASE, {
    kind: target.kind,
    ...(target.kind === 'skill'
      ? { skill_name: target.skillName }
      : { target_id: target.kind === 'thread' ? target.threadId : target.artifactId }),
    ...(title ? { title } : {}),
  });
  return data;
}

/** Install a skill item into the viewer's account, or update the copy an
 *  earlier install made. */
export async function installFromLibrary(itemId: string): Promise<LibraryItem> {
  const { data } = await api.post<LibraryItem>(`${BASE}/${encodeURIComponent(itemId)}/install`);
  return data;
}

export async function removeFromLibrary(itemId: string): Promise<void> {
  await api.delete(`${BASE}/${encodeURIComponent(itemId)}`);
}
