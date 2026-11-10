/**
 * Search across everything the user said and was told: thread titles, the
 * turns of every thread (their message and the agent's final answer), and the
 * files in their workspaces' folders, served by `GET /api/v1/search`. The command palette is its one reader.
 *
 * The wire types live here and nowhere else, so a field the server renames is
 * one edit.
 */
import { api } from '@/api/client';

/** `[start, end)` offsets into the string they arrive with. */
export type MatchRange = [number, number];

export type SearchKind = 'threads' | 'turns' | 'files';

export interface SearchThreadHit {
  thread_id: string;
  /** The title, or the first message when the thread has none. */
  title: string;
  workspace_id: string;
  workspace_name: string;
  /** Set on a thread an automation runs in. */
  automation_name: string | null;
  updated_at: string;
  /** Spans of `title` the query matched. */
  match: MatchRange[];
}

export interface SearchTurnHit {
  thread_id: string;
  thread_title: string;
  workspace_id: string;
  workspace_name: string;
  turn_index: number;
  /** Whose words matched: the user's message, or the agent's answer. */
  side: 'user' | 'agent';
  /** About 160 characters around the first match. */
  snippet: string;
  /** Spans of `snippet` the query matched. */
  matches: MatchRange[];
  turn_at: string;
}

export interface SearchFileHit {
  workspace_id: string;
  workspace_name: string;
  /** Relative to the workspace's folder. */
  path: string;
  name: string;
  size: number | null;
  modified_at: string | null;
  /** Spans of `path` the query matched, in UTF-16 units. */
  match: MatchRange[];
}

export interface SearchResponse {
  threads: SearchThreadHit[];
  turns: SearchTurnHit[];
  /** Files whose name matched first. None in a thread's scope. */
  files: SearchFileHit[];
  /** Where each kind's next page starts; null once it has no more. */
  next_cursor: Record<SearchKind, string | null>;
  /** Kinds whose search ran out of time; the others still answered. */
  failed: SearchKind[];
}

export interface SearchParams {
  q: string;
  /** Per kind; the server allows up to 50. */
  limit?: number;
  /** Every kind when unset. A cursor pages its own kind alone. */
  kinds?: SearchKind | readonly SearchKind[];
  cursor?: string | null;
  /** Only this workspace's threads, turns and files. */
  workspaceId?: string;
  /** Only this thread's turns, with no cap per thread; the threads come back empty. */
  threadId?: string;
}

export interface SearchPreview {
  thread_id: string;
  title: string;
  workspace_id: string;
  workspace_name: string;
  turn_index: number;
  turn_at: string;
  /** Each side clipped to about 4000 characters. */
  user_text: string;
  answer_text: string;
}

export async function searchConversations(
  { q, limit, kinds, cursor, workspaceId, threadId }: SearchParams,
  { signal }: { signal?: AbortSignal } = {},
): Promise<SearchResponse> {
  const { data } = await api.get<Partial<SearchResponse>>('/api/v1/search', {
    params: { q, limit, kinds: typeof kinds === 'string' ? kinds : kinds?.join(','), cursor: cursor ?? undefined, workspace_id: workspaceId, thread_id: threadId },
    signal,
  });
  // A kind not asked for (or a server that predates it) comes back absent.
  return {
    threads: data.threads ?? [],
    turns: data.turns ?? [],
    files: data.files ?? [],
    next_cursor: { threads: null, turns: null, files: null, ...data.next_cursor },
    failed: data.failed ?? [],
  };
}

/** One turn of a thread, or its newest indexed turn when `turnIndex` is unset; 404 until it is indexed. */
export async function getSearchPreview(
  threadId: string,
  turnIndex: number | null,
  { signal }: { signal?: AbortSignal } = {},
): Promise<SearchPreview> {
  const { data } = await api.get<SearchPreview>('/api/v1/search/preview', {
    params: { thread_id: threadId, turn_index: turnIndex ?? undefined },
    signal,
  });
  return data;
}
