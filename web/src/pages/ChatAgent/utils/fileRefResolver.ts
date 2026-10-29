/**
 * Resolves a file reference the agent wrote (a markdown link, a tool-call
 * path) to a path that exists in the workspace.
 *
 * Agents often name a file differently from where it landed: a bare
 * `report.md` for `results/report.md`, a path relative to the report that
 * links it, or an absolute sandbox path. The panel tries the cheap, certain
 * matches first and only guesses by name once a direct read has missed, so a
 * file that does exist at the named path is never swapped for a namesake.
 */

import { SYSTEM_DIR_PREFIXES } from '../components/filePanel/fileMeta';
import {
  folderWorkspacePath,
  normalizeAgentPath,
  parseAgentPath,
  ranElsewhere,
  workspaceScopedPath,
  type AgentPathParts,
  type ComputerFolders,
} from './agentPaths';

/** The tools whose path argument names a file the agent created or changed. */
export const WRITE_TOOLS = new Set(['Write', 'Edit']);

export function basename(path: string): string {
  const trimmed = path.replace(/\/+$/, '');
  return trimmed.slice(trimmed.lastIndexOf('/') + 1);
}

export function dirname(path: string): string {
  const idx = path.lastIndexOf('/');
  return idx <= 0 ? (idx === 0 ? '/' : '') : path.slice(0, idx);
}

/**
 * Candidate paths for a link found inside an open file, most likely first. A
 * relative link reads against the file's own directory, but agents just as
 * often write it from the workspace root, so both are offered. `root` is the
 * computer's own root (`ComputerFolders.rootDir`).
 */
export function linkCandidates(href: string, fromFile: string | null, root?: string | null): string[] {
  const { path: direct, absolute, workspaceId } = parseAgentPath(href, root);
  // A `__wsref__` destination names its own workspace, so the file the reader
  // has open says nothing about where it lives; joining would have produced
  // `work/__wsref__/<id>/…`, a path no workspace holds.
  if (!fromFile || absolute || workspaceId) return [direct];
  const dir = dirname(normalizeAgentPath(fromFile, root));
  if (!dir || dir === '/') return [direct];
  const joined = normalizeAgentPath(`${dir}/${href}`, root);
  return joined === direct ? [direct] : [joined, direct];
}

/**
 * Where a link inside an open file lands when it climbs into a workspace's
 * folder, by either reading `linkCandidates` offers. Read from the working
 * directory, `../NVDA/x.md` climbs into NVDA's folder; read from `reports/`,
 * it takes `../../NVDA/x.md`. Another workspace's folder names that workspace
 * (a sibling's, or one on another computer at its whole path there), and
 * this workspace's own reads from its root, with no `workspaceId`. At most one
 * reading climbs exactly out of the workspace, since the joined one starts as
 * many levels deeper as the file sits.
 */
export function folderLinkTarget(
  href: string,
  fromFile: string | null,
  folders: ComputerFolders | null | undefined,
): { path: string; workspaceId?: string } | null {
  if (!folders) return null;
  const root = folders.rootDir;
  const parts = parseAgentPath(href, root);
  // A candidate is a canonical string, which has lost the sandbox root.
  const readings = parts.absolute ? [parts] : linkCandidates(href, fromFile, root).map((c) => parseAgentPath(c, root));
  for (const reading of readings) {
    const other = folderWorkspacePath(reading, folders);
    if (other) return other;
    const own = workspaceScopedPath(reading, folders.dirName, folders.previousDirNames, folders.otherRoots);
    if (own !== reading.path) return { path: own || './' };
  }
  return null;
}

/**
 * Where a save should go: the reference resolved the way opening it is, and
 * whether the lookup could place it at all.
 *
 * A deliverable card carries the reference as the reply wrote it, which is
 * often not where the file landed, and that is the whole reason the lookup
 * exists. Open went through it and Download did not, so one card opened a
 * report and then failed to save the same file.
 *
 * The lookup is an improvement on the reference rather than a precondition, so
 * `placed` stays true whenever nothing contradicts the reference: no lookup, a
 * failed one, or a server that has not looked yet. It goes false only when the
 * server looked and could not pick, which is the one answer Open hands to the
 * user instead of guessing at.
 */
export async function downloadTarget(
  path: string,
  resolve: ((candidates: string[], recentWrites: string[]) => Promise<{ status: string; path?: string | null }>) | null,
  recentWrites: readonly string[] = [],
): Promise<{ path: string; placed: boolean }> {
  if (!resolve) return { path, placed: true };
  try {
    const result = await resolve([path], [...recentWrites]);
    if (result.status === 'resolved' && result.path) return { path: result.path, placed: true };
    return { path, placed: result.status === 'unavailable' || result.status === 'resolved' };
  } catch {
    return { path, placed: true };
  }
}

export function isSystemPath(path: string): boolean {
  if (path.startsWith('/')) return false;
  const first = path.split('/')[0];
  return SYSTEM_DIR_PREFIXES.includes(first);
}

/** The one match certain enough to open without a server round trip: the exact path. */
export function resolveExact(
  candidates: readonly string[],
  files: readonly string[],
  recentWrites: readonly string[],
): string | null {
  const known = new Set([...files, ...recentWrites]);
  return candidates.find((c) => known.has(c)) ?? null;
}

export interface ToolCallLike {
  toolName?: string;
  toolCall?: { args?: Record<string, unknown> } | null;
  isFailed?: boolean;
  isComplete?: boolean;
  /** Set only when a result arrived, which is the one proof the call returned. */
  toolCallResult?: unknown;
  order?: number;
}

/**
 * A message as the file collectors read it.
 *
 * Typed rather than `Record<string, unknown>` so the collectors below need no
 * casts of their own; the one cast lives where the untyped transcript enters.
 */
export interface TurnMessage {
  role?: unknown;
  contentSegments?: { type?: string; content?: string; order?: number }[];
  content?: unknown;
  isStreaming?: unknown;
  toolCallProcesses?: Record<string, ToolCallLike>;
}

/**
 * The write and edit calls in one message, in call order, each with the path
 * it named already canonical.
 *
 * The four spellings are the knowledge worth keeping in one place: a tool
 * names its path under whichever key its schema chose, and a fifth spelling
 * has to reach the deliverables deck and the link resolver together or they
 * disagree about what the turn wrote.
 */
export interface WriteCall {
  id: string;
  /** The named path, read once; `parts.path` is its canonical form. */
  parts: AgentPathParts;
  call: ToolCallLike;
}

export function writeCalls(message: TurnMessage, root?: string | null): WriteCall[] {
  const out: WriteCall[] = [];
  const calls = Object.entries(message.toolCallProcesses ?? {})
    // A call still in flight when the turn stopped names a file nothing said
    // it wrote. `isComplete` is not that evidence: a stop and a steering
    // rollback both fold every open call to complete with no result, so the
    // returned result itself is what a card is allowed to claim.
    .filter(([, p]) => p && WRITE_TOOLS.has(p.toolName ?? '') && p.toolCallResult != null && !p.isFailed)
    .sort(([, a], [, b]) => (a.order ?? 0) - (b.order ?? 0));
  for (const [id, call] of calls) {
    const args = call.toolCall?.args;
    if (ranElsewhere(args)) continue;
    const named = args?.file_path ?? args?.filePath ?? args?.path ?? args?.filename;
    if (typeof named !== 'string' || !named) continue;
    const parts = parseAgentPath(named, root);
    if (parts.path) out.push({ id, parts, call });
  }
  return out;
}

/**
 * What places a write in the workspace: its own folders, and the computer root
 * an absolute path starts with, or one it was written under before a move. A
 * sibling's folders do not, so a refreshed sibling list leaves the logs alone.
 */
export type WriteScope = Pick<ComputerFolders, 'dirName' | 'previousDirNames' | 'rootDir' | 'otherRoots'>;

/** One Write or Edit the agent made, as the changed-file dot sees it. */
export interface WriteEvent {
  /** The tool call's id, which is what tells two writes of one file apart. */
  id: string;
  path: string;
}

/**
 * Every write in the thread, newest first, repeats kept. `collectRecentWritePaths`
 * names each file once, which is what a lookup wants; a tab asking whether its
 * file changed needs the write itself, since a rewrite of the newest file
 * leaves that list byte-identical.
 */
export function collectWriteLog(messages: readonly TurnMessage[], scope?: WriteScope | null): WriteEvent[] {
  const out: WriteEvent[] = [];
  for (let i = messages.length - 1; i >= 0 && out.length < RECENT_WRITE_LIMIT; i--) {
    const calls = writeCalls(messages[i] ?? {}, scope?.rootDir);
    for (let j = calls.length - 1; j >= 0 && out.length < RECENT_WRITE_LIMIT; j--) {
      out.push({ id: calls[j].id, path: workspaceScopedPath(calls[j].parts, scope?.dirName, scope?.previousDirNames, scope?.otherRoots) });
    }
  }
  return out;
}

/**
 * The most the resolver will accept, and therefore the most worth collecting.
 *
 * `ResolveFileRefRequest.recent_writes` caps the list at 200 and FastAPI
 * rejects a longer one outright, so an uncapped walk would 422 a whole thread's
 * worth of clicks the moment it passed its 200th distinct file. The list is
 * only a tiebreak between namesakes and it is newest first, so the tail is what
 * a longer thread can afford to lose.
 */
export const RECENT_WRITE_LIMIT = 200;

/** Paths the agent wrote or edited in this thread, newest first. */
export function collectRecentWritePaths(messages: readonly TurnMessage[], scope?: WriteScope | null): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  for (let i = messages.length - 1; i >= 0 && out.length < RECENT_WRITE_LIMIT; i--) {
    const calls = writeCalls(messages[i] ?? {}, scope?.rootDir);
    for (let j = calls.length - 1; j >= 0 && out.length < RECENT_WRITE_LIMIT; j--) {
      const path = workspaceScopedPath(calls[j].parts, scope?.dirName, scope?.previousDirNames, scope?.otherRoots);
      if (seen.has(path)) continue;
      seen.add(path);
      out.push(path);
    }
  }
  return out;
}
