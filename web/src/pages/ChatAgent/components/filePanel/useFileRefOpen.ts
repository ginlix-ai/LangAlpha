import { useCallback, useRef } from 'react';
import { basename, isSystemPath, linkCandidates, resolveExact } from '../../utils/fileRefResolver';
import { normalizeAgentPath } from '../../utils/agentPaths';
import type { FileLocation } from '../../utils/fileLocation';
import type { FileRefResolution } from './types';
import { DOWNLOAD_ONLY_EXTENSIONS, getFileExtension } from './fileMeta';
import { categorizeFileError, type FileError } from './fileErrors';
import type { FileBodyCache } from './useFileBody';
import type { FileTabsApi, OpenFileOptions } from './useFileTabs';

interface RefOpenArgs {
  tabs: FileTabsApi;
  cache: FileBodyCache;
  /** Which open files the agent has rewritten since a tab read them. */
  hasChanged: (path: string) => boolean;
  files: string[];
  workspaceStatus?: string;
  resolveFileFn: ((candidates: string[], recentWrites: string[]) => Promise<FileRefResolution>) | null;
  getRecentWritePaths?: (() => string[]) | null;
  /** Clear whatever the panel is showing over the viewer before a file lands. */
  onBeforeOpen: () => void;
  /** A reference arrives from outside the tree, so it leaves no filter behind. */
  clearSearch: () => void;
  /** Nothing certain matched: show the tree filtered to the name, with the candidates. */
  onLandOnSearch: (ref: string, name: string, matches: string[]) => void;
  /** Re-read the file already open, which is what a retry with no reference does. */
  refetch: () => void;
  /** The computer's root, which an absolute reference may start with. */
  root?: string | null;
}

/**
 * Turning a reference into an open tab.
 *
 * A reference from chat or from inside a document may not name a real path —
 * the agent moves files, writes relative links, and quotes names it never
 * wrote. The order here is what keeps the common case free: an exact match or
 * an absolute path is read straight away, and only a miss pays for the
 * server's lookup.
 */
export function useFileRefOpen({
  tabs, cache, hasChanged, files, workspaceStatus, resolveFileFn, getRecentWritePaths,
  onBeforeOpen, clearSearch, onLandOnSearch, refetch, root,
}: RefOpenArgs) {
  // Bumped by every open, direct or by reference, so a slow lookup cannot land
  // after the reader has already opened something else: whatever asked last
  // holds the ticket, and a reference checks it after every await.
  const refSeq = useRef(0);
  // The ticket of a reference waiting on the server's lookup. It has no tab
  // until the answer comes back, so an empty strip does not mean nothing is coming.
  const lookupSeq = useRef(0);
  // The reference a lookup could not answer, kept against the tab it landed in
  // so only that tab's retry re-asks it; another tab's retry re-reads its own file.
  const unresolved = useRef<{ path: string; rawRef: string; fromFile: string | null; location: FileLocation | null } | null>(null);

  const openAt = useCallback(async (path: string, { pin = false, location = null }: OpenFileOptions = {}): Promise<FileError | null> => {
    unresolved.current = null;
    onBeforeOpen();
    tabs.openFile(path, { pin, location });
    if (DOWNLOAD_ONLY_EXTENSIONS.has(getFileExtension(path))) return { category: 'binary_file' };
    if (hasChanged(path)) cache.invalidate(path);
    try {
      await cache.fetchBody(path);
      return null;
    } catch (err) {
      const error = categorizeFileError(err, workspaceStatus);
      // Too large or binary is an answer the panel renders, not a fault.
      if (error.category !== 'too_large' && error.category !== 'binary_file') {
        console.error(`[FilePanel] Failed to load ${path}:`, err);
      }
      return error;
    }
  }, [tabs, cache, hasChanged, workspaceStatus, onBeforeOpen]);

  /** Open a file in a tab, having first proved its bytes are readable. */
  const openFileAt = useCallback((path: string, opts?: OpenFileOptions) => {
    refSeq.current += 1;
    return openAt(path, opts);
  }, [openAt]);

  /**
   * Take the ticket without opening a file: a chart, preview or settings tab
   * brought to the front must not be pushed aside by a lookup that started
   * before it. Cheap when nothing is pending.
   */
  const cancelPending = useCallback(() => {
    refSeq.current += 1;
  }, []);

  /** A reference the reader asked for is still being looked up, and will open a tab when it answers. */
  const isLookingUp = useCallback(() => lookupSeq.current !== 0 && lookupSeq.current === refSeq.current, []);

  const openFileRef = useCallback(async (
    rawRef: string,
    { fromFile = null, location = null, pin = false }: { fromFile?: string | null; location?: FileLocation | null; pin?: boolean } = {},
  ) => {
    const seq = ++refSeq.current;
    const current = () => seq === refSeq.current;
    const candidates = fromFile ? linkCandidates(rawRef, fromFile, root) : [normalizeAgentPath(rawRef, root)];
    const primary = candidates[0];
    if (!primary) return;
    clearSearch();
    const tried = new Set<string>();
    // A probe that missed replaced the tab in place, so whatever lands next
    // would keep it as a Back step onto a read error nobody asked for.
    const missed = new Set<string>();
    // A probe lands in the loaned tab whatever the caller asked: a pinned probe
    // that misses would leave a pinned "not found" behind for every path
    // tried. Only the attempt that lands is pinned.
    const attempt = async (path: string) => {
      tried.add(path);
      const error = await openAt(path, { location, pin: false });
      if (!error && pin && current()) tabs.openFile(path, { pin: true });
      return error;
    };
    // True once the path opened, or failed for a reason a search cannot fix.
    const landed = async (path: string) => {
      // A download-only file opens with no read, so opening one proves nothing
      // about the path: a moved .docx would sit behind a download card built on
      // a name the listing still remembers. Only the lookup settles it.
      if (resolveFileFn && DOWNLOAD_ONLY_EXTENSIONS.has(getFileExtension(path))) return false;
      const category = (await attempt(path))?.category;
      if (category !== 'not_found' && category !== 'not_backed_up') return true;
      missed.add(path);
      return false;
    };
    const search = async () => {
      const writes = getRecentWritePaths?.() ?? [];

      // A known path can still be stale (the agent moved it), so a miss falls through.
      const exact = resolveExact(candidates, files, writes);
      if (exact && await landed(exact)) return;
      if (!current()) return;
      // Absolute and system paths are not in the default listing and the agent
      // names them exactly, so read them before asking.
      const direct = candidates.find((c) => c.startsWith('/') || isSystemPath(c));
      if (direct && !tried.has(direct) && await landed(direct)) return;
      if (!current()) return;
      if (!resolveFileFn) {
        if (!tried.has(primary)) void attempt(primary);
        return;
      }

      let result: FileRefResolution | null = null;
      lookupSeq.current = seq;
      try {
        result = await resolveFileFn(candidates, writes);
      } catch (err) {
        console.error('[FilePanel] File reference lookup failed:', err);
      }
      if (lookupSeq.current === seq) lookupSeq.current = 0;
      if (!current()) return;

      if (!result || result.status === 'unavailable') {
        // Retrying the path alone asks the same unanswerable question: the lookup
        // is the only thing that knows where the file is, and the lookup is what
        // was unavailable. Keeping the reference is what lets a retry resolve.
        await attempt(primary);
        if (current()) unresolved.current = { path: primary, rawRef, fromFile, location };
        return;
      }
      if (result.status === 'resolved' && result.path) return void attempt(result.path);
      // Name the reference as written; the joined reading is only our guess.
      const named = candidates[candidates.length - 1];
      onLandOnSearch(named, basename(named), result.matches);
    };
    await search();
    if (missed.size) tabs.forgetPaths([...missed]);
  }, [openAt, tabs, clearSearch, onLandOnSearch, resolveFileFn, getRecentWritePaths, files, root]);

  /** The error card's retry: re-ask the lookup where this tab was owed one, else re-read. */
  const retryOpen = useCallback((path: string | null) => {
    const ref = unresolved.current;
    if (ref && ref.path === path) return void openFileRef(ref.rawRef, { fromFile: ref.fromFile, location: ref.location });
    refetch();
  }, [openFileRef, refetch]);

  return { openFileAt, openFileRef, retryOpen, cancelPending, isLookingUp };
}
