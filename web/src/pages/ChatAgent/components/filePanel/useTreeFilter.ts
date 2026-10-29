import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { RefObject } from 'react';
import { userLocalStorage } from '@/lib/userStorage';
import type { TreeNode } from './types';
import { getAvailableTypes, getFileType, sortFiles } from './fileMeta';
import { buildFileTree } from './fileTree';

interface TreeFilterArgs {
  workspaceId: string;
  files: string[];
  /** The folder a chat link pointed the tree at; it filters until cleared. */
  scopeDir?: string | null;
  /** The panel root, which is where a revealed row is looked up. */
  rootRef: RefObject<HTMLElement | null>;
}

function readExpandedDirs(key: string): Set<string> {
  try {
    const saved = userLocalStorage.getItem(key);
    return saved ? new Set(JSON.parse(saved) as string[]) : new Set();
  } catch { return new Set(); }
}

/**
 * Everything that decides which rows the tree shows: the search, the type and
 * folder filters, the sort, and which directories are open.
 *
 * Expanded directories persist per workspace, which is what makes the tree
 * come back the way it was left rather than collapsed to the root.
 */
export function useTreeFilter({ workspaceId, files, scopeDir, rootRef }: TreeFilterArgs) {
  const [searchQuery, setSearchQuery] = useState('');
  const [missedRef, setMissedRef] = useState<string | null>(null);
  const [extraMatches, setExtraMatches] = useState<string[]>([]);
  const [filterType, setFilterType] = useState('All');
  const [sortBy, setSortBy] = useState('name-asc');
  const [showSortMenu, setShowSortMenu] = useState(false);
  /** The sort button and its menu; a mousedown inside is a pick, not a dismissal. */
  const sortMenuRef = useRef<HTMLDivElement>(null);

  // The lookup can name files the listing does not carry (a system path, a
  // stale write), and those have to be reachable from the row it landed on.
  const listedFiles = useMemo(
    () => (extraMatches.length && searchQuery ? [...new Set([...files, ...extraMatches])] : files),
    [files, extraMatches, searchQuery],
  );
  const availableTypes = useMemo(() => getAvailableTypes(listedFiles), [listedFiles]);
  const trimmedQuery = searchQuery.trim().toLowerCase();
  const filteredSortedFiles = useMemo(() => {
    let result = listedFiles;
    if (trimmedQuery) result = result.filter((fp) => fp.toLowerCase().includes(trimmedQuery));
    if (scopeDir) {
      const prefix = scopeDir.endsWith('/') ? scopeDir : `${scopeDir}/`;
      result = result.filter((fp) => fp.startsWith(prefix));
    }
    if (filterType !== 'All') result = result.filter((fp) => getFileType(fp) === filterType);
    return sortFiles(result, sortBy);
  }, [listedFiles, trimmedQuery, filterType, sortBy, scopeDir]);

  const fileTree = useMemo(() => buildFileTree(filteredSortedFiles), [filteredSortedFiles]);

  // The set carries the key it was read for: a workspace switch queues the
  // new set and re-runs the persist below in the same flush, and without the
  // key it would write the old workspace's folders under the new one's.
  const expandKey = `filePanel.expandedDirs.${workspaceId}`;
  const [expanded, setExpanded] = useState<{ key: string; dirs: Set<string> }>(
    () => ({ key: expandKey, dirs: readExpandedDirs(expandKey) }),
  );
  useEffect(() => {
    if (expanded.key === expandKey) return;
    setExpanded({ key: expandKey, dirs: readExpandedDirs(expandKey) });
  }, [expandKey, expanded.key]);
  useEffect(() => {
    if (expanded.key !== expandKey) return;
    try { userLocalStorage.setItem(expandKey, JSON.stringify([...expanded.dirs])); } catch { /* blocked store */ }
  }, [expanded, expandKey]);
  const expandedDirs = expanded.dirs;
  const setExpandedDirs = useCallback((fn: (prev: Set<string>) => Set<string>) => {
    setExpanded((prev) => ({ ...prev, dirs: fn(prev.dirs) }));
  }, []);

  // A search shows every hit, so it opens every directory on the way to one.
  const visibleExpandedDirs = useMemo(() => {
    if (!trimmedQuery) return expandedDirs;
    const all = new Set<string>();
    const walk = (node: TreeNode) => { all.add(node.fullPath); node.children.forEach(walk); };
    fileTree.forEach(walk);
    return all;
  }, [trimmedQuery, expandedDirs, fileTree]);

  const toggleDir = useCallback((dir: string) => {
    setExpandedDirs((prev) => {
      const next = new Set(prev);
      if (next.has(dir)) next.delete(dir); else next.add(dir);
      return next;
    });
  }, [setExpandedDirs]);

  /** Open the tree down to a directory and bring its row into view. */
  const revealDir = useCallback((dir: string) => {
    const parts = dir.split('/').filter(Boolean);
    setExpandedDirs((prev) => {
      const next = new Set(prev);
      parts.forEach((_, i) => next.add(parts.slice(0, i + 1).join('/')));
      return next;
    });
    requestAnimationFrame(() => {
      rootRef.current?.querySelector<HTMLElement>(`[data-row-path="${CSS.escape(dir)}"]`)
        ?.scrollIntoView({ block: 'nearest' });
    });
  }, [rootRef, setExpandedDirs]);

  const clearSearch = useCallback(() => {
    setSearchQuery('');
    setMissedRef(null);
    setExtraMatches([]);
  }, []);

  /** Show a reference's candidates: the tree, filtered to the name it used. */
  const showMatches = useCallback((ref: string, name: string, matches: string[]) => {
    setFilterType('All');
    setSearchQuery(name);
    setExtraMatches(matches);
    setMissedRef(ref);
  }, []);

  // The resolver's extra paths belong to the reference that asked for them, and
  // `missedRef` (the banner that explains why they are listed) is cleared here
  // too: a query the reader typed must not keep unlisted rows the tree cannot
  // otherwise reach.
  const search = useCallback((value: string) => {
    setSearchQuery(value);
    setMissedRef(null);
    setExtraMatches([]);
  }, []);

  useEffect(() => {
    if (!showSortMenu) return;
    // Closing on the mousedown of a pick would unmount the item before its
    // click lands, so only a press outside the wrapper dismisses.
    const handler = (e: MouseEvent) => {
      if (!sortMenuRef.current?.contains(e.target as Node)) setShowSortMenu(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [showSortMenu]);

  return {
    searchQuery, search, clearSearch, showMatches, missedRef, extraMatches,
    filterType, setFilterType, availableTypes,
    sortBy, setSortBy, showSortMenu, setShowSortMenu, sortMenuRef,
    listedFiles, filteredSortedFiles, fileTree,
    expandedDirs: visibleExpandedDirs, toggleDir, revealDir,
  };
}

export type TreeFilter = ReturnType<typeof useTreeFilter>;
