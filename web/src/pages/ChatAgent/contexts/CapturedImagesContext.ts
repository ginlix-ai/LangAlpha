import { createContext, useContext } from 'react';
import type { ComputerFolders } from '../utils/agentPaths';
import { normalizeFilePath, parseFolderHref, parseWsPath } from '../utils/filePaths';

/**
 * The URL each sandbox image a reply named was captured to, by the path as
 * written. A reply streams with the paths; its run's `run_end` says where each
 * one went, which a reload shows in place of the path. A path names whatever
 * the file holds now, and the agent rewrites charts in place, so only the
 * captured copy is the image the reply was showing.
 */
export const CapturedImagesContext = createContext<Readonly<Record<string, string>> | null>(null);

/**
 * The captured URL of the image `src` names in this reply, if it has one. The
 * map is keyed by the path as the agent wrote it, and `src` is the destination
 * as markdown hands it over, encoded and with its root folded, so the two meet
 * on the file they read as: its workspace as well as its path, since another
 * workspace's `charts/a.png` is a different chart.
 */
export function useCapturedImageUrl(src: string | undefined, folders: ComputerFolders | null | undefined): string | null {
  const captured = useContext(CapturedImagesContext);
  if (!src || !captured) return null;
  if (captured[src]) return captured[src];
  const file = (dest: string) => {
    const other = parseFolderHref(dest, folders);
    return `${parseWsPath(dest)?.workspaceId ?? other?.workspaceId ?? ''}:${other?.path ?? normalizeFilePath(dest, folders)}`;
  };
  const want = file(src);
  const hit = Object.entries(captured).find(([written]) => file(written) === want);
  return hit ? hit[1] : null;
}
