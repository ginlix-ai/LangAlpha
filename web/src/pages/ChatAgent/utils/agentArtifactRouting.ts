import { DRIVE_ROUTE, driveHref, readDriveLocation, type DriveLocation, type ReadLocation } from '@/lib/driveLinks';
import {
  classifyAgentPath,
  DRIVE_PERSONAL,
  folderWorkspacePath,
  parseAgentPath,
  selectedWorkspaceLink,
  type ComputerFolders,
  type DrivePathInfo,
} from './agentPaths';
import type { UnsequencedTarget } from '../components/filePanel/types';

/** The place a Drive path names: the file, or the folder (a space's root is one). */
export function driveLocationOf(info: DrivePathInfo): DriveLocation {
  const space = info.space === DRIVE_PERSONAL ? null : info.space;
  return info.directory || !info.path ? { space, dir: info.path } : { space, file: info.path };
}

/** The Drive page's address for a Drive path. */
export function drivePathHref(info: DrivePathInfo): string {
  return driveHref(driveLocationOf(info));
}

/**
 * The place a Drive page address names (a folder, the memo list, the trash,
 * a file in its view), or null for any other address. A page link the agent
 * wrote opens beside the chat as its path would.
 */
export function driveRouteLocation(route: string): ReadLocation | null {
  let url: URL;
  try {
    url = new URL(route, 'http://drive.invalid');
  } catch {
    return null;
  }
  return url.pathname === DRIVE_ROUTE ? readDriveLocation(url.searchParams) : null;
}

/**
 * What the panel is asked to open, in the shapes its targets take
 * (`UnsequencedTarget`): a file or a folder in the Files tab, an entry in
 * the memory or memo store, or a place in the person's Drive. `''` is a real
 * answer for both a folder (the workspace root) and a memo (the memo list).
 * A click's location and pin are the caller's to add.
 */
export type PanelDestination =
  | { kind: 'file'; path: string }
  | { kind: 'file'; dir: string }
  | Extract<UnsequencedTarget, { kind: 'memory' | 'memo' }>
  | { kind: 'drive'; loc: DriveLocation };

/**
 * Where an agent artifact opens in the panel: in `workspaceId`'s files where
 * the link names a workspace, and the view's own where it names none (a
 * user-scoped store, the person's Drive, a plain path).
 */
export interface ArtifactDestination {
  target: PanelDestination;
  workspaceId: string | null;
}

/**
 * Pure routing decision, so it is testable without mounting the chat shell.
 *
 * A caller-supplied `targetWorkspaceId` wins over a `__wsref__/<wsid>/...`
 * id in the path. A user-scoped artifact (user memory, memos, profile and
 * automation files) names no workspace whatever the link carried, so a panel
 * left on a sibling's files from an earlier click returns to the view's own.
 *
 * A path through another workspace's folder (`folderWorkspacePath`) routes
 * as the `__wsref__` link to that workspace would. It is read as a path, which
 * is what an `OpenFileHandler` is handed, so a `#` inside a name stays.
 */
export function computeAgentArtifactRouting(
  rawPath: string,
  targetWorkspaceId?: string,
  folders?: ComputerFolders | null,
): ArtifactDestination {
  const root = folders?.rootDir;
  const other = targetWorkspaceId ? null : folderWorkspacePath(parseAgentPath(rawPath, root), folders);
  if (other) return computeAgentArtifactRouting(other.path, other.workspaceId);
  const { path: routedPath, parts } = selectedWorkspaceLink(rawPath, folders);
  const info = classifyAgentPath(routedPath, root);
  const named = targetWorkspaceId ?? ('crossWorkspaceId' in info ? info.crossWorkspaceId : undefined) ?? null;
  const panel = (target: PanelDestination, workspaceId: string | null): ArtifactDestination => ({ target, workspaceId });

  switch (info.kind) {
    // The Drive is browsed beside the chat, in a Drive tab, at the folder or
    // file the link names.
    case 'drive':
      return panel({ kind: 'drive', loc: driveLocationOf(info) }, null);
    case 'memory':
      return panel({ kind: 'memory', key: info.key, tier: info.tier }, info.tier === 'workspace' ? named : null);
    case 'memo':
      return panel({ kind: 'memo', key: info.isIndex ? '' : info.key }, null);
    case 'user-data':
      return panel({ kind: 'file', path: routedPath }, null);
    case 'skill':
    case 'file':
      return parts.directory
        ? panel({ kind: 'file', dir: parts.path.replace(/^\/+|\/+$/g, '') }, named)
        : panel({ kind: 'file', path: routedPath }, named);
  }
}
