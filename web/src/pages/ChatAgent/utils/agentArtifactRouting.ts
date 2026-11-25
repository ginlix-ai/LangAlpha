import { driveHref } from '@/lib/driveLinks';
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

/** The Drive page's address for a Drive path: the file, or the folder it names. */
export function drivePathHref(info: DrivePathInfo): string {
  const space = info.space === DRIVE_PERSONAL ? null : info.space;
  return info.directory
    ? driveHref({ space, dir: info.path })
    : driveHref({ space, file: info.path });
}

/**
 * What the panel is asked to open, in the shapes its targets take
 * (`UnsequencedTarget`): a file or a folder in the Files tab, or an entry in
 * the memory or memo store. `''` is a real answer for both a folder (the
 * workspace root) and a memo (the memo list). A click's location and pin are
 * the caller's to add.
 */
export type PanelDestination =
  | { kind: 'file'; path: string }
  | { kind: 'file'; dir: string }
  | Extract<UnsequencedTarget, { kind: 'memory' | 'memo' }>;

/**
 * Where an agent artifact opens: a page outside the chat (the Drive), or the
 * panel, in `workspaceId`'s files where the link names a workspace and the
 * view's own where it names none (a user-scoped store, a plain path).
 */
export type ArtifactDestination =
  | { kind: 'page'; href: string }
  | { kind: 'panel'; target: PanelDestination; workspaceId: string | null };

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
  const panel = (target: PanelDestination, workspaceId: string | null): ArtifactDestination => ({ kind: 'panel', target, workspaceId });

  switch (info.kind) {
    case 'drive':
      return { kind: 'page', href: drivePathHref(info) };
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
