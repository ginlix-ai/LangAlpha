import type { TreeNode } from './types';
import { dirSortKey } from './fileMeta';

/**
 * Builds a recursive file tree from flat file paths.
 * Returns array of top-level TreeNodes sorted by directory priority.
 */
export function buildFileTree(filePaths: string[]): TreeNode[] {
  interface DirEntry {
    files: string[];
    subdirs: Map<string, string>;
  }

  const dirMap = new Map<string, DirEntry>();

  const getOrCreateDir = (fullPath: string): DirEntry => {
    if (!dirMap.has(fullPath)) {
      dirMap.set(fullPath, { files: [], subdirs: new Map() });
    }
    return dirMap.get(fullPath)!;
  };

  // Root is a special case with fullPath = '/'
  getOrCreateDir('/');

  for (const fp of filePaths) {
    const slashIdx = fp.lastIndexOf('/');
    if (slashIdx < 0) {
      // Root-level file
      getOrCreateDir('/').files.push(fp);
    } else {
      const dirPath = fp.slice(0, slashIdx);
      getOrCreateDir(dirPath).files.push(fp);

      // Ensure all ancestor directories exist and link parent -> child
      const segments = dirPath.split('/');
      for (let i = 0; i < segments.length; i++) {
        const parentPath = i === 0 ? '/' : segments.slice(0, i).join('/');
        const childPath = segments.slice(0, i + 1).join('/');
        const childName = segments[i];
        const parent = getOrCreateDir(parentPath);
        if (!parent.subdirs.has(childName)) {
          parent.subdirs.set(childName, childPath);
        }
        getOrCreateDir(childPath);
      }
    }
  }

  // Convert dirMap into recursive TreeNode[] starting from a given path
  const buildNodes = (fullPath: string): { children: TreeNode[]; files: string[] } => {
    const entry = dirMap.get(fullPath);
    if (!entry) return { children: [], files: [] };

    const children = Array.from(entry.subdirs.entries())
      .sort(([a], [b]) => {
        const pa = dirSortKey(a);
        const pb = dirSortKey(b);
        if (pa !== pb) return pa - pb;
        return a.localeCompare(b);
      })
      .map(([name, childFullPath]) => {
        const sub = buildNodes(childFullPath);
        return {
          name,
          fullPath: childFullPath,
          children: sub.children,
          files: sub.files,
        };
      });

    return { children, files: entry.files };
  };

  const root = buildNodes('/');

  // Return top-level: root files become a { name: '/', ... } node, plus top-level dirs
  const result: TreeNode[] = [];
  if (root.files.length > 0) {
    result.push({ name: '/', fullPath: '/', children: [], files: root.files });
  }
  result.push(...root.children);
  return result;
}

/** Collect all file paths recursively under a tree node */
export function collectTreeFiles(node: TreeNode): string[] {
  const result = [...node.files];
  for (const child of node.children) {
    result.push(...collectTreeFiles(child));
  }
  return result;
}
