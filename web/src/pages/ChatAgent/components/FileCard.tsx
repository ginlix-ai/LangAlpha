/* eslint-disable react-refresh/only-export-components */
import React from 'react';
import type { LucideIcon } from 'lucide-react';
import { FileText, FileCode, Image, Table, ExternalLink, Folder } from 'lucide-react';
import './FileCard.css';

const EXT_ICONS: Record<string, LucideIcon> = {
  py: FileCode, js: FileCode, jsx: FileCode, ts: FileCode, tsx: FileCode,
  html: FileCode, css: FileCode, sh: FileCode, bash: FileCode, sql: FileCode,
  csv: Table, json: Table, yaml: Table, yml: Table, xml: Table, toml: Table, xlsx: Table, xls: Table,
  png: Image, jpg: Image, jpeg: Image, svg: Image, gif: Image, webp: Image,
};

export const KNOWN_EXTS = new Set([
  'md', 'txt', 'pdf', 'doc', 'docx', 'rtf',
  'py', 'js', 'jsx', 'ts', 'tsx', 'html', 'css', 'sh', 'bash', 'sql', 'r', 'ipynb',
  'csv', 'json', 'yaml', 'yml', 'xml', 'toml', 'ini', 'cfg', 'log', 'env', 'xlsx', 'xls',
  'png', 'jpg', 'jpeg', 'gif', 'svg', 'webp', 'bmp',
  'zip', 'tar', 'gz',
]);

/** Prefix used for cross-workspace file references: __wsref__/{workspaceId}/path */
const WSREF_PREFIX = '__wsref__/';

/**
 * Parse a __wsref__/{workspaceId}/path reference.
 * Returns { workspaceId, path } or null if not a workspace-qualified path.
 *
 * Expects pre-normalized input (no file:// or /home/workspace/ inside the path).
 * Use normalizeFileRefs() at the content level to clean paths before they reach here.
 */
export function parseWsPath(href: string | undefined): { workspaceId: string; path: string } | null {
  if (!href || !href.startsWith(WSREF_PREFIX)) return null;
  const rest = href.slice(WSREF_PREFIX.length);
  const slashIdx = rest.indexOf('/');
  if (slashIdx < 1) return null;
  return {
    workspaceId: rest.slice(0, slashIdx),
    path: rest.slice(slashIdx + 1),
  };
}

function hasKnownExt(path: string): boolean {
  const ext = path.split('.').pop()?.split(/[?#]/)[0]?.toLowerCase();
  return !!ext && KNOWN_EXTS.has(ext);
}

/**
 * Check if an href looks like a sandbox file path (not an external URL).
 *
 * Expects pre-normalized input (normalizeFileRefs already stripped file://
 * and /home/workspace/ prefixes). Only needs to distinguish relative paths
 * and __wsref__/ paths from external URLs.
 */
export function isFilePath(href: string | undefined): boolean {
  if (!href) return false;
  if (href.startsWith(WSREF_PREFIX)) {
    const parsed = parseWsPath(href);
    return !!parsed && hasKnownExt(parsed.path);
  }
  if (/^[a-z][a-z0-9+.-]*:/i.test(href) || href.startsWith('//') || href.startsWith('#')) return false;
  return hasKnownExt(href);
}

/**
 * Normalize a file path for API calls: strip __wsref__ prefix, return relative path.
 *
 * Expects pre-normalized input (no file:// or /home/workspace/ prefixes).
 */
export function normalizeFilePath(path: string): string {
  const ws = parseWsPath(path);
  if (ws) return ws.path;
  return path;
}

const IMAGE_EXTS = new Set(['png', 'jpg', 'jpeg', 'gif', 'svg', 'webp', 'bmp']);

/**
 * Check if an href points to an image file.
 */
export function isImagePath(href: string | undefined): boolean {
  if (!href) return false;
  const ext = href.split('.').pop()?.split(/[?#]/)[0]?.toLowerCase();
  return !!ext && IMAGE_EXTS.has(ext);
}

/**
 * Extract workspace-qualified file paths from message text.
 *
 * Only extracts __wsref__/ paths (backend-qualified, guaranteed real).
 * General bare-text file detection is disabled to avoid false positives
 * from hallucinated/system paths. This catches __wsref__ links even when
 * the agent wraps them in backticks (code spans).
 */
export function extractFilePaths(text: string | undefined): string[] {
  if (!text) return [];
  const paths: string[] = [];
  const seen = new Set<string>();
  const re = /\((__wsref__\/[^)\s]+)\)/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) {
    const p = m[1];
    if (!seen.has(p)) {
      seen.add(p);
      paths.push(p);
    }
  }
  return paths;
}

interface FileCardProps {
  path: string;
  onOpen: () => void;
}

function FileCard({ path, onOpen }: FileCardProps): React.ReactElement {
  // Strip __wsref__/{workspaceId}/ prefix for display
  const wsRef = parseWsPath(path);
  const displayPath = wsRef ? wsRef.path : path;
  const ext = displayPath.split('.').pop()!.toLowerCase();
  const fileName = displayPath.split('/').pop();
  const dirPath = displayPath.split('/').slice(0, -1).join('/');
  const Icon = EXT_ICONS[ext] || FileText;

  return (
    <button className="file-mention-card" onClick={onOpen} title={`Open ${displayPath}`}>
      <Icon className="file-mention-card-icon" />
      <div className="file-mention-card-info">
        <span className="file-mention-card-name">{fileName}</span>
        {dirPath && <span className="file-mention-card-dir">{dirPath}/</span>}
      </div>
      <ExternalLink className="file-mention-card-action" />
    </button>
  );
}

interface DirCardProps {
  dir: string;
  fileCount: number;
  onOpen: () => void;
}

function DirCard({ dir, fileCount, onOpen }: DirCardProps): React.ReactElement {
  return (
    <button className="file-mention-card file-mention-card-dir-card" onClick={onOpen} title={`Open ${dir}/ in file panel`}>
      <Folder className="file-mention-card-icon" />
      <div className="file-mention-card-info">
        <span className="file-mention-card-name">{dir}/</span>
        <span className="file-mention-card-dir">{fileCount} file{fileCount !== 1 ? 's' : ''}</span>
      </div>
      <ExternalLink className="file-mention-card-action" />
    </button>
  );
}

interface FileMentionCardsProps {
  filePaths: string[] | null;
  onOpenFile: (path: string, workspaceId?: string) => void;
  onOpenDir?: (dir: string) => void;
}

/** Open a file card — parses __wsref__ prefix to extract workspace context. */
function openFileCard(path: string, onOpenFile: (path: string, workspaceId?: string) => void) {
  const wsRef = parseWsPath(path);
  if (wsRef) {
    onOpenFile(wsRef.path, wsRef.workspaceId);
  } else {
    onOpenFile(path);
  }
}

/**
 * Renders file mention cards below a message.
 * If <= 5 files: show individual file cards.
 * If > 5 files: group by top-level directory, show dir cards + root file cards.
 */
export function FileMentionCards({ filePaths, onOpenFile, onOpenDir }: FileMentionCardsProps): React.ReactElement | null {
  if (!filePaths || filePaths.length === 0) return null;

  if (filePaths.length <= 5) {
    return (
      <div className="file-mention-cards">
        {filePaths.map((path) => (
          <FileCard key={path} path={path} onOpen={() => openFileCard(path, onOpenFile)} />
        ))}
      </div>
    );
  }

  // Group by top-level directory (use display path for grouping)
  const groups: Record<string, string[]> = {};
  const rootFiles: string[] = [];
  for (const path of filePaths) {
    const wsRef = parseWsPath(path);
    const displayPath = wsRef ? wsRef.path : path;
    const parts = displayPath.split('/');
    if (parts.length > 1) {
      const dir = parts[0];
      if (!groups[dir]) groups[dir] = [];
      groups[dir].push(path);
    } else {
      rootFiles.push(path);
    }
  }

  // Sort directories: results -> data -> rest alphabetical
  const dirPriority: Record<string, number> = { results: 0, data: 1 };
  const sortedDirs = Object.entries(groups).sort(([a], [b]) => {
    const pa = dirPriority[a] ?? 2;
    const pb = dirPriority[b] ?? 2;
    if (pa !== pb) return pa - pb;
    return a.localeCompare(b);
  });

  return (
    <div className="file-mention-cards">
      {rootFiles.map((path) => (
        <FileCard key={path} path={path} onOpen={() => openFileCard(path, onOpenFile)} />
      ))}
      {sortedDirs.map(([dir, files]) => (
        <DirCard
          key={dir}
          dir={dir}
          fileCount={files.length}
          onOpen={() => onOpenDir ? onOpenDir(dir) : openFileCard(files[0], onOpenFile)}
        />
      ))}
    </div>
  );
}

export default FileCard;
