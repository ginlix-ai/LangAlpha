import React, { useState, useEffect, useCallback, useRef, useMemo, Suspense } from 'react';
import type { editor } from 'monaco-editor';
import { ArrowLeft, X, FileText, FileImage, File, RefreshCw, Upload, Folder, ChevronRight, ChevronDown, ArrowUpDown, AlertTriangle, Trash2, CheckSquare, Square, HardDrive, Pencil, TextSelect, FolderOpen, Settings, ScrollText } from 'lucide-react';
import {
  memoMimeForName,
  useAddToMemo,
  useWorkspaceMemoIndex,
  useMemoStaleCheck,
  MemoStaleBanner,
  MemoDiffModal,
} from './FilePanelMemo';
import { useWorkspace } from '@/hooks/useWorkspace';
import { SandboxSettingsContent } from './SandboxSettingsPanel';
import { useIsMobile } from '@/hooks/useIsMobile';
import SyntaxHighlighter, { oneDark, oneLight } from './SyntaxHighlighter';
import { useTranslation } from 'react-i18next';
import { readWorkspaceFile, readWorkspaceFileFull, writeWorkspaceFile, downloadWorkspaceFile, downloadWorkspaceFileAsArrayBuffer, triggerFileDownload, uploadWorkspaceFile, deleteWorkspaceFiles, backupWorkspaceFiles, getBackupStatus } from '../utils/api';
import type { MemoEntry } from '../utils/api';
import { stripLineNumbers } from './toolDisplayConfig';
import Markdown from './Markdown';
import ImageLightbox from './ImageLightbox';
import DocumentErrorBoundary from './viewers/DocumentErrorBoundary';
import FileHeaderActions from './FileHeaderActions';
import './FilePanel.css';
import type { LucideIcon } from 'lucide-react';

const PdfViewer = React.lazy(() => import('./viewers/PdfViewer'));
const ExcelViewer = React.lazy(() => import('./viewers/ExcelViewer'));
const CsvViewer = React.lazy(() => import('./viewers/CsvViewer'));
const HtmlViewer = React.lazy(() => import('./viewers/HtmlViewer'));
const CodeEditor = React.lazy(() => import('./viewers/CodeEditor'));
const ExportPreviewModal = React.lazy(() => import('./ExportPreviewModal'));

// --- Types ---

interface TreeNode {
  name: string;
  fullPath: string;
  children: TreeNode[];
  files: string[];
}

interface SelectionTooltipData {
  x: number;
  y: number;
  text: string;
  lineStart?: number | null;
  lineEnd?: number | null;
}

interface ContextMenuData {
  x: number;
  y: number;
  filePath: string;
}

export interface ContextPayload {
  path?: string;
  snippet?: string;
  label?: string;
  lineStart?: number | null;
  lineEnd?: number | null;
  lineCount?: number;
}

interface EditorTextSelectData {
  text: string;
  startLine: number;
  endLine: number;
  rect: { left: number; top: number; width: number; height: number } | null;
}

interface ApiAdapter {
  readFile?: (path: string) => Promise<{ content: string; mime?: string }>;
  readFileFull?: (path: string) => Promise<{ content: string }>;
  writeFile?: (path: string, content: string) => Promise<unknown>;
  downloadFile?: (path: string) => Promise<string>;
  downloadFileAsArrayBuffer?: (path: string) => Promise<ArrayBuffer>;
  triggerDownload?: (path: string) => Promise<void>;
}

interface BackupResult {
  synced?: number;
  skipped?: number;
  error?: string;
  [key: string]: unknown;
}

interface SortOption {
  value: string;
  label: string;
}

// --- Constants ---

const EXT_TO_LANG: Record<string, string> = {
  py: 'python', js: 'javascript', jsx: 'jsx', ts: 'typescript', tsx: 'tsx',
  json: 'json', html: 'html', css: 'css', sql: 'sql', sh: 'bash', bash: 'bash',
  yaml: 'yaml', yml: 'yaml', xml: 'xml', java: 'java', go: 'go', rs: 'rust', rb: 'ruby',
};

const EDITABLE_EXTENSIONS = new Set([
  ...Object.keys(EXT_TO_LANG),
  'md', 'txt', 'csv', 'env', 'toml', 'cfg', 'ini', 'log',
]);

function getFileIcon(fileName: string): LucideIcon {
  const ext = fileName.split('.').pop()?.toLowerCase();
  if (['md', 'txt', 'csv', 'json', 'py', 'js', 'html'].includes(ext!)) return FileText;
  if (['png', 'jpg', 'jpeg', 'gif', 'svg', 'webp'].includes(ext!)) return FileImage;
  return File;
}

function getFileExtension(fileName: string): string {
  return fileName.split('.').pop()?.toLowerCase() || '';
}

// Map extensions to human-readable type categories
const EXT_TO_TYPE: Record<string, string> = {
  md: 'Docs', txt: 'Docs', pdf: 'Docs',
  py: 'Code', js: 'Code', jsx: 'Code', ts: 'Code', tsx: 'Code',
  html: 'Code', css: 'Code', sql: 'Code', sh: 'Code', bash: 'Code',
  java: 'Code', go: 'Code', rs: 'Code', rb: 'Code',
  json: 'Data', csv: 'Data', yaml: 'Data', yml: 'Data', xml: 'Data',
  xlsx: 'Data', xls: 'Data',
  png: 'Image', jpg: 'Image', jpeg: 'Image', gif: 'Image', svg: 'Image', webp: 'Image',
};

function getFileType(filePath: string): string {
  const ext = getFileExtension(filePath.split('/').pop() || '');
  return EXT_TO_TYPE[ext] || 'Other';
}

/** Derive available type categories from current file list */
function getAvailableTypes(filePaths: string[]): string[] {
  const types = new Set<string>();
  for (const fp of filePaths) types.add(getFileType(fp));
  // Fixed display order, filtered to only those present
  return ['Docs', 'Code', 'Data', 'Image', 'Other'].filter((t) => types.has(t));
}

const SORT_OPTIONS: SortOption[] = [
  { value: 'name-asc', label: 'Name A-Z' },
  { value: 'name-desc', label: 'Name Z-A' },
  { value: 'type', label: 'Type' },
];

function sortFiles(filePaths: string[], sortBy: string): string[] {
  const sorted = [...filePaths];
  switch (sortBy) {
    case 'name-asc':
      return sorted.sort((a, b) => {
        const na = a.split('/').pop()!.toLowerCase();
        const nb = b.split('/').pop()!.toLowerCase();
        return na.localeCompare(nb);
      });
    case 'name-desc':
      return sorted.sort((a, b) => {
        const na = a.split('/').pop()!.toLowerCase();
        const nb = b.split('/').pop()!.toLowerCase();
        return nb.localeCompare(na);
      });
    case 'type':
      return sorted.sort((a, b) => {
        const ea = getFileExtension(a.split('/').pop() || '');
        const eb = getFileExtension(b.split('/').pop() || '');
        if (ea !== eb) return ea.localeCompare(eb);
        return a.split('/').pop()!.toLowerCase().localeCompare(b.split('/').pop()!.toLowerCase());
      });
    default:
      return sorted;
  }
}

/** Directory display priority: root first, then results/, data/, rest alphabetical */
const DIR_PRIORITY: Record<string, number> = { '/': 0, 'results': 1, 'data': 2 };

/** System directory prefixes -- collapsed by default when visible.
 *  Source of truth: src/ptc_agent/core/paths.py -> AGENT_SYSTEM_DIRS */
// eslint-disable-next-line react-refresh/only-export-components
export const SYSTEM_DIR_PREFIXES = ['.system', 'code', 'tools', 'mcp_servers', '.agents', '.self-improve'];

function dirSortKey(dir: string): number {
  if (DIR_PRIORITY[dir] != null) return DIR_PRIORITY[dir];
  if (SYSTEM_DIR_PREFIXES.includes(dir)) return 99;
  return 3;
}

/**
 * Builds a recursive file tree from flat file paths.
 * Returns array of top-level TreeNodes sorted by directory priority.
 */
function buildFileTree(filePaths: string[]): TreeNode[] {
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
function collectTreeFiles(node: TreeNode): string[] {
  const result = [...node.files];
  for (const child of node.children) {
    result.push(...collectTreeFiles(child));
  }
  return result;
}

// --- IndentGuides ---

interface IndentGuidesProps {
  depth: number;
}

/** Renders vertical indent guide lines for a given depth */
function IndentGuides({ depth }: IndentGuidesProps): React.ReactElement | null {
  if (depth <= 0) return null;
  const guides: React.ReactElement[] = [];
  for (let i = 0; i < depth; i++) {
    guides.push(
      <span
        key={i}
        className="file-tree-indent-guide"
        style={{ left: i * 16 + 20 }}
      />
    );
  }
  return <>{guides}</>;
}

// --- DirectoryNode ---

interface DirectoryNodeProps {
  node: TreeNode;
  depth: number;
  showHeader: boolean;
  expandedDirs: Set<string>;
  toggleDir: (dir: string) => void;
  selectMode: boolean;
  selectedPaths: Set<string>;
  toggleSelect: (path: string) => void;
  toggleDirSelect: (dirFiles: string[]) => void;
  handleFileClick: (filePath: string) => void;
  readOnly: boolean;
  backedUpSet: Set<string>;
  modifiedSet: Set<string>;
  memoedMap: Map<string, MemoEntry>;
  memoedTitle: string;
  onAddContext: ((ctx: ContextPayload) => void) | null;
  setContextMenu: (menu: ContextMenuData | null) => void;
  activeContextPath: string | null;
}

/** Recursive directory node renderer for the file tree */
function DirectoryNode({
  node, depth, showHeader,
  expandedDirs, toggleDir,
  selectMode, selectedPaths, toggleSelect, toggleDirSelect,
  handleFileClick, readOnly, backedUpSet, modifiedSet, memoedMap, memoedTitle,
  onAddContext, setContextMenu, activeContextPath,
}: DirectoryNodeProps): React.ReactElement {
  const isRoot = node.name === '/';
  const isCollapsed = isRoot ? false : !expandedDirs.has(node.fullPath);
  const allFiles = collectTreeFiles(node);
  const totalCount = allFiles.length;
  const indent = (depth + 1) * 16 + 8; // base 8px + 16px per depth level

  return (
    <div key={node.fullPath}>
      {showHeader && (
        <div
          className="file-panel-dir-header file-tree-row"
          style={depth > 0 ? { paddingLeft: depth * 16 + 8 } : undefined}
          onClick={() => selectMode ? toggleDirSelect(allFiles) : toggleDir(node.fullPath)}
        >
          <IndentGuides depth={depth} />
          {selectMode ? (
            allFiles.every((f) => selectedPaths.has(f))
              ? <CheckSquare className="h-3.5 w-3.5 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
              : <Square className="h-3.5 w-3.5 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
          ) : isCollapsed
            ? <ChevronRight className="h-3.5 w-3.5 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
            : <ChevronDown className="h-3.5 w-3.5 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
          }
          <Folder className="h-3.5 w-3.5 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
          <span className="text-xs font-medium truncate" style={{ color: 'var(--color-text-tertiary)' }}>
            {isRoot ? '/' : `${node.name}/`}
          </span>
          <span className="text-xs" style={{ color: 'var(--color-icon-muted)' }}>
            {totalCount}
          </span>
        </div>
      )}
      {(!isCollapsed || selectMode) && (
        <>
          {/* Subdirectories */}
          {node.children.map((child) => (
            <DirectoryNode
              key={child.fullPath}
              node={child}
              depth={showHeader ? depth + 1 : depth}
              showHeader={true}
              expandedDirs={expandedDirs}
              toggleDir={toggleDir}
              selectMode={selectMode}
              selectedPaths={selectedPaths}
              toggleSelect={toggleSelect}
              toggleDirSelect={toggleDirSelect}
              handleFileClick={handleFileClick}
              readOnly={readOnly}
              backedUpSet={backedUpSet}
              modifiedSet={modifiedSet}
              memoedMap={memoedMap}
              memoedTitle={memoedTitle}
              onAddContext={onAddContext}
              setContextMenu={setContextMenu}
              activeContextPath={activeContextPath}
            />
          ))}
          {/* Files in this directory */}
          {node.files.map((filePath) => {
            const name = filePath.split('/').pop()!;
            const Icon = getFileIcon(name);
            const isSelected = selectedPaths.has(filePath);
            const fileDepth = showHeader ? depth + 1 : depth;
            return (
              <div
                key={filePath}
                className={`file-panel-item file-tree-row ${selectMode && isSelected ? 'file-panel-item-selected' : ''} ${activeContextPath === filePath ? 'file-panel-item-context-active' : ''}`}
                style={{ paddingLeft: showHeader ? indent : undefined }}
                onClick={() => selectMode ? toggleSelect(filePath) : handleFileClick(filePath)}
                onContextMenu={!selectMode && onAddContext ? (e: React.MouseEvent) => {
                  e.preventDefault();
                  setContextMenu({ x: e.clientX, y: e.clientY, filePath });
                } : undefined}
              >
                <IndentGuides depth={fileDepth} />
                {selectMode ? (
                  isSelected
                    ? <CheckSquare className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
                    : <Square className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                ) : (
                  <Icon className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                )}
                <span className="text-sm truncate" style={{ color: 'var(--color-text-primary)' }}>{name}</span>
                {!selectMode && (memoedMap.has(filePath) || (!readOnly && (backedUpSet.has(filePath) || modifiedSet.has(filePath)))) && (
                  <span className="file-panel-row-status">
                    {memoedMap.has(filePath) && (
                      <span className="file-panel-memo-badge" title={memoedTitle}>
                        <ScrollText
                          className="h-3.5 w-3.5"
                          style={{ color: 'var(--color-text-tertiary)', opacity: 0.85 }}
                        />
                      </span>
                    )}
                    {!readOnly && (backedUpSet.has(filePath) || modifiedSet.has(filePath)) && (
                      <span
                        className={`file-panel-backup-dot ${backedUpSet.has(filePath) ? 'backed-up' : 'modified'}`}
                        title={backedUpSet.has(filePath) ? 'Backed up' : 'Modified since last backup'}
                      />
                    )}
                  </span>
                )}
              </div>
            );
          })}
        </>
      )}
    </div>
  );
}

function DocumentLoadingFallback(): React.ReactElement {
  return (
    <div className="flex items-center justify-center py-12">
      <RefreshCw className="h-5 w-5 animate-spin" style={{ color: 'var(--color-text-tertiary)' }} />
    </div>
  );
}

interface DocumentErrorFallbackProps {
  onDownload: () => void;
}

function DocumentErrorFallback({ onDownload }: DocumentErrorFallbackProps): React.ReactElement {
  return (
    <div className="flex flex-col items-center justify-center gap-3 py-12">
      <AlertTriangle className="h-6 w-6" style={{ color: 'var(--color-text-tertiary)' }} />
      <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>Unable to preview this file</p>
      <button
        className="text-xs px-3 py-1.5 rounded"
        style={{ background: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)', border: '1px solid var(--color-accent-overlay)' }}
        onClick={onDownload}
      >
        Download instead
      </button>
    </div>
  );
}

// --- File error categorization ---

type FileErrorCategory =
  | 'not_found'
  | 'not_backed_up'
  | 'sandbox_starting'
  | 'sandbox_unavailable'
  | 'binary_file'
  | 'no_sandbox'
  | 'access_denied'
  | 'unknown';

interface FileError {
  category: FileErrorCategory;
  detail?: string;
}

const ERROR_I18N_KEY: Record<FileErrorCategory, string> = {
  not_found: 'notFound',
  not_backed_up: 'notBackedUp',
  sandbox_starting: 'sandboxStarting',
  sandbox_unavailable: 'sandboxUnavailable',
  binary_file: 'binaryFile',
  no_sandbox: 'noSandbox',
  access_denied: 'accessDenied',
  unknown: 'unknown',
};

export function categorizeFileError(err: unknown, wsStatus?: string): FileError {
  const response = (err as any)?.response;
  const status: number | undefined = response?.status;
  const raw = response?.data?.detail;
  const detail: string = typeof raw === 'string' ? raw : '';

  switch (status) {
    case 404:
      if (wsStatus === 'stopped' || wsStatus === 'stopping') {
        return { category: 'not_backed_up', detail };
      }
      return { category: 'not_found', detail };
    case 415:
      return { category: 'binary_file', detail };
    case 503:
      if (detail.toLowerCase().includes('starting')) {
        return { category: 'sandbox_starting', detail };
      }
      return { category: 'sandbox_unavailable', detail };
    case 400:
      if (detail.toLowerCase().includes('flash')) {
        return { category: 'no_sandbox', detail };
      }
      return { category: 'unknown', detail };
    case 403:
      return { category: 'access_denied', detail };
    default:
      return { category: 'unknown', detail };
  }
}

interface FileErrorDisplayProps {
  error: FileError;
  onRetry?: () => void;
  onDownload?: () => void;
}

export function FileErrorDisplay({ error, onRetry, onDownload }: FileErrorDisplayProps): React.ReactElement {
  const { t } = useTranslation();
  const key = ERROR_I18N_KEY[error.category];

  const showRetry = error.category === 'sandbox_starting' || error.category === 'sandbox_unavailable' || error.category === 'unknown';
  const showDownload = error.category === 'binary_file';

  return (
    <div className="flex flex-col items-center justify-center gap-3 py-12">
      <AlertTriangle className="h-6 w-6" style={{ color: 'var(--color-text-tertiary)' }} />
      <p className="text-sm font-medium" style={{ color: 'var(--color-text-secondary)' }}>
        {t(`filePanel.error.${key}`)}
      </p>
      <p className="text-xs text-center max-w-xs" style={{ color: 'var(--color-text-tertiary)' }}>
        {t(`filePanel.error.${key}Hint`)}
      </p>
      <div className="flex gap-2 mt-1">
        {showRetry && onRetry && (
          <button
            className="text-xs px-3 py-1.5 rounded"
            style={{ background: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)', border: '1px solid var(--color-accent-overlay)' }}
            onClick={onRetry}
          >
            {t('filePanel.error.retry')}
          </button>
        )}
        {showDownload && onDownload && (
          <button
            className="text-xs px-3 py-1.5 rounded"
            style={{ background: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)', border: '1px solid var(--color-accent-overlay)' }}
            onClick={onDownload}
          >
            {t('filePanel.error.download')}
          </button>
        )}
      </div>
    </div>
  );
}

// --- FilePanel ---

interface FilePanelProps {
  workspaceId: string;
  onClose: () => void;
  targetFile?: string | null;
  onTargetFileHandled?: () => void;
  targetDirectory?: string | null;
  onTargetDirHandled?: () => void;
  files?: string[];
  filesLoading?: boolean;
  filesError?: string | null;
  onRefreshFiles?: () => void;
  readOnly?: boolean;
  /** Lock to a single file — back button closes the panel instead of returning to the file tree. */
  singleFileMode?: boolean;
  apiAdapter?: ApiAdapter | null;
  onAddContext?: ((ctx: ContextPayload) => void) | null;
  showSystemFiles?: boolean;
  onToggleSystemFiles?: (() => void) | null;
  /** Hide the panel-close affordances (the mobile back arrow and the trailing X)
   * when FilePanel is embedded inside a tabbed wrapper that owns the close button. */
  hideClose?: boolean;
  onSwitchToMemoTab?: (() => void) | null;
}

function FilePanel({
  workspaceId,
  onClose,
  targetFile,
  onTargetFileHandled,
  targetDirectory,
  onTargetDirHandled,
  // Shared file list from useWorkspaceFiles hook
  files = [],
  filesLoading = false,
  filesError = null,
  onRefreshFiles,
  readOnly = false,
  singleFileMode = false,
  apiAdapter = null,
  onAddContext = null,
  showSystemFiles = false,
  onToggleSystemFiles = null,
  hideClose = false,
  onSwitchToMemoTab = null,
}: FilePanelProps): React.ReactElement {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  // Resolve API functions -- use adapter overrides if provided, otherwise fall back to authenticated imports
  const readFileFn = apiAdapter?.readFile
    ? (_: string, path: string) => apiAdapter.readFile!(path)
    : readWorkspaceFile;
  const downloadFileFn = apiAdapter?.downloadFile
    ? (_: string, path: string) => apiAdapter.downloadFile!(path)
    : downloadWorkspaceFile;
  const downloadFileAsArrayBufferFn = apiAdapter?.downloadFileAsArrayBuffer
    ? (_: string, path: string) => apiAdapter.downloadFileAsArrayBuffer!(path)
    : downloadWorkspaceFileAsArrayBuffer;
  const triggerDownloadFn = apiAdapter?.triggerDownload
    ? (_: string, path: string) => apiAdapter.triggerDownload!(path)
    : triggerFileDownload;
  const writeFileFn = apiAdapter?.writeFile
    ? (_: string, path: string, content: string) => apiAdapter.writeFile!(path, content)
    : writeWorkspaceFile;
  const readFileFullFn = apiAdapter?.readFileFull
    ? (_: string, path: string) => apiAdapter.readFileFull!(path)
    : readWorkspaceFileFull;

  // Workspace settings inline view
  const [showSettings, setShowSettings] = useState(false);
  const { data: wsData } = useWorkspace(workspaceId);
  const isFlashWorkspace = wsData?.status === 'flash';
  const workspaceName = wsData?.name;

  // File detail view state
  const [selectedFile, setSelectedFile] = useState<string | null>(null);
  const [fileContent, setFileContent] = useState<string | null>(null);
  const [fileArrayBuffer, setFileArrayBuffer] = useState<ArrayBuffer | null>(null);
  const [fileMime, setFileMime] = useState<string | null>(null);
  const [imageLightboxOpen, setImageLightboxOpen] = useState(false);
  const [fileLoading, setFileLoading] = useState(false);
  const [fileError, setFileError] = useState<FileError | null>(null);

  // Upload state
  const [uploadProgress, setUploadProgress] = useState<number | null>(null);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Edit mode state
  const [isEditing, setIsEditing] = useState(false);
  const [editContent, setEditContent] = useState<string | null>(null);
  const [isSaving, setIsSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [showDiff, setShowDiff] = useState(false);
  const [originalContent, setOriginalContent] = useState<string | null>(null);
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null);
  const [canUndo, setCanUndo] = useState(false);
  const [canRedo, setCanRedo] = useState(false);

  const handleUndoRedoChange = useCallback(({ canUndo: u, canRedo: r }: { canUndo: boolean; canRedo: boolean }) => {
    setCanUndo(u);
    setCanRedo(r);
  }, []);

  // Selection tooltip state ("Add to context")
  const [selectionTooltip, setSelectionTooltip] = useState<SelectionTooltipData | null>(null);
  const contentWrapperRef = useRef<HTMLDivElement>(null);

  // Right-click context menu state
  const [contextMenu, setContextMenu] = useState<ContextMenuData | null>(null);

  // Close context menu on outside click
  useEffect(() => {
    if (!contextMenu) return;
    const handler = () => setContextMenu(null);
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [contextMenu]);

  // Clear selection tooltip when navigating away from a file
  useEffect(() => {
    setSelectionTooltip(null);
  }, [selectedFile]);

  // Clear selection tooltip on mousedown if selection is empty
  useEffect(() => {
    if (!selectionTooltip) return;
    const handler = () => {
      setTimeout(() => {
        const sel = window.getSelection();
        if (!sel || !sel.toString().trim()) setSelectionTooltip(null);
      }, 10);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [selectionTooltip]);

  // Handle text selection in read-only views (Markdown, SyntaxHighlighter, etc.)
  const handleContentMouseUp = useCallback(() => {
    if (!onAddContext || !selectedFile) return;
    // Small delay to let the browser finalize the selection
    setTimeout(() => {
      const sel = window.getSelection();
      if (!sel || !sel.toString().trim()) {
        setSelectionTooltip(null);
        return;
      }
      const text = sel.toString();
      const range = sel.getRangeAt(0);
      const rect = range.getBoundingClientRect();
      const wrapper = contentWrapperRef.current;
      const wrapperRect = wrapper?.getBoundingClientRect();
      if (!wrapperRect) return;

      // Account for scroll position
      const scrollTop = wrapper!.scrollTop || 0;
      const scrollLeft = wrapper!.scrollLeft || 0;

      // Determine accurate SOURCE line numbers (matching what the agent sees)
      let lineStart: number | undefined | null;
      let lineEnd: number | undefined | null;

      const startNode = range.startContainer.nodeType === 3
        ? range.startContainer.parentElement
        : range.startContainer as Element;
      const endNode = range.endContainer.nodeType === 3
        ? range.endContainer.parentElement
        : range.endContainer as Element;

      // Method 1: data-line attributes from SyntaxHighlighter
      const startLineEl = startNode?.closest?.('[data-line]');
      const endLineEl = endNode?.closest?.('[data-line]');
      if (startLineEl && endLineEl) {
        lineStart = parseInt((startLineEl as HTMLElement).dataset.line!, 10);
        lineEnd = parseInt((endLineEl as HTMLElement).dataset.line!, 10);
      }

      // Method 2: Source-text matching
      if (lineStart == null && fileContent && typeof fileContent === 'string') {
        const selectedLines = text.split('\n').filter((l: string) => l.trim());
        const firstLine = (selectedLines[0] || '').trim();
        const lastLine = selectedLines.length > 1 ? (selectedLines[selectedLines.length - 1] || '').trim() : firstLine;

        const getSearchWords = (line: string) => line.split(/\s+/).filter((w: string) => w.replace(/[^a-zA-Z0-9]/g, '').length > 2);

        const findLineInSource = (searchLine: string, fromLine = 0): number | null => {
          const words = getSearchWords(searchLine);
          if (words.length < 2) {
            const fragment = searchLine.substring(0, Math.min(searchLine.length, 40));
            if (fragment.length >= 5) {
              const sourceLines = fileContent!.split('\n');
              for (let i = fromLine; i < sourceLines.length; i++) {
                if (sourceLines[i].includes(fragment)) return i + 1;
              }
            }
            return null;
          }
          try {
            const pattern = words.slice(0, 8).map((w: string) =>
              w.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
            ).join('[\\s\\S]{0,30}');
            const regex = new RegExp(pattern);
            const sourceLines = fileContent!.split('\n');
            for (let i = fromLine; i < sourceLines.length; i++) {
              if (regex.test(sourceLines[i])) return i + 1;
            }
          } catch { /* regex failed */ }
          return null;
        };

        lineStart = findLineInSource(firstLine);
        if (lineStart != null) {
          if (firstLine !== lastLine) {
            lineEnd = findLineInSource(lastLine, lineStart - 1);
          }
          if (lineEnd == null) lineEnd = lineStart + (text.match(/\n/g) || []).length;
        }
      }

      // Method 3: DOM range counting fallback
      if (lineStart == null) {
        try {
          const contentRoot = startNode?.closest?.('pre') || startNode?.closest?.('.p-4');
          if (contentRoot && !startNode?.closest?.('.markdown-print-content')) {
            const preRange = document.createRange();
            preRange.selectNodeContents(contentRoot);
            preRange.setEnd(range.startContainer, range.startOffset);
            const fragment = preRange.cloneContents();
            fragment.querySelectorAll('[style*="user-select"]').forEach((el) => el.remove());
            const textBefore = fragment.textContent;
            lineStart = (textBefore!.match(/\n/g) || []).length + 1;
            lineEnd = lineStart + (text.match(/\n/g) || []).length;
          }
        } catch {
          // Range operations can throw in edge cases
        }
      }

      setSelectionTooltip({
        x: rect.left - wrapperRect.left + scrollLeft + rect.width / 2,
        y: rect.top - wrapperRect.top + scrollTop - 8,
        text,
        lineStart,
        lineEnd,
      });
    }, 10);
  }, [onAddContext, selectedFile, fileContent]);

  // Handle text selection from Monaco editor (CodeEditor)
  const handleEditorTextSelect = useCallback((selData: EditorTextSelectData | null) => {
    if (!onAddContext || !selectedFile) return;
    if (!selData) {
      setSelectionTooltip(null);
      return;
    }
    const wrapper = contentWrapperRef.current;
    const wrapperRect = wrapper?.getBoundingClientRect();
    let x = 120, y = 8;
    if (selData.rect && wrapperRect) {
      x = selData.rect.left - wrapperRect.left + 50;
      y = selData.rect.top - wrapperRect.top - 8;
    }
    setSelectionTooltip({
      x, y,
      text: selData.text,
      lineStart: selData.startLine,
      lineEnd: selData.endLine,
    });
  }, [onAddContext, selectedFile]);

  const handleAddSelectionContext = useCallback(() => {
    if (!selectionTooltip || !selectedFile || !onAddContext) return;
    const { text, lineStart, lineEnd } = selectionTooltip;
    const fileName = selectedFile.split('/').pop()!;
    const lineCount = lineStart != null && lineEnd != null ? lineEnd - lineStart + 1 : (text.match(/\n/g) || []).length + 1;
    const label = lineStart != null
      ? (lineStart === lineEnd ? `${fileName}:L${lineStart}` : `${fileName}:L${lineStart}-${lineEnd}`)
      : fileName;
    onAddContext({ path: selectedFile, snippet: text, label, lineStart, lineEnd, lineCount });
    setSelectionTooltip(null);
    window.getSelection()?.removeAllRanges();
  }, [selectionTooltip, selectedFile, onAddContext]);

  const handleAddToMemo = useAddToMemo({
    workspaceId,
    downloadFileAsArrayBufferFn,
    readFileFullFn,
    onSwitchToMemoTab,
  });

  const handleContextMenuAction = useCallback((action: string, filePath: string) => {
    setContextMenu(null);
    if (action === 'add-context' && onAddContext) {
      onAddContext({ path: filePath });
    } else if (action === 'add-to-memo') {
      handleAddToMemo(filePath);
    } else if (action === 'open') {
      handleFileClick(filePath);
    }
  }, [onAddContext, handleAddToMemo]); // eslint-disable-line react-hooks/exhaustive-deps

  // Export modal state
  const [exportModalOpen, setExportModalOpen] = useState(false);

  // Filter and sort state
  const [filterType, setFilterType] = useState('All');
  const [sortBy, setSortBy] = useState('name-asc');
  const [showSortMenu, setShowSortMenu] = useState(false);
  const sortMenuRef = useRef<HTMLDivElement>(null);

  // Selection / delete state
  const [selectMode, setSelectMode] = useState(false);
  const [selectedPaths, setSelectedPaths] = useState<Set<string>>(new Set());
  const [deleteLoading, setDeleteLoading] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [deleteConfirm, setDeleteConfirm] = useState(false);

  // Memo'd lookup + stale-check verdict — see FilePanelMemo.tsx.
  const memoedMap = useWorkspaceMemoIndex(workspaceId);
  const memoedTitle = t('context.inMemo');
  const memoEntryForSelected = selectedFile ? memoedMap.get(selectedFile) ?? null : null;
  const {
    status: memoStaleStatus,
    sandboxText: memoStaleSandboxText,
    refresh: refreshMemoStale,
  } = useMemoStaleCheck({
    workspaceId,
    selectedFile,
    fileMime,
    memoSha256: memoEntryForSelected?.sha256 ?? null,
    readFileFullFn,
  });
  const [memoSyncing, setMemoSyncing] = useState(false);
  const [memoDiffOpen, setMemoDiffOpen] = useState(false);

  const handleSyncMemo = useCallback(async () => {
    if (!selectedFile || memoSyncing) return;
    setMemoSyncing(true);
    try {
      await handleAddToMemo(selectedFile);
      // Re-run the stale check even if memoListData is still revalidating.
      refreshMemoStale();
    } finally {
      setMemoSyncing(false);
    }
  }, [selectedFile, memoSyncing, handleAddToMemo, refreshMemoStale]);

  const handleViewMemoDiff = useCallback(() => {
    setMemoDiffOpen(true);
  }, []);

  // Backup state
  const [backedUpSet, setBackedUpSet] = useState<Set<string>>(new Set());
  const [modifiedSet, setModifiedSet] = useState<Set<string>>(new Set());
  const [backingUp, setBackingUp] = useState(false);
  const [backupResult, setBackupResult] = useState<BackupResult | null>(null);

  const updateBackupStatus = useCallback((data: { backed_up?: string[]; modified?: string[] }) => {
    setBackedUpSet(new Set(data.backed_up || []));
    setModifiedSet(new Set(data.modified || []));
  }, []);

  // Fetch backup status on mount and when files change (skip in readOnly mode)
  useEffect(() => {
    if (!workspaceId || readOnly) return;
    getBackupStatus(workspaceId)
      .then(updateBackupStatus)
      .catch(() => {});
  }, [workspaceId, files, updateBackupStatus, readOnly]);

  const handleBackup = useCallback(async () => {
    if (!workspaceId || backingUp) return;
    setBackingUp(true);
    setBackupResult(null);
    try {
      const result = await backupWorkspaceFiles(workspaceId);
      setBackupResult(result as BackupResult);
      const status = await getBackupStatus(workspaceId);
      updateBackupStatus(status);
      setTimeout(() => setBackupResult(null), 3000);
    } catch (err: unknown) {
      const e = err as { response?: { data?: { detail?: string } }; message?: string };
      const msg = e?.response?.data?.detail || e?.message || 'Backup failed';
      setBackupResult({ error: msg });
      setTimeout(() => setBackupResult(null), 4000);
    } finally {
      setBackingUp(false);
    }
  }, [workspaceId, backingUp, updateBackupStatus]);

  const availableTypes = useMemo(() => getAvailableTypes(files), [files]);

  // Apply directory filter, type filter, sort, then group
  const filteredSortedFiles = useMemo(() => {
    let result = files;
    if (targetDirectory) {
      const prefix = targetDirectory.endsWith('/') ? targetDirectory : targetDirectory + '/';
      result = result.filter((fp) => fp.startsWith(prefix));
    }
    if (filterType !== 'All') {
      result = result.filter((fp) => getFileType(fp) === filterType);
    }
    return sortFiles(result, sortBy);
  }, [files, filterType, sortBy, targetDirectory]);

  // Directory expand state
  const storageKey = `filePanel.expandedDirs.${workspaceId}`;
  const [expandedDirs, setExpandedDirs] = useState<Set<string>>(() => {
    try {
      const saved = localStorage.getItem(storageKey);
      return saved ? new Set(JSON.parse(saved) as string[]) : new Set();
    } catch { return new Set(); }
  });
  const fileTree = useMemo(() => buildFileTree(filteredSortedFiles), [filteredSortedFiles]);

  useEffect(() => {
    localStorage.setItem(storageKey, JSON.stringify([...expandedDirs]));
  }, [expandedDirs, storageKey]);

  const toggleDir = useCallback((dir: string) => {
    setExpandedDirs((prev) => {
      const next = new Set(prev);
      if (next.has(dir)) next.delete(dir);
      else next.add(dir);
      return next;
    });
  }, []);

  const toggleSelect = useCallback((path: string) => {
    setSelectedPaths((prev) => {
      const next = new Set(prev);
      next.has(path) ? next.delete(path) : next.add(path);
      return next;
    });
  }, []);

  const toggleSelectAll = useCallback(() => {
    setSelectedPaths((prev) => {
      if (prev.size === filteredSortedFiles.length) return new Set<string>();
      return new Set(filteredSortedFiles);
    });
  }, [filteredSortedFiles]);

  const toggleDirSelect = useCallback((dirFiles: string[]) => {
    setSelectedPaths((prev) => {
      const next = new Set(prev);
      const allSelected = dirFiles.every((f) => next.has(f));
      dirFiles.forEach((f) => (allSelected ? next.delete(f) : next.add(f)));
      return next;
    });
  }, []);

  const exitSelectMode = useCallback(() => {
    setSelectMode(false);
    setSelectedPaths(new Set());
    setDeleteError(null);
    setDeleteConfirm(false);
  }, []);

  const handleDelete = useCallback(() => {
    if (selectedPaths.size === 0) return;
    if (!deleteConfirm) {
      setDeleteConfirm(true);
      return;
    }
    const paths = Array.from(selectedPaths);
    exitSelectMode();
    setDeleteLoading(true);
    setDeleteError(null);
    deleteWorkspaceFiles(workspaceId, paths)
      .then((result: { errors?: unknown[] }) => {
        if (result.errors?.length && result.errors.length > 0) {
          setDeleteError(t('filePanel.deletePartialFail', { count: result.errors.length }));
        }
      })
      .catch((err: unknown) => {
        const e = err as { response?: { data?: { detail?: string } }; message?: string };
        setDeleteError(e?.response?.data?.detail || e?.message || t('filePanel.deleteFailed'));
      })
      .finally(() => {
        setDeleteLoading(false);
        onRefreshFiles?.();
      });
  }, [selectedPaths, workspaceId, deleteConfirm, exitSelectMode, onRefreshFiles, t]);

  useEffect(() => {
    if (!deleteConfirm) return;
    const timer = setTimeout(() => setDeleteConfirm(false), 4000);
    return () => clearTimeout(timer);
  }, [deleteConfirm]);

  useEffect(() => { exitSelectMode(); }, [targetDirectory]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (!showSortMenu) return;
    const handler = (e: MouseEvent) => {
      if (sortMenuRef.current && !sortMenuRef.current.contains(e.target as Node)) {
        setShowSortMenu(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [showSortMenu]);

  // Drag-and-drop state
  const [isDragOver, setIsDragOver] = useState(false);
  const dragCounterRef = useRef(0);

  useEffect(() => {
    return () => {
      if (fileMime === 'image' && fileContent) {
        URL.revokeObjectURL(fileContent);
      }
    };
  }, [fileContent, fileMime]);

  useEffect(() => {
    if (targetFile) {
      handleFileClick(targetFile);
      onTargetFileHandled?.();
    }
  }, [targetFile]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleFileClick = async (filePath: string) => {
    const ext = getFileExtension(filePath);
    setFileError(null);

    // Binary files
    if (['pdf', 'png', 'jpg', 'jpeg', 'gif', 'svg', 'webp', 'xlsx', 'xls', 'docx', 'zip'].includes(ext)) {
      if (['png', 'jpg', 'jpeg', 'gif', 'svg', 'webp'].includes(ext)) {
        if (fileMime === 'image' && fileContent) {
          URL.revokeObjectURL(fileContent);
        }
        setSelectedFile(filePath);
        setFileLoading(true);
        setFileMime('image');
        try {
          const blobUrl = await downloadFileFn(workspaceId, filePath);
          setFileContent(blobUrl);
        } catch (err) {
          console.error('[FilePanel] Failed to download image:', err);
          setFileError(categorizeFileError(err, wsData?.status));
          setFileContent(null);
          setFileMime(null);
        } finally {
          setFileLoading(false);
        }
        return;
      }
      if (ext === 'pdf') {
        setSelectedFile(filePath);
        setFileLoading(true);
        setFileMime('pdf');
        try {
          const buf = await downloadFileAsArrayBufferFn(workspaceId, filePath);
          setFileArrayBuffer(buf);
        } catch (err) {
          console.error('[FilePanel] Failed to load PDF:', err);
          setFileError(categorizeFileError(err, wsData?.status));
          setFileMime(null);
        } finally {
          setFileLoading(false);
        }
        return;
      }
      if (ext === 'xlsx' || ext === 'xls') {
        setSelectedFile(filePath);
        setFileLoading(true);
        setFileMime('excel');
        try {
          const buf = await downloadFileAsArrayBufferFn(workspaceId, filePath);
          setFileArrayBuffer(buf);
        } catch (err) {
          console.error('[FilePanel] Failed to load Excel file:', err);
          setFileError(categorizeFileError(err, wsData?.status));
          setFileMime(null);
        } finally {
          setFileLoading(false);
        }
        return;
      }
      try {
        await triggerDownloadFn(workspaceId, filePath);
      } catch (err) {
        console.error('[FilePanel] Failed to download file:', err);
      }
      return;
    }

    // Text files - read content
    setSelectedFile(filePath);
    setFileLoading(true);
    try {
      const data = await readFileFn(workspaceId, filePath);
      setFileContent(data.content || '');
      setFileMime(data.mime || 'text/plain');
    } catch (err) {
      console.error('[FilePanel] Failed to read file:', err);
      setFileError(categorizeFileError(err, wsData?.status));
      setFileContent(null);
      setFileMime(null);
    } finally {
      setFileLoading(false);
    }
  };

  const selectedExt = selectedFile ? getFileExtension(selectedFile.split('/').pop() || '') : '';
  const canEdit = !!(selectedFile
    && !readOnly
    && !fileError
    && EDITABLE_EXTENSIONS.has(selectedExt)
    && fileMime !== 'image'
    && fileMime !== 'pdf'
    && fileMime !== 'excel'
    && !['html', 'htm'].includes(selectedExt)
    && !selectedFile.startsWith('/large_tool_results/'));

  const hasUnsavedChanges = isEditing && editContent !== null && editContent !== fileContent;

  const handleBack = () => {
    // In single-file mode, back closes the panel instead of returning to file tree
    if (singleFileMode) {
      onClose();
      return;
    }
    if (hasUnsavedChanges) {
      if (!window.confirm(t('filePanel.discardUnsaved'))) return;
    }
    if (fileMime === 'image' && fileContent) {
      URL.revokeObjectURL(fileContent);
    }
    setSelectedFile(null);
    setFileContent(null);
    setFileArrayBuffer(null);
    setFileMime(null);
    setFileError(null);
    setExportModalOpen(false);
    setIsEditing(false);
    setEditContent(null);
    setShowDiff(false);
    setOriginalContent(null);
    editorRef.current = null;
    setCanUndo(false);
    setCanRedo(false);
    setSaveError(null);
  };

  const handleStartEdit = useCallback(async () => {
    if (!selectedFile || !workspaceId) return;
    setSaveError(null);
    try {
      const data = await readFileFullFn(workspaceId, selectedFile);
      const fullContent = data.content || '';
      if (fullContent.length > 500 * 1024) {
        setSaveError(t('filePanel.fileTooLarge'));
        return;
      }
      setEditContent(fullContent);
      setOriginalContent(fullContent);
      setFileContent(fullContent);
      setIsEditing(true);
    } catch (err: unknown) {
      const e = err as { response?: { data?: { detail?: string } }; message?: string };
      console.error('[FilePanel] Failed to fetch full file for editing:', err);
      setSaveError(e?.response?.data?.detail || e?.message || t('filePanel.loadEditFailed'));
    }
  }, [selectedFile, workspaceId, readFileFullFn, t]);

  const handleEditorChange = useCallback((value: string) => {
    setEditContent(value);
  }, []);

  const handleSave = useCallback(async () => {
    if (!selectedFile || !workspaceId || editContent === null) return;
    if (!window.confirm(t('filePanel.confirmSave'))) return;
    setIsSaving(true);
    setSaveError(null);
    try {
      await writeFileFn(workspaceId, selectedFile, editContent);
      setFileContent(editContent);
      setIsEditing(false);
      setEditContent(null);
      setShowDiff(false);
      setOriginalContent(null);
    } catch (err: unknown) {
      const e = err as { response?: { data?: { detail?: string } }; message?: string };
      console.error('[FilePanel] Save failed:', err);
      setSaveError(e?.response?.data?.detail || e?.message || t('filePanel.saveFailed'));
    } finally {
      setIsSaving(false);
    }
  }, [selectedFile, workspaceId, editContent, writeFileFn, t]);

  const handleCancelEdit = useCallback(() => {
    if (hasUnsavedChanges) {
      if (!window.confirm(t('filePanel.discardChanges'))) return;
    }
    setIsEditing(false);
    setEditContent(null);
    setShowDiff(false);
    setOriginalContent(null);
    setSaveError(null);
  }, [hasUnsavedChanges, t]);

  useEffect(() => {
    if (!isEditing) return;
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 's') {
        e.preventDefault();
        if (editContent !== null && editContent !== fileContent) {
          handleSave();
        }
      }
    };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [isEditing, editContent, fileContent, handleSave]);

  useEffect(() => {
    if (!hasUnsavedChanges) return;
    const handler = (e: BeforeUnloadEvent) => {
      e.preventDefault();
      e.returnValue = '';
    };
    window.addEventListener('beforeunload', handler);
    return () => window.removeEventListener('beforeunload', handler);
  }, [hasUnsavedChanges]);

  const handleUpload = useCallback(async (file: globalThis.File) => {
    if (!file || !workspaceId) return;
    setUploadError(null);
    setUploadProgress(0);
    try {
      await uploadWorkspaceFile(workspaceId, file, null, (pct: number) => setUploadProgress(pct));
      setUploadProgress(null);
      onRefreshFiles?.();
    } catch (err: unknown) {
      const e = err as { response?: { status?: number; data?: { detail?: string } }; message?: string };
      console.error('[FilePanel] Upload failed:', err);
      let msg = e?.response?.data?.detail || e?.message || 'Upload failed';
      if (e?.response?.status === 413 && !e?.response?.data?.detail) {
        const sizeMB = (file.size / (1024 * 1024)).toFixed(1);
        msg = `File is too large (${sizeMB} MB). Maximum upload size is 250 MB.`;
      }
      setUploadError(msg);
      setUploadProgress(null);
    }
  }, [workspaceId, onRefreshFiles]);

  const handleFileInputChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) handleUpload(file);
    if (fileInputRef.current) fileInputRef.current.value = '';
  }, [handleUpload]);

  const handleDragEnter = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragCounterRef.current++;
    if (e.dataTransfer.types.includes('Files')) {
      setIsDragOver(true);
    }
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragCounterRef.current--;
    if (dragCounterRef.current === 0) {
      setIsDragOver(false);
    }
  }, []);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
  }, []);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragCounterRef.current = 0;
    setIsDragOver(false);
    const file = e.dataTransfer.files?.[0];
    if (file) handleUpload(file);
  }, [handleUpload]);

  const fileName = selectedFile?.split('/').pop() || '';

  // The JSX return is very large. Due to its size and the fact that it is
  // purely template code with no logic changes, we keep it identical to the
  // original JS version. TypeScript inference handles the JSX elements.
  return (
    <div className="file-panel">
      {/* Header */}
      <div className="file-panel-header">
        <div className="flex items-center gap-2 min-w-0">
          {showSettings ? (
            <button onClick={() => setShowSettings(false)} className="file-panel-icon-btn" title={t('filePanel.backToFileList')}>
              <ArrowLeft className="h-4 w-4" />
            </button>
          ) : selectedFile ? (
            <button onClick={handleBack} className="file-panel-icon-btn" title={t('filePanel.backToFileList')}>
              <ArrowLeft className="h-4 w-4" />
            </button>
          ) : targetDirectory ? (
            <button onClick={() => onTargetDirHandled?.()} className="file-panel-icon-btn" title={t('filePanel.backToAllFiles')}>
              <ArrowLeft className="h-4 w-4" />
            </button>
          ) : isMobile && !hideClose ? (
            <button onClick={onClose} className="file-panel-icon-btn" title={t('filePanel.close')}>
              <ArrowLeft className="h-4 w-4" />
            </button>
          ) : null}
          <span className="text-sm font-semibold truncate" style={{ color: 'var(--color-text-primary)' }}>
            {showSettings ? t('chat.workspaceSettings') : selectedFile ? (<>{fileName}{hasUnsavedChanges && <span style={{ color: 'var(--color-text-tertiary)' }}> *</span>}</>) : targetDirectory ? `${targetDirectory}/` : t('chat.workspaceFiles')}
          </span>
        </div>
        <div className="flex items-center gap-1">
          {!showSettings && !selectedFile && !selectMode && (
            <>
              {!readOnly && files.length > 0 && (
                <button
                  onClick={() => setSelectMode(true)}
                  className="file-panel-icon-btn"
                  title={t('filePanel.selectFiles')}
                >
                  <CheckSquare className="h-4 w-4" />
                </button>
              )}
              {!readOnly && (
                <>
                  <button
                    onClick={() => fileInputRef.current?.click()}
                    className="file-panel-icon-btn"
                    title={t('filePanel.uploadFile')}
                    disabled={uploadProgress !== null}
                  >
                    <Upload className="h-4 w-4" />
                  </button>
                  <input
                    ref={fileInputRef}
                    type="file"
                    className="hidden"
                    onChange={handleFileInputChange}
                  />
                  <button
                    onClick={handleBackup}
                    className="file-panel-icon-btn"
                    title={t('filePanel.backupFiles')}
                    disabled={backingUp}
                  >
                    <HardDrive className={`h-4 w-4 ${backingUp ? 'animate-pulse' : ''}`} />
                  </button>
                </>
              )}
              {!readOnly && (
                <button
                  onClick={onRefreshFiles}
                  className="file-panel-icon-btn"
                  title={t('filePanel.refresh')}
                >
                  <RefreshCw className={`h-4 w-4 ${filesLoading ? 'animate-spin' : ''}`} />
                </button>
              )}
            </>
          )}
          {!readOnly && !selectedFile && selectMode && (
            <>
              <span className="text-xs" style={{ color: 'var(--color-text-tertiary)', whiteSpace: 'nowrap' }}>
                {selectedPaths.size} selected
              </span>
              <button
                onClick={toggleSelectAll}
                className="file-panel-chip"
                style={{ marginLeft: 2, fontSize: 10, padding: '1px 6px' }}
              >
                {selectedPaths.size === filteredSortedFiles.length ? 'Deselect All' : 'Select All'}
              </button>
              {deleteConfirm ? (
                <button
                  onClick={handleDelete}
                  className="file-panel-delete-confirm-btn"
                  disabled={deleteLoading}
                >
                  Delete {selectedPaths.size}?
                </button>
              ) : (
                <button
                  onClick={handleDelete}
                  className="file-panel-icon-btn"
                  title={t('filePanel.deleteSelected')}
                  disabled={selectedPaths.size === 0 || deleteLoading}
                  style={selectedPaths.size > 0 ? { color: 'var(--color-icon-danger)' } : undefined}
                >
                  <Trash2 className="h-4 w-4" />
                </button>
              )}
              <button onClick={exitSelectMode} className="file-panel-icon-btn" title={t('filePanel.cancelSelection')}>
                <X className="h-4 w-4" />
              </button>
            </>
          )}
          <FileHeaderActions
            selectedFile={selectedFile}
            isEditing={isEditing}
            workspaceId={workspaceId}
            fileContent={fileContent}
            fileMime={fileMime}
            canEdit={canEdit}
            onStartEdit={handleStartEdit}
            onOpenExportModal={() => setExportModalOpen(true)}
            triggerDownloadFn={triggerDownloadFn}
            readFileFullFn={readFileFullFn}
            editorRef={editorRef}
            canUndo={canUndo}
            canRedo={canRedo}
            hasUnsavedChanges={hasUnsavedChanges}
            showDiff={showDiff}
            setShowDiff={setShowDiff}
            isSaving={isSaving}
            saveError={saveError}
            onSave={handleSave}
            onCancelEdit={handleCancelEdit}
          />
          {!selectMode && !isEditing && !hideClose && (
            <button onClick={onClose} className="file-panel-icon-btn" title={t('filePanel.close')}>
              <X className="h-4 w-4" />
            </button>
          )}
        </div>
      </div>

      {/* Upload progress bar */}
      {uploadProgress !== null && (
        <div className="file-panel-upload-progress">
          <div className="file-panel-upload-progress-bar" style={{ width: `${uploadProgress}%` }} />
        </div>
      )}

      {uploadError && (
        <div className="file-panel-upload-error">
          <span>{uploadError}</span>
          <button onClick={() => setUploadError(null)} className="file-panel-icon-btn">
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      )}

      {deleteLoading && <div className="file-panel-progress-indeterminate" />}

      {deleteError && (
        <div className="file-panel-upload-error">
          <span>{deleteError}</span>
          <button onClick={() => setDeleteError(null)} className="file-panel-icon-btn">
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      )}

      {backupResult && (
        <div className={`file-panel-backup-result ${backupResult.error ? 'error' : ''}`}>
          <span>
            {backupResult.error
              ? backupResult.error
              : `Backed up ${backupResult.synced} file${backupResult.synced !== 1 ? 's' : ''}${backupResult.skipped ? `, ${backupResult.skipped} unchanged` : ''}`}
          </span>
          <button onClick={() => setBackupResult(null)} className="file-panel-icon-btn" style={{ padding: 2 }}>
            <X className="h-3 w-3" />
          </button>
        </div>
      )}

      {backingUp && <div className="file-panel-progress-indeterminate" />}

      {isEditing && (
        <div className="file-panel-edit-hint">
          <Pencil className="h-3 w-3" style={{ flexShrink: 0 }} />
          <span>{t('filePanel.editingHint')}</span>
        </div>
      )}


      {/* Filter & Sort toolbar */}
      {!showSettings && !selectedFile && !filesLoading && !filesError && files.length > 0 && (
        <div className="file-panel-toolbar">
          <div className="file-panel-filter-chips">
            <button className={`file-panel-chip ${filterType === 'All' ? 'active' : ''}`} onClick={() => setFilterType('All')}>
              All
            </button>
            {availableTypes.map((tp) => (
              <button
                key={tp}
                className={`file-panel-chip ${filterType === tp ? 'active' : ''}`}
                onClick={() => setFilterType(filterType === tp ? 'All' : tp)}
              >
                {tp}
              </button>
            ))}
          </div>
          {onToggleSystemFiles && (
            <button
              className={`file-panel-chip ${showSystemFiles ? 'active' : ''}`}
              onClick={onToggleSystemFiles}
              title="Show system directories (.agents/, .system/, tools/, etc.)"
            >
              System
            </button>
          )}
          <div className="file-panel-sort-wrapper" ref={sortMenuRef}>
            <button className="file-panel-icon-btn" title={t('filePanel.sortFiles')} onClick={() => setShowSortMenu((v) => !v)}>
              <ArrowUpDown className="h-3.5 w-3.5" />
            </button>
            {showSortMenu && (
              <div className="file-panel-sort-menu">
                {SORT_OPTIONS.map((opt) => (
                  <div
                    key={opt.value}
                    className={`file-panel-sort-item ${sortBy === opt.value ? 'active' : ''}`}
                    onClick={() => { setSortBy(opt.value); setShowSortMenu(false); }}
                  >
                    {opt.label}
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      )}

      {/* Workspace settings card */}
      {!showSettings && !selectedFile && !readOnly && !isFlashWorkspace && !selectMode && (
        <div
          className="flex items-center justify-between mx-3 mt-2 mb-1 px-3 py-2 rounded-lg cursor-pointer transition-colors hover:opacity-80"
          style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}
          onClick={() => setShowSettings(true)}
        >
          <span className="text-xs truncate" style={{ color: 'var(--color-text-secondary)' }}>
            {workspaceName || t('thread.workspace')}
          </span>
          <Settings className="h-3.5 w-3.5 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
        </div>
      )}

      {/* Inline settings view */}
      {showSettings ? (
        <div style={{ flex: 1, minHeight: 0, overflow: 'hidden', padding: '0 12px 12px' }}>
          <SandboxSettingsContent workspaceId={workspaceId} />
        </div>
      ) : (
      /* Content */
      <div
        className="file-panel-content-wrapper"
        onDragEnter={!readOnly && !selectedFile ? handleDragEnter : undefined}
        onDragLeave={!readOnly && !selectedFile ? handleDragLeave : undefined}
        onDragOver={!readOnly && !selectedFile ? handleDragOver : undefined}
        onDrop={!readOnly && !selectedFile ? handleDrop : undefined}
        style={{ position: 'relative', flex: 1, minHeight: 0, overflow: 'hidden' }}
      >
        {!readOnly && isDragOver && !selectedFile && (
          <div className="file-panel-drag-overlay">
            <Upload className="h-8 w-8" style={{ color: 'var(--color-accent-primary)' }} />
            <span>Drop file to upload</span>
          </div>
        )}

        <div className="file-panel-content" ref={contentWrapperRef}>
          {selectionTooltip && onAddContext && (
            <div
              className="file-panel-selection-tooltip"
              style={{ left: Math.max(8, selectionTooltip.x - 60), top: Math.max(4, selectionTooltip.y - 32) }}
              onMouseDown={(e: React.MouseEvent) => { e.preventDefault(); e.stopPropagation(); handleAddSelectionContext(); }}
            >
              <TextSelect className="h-3.5 w-3.5" style={{ color: 'var(--color-accent-primary)' }} />
              {selectionTooltip.lineStart != null
                ? (selectionTooltip.lineEnd !== selectionTooltip.lineStart
                    ? t('context.addLinesToContext', { start: selectionTooltip.lineStart, end: selectionTooltip.lineEnd })
                    : t('context.addLineToContext', { line: selectionTooltip.lineStart }))
                : t('context.addToContext')}
            </div>
          )}

          {contextMenu && (
            <div
              className="file-panel-context-menu"
              style={{ left: contextMenu.x, top: contextMenu.y }}
              onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
            >
              {onAddContext && (
                <div className="file-panel-context-menu-item" onClick={() => handleContextMenuAction('add-context', contextMenu.filePath)}>
                  <TextSelect className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
                  {t('context.addToContext')}
                </div>
              )}
              {memoMimeForName(contextMenu.filePath) && (
                <div className="file-panel-context-menu-item" onClick={() => handleContextMenuAction('add-to-memo', contextMenu.filePath)}>
                  {memoedMap.has(contextMenu.filePath) ? (
                    <>
                      <RefreshCw className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
                      {t('context.syncWithMemo')}
                    </>
                  ) : (
                    <>
                      <ScrollText className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
                      {t('context.addToMemo')}
                    </>
                  )}
                </div>
              )}
              <div className="file-panel-context-menu-item" onClick={() => handleContextMenuAction('open', contextMenu.filePath)}>
                <FolderOpen className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
                {t('context.openFile')}
              </div>
            </div>
          )}

          {selectedFile ? (
            <>
              {memoEntryForSelected && (
                <MemoStaleBanner
                  status={memoStaleStatus}
                  syncing={memoSyncing}
                  onSwitchToMemoTab={onSwitchToMemoTab}
                  onSync={handleSyncMemo}
                  onViewDiff={memoStaleSandboxText !== null ? handleViewMemoDiff : null}
                />
              )}
              {fileLoading ? (
              <div className="p-4">
                <div className="flex items-center justify-center py-12">
                  <RefreshCw className="h-5 w-5 animate-spin" style={{ color: 'var(--color-text-tertiary)' }} />
                </div>
              </div>
            ) : fileError ? (
              <FileErrorDisplay
                error={fileError}
                onRetry={() => handleFileClick(selectedFile)}
                onDownload={() => triggerDownloadFn(workspaceId, selectedFile).catch((err: unknown) => console.error('[FilePanel] Download failed:', err))}
              />
            ) : fileMime === 'pdf' ? (
              <Suspense fallback={<DocumentLoadingFallback />}>
                <DocumentErrorBoundary fallback={<DocumentErrorFallback onDownload={() => triggerDownloadFn(workspaceId, selectedFile).catch((err: unknown) => console.error('[FilePanel] Download failed:', err))} />}>
                  <PdfViewer data={fileArrayBuffer!} />
                </DocumentErrorBoundary>
              </Suspense>
            ) : fileMime === 'excel' ? (
              <Suspense fallback={<DocumentLoadingFallback />}>
                <DocumentErrorBoundary fallback={<DocumentErrorFallback onDownload={() => triggerDownloadFn(workspaceId, selectedFile).catch((err: unknown) => console.error('[FilePanel] Download failed:', err))} />}>
                  <ExcelViewer data={fileArrayBuffer!} />
                </DocumentErrorBoundary>
              </Suspense>
            ) : getFileExtension(selectedFile) === 'csv' ? (
              isEditing ? (
                <div className="file-panel-editor-container">
                  <Suspense fallback={<DocumentLoadingFallback />}>
                    <CodeEditor value={editContent ?? undefined} onChange={handleEditorChange} fileName={selectedFile} diffMode={showDiff} originalValue={originalContent ?? undefined} editorRef={editorRef} onUndoRedoChange={handleUndoRedoChange} onTextSelect={onAddContext ? handleEditorTextSelect : undefined} />
                  </Suspense>
                </div>
              ) : (
                <Suspense fallback={<DocumentLoadingFallback />}>
                  <DocumentErrorBoundary fallback={<DocumentErrorFallback onDownload={() => triggerDownloadFn(workspaceId, selectedFile).catch((err: unknown) => console.error('[FilePanel] Download failed:', err))} />}>
                    <CsvViewer content={fileContent ?? ''} />
                  </DocumentErrorBoundary>
                </Suspense>
              )
            ) : ['html', 'htm'].includes(getFileExtension(selectedFile)) ? (
              <Suspense fallback={<DocumentLoadingFallback />}>
                <DocumentErrorBoundary fallback={<DocumentErrorFallback onDownload={() => triggerDownloadFn(workspaceId, selectedFile).catch((err: unknown) => console.error('[FilePanel] Download failed:', err))} />}>
                  <HtmlViewer content={fileContent ?? ''} />
                </DocumentErrorBoundary>
              </Suspense>
            ) : isEditing ? (
              <div className="file-panel-editor-container">
                <Suspense fallback={<DocumentLoadingFallback />}>
                  <CodeEditor value={editContent ?? undefined} onChange={handleEditorChange} fileName={selectedFile} diffMode={showDiff} originalValue={originalContent ?? undefined} editorRef={editorRef} onUndoRedoChange={handleUndoRedoChange} onTextSelect={onAddContext ? handleEditorTextSelect : undefined} />
                </Suspense>
              </div>
            ) : (
              <div className="p-4" onMouseUp={handleContentMouseUp}>
                {fileMime === 'image' ? (
                  <>
                    <img src={fileContent!} alt={fileName} className="max-w-full rounded cursor-pointer" onClick={() => setImageLightboxOpen(true)} />
                    <ImageLightbox src={fileContent!} alt={fileName} open={imageLightboxOpen} onClose={() => setImageLightboxOpen(false)} />
                  </>
                ) : selectedFile?.startsWith('/large_tool_results/') ? (
                  <div className="markdown-print-content">
                    <Markdown variant="panel" content={stripLineNumbers(fileContent) ?? ''} className="text-sm" />
                  </div>
                ) : fileMime?.includes('markdown') || getFileExtension(selectedFile) === 'md' ? (
                  <div className="markdown-print-content">
                    <Markdown variant="panel" content={fileContent ?? ''} className="text-sm" />
                  </div>
                ) : (
                  <SyntaxHighlighter
                    language={EXT_TO_LANG[getFileExtension(selectedFile)] || 'text'}
                    style={typeof window !== 'undefined' && document.documentElement.getAttribute('data-theme') === 'light' ? oneLight : oneDark}
                    customStyle={{ margin: 0, padding: 0, backgroundColor: 'transparent', fontSize: '12px', lineHeight: '1.6' }}
                    codeTagProps={{ style: { backgroundColor: 'transparent' } }}
                    showLineNumbers
                    lineNumberStyle={{ minWidth: '2.5em', paddingRight: '1em', color: 'var(--color-text-tertiary)', userSelect: 'none', fontSize: '11px', opacity: 0.5 }}
                    wrapLines
                    lineProps={(lineNumber: number) => ({ 'data-line': lineNumber } as React.HTMLProps<HTMLElement>)}
                    wrapLongLines
                  >
                    {fileContent!}
                  </SyntaxHighlighter>
                )}
              </div>
            )}
            </>
          ) : (
            <div className="py-1 file-tree-root">
              {filesLoading ? (
                Array.from({ length: 5 }).map((_, i) => (
                  <div key={i} className="file-panel-item animate-pulse">
                    <div className="h-4 w-4 rounded" style={{ backgroundColor: 'var(--color-border-muted)' }} />
                    <div className="h-4 flex-1 rounded" style={{ backgroundColor: 'var(--color-border-muted)', width: `${50 + i * 10}%` }} />
                  </div>
                ))
              ) : filesError ? (
                <div className="px-4 py-8 text-center">
                  <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>{filesError}</p>
                </div>
              ) : files.length === 0 ? (
                <div className="px-4 py-8 text-center">
                  <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>No files yet</p>
                </div>
              ) : filteredSortedFiles.length === 0 ? (
                <div className="px-4 py-8 text-center">
                  <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>No {filterType.toLowerCase()} files</p>
                </div>
              ) : (
                fileTree.map((node) => (
                  <DirectoryNode
                    key={node.fullPath}
                    node={node}
                    depth={0}
                    showHeader={node.name !== '/'}
                    expandedDirs={expandedDirs}
                    toggleDir={toggleDir}
                    selectMode={selectMode}
                    selectedPaths={selectedPaths}
                    toggleSelect={toggleSelect}
                    toggleDirSelect={toggleDirSelect}
                    handleFileClick={handleFileClick}
                    readOnly={readOnly}
                    backedUpSet={backedUpSet}
                    modifiedSet={modifiedSet}
                    memoedMap={memoedMap}
                    memoedTitle={memoedTitle}
                    onAddContext={onAddContext}
                    setContextMenu={setContextMenu}
                    activeContextPath={contextMenu?.filePath ?? null}
                  />
                ))
              )}
            </div>
          )}
        </div>
      </div>
      )}

      {selectedFile && exportModalOpen && (
        <Suspense fallback={null}>
          <ExportPreviewModal
            open={exportModalOpen}
            onOpenChange={setExportModalOpen}
            content={fileContent ?? ''}
            fileName={selectedFile}
            workspaceId={workspaceId}
            readFileFullFn={readFileFullFn}
          />
        </Suspense>
      )}
      {selectedFile && memoEntryForSelected && memoStaleSandboxText !== null && (
        <MemoDiffModal
          open={memoDiffOpen}
          memoKey={memoEntryForSelected.key}
          fileName={selectedFile.split('/').pop() || selectedFile}
          sandboxText={memoStaleSandboxText}
          onClose={() => setMemoDiffOpen(false)}
        />
      )}
    </div>
  );
}

export default FilePanel;
