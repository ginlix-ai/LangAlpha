// --- Types ---

export interface TreeNode {
  name: string;
  fullPath: string;
  children: TreeNode[];
  files: string[];
}

export interface SelectionTooltipData {
  x: number;
  y: number;
  text: string;
  lineStart?: number | null;
  lineEnd?: number | null;
}

export interface ContextMenuData {
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

export interface EditorTextSelectData {
  text: string;
  startLine: number;
  endLine: number;
  rect: { left: number; top: number; width: number; height: number } | null;
}

export interface ApiAdapter {
  readFile?: (path: string) => Promise<{ content: string; mime?: string }>;
  readFileFull?: (path: string) => Promise<{ content: string }>;
  writeFile?: (path: string, content: string) => Promise<unknown>;
  downloadFile?: (path: string) => Promise<string>;
  downloadFileAsArrayBuffer?: (path: string) => Promise<ArrayBuffer>;
  triggerDownload?: (path: string) => Promise<void>;
  /** Override the served URL for HTML preview (e.g. the public share serve URL,
   *  used on /s/:shareToken where the workspace UUID isn't available). */
  buildServedUrl?: (path: string, opts?: { injectTheme?: boolean }) => string;
}

export interface BackupResult {
  synced?: number;
  skipped?: number;
  error?: string;
  [key: string]: unknown;
}

export interface SortOption {
  value: string;
  label: string;
}
