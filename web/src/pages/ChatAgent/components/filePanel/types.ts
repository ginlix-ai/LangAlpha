import type { FileLocation } from '../../utils/fileLocation';
import type { MemoryTier } from '../../utils/agentPaths';
import type { BackupResponse } from '@/types/api';

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
  /**
   * Where inside the file the snippet came from, when lines are the wrong unit
   * — a spreadsheet range writes `Model!B4:D9`. Fragment syntax without the
   * leading `#`, so the composer can write `@path#locator` and the link it
   * makes reopens exactly what was referenced.
   */
  locator?: string;
  lineStart?: number | null;
  lineEnd?: number | null;
  lineCount?: number;
  /** What made the snippet, when it is not a file's text: `chat`, `paste`, `chart`. */
  source?: string;
}

export interface EditorTextSelectData {
  text: string;
  startLine: number;
  endLine: number;
  rect: { left: number; top: number; width: number; height: number } | null;
}

/** How the server settled a file reference; `matches` are ranked best first. */
export interface FileRefResolution {
  status: 'resolved' | 'ambiguous' | 'missing' | 'unavailable';
  path?: string;
  matches: string[];
  reason?: string;
}

export interface ApiAdapter {
  readFile?: (path: string) => Promise<{ content: string; mime?: string }>;
  readFileFull?: (path: string) => Promise<{ content: string }>;
  writeFile?: (path: string, content: string) => Promise<unknown>;
  downloadFile?: (path: string) => Promise<string>;
  downloadFileAsArrayBuffer?: (path: string) => Promise<ArrayBuffer>;
  triggerDownload?: (path: string) => Promise<void>;
  resolveFile?: (candidates: string[], recentWrites: string[]) => Promise<FileRefResolution>;
  /** The serve prefix, ending in `/`, a share's HTML is served under (the served
   *  URL is this plus the encoded path). Omitted, the owner's grant serves it. */
  servePrefix?: string;
}

export type { UnsavedFile, UnsavedReason } from '@/types/api';

/** A finished backup, or the reason it did not run. */
export type BackupResult = Partial<BackupResponse> & { error?: string };

export interface SortOption {
  value: string;
  label: string;
}

// --- Panel targets ---

/** The port range the sandbox preview endpoint serves; a tab outside it can never resolve. */
export const PREVIEW_PORT_MIN = 3000;
export const PREVIEW_PORT_MAX = 9999;

/** What a preview tab is opened with. The port is the identity; the rest is labelling. */
export interface PreviewSpec {
  port: number;
  title?: string;
  /** A path suffix on the served app, e.g. `/timeline.html`. */
  path?: string;
  command?: string;
}

/**
 * What a chart tab is opened with. The symbol is the identity; the interval is
 * where it starts. `workspaceId` is whose annotations the chart draws, when an
 * artifact names one other than the panel's; absent, the panel's own.
 */
export interface ChartTabSpec {
  symbol: string;
  timeframe?: string;
  workspaceId?: string;
}

/**
 * What a tool tab is opened with: the call id alone. A tab holds no record of
 * its own; it reads the transcript's live one at render, since stream handlers
 * replace a call's record as its result lands, and a copy taken at click time
 * would show a running call forever.
 */
export interface ToolTabSpec {
  toolCallId: string;
}

/**
 * What the chat last asked the panel to show: one discriminated value, set by
 * the landing, consumed by `usePanelTarget` and cleared by the handled
 * callback once the tab it names has it. It is an ask, not the panel's state:
 * the strip decides what is in front.
 *
 * `dir` outlives the click that set it: it is the tree's active filter, shown
 * in the header and cleared by the back button. So it cannot also say that a
 * request happened, and `seq` does, counting the clicks. The same folder asked
 * for twice is two requests carrying one directory.
 */
export type PanelTarget =
  | {
    kind: 'file';
    path?: string | null;
    dir?: string | null;
    location?: FileLocation | null;
    seq?: number;
    /** Open in a tab of its own rather than the preview slot. */
    pin?: boolean;
  }
  /** A dev server the agent started in the sandbox; it opens as a tab in the
   *  Files panel. `seq` counts the asks, so the same port twice is two. */
  | ({ kind: 'preview'; seq: number } & PreviewSpec)
  /** A live market chart; it opens as a tab in the Files panel, one per symbol. */
  | ({ kind: 'chart'; seq: number } & ChartTabSpec)
  /** A tool call's result, opened as a tab in the Files panel. */
  | ({ kind: 'tool'; seq: number } & ToolTabSpec)
  /** A turn's provenance, opened as a tab in the Files panel. */
  | { kind: 'sources'; seq: number; messageId: string }
  /** An entry in the reader's memory store, opened in the singleton Memory tab. */
  | { kind: 'memory'; seq: number; key: string; tier: MemoryTier }
  /** An entry in the memo store; `''` opens the Memo tab on its list. */
  | { kind: 'memo'; seq: number; key: string }
  /** The live market watch, opened in the singleton Status tab. */
  | { kind: 'status'; seq: number };

/**
 * The kinds the panel consumes on arrival and clears once handled. A memory
 * or memo target outlives its arrival: the tab it opens selects the entry
 * once the store's list resolves, and clears the target itself then. Either
 * way the clear names the ask's `seq`, so an ask landed in between is kept.
 */
export const ONE_SHOT_KINDS = ['file', 'preview', 'chart', 'tool', 'sources', 'status'] as const satisfies readonly PanelTarget['kind'][];
export type OneShotKind = (typeof ONE_SHOT_KINDS)[number];

export function isOneShotKind(kind: PanelTarget['kind'] | null | undefined): kind is OneShotKind {
  return (ONE_SHOT_KINDS as readonly string[]).includes(kind ?? '');
}

/** A target as a caller states it; the landing stamps `seq`. */
export type UnsequencedTarget = PanelTarget extends infer T ? (T extends { seq?: number } ? Omit<T, 'seq'> : T) : never;

/** Stamp an ask with its sequence number, so the same ask twice arrives twice. */
export function stampTarget(target: UnsequencedTarget, seq: number): PanelTarget {
  return { ...target, seq } as PanelTarget;
}

export function fileTarget(
  path: string,
  { location = null, pin = false }: { location?: FileLocation | null; pin?: boolean },
  seq: number,
): PanelTarget {
  return stampTarget({ kind: 'file', path, location, pin }, seq);
}

/** `''` is the workspace root, not the absence of a folder. */
export function dirTarget(dir: string, seq: number): PanelTarget {
  return stampTarget({ kind: 'file', dir }, seq);
}
