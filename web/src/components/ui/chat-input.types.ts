import type { WidgetContextSnapshot } from '@/pages/Dashboard/widgets/framework/contextSnapshot';

export interface FileAttachment {
  id: string;
  file: File;
  type: string;
  preview: string | null;
  uploadStatus: 'pending' | 'uploading' | 'complete';
  dataUrl: string | null;
}

export interface MentionedFile {
  path: string;
  snippet?: string;
  label?: string;
  lineStart?: number;
  lineEnd?: number;
  lineCount?: number;
  /**
   * Where inside the file the snippet came from, when lines are the wrong unit:
   * a spreadsheet range writes `Model!B4:D9`. Fragment syntax without the `#`,
   * so `@path#locator` round-trips back through `parseFragment` as a link the
   * agent can answer with.
   */
  locator?: string;
  source?: string;
}

export interface SlashCommand {
  type: string;
  name: string;
  skillName?: string;
  description?: string;
  aliases?: string[];
}

/**
 * A thread's own reasoning effort and fast mode, over what Settings gives its
 * model. Null follows Settings; undefined is a thread whose row has not been
 * read, which claims neither.
 */
export interface ThreadTuning {
  reasoningEffort: string | null | undefined;
  fastMode: boolean | null | undefined;
}

export interface ModelOptions {
  model: string | null;
  /** This turn's level, as the composer shows it; null names none, and the
   *  server runs the thread's own, else Settings. */
  reasoningEffort: string | null;
  fastMode: boolean | null;
  /** Per-message market-watch toggle — stamps live prices for tracked tickers. */
  marketWatch?: boolean;
  /** The Subagents pick of a composer with no thread host, for the thread its
   *  PTC send starts; absent leaves the user's default to the server. */
  subagentsAllowed?: boolean;
  /** The effort and fast-mode picks of a composer with no thread host, which
   *  the thread its send starts keeps as its own; absent follows Settings.
   *  `reasoningEffort`/`fastMode` stay the turn's level either way. */
  reasoningEffortOverride?: string;
  fastModeOverride?: boolean;
  /**
   * Widget context snapshots attached via the deck rail. Forwarded to the
   * backend as `additional_context` items of `type: "widget"`. Image-bearing
   * snapshots also produce a sibling `type: "image"` MultimodalContext item;
   * see `widgetSnapshotsToContexts` in `pages/ChatAgent/utils/fileUpload.ts`.
   */
  widgetSnapshots?: WidgetContextSnapshot[];
}

export interface ReadyAttachment {
  file: File;
  dataUrl: string | null;
  type: string;
  preview: string | null;
}

/**
 * Where a send goes: the Chief of Staff, which answers in Home,
 * or the selected workspace's analyst. Both run the full agent, so the scope
 * is a target, not a mode.
 */
export type ComposerScope = 'all' | 'workspace';

export type { Workspace } from '@/types/api';
