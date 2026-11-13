import type { UploadFailure } from '@/pages/ChatAgent/utils/partUpload';
import type { WidgetContextSnapshot } from '@/pages/Dashboard/widgets/framework/contextSnapshot';

/** Where a composer file's upload stands. */
export type TileUpload =
  /** `percent` sent; null once the bytes are in and the server is storing them. */
  | { status: 'uploading'; percent: number | null }
  | { status: 'ready'; attachmentId: string }
  | { status: 'failed'; failure: UploadFailure };

/**
 * A file in the composer. It uploads from the moment it is added; the message
 * names it by its attachment id once it is ready.
 */
export interface FileAttachment {
  id: string;
  file: File;
  type: string;
  upload: TileUpload;
}

/** A whole file, whose `@path` is a word of the draft. */
export interface FileMention {
  kind: 'file';
  path: string;
}

/** Where a quote came from, which is what the agent is told. */
export type QuoteOrigin =
  | { kind: 'file'; path: string; label: string; lineStart?: number | null; lineEnd?: number | null; lineCount: number }
  /** `message` is the bubble quoted from. A quote read back from what was sent no longer knows it. */
  | { kind: 'message'; speaker: string; message?: string };

export interface QuoteContext {
  kind: 'quote';
  snippet: string;
  /** What the user wrote about the quote; goes out right after it. */
  reply?: string;
  from: QuoteOrigin;
}

export interface QuoteMention extends QuoteContext {
  /** The words standing for the quote in the draft; it goes out where they sit. */
  token: string;
}

/**
 * What goes out beside the draft without being a word of it: a long paste, a
 * live chart's hint, a sheet range. A range's `locator` is where in the file it
 * came from, in the file's own vocabulary (`Model!B4:D9`). Fragment syntax
 * without the `#`, so `@path#locator` round-trips back through `parseFragment`
 * as a link the agent can answer with.
 */
export type ChipContext = { kind: 'chip'; label: string; snippet: string } & (
  | { source: 'paste' | 'chart'; path?: never; locator?: never }
  | { source: 'range'; path: string; locator: string }
);

export type MentionedFile = FileMention | QuoteMention | ChipContext;

/** What a surface outside the composer hands it (`ChatInputHandle.addContext`). */
export type ContextPayload =
  | FileMention
  | QuoteContext
  /** Images for the next message and words for the draft. `files` is empty when
   *  only an image's address is known. */
  | { kind: 'attachment'; files: File[]; text?: string }
  | ChipContext;

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

/** A file a send carries, already on the server under `attachmentId`. */
export interface ReadyAttachment {
  file: File;
  attachmentId: string;
  type: string;
  /** When the server had it ready, which its expiry counts from; unknown is now. */
  readyAt?: number;
}

/**
 * What the box held when a send took it, as the person wrote it: the text
 * before any snippet is spelled out into it, and the pills beside it. A host
 * whose send fails out of the person's sight hands it back (`giveBack`).
 */
export interface ComposerDraft {
  text: string;
  files: ReadyAttachment[];
  mentions: MentionedFile[];
  commands: SlashCommand[];
}

/**
 * One send from the box: the box as the person wrote it, which a host whose
 * send fails out of their sight hands back whole, and what the send carries
 * beyond it.
 */
export interface ComposerSend extends ComposerDraft {
  /** The text the send carries: the person's, with each snippet spelled out into it. */
  message: string;
  modelOptions: ModelOptions;
}

/**
 * Where a send goes: the Chief of Staff, which answers in Home,
 * or the selected workspace's analyst. Both run the full agent, so the scope
 * is a target, not a mode.
 */
export type ComposerScope = 'all' | 'workspace';

export type { Workspace } from '@/types/api';
