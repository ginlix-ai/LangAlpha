/**
 * Shared types for stream and history event handlers.
 */

import type { PreviewFailure } from '../../utils/api/sandbox';

/** A loosely-typed message record used throughout the chat state. */
export type MessageRecord = Record<string, unknown>;

/** React-style state setter for the messages array. */
export type SetMessages = (updater: (prev: MessageRecord[]) => MessageRecord[]) => void;

/** Shape of a tool call object from the SSE event. */
export interface ToolCallRecord {
  id?: string;
  name?: string;
  args?: Record<string, unknown>;
  [key: string]: unknown;
}

/** Shape of a tool call result from the SSE event. */
export interface ToolCallResultRecord {
  content?: unknown;
  content_type?: string;
  tool_call_id?: string;
  artifact?: unknown;
  [key: string]: unknown;
}

/** Data for an inline HTML widget. */
export interface HtmlWidgetData {
  html: string;
  title: string;
  /** Inline data file contents — injected as window.__WIDGET_DATA__ in the iframe. */
  data?: Record<string, string>;
}

/** Data for a preview URL panel. */
export interface PreviewData {
  url: string;
  port: number;
  title?: string;
  command?: string;
  /** URL path suffix (e.g. "/timeline.html") appended to the resolved signed URL. */
  path?: string;
  loading?: boolean;
  /** Set when the app could not be shown, saying why. */
  error?: PreviewFailure;
  /** Monotonic counter — incremented on user clicks to force iframe reload even when URL is unchanged. */
  reloadToken?: number;
  /** The workspace the app runs in, when not the view's own: an `/a/` link from a sibling. */
  workspaceId?: string;
}
