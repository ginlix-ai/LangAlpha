/**
 * Shared types for stream and history event handlers.
 */

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

/** Shape of the todo update payload. */
export interface TodoPayload {
  todos?: unknown[];
  total?: number;
  completed?: number;
  in_progress?: number;
  pending?: number;
  [key: string]: unknown;
}

/** Data for a preview URL panel. */
export interface PreviewData {
  url: string;
  port: number;
  title?: string;
  command?: string;
  loading?: boolean;
  error?: boolean;
}
