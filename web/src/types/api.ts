/** Core API types — User, Workspace, Thread, and response wrappers */

// --- User ---

export interface User {
  user_id: string;
  email: string;
  name?: string | null;
  avatar_url?: string | null;
  created_at?: string;
  updated_at?: string;
  [key: string]: unknown;
}

export interface UserPreferences {
  [key: string]: unknown;
}

// --- Workspace ---

export interface Workspace {
  workspace_id: string;
  name: string;
  status?: string;
  description?: string;
  config?: Record<string, unknown>;
  created_at?: string;
  updated_at?: string;
  [key: string]: unknown;
}

export interface WorkspacesResponse {
  workspaces: Workspace[];
  total?: number;
}

export interface ReorderItem {
  workspace_id: string;
  position: number;
}

// --- Thread ---

export interface Thread {
  workspace_id: string;
  thread_id: string;
  title: string | null;
  created_at?: string;
  updated_at?: string;
  [key: string]: unknown;
}

export interface ThreadsResponse {
  threads: Thread[];
  total: number;
  limit: number;
  offset: number;
}

export interface DeleteThreadResponse {
  success: boolean;
  thread_id: string;
  message: string;
}

export interface ThreadTurn {
  turn_index: number;
  edit_checkpoint_id: string | null;
  regenerate_checkpoint_id: string;
}

export interface ThreadTurnsResponse {
  thread_id: string;
  turns: ThreadTurn[];
  retry_checkpoint_id: string | null;
}

export interface WorkflowStatus {
  can_reconnect: boolean;
  status: string;
  [key: string]: unknown;
}

// --- Thread Sharing ---

export interface ThreadSharePermissions {
  allow_files?: boolean;
  allow_download?: boolean;
}

export interface ThreadShareStatus {
  is_shared: boolean;
  share_token: string;
  share_url: string;
  permissions: ThreadSharePermissions;
}

// --- Workspace Files ---

export interface WorkspaceFile {
  name: string;
  path: string;
  type: 'file' | 'directory';
  size?: number;
  modified?: string;
  [key: string]: unknown;
}

export interface ListFilesResponse {
  workspace_id: string;
  path: string;
  files: WorkspaceFile[];
}

export interface ReadFileResponse {
  workspace_id: string;
  path: string;
  content: string;
  mime: string;
  truncated: boolean;
}

export interface WriteFileResponse {
  workspace_id: string;
  path: string;
  size: number;
}

export interface BackupResponse {
  synced: number;
  skipped: number;
  deleted: number;
  errors: number;
  total_size: number;
}

export interface BackupStatusResponse {
  persisted_files: Record<string, string>;
  total_size: number;
}

// --- Subagent ---

export interface SubagentMessageResponse {
  success: boolean;
  tool_call_id: string;
  display_id: string;
  queue_position: number;
}

// --- Feedback ---

export interface FeedbackPayload {
  turn_index: number;
  rating: number;
  issue_categories?: string[] | null;
  comment?: string | null;
  consent_human_review?: boolean;
}

// --- OAuth ---

export interface OAuthStatus {
  connected: boolean;
  account_id: string | null;
  email: string | null;
  plan_type: string | null;
}

export interface CodexDeviceInitResponse {
  user_code: string;
  verification_url: string;
  interval: number;
}

export type CodexDevicePollResponse =
  | { pending: true }
  | { success: true; email: string; plan_type: string; account_id: string };

// --- News ---

export interface NewsArticle {
  id: string;
  title: string;
  url: string;
  source?: string;
  published_at?: string;
  tickers?: string[];
  [key: string]: unknown;
}

export interface NewsResponse {
  results: NewsArticle[];
  count: number;
  next_cursor: string | null;
}

// --- InfoFlow ---

export interface InfoFlowResponse {
  results: unknown[];
  total: number;
  limit: number;
  offset: number;
  has_more: boolean;
}

// --- Earnings ---

export interface EarningsEntry {
  symbol: string;
  date: string;
  epsEstimated?: number;
  revenueEstimated?: number;
  [key: string]: unknown;
}

export interface EarningsCalendarResponse {
  data: EarningsEntry[];
  count: number;
}

// --- SSE Streaming ---

export interface StreamFetchResult {
  disconnected: boolean;
}

export type SSEEventCallback = (event: SSEEventData) => void;

export interface SSEEventData {
  event?: string;
  agent?: string;
  content?: string;
  timestamp?: string | number;
  metadata?: Record<string, unknown>;
  _eventId?: number | string;
  [key: string]: unknown;
}

// --- Chat Message Send Body ---

export interface ChatMessageBody {
  workspace_id: string;
  messages: Array<{ role: string; content: string }>;
  agent_mode: string;
  plan_mode: boolean;
  locale: string;
  timezone: string;
  additional_context?: unknown;
  checkpoint_id?: string;
  fork_from_turn?: number;
  llm_model?: string;
  reasoning_effort?: string;
  fast_mode?: true;
  hitl_response?: Record<string, { decisions: Array<{ type: string }> }>;
}
