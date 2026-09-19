/** Core API types — User, Workspace, Thread, and response wrappers */

// --- User ---

export interface User {
  user_id: string;
  email: string;
  name?: string | null;
  avatar_url?: string | null;
  timezone?: string | null;
  locale?: string | null;
  has_api_key?: boolean;
  has_oauth_token?: boolean;
  access_tier?: number;
  plan_display_name?: string | null;
  /** Completed the personalization flow (i.e. configured a BYOK key). */
  personalization_completed?: boolean;
  /** Completed the legacy first-run onboarding flow. */
  onboarding_completed?: boolean;
  created_at?: string;
  updated_at?: string;
  [key: string]: unknown;
}

export interface UserPreferences {
  [key: string]: unknown;
}

// --- Feature flags ---

/**
 * One feature flag as seen by the current user. `enabled` is the effective
 * value (user override when allowed + set, else the system default);
 * `user_override` is the raw override (null = unset, follows the default).
 * `gate` is the access model — only `opt_in`/`opt_out` accept user overrides.
 */
export interface FeatureState {
  key: string;
  label: string;
  description: string;
  /** Honest cost of opting in to an experimental feature; English-only, null when none. */
  tradeoffs: string | null;
  enabled: boolean;
  gate: 'none' | 'opt_in' | 'opt_out' | 'plan';
  min_tier: number | null;
  user_override: boolean | null;
}

// --- Workspace ---

/** Sandbox resource tier. */
export type ResourceTier = 'standard' | 'performance' | 'max';

export interface Workspace {
  workspace_id: string;
  name: string;
  status?: string;
  description?: string;
  config?: Record<string, unknown>;
  /** Sandbox resource tier. Absent on flash / legacy rows. */
  resource_tier?: ResourceTier;
  /** Keep the sandbox running (idle auto-stop disabled). Absent on flash / legacy rows. */
  is_always_on?: boolean;
  /**
   * The machine this workspace lives on. Absent on flash workspaces and on rows
   * that predate the split, which is exactly when the per-workspace status
   * channel stays the only source for this row's state.
   */
  computer_id?: string | null;
  /** Folder name under the computer's root: the workspace's address on disk. */
  dir_name?: string | null;
  /** The restore from this workspace's file backup did not finish, so the tree
   *  is short some files until the next start retries it. */
  files_restore_incomplete?: boolean;
  created_at?: string;
  updated_at?: string;
  [key: string]: unknown;
}

export interface WorkspacesResponse {
  workspaces: Workspace[];
  total?: number;
}

/** Count-quota status for one elevated capability. `limit === -1` means unlimited. */
export interface WorkspaceCapacity {
  used: number;
  limit: number;
}

/**
 * Per-capability workspace quotas (platform mode only). Each field is null when the
 * quota does not apply — OSS mode, the platform is unreachable, or no count reported.
 */
export interface WorkspaceQuota {
  performance: WorkspaceCapacity | null;
  max: WorkspaceCapacity | null;
  always_on: WorkspaceCapacity | null;
}

// --- Computer ---

/**
 * Lifecycle of the machine a workspace runs on. `running`, `error` and
 * `deleted` are the statuses the status stream treats as terminal; the rest
 * either settle on their own or wait for the user.
 */
export type ComputerStatus =
  | 'creating'
  | 'starting'
  | 'running'
  | 'stopping'
  | 'stopped'
  | 'error'
  | 'deleted';

export interface Computer {
  computer_id: string;
  user_id: string;
  /** Execution backend, e.g. `daytona` or `docker`. */
  kind: string;
  name: string;
  /** Wire value, kept plain: an unrecognized state is real, and
   *  `computerStatusUi()` fails safe on one. {@link ComputerStatus} types the
   *  status table instead, so adding a state there forces its copy. */
  status: string;
  resource_tier: ResourceTier;
  is_always_on: boolean;
  /** The machine a workspace is bound to when it names no other. */
  is_primary: boolean;
  /** How many workspaces live on the machine, counted by the server: a list
   *  page a caller happens to hold is not that number. */
  workspace_count?: number;
  root_dir: string;
  /** Vendor id of the running machine. Owner-only, and null before first boot. */
  provider_ref?: string | null;
  created_at?: string;
  updated_at?: string;
  last_activity_at?: string | null;
  stopped_at?: string | null;
  config?: Record<string, unknown>;
}

export interface ComputersResponse {
  computers: Computer[];
  total?: number;
}

/**
 * Payload of a `file_operation` artifact event. `file_path` is workspace
 * relative, the spelling every path helper classifies; `sandbox_path` is the
 * one the tool was called with, kept for opening the file where it sits.
 */
export interface FileOperationArtifactPayload {
  operation: 'Write' | 'Edit' | string;
  file_path: string;
  sandbox_path?: string;
  line_count?: number;
  content?: string;
  old_string?: string;
  new_string?: string;
  error?: string;
}

export interface ComputerActionResponse {
  computer_id: string;
  status: string;
  message?: string;
}

/** Request body for `POST /api/v1/computers`. Tier is the machine's, not a workspace's. */
export interface ComputerCreate {
  name?: string;
  resource_tier?: ResourceTier;
}

export interface ReorderItem {
  workspace_id: string;
  position: number;
}

// --- Thread ---

/** Who initiated a thread; absent origin (or empty metadata) = user-initiated. */
export interface ThreadOrigin {
  type: 'agent' | 'automation' | 'system';
  /** agent → dispatching flash thread id; automation → automation id */
  id?: string;
}

export interface ThreadMetadata {
  origin?: ThreadOrigin;
  [key: string]: unknown;
}

export interface Thread {
  workspace_id: string;
  thread_id: string;
  title: string | null;
  metadata?: ThreadMetadata;
  /** Pinned threads sort first within their workspace. */
  is_pinned?: boolean;
  /** Archive stamp; null/absent = active. Archived rows only appear when explicitly requested. */
  archived_at?: string | null;
  /** Turn count (list responses only). */
  turn_count?: number;
  created_at?: string;
  updated_at?: string;
  // Lifecycle enrichment (present on list responses for threads with runs).
  /** Public status of the latest run: running|stopping|recovering|queued|completed|interrupted|failed|cancelled */
  run_status?: string;
  interrupt_reason?: string | null;
  latest_run_seq?: number;
  latest_run_id?: string | null;
  last_seen_run_seq?: number;
  run_started_at?: string | null;
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

/**
 * The backend's public workflow-run status vocabulary — the `status` field of the
 * `/status` and dispatch-liveness responses. Single source of truth for the wire
 * spellings: `idle` = not yet registered; `queued`/`recovering` = registered but
 * no output yet; `stopping` = a live run being cancelled. Consumers that read
 * untrusted wire values still take `unknown` and narrow against these members.
 */
export type WorkflowRunStatus =
  | 'idle'
  | 'queued'
  | 'running'
  | 'stopping'
  | 'recovering'
  | 'completed'
  | 'interrupted'
  | 'failed'
  | 'cancelled';

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
  hitl_response?: HitlResponseBody;
}

/** One answer on a resume: the verdict, plus the user's own message if they
 *  typed one. */
export interface HitlDecisionBody {
  type: string;
  message?: string;
}

/**
 * What one interrupt is answered with.
 *
 * `decisions` stays positional and answers every request the interrupt raised,
 * in the order it raised them. `order_decisions` answers the keyed ones by
 * attempt id, and both travel together on a mixed interrupt so neither half
 * has to be inferred from the other. A keyed request missing from the map is
 * refused by the server, so the client sends every one of them rather than
 * letting absence stand for approval.
 */
export interface HitlResumeEntry {
  decisions: HitlDecisionBody[];
  order_decisions?: Record<string, HitlDecisionBody>;
}

/** The `hitl_response` map a resume sends, keyed by interrupt id. */
export type HitlResponseBody = Record<string, HitlResumeEntry>;
