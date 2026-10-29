/** Core API types — User, Workspace, Thread, and response wrappers */

import type { CommandFailureReason, CommandTool } from './commands';

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
  /** The deployment's own key runs its default model (self-hosted with keys in its config). */
  deployment_serves?: boolean;
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

// --- Trading permission ---

/** The two levels that let a live or staged order skip the approval gate, and
 *  so the two the server takes only with the signed agreement. */
export type AgreementLevel = 'plan_first' | 'autonomous';

/** How far the agent may go with orders at the user's brokerages, least first.
 *  What each one means to the app is `TRADING_LEVEL_INFO`. */
export type TradingLevel = 'no_trading' | 'approve_each' | AgreementLevel;

/**
 * The level in force, read from its own endpoint rather than from preferences:
 * the agent can write preferences, and must never be able to write this.
 * `agreement_version` is the version the server currently accepts. It is
 * never sent back: opting in sends the version of the text the client showed
 * (`TRADING_AGREEMENT_VERSION`), and this one only tells the client the two
 * have parted.
 */
export interface TradingPermission {
  level: TradingLevel;
  agreement_version: number;
  agreed_at: string | null;
  updated_at: string | null;
}

/** A level behind the agreement cannot be written without the version of the
 *  text that was accepted; one that asks before every order carries none. */
export type TradingPermissionUpdate =
  | { level: Exclude<TradingLevel, AgreementLevel> }
  | { level: AgreementLevel; agreement_version: number };

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

/** Sandbox resource tier. Shown as Small, Medium, Large and Extra Large. */
export type ResourceTier = 'standard' | 'performance' | 'max' | 'xlarge';

export interface Workspace {
  workspace_id: string;
  name: string;
  /** Lifecycle of the workspace's computer. Home reads `'unbound'` until its
   *  first start or turn puts it on one. */
  status?: string;
  /** The user's Home, where the Chief of Staff runs. Never read Home off `status`. */
  is_home: boolean;
  description?: string;
  config?: Record<string, unknown>;
  /** Sandbox resource tier. Absent on Home and legacy rows. */
  resource_tier?: ResourceTier;
  /** Keep the sandbox running (idle auto-stop disabled). Absent on Home and legacy rows. */
  is_always_on?: boolean;
  /**
   * The machine this workspace lives on. Absent on Home before its first start
   * and on rows that predate the split, which is exactly when the
   * per-workspace status channel stays the only source for this row's state.
   */
  computer_id?: string | null;
  /** Folder name under the computer's root: the workspace's address on disk. */
  dir_name?: string | null;
  /** Folders a rename moved this workspace out of, most recent first. Paths in
   *  older turns still name them. */
  previous_dir_names?: string[];
  /** The root of the computer this workspace is on, read with the row so its
   *  paths read from the moment the row does. Null on the row a change answers
   *  with, where the computer list's root stands in. */
  root_dir?: string | null;
  /** Roots of the computers a move took this workspace off, most recent first.
   *  Paths in older turns still start under them. */
  previous_root_dirs?: string[];
  /** The restore from this workspace's file backup did not finish, so the tree
   *  is short some files until the next start retries it. */
  files_restore_incomplete?: boolean;
  /** When the server found this workspace's folder gone from a machine the
   *  user brought and held its backup for the user to choose: restore it, or
   *  keep the folder empty. Set, the backup stops following the folder until
   *  the user answers. */
  files_restore_offered_at?: string | null;
  /** When the last complete backup pass ran; null when unknown. */
  files_backed_up_at?: string | null;
  created_at?: string;
  updated_at?: string;
  /** When the workspace was last used; null until it first is. The `activity`
   *  order reads this, falling back to `updated_at`, which a rename, pin or
   *  reorder also bumps. */
  last_activity_at?: string | null;
  [key: string]: unknown;
}

export interface WorkspacesResponse {
  workspaces: Workspace[];
  total?: number;
}

/** Request body for `POST /api/v1/workspaces`. */
export interface WorkspaceCreate {
  name: string;
  description?: string;
  config?: Record<string, unknown>;
  /** The caller's live computer to put the folder on. Omitted means the primary. */
  computer_id?: string;
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
  /** Absent from an older server, which is read as unknown. */
  xlarge?: WorkspaceCapacity | null;
  always_on: WorkspaceCapacity | null;
  /**
   * Computers the user holds against the plan's cap, stopped ones included.
   * Only computers the deployment runs count; a machine the user brings never
   * does. Absent from an older server, which is read as unknown.
   */
  computers?: WorkspaceCapacity | null;
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

/** Where an `ssh` computer lives. */
export interface ComputerSsh {
  host: string;
  port: number;
  username: string;
  fingerprint: string | null;
  /** Reached through the tunnel the machine opened; `host` is then only its name. */
  tunnel: boolean;
  /** Whether the tunnel is up now; null when the computer has none. */
  tunnel_connected: boolean | null;
  /** Answered by LangAlpha's helper (remote control of a Mac) rather than the machine's own sshd. */
  helper: boolean;
  /** The system as `uname -s` names it ("Darwin", "Linux"); null on a computer added before it was kept. */
  os?: string | null;
  /** A Mac's model name ("MacBook Pro", "Mac Studio"); null elsewhere, and before it was read. */
  model?: string | null;
  /** The system and its version ("macOS 15.4", "Ubuntu 24.04 LTS"), read as the machine starts. Each of these is null until read. */
  distro?: string | null;
  /** As `uname -m` names it ("arm64", "x86_64"). */
  arch?: string | null;
  /** The processor as the system names it ("Apple M4 Max"). */
  cpu?: string | null;
  cores?: number | null;
  memory_bytes?: number | null;
}

export interface Computer {
  computer_id: string;
  user_id: string;
  /** Execution backend. Kept plain like `status`: a kind this build does not
   *  know is still a real machine, and `providerLabel()` shows it as is. */
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
  /** The user's own machine, for an `ssh` computer; null for every other kind. */
  ssh: ComputerSsh | null;
  /** Whether the server keeps a backup of each workspace's files. Only a
   *  machine the user brings can turn it off; read absent as on. */
  files_backup?: boolean;
  /** Vendor id of the running machine. Owner-only, and null before first boot. */
  provider_ref?: string | null;
  created_at?: string;
  updated_at?: string;
  last_activity_at?: string | null;
  stopped_at?: string | null;
  config?: Record<string, unknown>;
  /** Last disk reading. Null when never measured, or on a local machine with
   *  no storage quota, where `df` would report the host's disk. */
  disk?: ComputerDisk | null;
  /** The last spec change and how it went. The change runs after its request
   *  has answered, so this row is where its outcome is read. */
  spec_change?: ComputerSpecChange | null;
  /** Why the last start failed, kept until a start succeeds or a reinstall or
   *  reconnect is accepted. Absent from an older server. */
  start_failure?: ComputerStartFailure | null;
}

/** What fixes a failed start, decided by the server from its code. */
export type StartFailureRemedy = 'reinstall' | 'reconnect' | 'retry';

export interface ComputerStartFailure {
  /** `folder_marked`, an SSH connect code (`root_dir`, `not_connected`, ...),
   *  or `start_failed`. Kept plain: the remedy is what the client acts on. */
  code: string;
  /** A user-facing sentence from the server. */
  message: string;
  /** On `folder_marked`: the computer whose folder it is, one this account
   *  does not have. */
  owner?: string | null;
  remedy: StartFailureRemedy;
  at: string;
}

/** `POST /computers/{id}/reinstall`: keep the folder (taking it back) or move to a new one. */
export type ComputerReinstall =
  | {
      folder: 'take_back';
      /** The computer the user was shown holding the folder (`start_failure.owner`). */
      take_over?: string;
    }
  | {
      folder: 'new';
      root_dir: string;
      /** The owner a 409 `folder_marked` named for the new folder, which the user chose to take. */
      take_over?: string;
    };

/** `POST /computers/{id}/reconnect`: the target a pairing bound to the computer reached. */
export type ComputerReconnectTarget = SshTarget & {
  host_key: string;
  /** The owner a 409 `folder_marked` named for the computer's folder, which the user chose to take back. */
  take_over?: string;
  /** After a 409 `different_machine`: the user chose to move the computer to this machine. */
  move?: boolean;
};

export type SpecChangeState = 'in_progress' | 'succeeded' | 'failed';

/** Why a spec change that was accepted later failed. */
export type SpecChangeErrorCode =
  | 'turn_active'
  | 'backup_incomplete'
  | 'busy'
  | 'interrupted'
  | 'disk_too_small'
  | 'not_allowed'
  | 'unknown';

export interface ComputerSpecChangeError {
  code: SpecChangeErrorCode;
  /** A user-facing sentence from the server, always present. */
  message: string;
  /** The files a backup could not take; may be empty. */
  files: UnsavedFile[];
}

export interface ComputerSpecChange {
  target_tier: ResourceTier;
  from_tier: ResourceTier;
  state: SpecChangeState;
  error?: ComputerSpecChangeError | null;
  started_at: string;
  finished_at?: string | null;
}

/** How close a machine's disk is to full, decided server-side from free bytes. */
export type ComputerDiskLevel = 'healthy' | 'notice' | 'warning' | 'critical';

export interface ComputerDisk {
  used_bytes: number;
  total_bytes: number;
  free_bytes: number;
  measured_at: string;
  level: ComputerDiskLevel;
}

/** One workspace folder's share of the machine's disk. */
export interface ComputerStorageWorkspace {
  workspace_id: string;
  name: string;
  dir_name: string | null;
  bytes: number;
}

/**
 * `GET /computers/{id}/storage`. `live` is false when the machine is not
 * running: the reading is the stored one and the breakdown is empty.
 */
export interface ComputerStorage {
  disk: ComputerDisk | null;
  workspaces: ComputerStorageWorkspace[];
  other_bytes: number;
  live: boolean;
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

/** `DELETE /api/v1/computers/{id}` */
export interface ComputerRemoveResponse {
  /** What happened to our key line on an SSH computer's machine; null for other kinds, and when remove_key was false. */
  key: ComputerKeyRemoval | null;
  /** What happened to LangAlpha's files on an SSH computer's machine; null unless delete_files was asked. */
  files?: ComputerFilesRemoval | null;
  /** The named workspaces it deleted, not one moved off it meanwhile; absent from an older backend. */
  workspaces_deleted?: string[];
}

export interface ComputerFilesRemoval {
  outcome: 'deleted' | 'unreached' | 'failed';
  /** The root folder stays, holding files the owner put there. */
  root_kept: boolean;
  /** ~/.langalpha stays, since another computer on the login uses it. */
  runtime_kept: boolean;
}

export interface ComputerKeyRemoval {
  outcome: 'removed' | 'kept' | 'skipped' | 'unreached';
  /** On `kept`: the name of the user's other computer that still logs in with the key. */
  used_by: string | null;
  /** The key's SHA256 fingerprint ("SHA256:...") and its comment ("langalpha"), for removing it by hand. */
  fingerprint: string | null;
  comment: string | null;
}

/** One entry of `GET /api/v1/computers/providers`. */
export interface ComputerProviderOption {
  /** A sandbox backend, plain like {@link Computer.kind}: a deployment can
   *  offer one this build does not know. */
  kind: string;
  /** The deployment's default, which a create without `kind` lands on. */
  is_default: boolean;
  /** False when the user's plan lacks it; null when it is not gated or the
   *  platform did not say, which is treated as allowed. */
  allowed: boolean | null;
  /** The sizes it offers, smallest first, from its server config. Empty for a
   *  machine the user sizes; absent from an older server, which is unknown. */
  tiers?: ComputerTierSpec[];
  /** What a computer created here unnamed is called, before numbering; absent
   *  from an older server. */
  default_name?: string;
}

/** One size a provider offers, as its server config defines it. */
export interface ComputerTierSpec {
  tier: string;
  cpu: number;
  memory_gib: number;
  disk_gib: number;
}

/** The providers a user can create a computer on, default first. */
export interface ComputerProvidersResponse {
  providers: ComputerProviderOption[];
  /** Whether this server runs a tunnel gateway, which a machine that cannot be dialed connects out to. */
  tunnels: boolean;
}

// --- Workspace move ---

/**
 * Where a move to another computer is. The first four are a move still
 * running (`preparing` and `backing_up` can still be cancelled); the last
 * three are how it ended.
 */
export type WorkspaceMovePhase =
  | 'preparing'
  | 'backing_up'
  | 'staged'
  | 'committed'
  | 'done'
  | 'failed'
  | 'cancelled';

/**
 * Why a move was refused up front (the 409's `detail`) or failed later (the
 * move's `error`): one shape for both. `message` is the server's sentence,
 * written to be shown as is.
 */
export interface WorkspaceMoveError {
  code: string;
  message: string;
  /** For `backup_incomplete`: the files the backup could not save. */
  files?: UnsavedFile[];
}

/** `GET /workspaces/{id}/move`: the workspace's latest move. */
export interface WorkspaceMove {
  move_id: string;
  workspace_id: string;
  source_computer_id: string;
  target_computer_id: string;
  /** Home moving because its target is being made primary; the flip lands when the move commits. */
  make_primary: boolean;
  /** Restored onto the target from the workspace's backup, without the source. */
  from_backup?: boolean;
  phase: WorkspaceMovePhase;
  error?: WorkspaceMoveError | null;
  created_at: string;
  committed_at?: string | null;
  finished_at?: string | null;
}

/** `POST /computers/{id}/make-primary`: flipped now, or Home moves first and `move` runs it. */
export interface MakePrimaryResponse {
  computer_id: string;
  is_primary: boolean;
  move: WorkspaceMove | null;
}

/** Request body for `POST /api/v1/computers`. Tier is the machine's, not a workspace's. */
export interface ComputerCreate {
  name?: string;
  resource_tier?: ResourceTier;
  /** Omitted means the deployment's default provider. */
  kind?: string;
  /** The target of an `ssh` computer, with the host key it answered with when the server connected. */
  ssh?: SshTarget & {
    host_key: string;
    root_dir?: string;
    /** The owner a 409 `folder_marked` named, whose folder the user chose to take over. */
    take_over?: string;
  };
  /** Keep a backup of each workspace's files. Omitted means on; the server
   *  refuses off for a machine it runs. */
  files_backup?: boolean;
}

export interface SshTarget {
  host: string;
  port: number;
  username: string;
  /** Set for a machine reached through a tunnel; `host` is then only its name and `port` its own SSH port. */
  tunnel_id?: string | null;
}

/** `GET /api/v1/computers/ssh/key`: the key the user authorizes on their machine. */
export interface SshKeyResponse {
  public_key: string;
  fingerprint: string;
  authorized_keys_command: string;
}

/** `POST /api/v1/computers/ssh/probe` success. */
export interface SshProbeResult {
  host_key: string;
  fingerprint: string;
  os: string;
  arch: string;
  distro: string;
  home: string;
  shell: string;
  is_root: boolean;
  has_sudo: boolean;
  disk_free_bytes: number | null;
  supported: boolean;
  problems: string[];
  /** The folder a computer created without one works in: `~/langalpha`, or the next free `~/langalpha-N`. */
  suggested_root_dir: string;
  /** The machine's own name, the default name of the computer; empty when it gave none. */
  hostname: string;
  /** A Mac's model name ("MacBook Pro"); empty elsewhere, and from an older server. */
  model?: string;
  /** Answered by LangAlpha's helper (remote control of a Mac) rather than the machine's own sshd. */
  helper: boolean;
}

export type SshProbeErrorCode =
  | 'auth_failed'
  | 'unreachable'
  | 'not_connected'
  | 'gateway_unreachable'
  | 'script_failed'
  | 'private_address'
  | 'timeout'
  | 'host_key_mismatch'
  | 'unsupported'
  | 'expired'
  | 'rate_limited';

/** A refusal's code: one this build words, or one it shows in the server's words. */
export type SshErrorCode = SshProbeErrorCode | (string & {});

/** `POST /api/v1/computers/ssh/pairings`: a one-line command that checks a machine in. */
export interface SshPairing {
  token: string;
  /** `echo 'url = "<script_url>"' | curl -fsSL -K - | sh`, run on the machine as the login to use. */
  command: string;
  script_url: string;
  expires_at: string;
  /** The command reaches the server only from the machine running it, so it adds that machine alone. */
  local_only: boolean;
}

export type SshPairingState = 'waiting' | 'checking' | 'tunneling' | 'ready' | 'failed';

/** What the pairing script reported about the machine it ran on. */
export interface SshPairingMachine {
  username: string;
  hostname: string;
  /** `uname -s`: Linux or Darwin; empty when the script could not tell. */
  os: string;
  addresses: string[];
}

/** `GET /api/v1/computers/ssh/pairings/{token}`, and the answer to its `/connect`. */
export interface SshPairingStatus {
  status: SshPairingState;
  expires_at: string;
  /** Set once the script checked in. */
  machine: SshPairingMachine | null;
  /** Set with `probe` once ready: the address the server reached. */
  target: SshTarget | null;
  probe: SshProbeResult | null;
  /** Set when failed. */
  error: { code: SshErrorCode; message: string } | null;
}

export interface ReorderItem {
  workspace_id: string;
  position: number;
}

// --- Thread ---

/** An agent that hands turns to a workspace's thread. */
export type DispatchingAgent = 'chief_of_staff';

/**
 * Who initiated a thread, or one turn of it (a query row's metadata carries
 * its own); absent origin (or empty metadata) = user-initiated.
 */
export interface ThreadOrigin {
  type: 'agent' | 'automation' | 'system';
  /** agent → dispatching thread id; automation → automation id */
  id?: string;
  /** automation → its name when the turn was sent. Display only. */
  name?: string;
  /** agent → which agent dispatched it. Absent on rows from before it was recorded. */
  agent?: DispatchingAgent;
}

/** How an analyst's reported run ended. */
export type ReportOutcome = 'completed' | 'error' | 'cancelled';

/**
 * What a report-back turn reports on, stamped on its query row. The turn's own
 * message is written for the model, so history labels the turn from this
 * instead. Shares drop it. Mirrors `ReportSource` in src/server/models/chat.py.
 */
export type ReportSource =
  /** A dispatched analyst's thread reporting back, and its workspace. */
  | {
      kind: 'analyst'; thread_id: string; workspace_id?: string; workspace_name?: string;
      /** How the reported run ended. Absent on reports from before it was kept. */
      outcome?: ReportOutcome;
      /** The run reported on and the interrupted runs it continued, oldest
       *  first, so the run reported on is last. */
      run_ids?: string[];
      /** The hand-off steers those runs took in: empty when they took none,
       *  absent when unknown (a report from before it was kept, or one the
       *  server could not read it for). */
      steer_ids?: string[];
    }
  /** A background subagent task in this thread, finished or failed. */
  | {
      kind: 'task'; task_id: string; display_id: string; description?: string; failed: boolean;
      /** The subagent's role. Absent on reports from before it was kept. */
      subagent_type?: string;
    }
  /** A Bash or ExecuteCode call in this thread that ran on in the background
   *  and has now ended. Of the stops, only the user's reports. */
  | {
      kind: 'command_report'; command_id: string; tool: CommandTool; description?: string | null;
      status: 'completed' | 'failed' | 'stopped';
      /** Why a failed one failed; `after_turn_limit` is one stopped an hour
       *  after its turn ended, which reads as stopped early. */
      failure_reason?: CommandFailureReason | null;
      /** The call it started from, whose tool card the report opens; null
       *  for a command whose call had no id. */
      tool_call_id?: string | null;
    };

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
  /** The model this thread runs on; null follows the account default for its mode. */
  llm_model?: string | null;
  /** Whether the agent may hand work to subagents; null (or absent, from an older
   *  server) follows the user's subagents default. */
  subagents_allowed?: boolean | null;
  /** The thread's own reasoning effort; null (or absent, from an older server)
   *  follows Settings for the thread's model. */
  reasoning_effort?: string | null;
  /** The thread's own fast mode; null (or absent) follows Settings. */
  fast_mode?: boolean | null;
  /** Only on a model change's response: the model the thread was on is
   *  writing out, for the new one, a summary only its provider can read. */
  summary_handoff?: boolean | null;
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
  root_path?: string;
}

export interface ThreadShareStatus {
  is_shared: boolean;
  share_token: string;
  share_url: string;
  permissions: ThreadSharePermissions;
}

// --- Share links (files and apps) ---

export type ShareLinkKind = 'file' | 'app';

interface ShareLinkBase {
  code: string;
  /** The workspace the item lives in. */
  workspace_id: string;
  /** App-relative, `/a/<code>`. */
  url: string;
  title: string | null;
  shared: boolean;
  shared_at: string | null;
  /** The confirmed list while shared, null otherwise. */
  shared_files: string[] | null;
  created_at: string;
}

export interface FileShareLink extends ShareLinkBase {
  kind: 'file';
  /** Workspace-relative entry path. */
  path: string;
  port: null;
}

export interface AppShareLink extends ShareLinkBase {
  kind: 'app';
  /** The optional entry path on the served app. */
  path: string | null;
  port: number;
}

/** One item's stable `/a/<code>` link. Private unless `shared`. */
export type ShareLink = FileShareLink | AppShareLink;

export type ShareLinkTarget =
  | { kind: 'file'; path: string }
  | { kind: 'app'; port: number };

export type ShareFileReason = 'entry' | 'page' | 'style' | 'markdown' | 'script';

export interface ShareFileEntry {
  path: string;
  size: number;
  reason: ShareFileReason;
}

export interface ShareLinkDrift {
  added: string[];
  removed: string[];
}

/** The current file list for a file link, with the drift against what was confirmed. */
export interface ShareLinkFiles {
  files: ShareFileEntry[];
  total_size: number;
  drift: ShareLinkDrift | null;
}

export interface SharedLinksResponse {
  links: ShareLink[];
}

/** A signed, expiring prefix the owner's iframes serve workspace files under. */
export interface FileGrant {
  /** `/api/v1/wsfiles/g/<grant>/`, relative to the API base. */
  prefix: string;
  /** Seconds the grant has left when it is answered. */
  expires_in: number;
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

export type UnsavedReason = 'too_large' | 'path_too_long' | 'unreadable' | 'changed' | 'failed';

export interface UnsavedFile {
  path: string;
  reason: UnsavedReason;
  size?: number | null;
}

export interface BackupResponse {
  workspace_id: string;
  synced: number;
  skipped: number;
  deleted: number;
  errors: number;
  oversized: number;
  total_size: number;
  max_file_bytes: number | null;
  /** At most the first 100; `unsaved_count` is the full total. */
  unsaved: UnsavedFile[];
  unsaved_count: number;
}

export interface BackupStatusResponse {
  workspace_id: string;
  backed_up: string[];
  modified: string[];
  untracked: string[];
  total_backed_up_size: number;
  files_restore_incomplete: boolean;
  /** The computer's backup switch. */
  files_backup?: boolean;
  /** As on the workspace row: a restore offer waiting on the user. */
  files_restore_offered_at?: string | null;
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
  locale: string;
  timezone: string;
  additional_context?: unknown;
  checkpoint_id?: string;
  fork_from_turn?: number;
  expected_latest_run_id?: string;
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
