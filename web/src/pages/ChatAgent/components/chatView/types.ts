/** ChatView's local type contracts, carved out of ChatView.tsx (5.9b). */
import type { WidgetContextSnapshot } from '@/pages/Dashboard/widgets/framework/contextSnapshot';
import type { ChartSelectionSnapshot } from '@/pages/MarketView/stores/chartSelectionStore';
import type { SubagentTokenUsage } from '../../utils/tokenUsage';

export type { MessageRecord } from '../../hooks/utils/types';

export interface LocationState {
  workspaceStatus?: string | null;
  initialMessage?: string;
  additionalContext?: Record<string, unknown>[] | null;
  attachmentMeta?: Record<string, unknown>[] | null;
  model?: string;
  /** The level `initialMessage` runs at, as the sending composer showed it. */
  reasoningEffort?: string;
  fastMode?: boolean;
  /**
   * The effort and fast-mode picks of the composer that sent `initialMessage`,
   * which the thread that send starts keeps as its own. Absent follows
   * Settings.
   */
  reasoningEffortOverride?: string;
  fastModeOverride?: boolean;
  /**
   * The Subagents toggle of the PTC composer that sent `initialMessage`, for
   * the thread that send starts. Absent leaves it to the user's default.
   */
  subagentsAllowed?: boolean;
  /**
   * Text to leave in the composer, unsent, for the user to finish: "Set up a
   * subagent that ". Ignored alongside `initialMessage`, which is sent.
   */
  draftMessage?: string;
  /**
   * The first send opens its turn with no message of its own, its
   * `initialMessage` empty (`SendOptions.hiddenOpener`): the welcome flow
   * asking the agent where to start (pages/Onboarding/welcome/kickoff.ts).
   */
  hiddenOpener?: boolean;
  /** The title of the thread the first send creates (`SendOptions.threadTitle`). */
  threadTitle?: string;
  /**
   * Opened by the welcome flow's hand-off, a view transition that holds the
   * page until this view draws the first send (pages/Onboarding/welcome/
   * handoff.ts). The send goes out at once, and no empty chat is drawn while
   * it does.
   */
  handoff?: boolean;
  workspaceId?: string;
  workspaceName?: string;
  fromThreadId?: string;
  fromWorkspaceId?: string;
  /**
   * Widget context snapshots forwarded from the dashboard's send. Re-seeds
   * the chat input's deck rail on mount so the user sees the same chips they
   * had on the dashboard. The auto-fire effect already includes the same
   * snapshots in `additionalContext`, so the first message is unaffected;
   * the bus state powers the visual chips and any follow-up the user types.
   */
  widgetSnapshots?: WidgetContextSnapshot[];
  /**
   * Chart selection snapshots forwarded from MarketView's PTC send so the
   * auto-fired user message renders the selection cards live (they also persist
   * to metadata, so replay re-renders them regardless).
   */
  chartSelections?: import('@/pages/MarketView/stores/chartSelectionStore').ChartSelectionSnapshot[];
  /**
   * Skill names to preload as hidden skills (chart-annotation forwarding from
   * MarketView). Merged into `additionalContext` as `{type:'skills',name}` on
   * the auto-fire send so hidden skills stay active on the PTC side.
   */
  skills?: string[];
  [key: string]: unknown;
}

/** Subagent message shape (matches useCardState's SubagentMessage) */
export interface SubagentMessage {
  role: string;
  isStreaming?: boolean;
  toolCallProcesses?: Record<string, { isInProgress?: boolean; toolName?: string; [key: string]: unknown }>;
  [key: string]: unknown;
}

export interface AgentInfo {
  id: string;
  name: string;
  displayName?: string;
  taskId: string;
  description: string;
  prompt?: string;
  type: string;
  status: string;
  /** Ledger failure reason for a task that settled with one (shown in the
   *  detail header). */
  error?: string;
  /** That reason's machine spelling (``credit_stop``, ``transport_lost``, …). */
  errorType?: string;
  toolCalls: number;
  tokenUsage: SubagentTokenUsage;
  currentTool: string;
  messages: SubagentMessage[];
  isActive: boolean;
  isMainAgent: boolean;
  /** Owning workflow run's agent id (`task:<id>`) for workflow-owned children. */
  ownerTaskId?: string;
  /** Reduced workflow-run progress, present only on a workflow run task. */
  workflowRun?: import('../../session/subagents/workflowRunState').WorkflowRunState;
  [key: string]: unknown;
}

/** Subagent card update data passed to updateSubagentCard */
export interface SubagentUpdateData {
  agentId: string;
  taskId: string;
  description: string;
  prompt: string;
  type: string;
  isHistory: boolean;
  isActive: boolean;
  status?: string;
  error?: string;
  errorType?: string;
  currentTool?: string;
  messages?: SubagentMessage[];
  tokenUsage?: SubagentTokenUsage;
  [key: string]: unknown;
}

export interface SubagentInfo {
  subagentId: string;
  description?: string;
  prompt?: string;
  type?: string;
  status?: string;
  error?: string;
  errorType?: string;
  /** Owning workflow run's agent id (`task:<id>`) when opened via its drill-in. */
  ownerTaskId?: string;
}

export interface SlashCommand {
  type: string;
  name: string;
  skillName?: string;
}

export interface ModelOptions {
  model?: string | null;
  reasoningEffort?: string | null;
  fastMode?: boolean | null;
  /**
   * Per-message Watch-toggle state. When on, `handleSendWithAttachments`
   * appends a `market-watch` skills item to `additional_context`.
   */
  marketWatch?: boolean | null;
  /**
   * Widget context snapshots from the chat input's deck rail. Serialized into
   * `additional_context` items (one widget directive + optional sibling image
   * per snapshot) by `handleSendWithAttachments`.
   */
  widgetSnapshots?: WidgetContextSnapshot[];
  /** Chart selections riding this send, so the user bubble renders their cards. */
  chartSelections?: ChartSelectionSnapshot[];
  /**
   * The thread's subagents setting, carried only by the send that creates the
   * thread (`useThreadSubagents`).
   */
  subagentsAllowed?: boolean;
  /** The thread's own effort and fast mode, carried only by the send that
   *  creates the thread (`useThreadTuning`). */
  reasoningEffortOverride?: string;
  fastModeOverride?: boolean;
}

export interface ActionCommand {
  name: string;
  type?: string;
  skillName?: string;
  description?: string;
  aliases?: string[];
}

export interface MsgSelectionTooltipData {
  x: number;
  y: number;
  text: string;
}

export interface ChatViewProps {
  workspaceId: string;
  threadId: string;
  initialTaskId?: string;
  onBack: () => void;
  workspaceName?: string;
  isActive?: boolean;
  onThreadResolved?: (oldThreadId: string, newThreadId: string) => void;
  // Warming state from the entry-time /events stream (useWarmWorkspaceSandbox).
  // Lets the spinner show the slow-restore copy when a background warm — not a
  // chat message — owns the sandbox start.
  warmingState?: false | 'starting' | 'archived';
}
