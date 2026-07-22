/** ChatView's local type contracts, carved out of ChatView.tsx (5.9b). */
import type { WidgetContextSnapshot } from '@/pages/Dashboard/widgets/framework/contextSnapshot';
import type { SubagentTokenUsage } from '../../utils/tokenUsage';

export type MessageRecord = Record<string, unknown>;

export interface LocationState {
  agentMode?: string;
  workspaceStatus?: string | null;
  initialMessage?: string;
  planMode?: boolean;
  additionalContext?: Record<string, unknown>[] | null;
  attachmentMeta?: Record<string, unknown>[] | null;
  model?: string;
  reasoningEffort?: string;
  isOnboarding?: boolean;
  isPersonalizing?: boolean;
  isModifyingPreferences?: boolean;
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

export interface ToolCallProcessRecord {
  toolName?: string;
  toolCallResult?: { artifact?: { type?: string } };
  [key: string]: unknown;
}

export interface PlanData {
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
  /** Ledger failure reason for an errored task (shown in the detail header). */
  error?: string;
  toolCalls: number;
  tokenUsage: SubagentTokenUsage;
  currentTool: string;
  messages: SubagentMessage[];
  isActive: boolean;
  isMainAgent: boolean;
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
}

export interface SlashCommand {
  type: string;
  name: string;
  skillName?: string;
}

export interface ModelOptions {
  model?: string | null;
  reasoningEffort?: string | null;
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

export interface WorkspaceRecord {
  status?: string;
  name?: string;
  [key: string]: unknown;
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

export interface SubagentStatusIndicatorProps {
  status: string;
  currentTool: string;
  toolCalls?: number;
  messages?: SubagentMessage[];
}
