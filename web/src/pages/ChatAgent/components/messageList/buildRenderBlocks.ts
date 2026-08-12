import { chartInstanceKey, planChartAnnotationCards } from '../chartAnnotationGrouping';
import { INLINE_ARTIFACT_TOOLS } from '../charts/InlineArtifactCards';
import { normalizeSubagentText } from './normalizeSubagentText';
import type { ContentSegmentRecord, ToolCallProcessRecord } from './types';

const MIN_LIVE_EXPOSURE_MS = 1800; // minimum time a just-completed item stays in the live zone before folding
const MAX_IN_PROGRESS_MS = 15000; // max time a tool call can stay in-progress in live view before archiving (independent of MIN_LIVE_EXPOSURE_MS)
/** Tools that should stay in the live zone for their entire duration (no MAX_IN_PROGRESS_MS cap) */
const ALWAYS_LIVE_TOOLS = new Set(['TaskOutput', 'WebFetch']);
/** Tool calls that are never rendered as visible activity items — they have dedicated UI or are internal */
export const HIDDEN_TOOL_CALL_NAMES = new Set(['TodoWrite', 'task', 'Task', 'SubmitPlan', 'AskUserQuestion', 'manage_workspaces', 'ptc_agent', 'agent_output', 'manage_threads', 'ShowWidget']);

/** Render block types for the textOnly activity grouping */
export interface ActivityRenderBlock {
  type: 'activity';
  key: string;
  items: Array<Record<string, unknown>>;
}
export interface TextRenderBlock {
  type: 'text';
  key: string;
  segment: ContentSegmentRecord;
}
export interface CompactArtifactRenderBlock {
  type: 'compact_artifact';
  key: string;
  toolCallId: string;
  proc: ToolCallProcessRecord;
}
export interface SubagentTaskRenderBlock {
  type: 'subagent_task';
  key: string;
  segment: ContentSegmentRecord;
}
export interface PlanApprovalRenderBlock {
  type: 'plan_approval';
  key: string;
  segment: ContentSegmentRecord;
}
export interface UserQuestionRenderBlock {
  type: 'user_question';
  key: string;
  segment: ContentSegmentRecord;
}
export interface CreateWorkspaceRenderBlock {
  type: 'create_workspace';
  key: string;
  segment: ContentSegmentRecord;
}
export interface StartQuestionRenderBlock {
  type: 'start_question';
  key: string;
  segment: ContentSegmentRecord;
}
export interface PTCAgentRenderBlock {
  type: 'ptc_agent';
  key: string;
  segment: ContentSegmentRecord;
}
export interface SecretaryActionRenderBlock {
  type: 'delete_workspace' | 'stop_workspace' | 'delete_thread';
  key: string;
  segment: ContentSegmentRecord;
}
export interface NotificationRenderBlock {
  type: 'notification';
  key: string;
  segment: ContentSegmentRecord;
}
export interface HtmlWidgetRenderBlock {
  type: 'html_widget';
  key: string;
  segment: ContentSegmentRecord;
}

export type RenderBlock =
  | ActivityRenderBlock
  | TextRenderBlock
  | CompactArtifactRenderBlock
  | SubagentTaskRenderBlock
  | PlanApprovalRenderBlock
  | UserQuestionRenderBlock
  | CreateWorkspaceRenderBlock
  | StartQuestionRenderBlock
  | PTCAgentRenderBlock
  | SecretaryActionRenderBlock
  | NotificationRenderBlock
  | HtmlWidgetRenderBlock;

/** Sort segments by `order` and merge consecutive text segments into one group. */
export function groupSegments(segments: ContentSegmentRecord[]): ContentSegmentRecord[] {
    const sorted = [...segments].sort((a, b) => a.order - b.order);
    const groups: ContentSegmentRecord[] = [];
    let currentTextGroup: ContentSegmentRecord | null = null;

    for (const segment of sorted) {
      if (segment.type === 'text') {
        if (currentTextGroup) {
          const prev: ContentSegmentRecord = currentTextGroup;
          currentTextGroup = {
            ...prev,
            content: (prev.content || '') + (segment.content || ''),
            lastOrder: segment.order,
          };
          // Replace the last entry (the current text group) with the updated one
          groups[groups.length - 1] = currentTextGroup;
        } else {
          currentTextGroup = {
            type: 'text',
            content: segment.content,
            order: segment.order,
            lastOrder: segment.order,
          };
          groups.push(currentTextGroup);
        }
      } else {
        currentTextGroup = null;
        groups.push(segment);
      }
    }
    return groups;
}

/** The textOnly-mode reducer: folds grouped segments into render blocks (live
 * activity zone vs archived accordion) and reports the next live→completed
 * expiry so the caller can schedule a recompute timer. */
export function buildRenderBlocks(
  groupedSegments: ContentSegmentRecord[],
  {
    reasoningProcesses,
    toolCallProcesses,
    isStreaming,
    isSubagentView,
  }: {
    reasoningProcesses: Record<string, Record<string, unknown>>;
    toolCallProcesses: Record<string, ToolCallProcessRecord>;
    isStreaming?: boolean;
    isSubagentView?: boolean;
  },
): { blocks: RenderBlock[]; nextExpiry: number | null } {
    const filtered = groupedSegments.filter((s) => {
        if (s.type === 'text' || s.type === 'reasoning') return true;
        if (s.type === 'notification') return true;
        if (s.type === 'subagent_task') return true;
        if (s.type === 'plan_approval') return true;
        if (s.type === 'user_question') return true;
        if (s.type === 'create_workspace') return true;
        if (s.type === 'start_question') return true;
        if (s.type === 'ptc_agent') return true;
        if (s.type === 'delete_workspace') return true;
        if (s.type === 'stop_workspace') return true;
        if (s.type === 'delete_thread') return true;
        if (s.type === 'html_widget') return true;
        if (s.type === 'tool_call') {
          const toolName = toolCallProcesses[s.toolCallId!]?.toolName as string | undefined;
          if (HIDDEN_TOOL_CALL_NAMES.has(toolName || '')) return false;
          return true;
        }
        return false;
      });

      // One card per chart instance, pinned at the first draw and fed the
      // latest cumulative artifact (so it grows in place); every other draw
      // folds into the timeline as an ordinary row.
      const chartCardPlan = planChartAnnotationCards(filtered, toolCallProcesses);

      const blocks: RenderBlock[] = [];
      let pendingItems: Array<Record<string, unknown>> = [];
      let activityCounter = 0;
      let computedNextExpiry: number | null = null;

      const now = Date.now();
      // Stream end folds just-COMPLETED items into the accordion immediately
      // instead of waiting out the cooldown. It does NOT evict in-progress work:
      // always-live tools (TaskOutput) are kept live by the active branch below
      // regardless of isStreaming, so a running subagent stays visible after the
      // main stream ends. History/replay items (isStreaming always false) land
      // directly in the accordion regardless of timestamps.
      const streamEnded = !isStreaming;

      const flushActivity = () => {
        if (pendingItems.length > 0) {
          blocks.push({
            type: 'activity',
            key: `activity-${activityCounter++}`,
            items: pendingItems,
          });
          pendingItems = [];
        }
      };

      for (const seg of filtered) {
        if (seg.type === 'reasoning') {
          const proc = reasoningProcesses[seg.reasoningId!];
          if (!proc) continue;
          const rawContent = (proc.content as string) || '';
          const reasoningContent = isSubagentView ? normalizeSubagentText(rawContent) : rawContent;

          if (proc.isReasoning) {
            pendingItems.push({
              type: 'reasoning',
              id: seg.reasoningId,
              reasoningTitle: proc.reasoningTitle || null,
              content: reasoningContent,
              _liveState: 'active',
            });
          } else {
            const completedAt = proc._completedAt as number | undefined;
            const completedAge = completedAt ? now - completedAt : Infinity;

            if (!streamEnded && completedAge < MIN_LIVE_EXPOSURE_MS) {
              pendingItems.push({
                type: 'reasoning',
                id: seg.reasoningId,
                reasoningTitle: proc.reasoningTitle || null,
                content: reasoningContent,
                reasoningComplete: proc.reasoningComplete,
                _liveState: 'completing',
              });
              const expiry = completedAt! + MIN_LIVE_EXPOSURE_MS;
              if (computedNextExpiry === null || expiry < computedNextExpiry) {
                computedNextExpiry = expiry;
              }
            } else {
              pendingItems.push({
                type: 'reasoning',
                id: seg.reasoningId,
                reasoningTitle: proc.reasoningTitle || null,
                content: reasoningContent,
                reasoningComplete: proc.reasoningComplete,
                _liveState: 'completed',
              });
            }
          }
        } else if (seg.type === 'tool_call') {
          const proc = toolCallProcesses[seg.toolCallId!];
          if (!proc) continue;

          const createdAt = proc._createdAt as number | undefined;
          const age = createdAt ? now - createdAt : Infinity;

          const artifactResult = (proc.toolCallResult as Record<string, unknown> | undefined)?.artifact as Record<string, unknown> | undefined;
          const isArtifactReady = INLINE_ARTIFACT_TOOLS.has(proc.toolName as string) && artifactResult;

          const isAlwaysLive = ALWAYS_LIVE_TOOLS.has(proc.toolName as string);

          // Always-live tools (TaskOutput / WebFetch) stay pinned in the live zone
          // for their entire in-progress duration — including after the main stream
          // ends (isStreaming false) while a background subagent keeps running, so
          // the "waiting on a subagent" indicator never disappears. Safe on history/
          // replay: reconstructed tool calls are always isInProgress=false, so this
          // branch can't fire there. Regular in-progress tools still require a live
          // stream and fold once age passes MAX_IN_PROGRESS_MS.
          if ((proc.isInProgress as boolean) && (isAlwaysLive || (isStreaming && age < MAX_IN_PROGRESS_MS))) {
            pendingItems.push({
              type: 'tool_call',
              id: seg.toolCallId,
              toolCallId: seg.toolCallId,
              ...proc,
              _liveState: 'active',
            });
            if (!isAlwaysLive) {
              const expiry = createdAt! + MAX_IN_PROGRESS_MS;
              if (computedNextExpiry === null || expiry < computedNextExpiry) {
                computedNextExpiry = expiry;
              }
            }
          } else if (isArtifactReady) {
            const isChartAnnotation = (artifactResult as Record<string, unknown>).type === 'chart_annotation';
            const plan = isChartAnnotation
              ? chartCardPlan.get(chartInstanceKey(artifactResult as Record<string, unknown>))
              : undefined;
            if (isChartAnnotation && plan && plan.anchorCallId !== seg.toolCallId) {
              // A later draw on a chart whose card is already pinned at its first
              // draw: render as an ordinary completed row (its content shows in
              // the pinned card above). `_annotationStep` stops ActivityBlock
              // (see its partition guard) from re-promoting it into a card.
              pendingItems.push({
                type: 'tool_call',
                id: seg.toolCallId,
                toolCallId: seg.toolCallId,
                ...proc,
                _liveState: 'completed',
                _annotationStep: true,
              });
            } else if (isChartAnnotation && plan) {
              // The anchor (first) draw: pin the card here but feed it the LATEST
              // cumulative artifact so it grows in place. Key is the chart
              // instance, not the tool-call id, so the element persists across
              // draws (no remount) and its legend can animate the new annotations.
              const latestProc = (toolCallProcesses[plan.latestCallId] as typeof proc) ?? proc;
              flushActivity();
              blocks.push({
                type: 'compact_artifact',
                key: `chart-${chartInstanceKey(artifactResult as Record<string, unknown>)}`,
                toolCallId: plan.latestCallId,
                proc: latestProc,
              });
            } else {
              flushActivity();
              blocks.push({
                type: 'compact_artifact',
                key: `compact-${seg.toolCallId}`,
                toolCallId: seg.toolCallId!,
                proc,
              });
            }
          } else if (!streamEnded && age < MIN_LIVE_EXPOSURE_MS && !INLINE_ARTIFACT_TOOLS.has(proc.toolName as string)) {
            pendingItems.push({
              type: 'tool_call',
              id: seg.toolCallId,
              toolCallId: seg.toolCallId,
              ...proc,
              _recentlyCompleted: true,
              // Failure flips the completing-window state to 'failed' so
              // ActivityBlock renders the gray ✕ badge variant. Older failed
              // calls drop to 'completed' below and merge into the accordion
              // alongside successful ones.
              _liveState: (proc.isFailed as boolean) ? 'failed' : 'completing',
            });
            const expiry = createdAt! + MIN_LIVE_EXPOSURE_MS;
            if (computedNextExpiry === null || expiry < computedNextExpiry) {
              computedNextExpiry = expiry;
            }
          } else {
            pendingItems.push({
              type: 'tool_call',
              id: seg.toolCallId,
              toolCallId: seg.toolCallId,
              ...proc,
              _liveState: 'completed',
            });
          }
        } else if (seg.type === 'subagent_task') {
          flushActivity();
          blocks.push({ type: 'subagent_task', key: `subagent-${seg.subagentId}`, segment: seg });
        } else if (seg.type === 'plan_approval') {
          flushActivity();
          blocks.push({ type: 'plan_approval', key: `plan-${seg.planApprovalId}`, segment: seg });
        } else if (seg.type === 'user_question') {
          flushActivity();
          blocks.push({ type: 'user_question', key: `question-${seg.questionId}`, segment: seg });
        } else if (seg.type === 'create_workspace') {
          flushActivity();
          blocks.push({ type: 'create_workspace', key: `workspace-${seg.proposalId}`, segment: seg });
        } else if (seg.type === 'start_question') {
          flushActivity();
          blocks.push({ type: 'start_question', key: `start-question-${seg.proposalId}`, segment: seg });
        } else if (seg.type === 'ptc_agent') {
          flushActivity();
          blocks.push({ type: 'ptc_agent', key: `ptc-agent-${seg.proposalId}`, segment: seg });
        } else if (seg.type === 'delete_workspace' || seg.type === 'stop_workspace' || seg.type === 'delete_thread') {
          flushActivity();
          blocks.push({ type: seg.type, key: `secretary-${seg.type}-${seg.proposalId}`, segment: seg });
        } else if (seg.type === 'html_widget') {
          flushActivity();
          blocks.push({ type: 'html_widget', key: `widget-${seg.widgetId}`, segment: seg });
        } else if (seg.type === 'notification') {
          flushActivity();
          blocks.push({ type: 'notification', key: `notification-${seg.order}`, segment: seg });
        } else if (seg.type === 'text') {
          flushActivity();
          blocks.push({ type: 'text', key: `text-${seg.order}`, segment: seg });
        }
      }
      // Flush trailing activity items
      flushActivity();

      // Per chart instance, only the anchor (first) draw became a
      // `compact_artifact` block — fed the latest cumulative proc via
      // `chartCardPlan` (see `planChartAnnotationCards` above); every later draw
      // was forced to an ordinary `_annotationStep` row, so no post-pass dedup
      // is needed.
      return { blocks, nextExpiry: computedNextExpiry };
}
