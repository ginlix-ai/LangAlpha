import { chartInstanceKey, planChartAnnotationCards } from '../chartAnnotationGrouping';
import { INLINE_ARTIFACT_MAP, INLINE_ARTIFACT_TOOLS, isInlineArtifactReady } from '../charts/InlineArtifactCards';
import { isPillArtifactType } from '../charts/pillArtifacts';
import { normalizeSubagentText } from './normalizeSubagentText';
import { isUserDataReadmePath } from '../../utils/agentPaths';
import { MIN_LIVE_EXPOSURE_MS } from './liveZoneTiming';
import { inChunkOrder } from '../../session/stream/textChunks';
import { preapprovedCardOf, type PreapprovedCard } from '../../utils/preapprovedCards';
import { isPlanTool, readPastPlan, type PastPlanOutcome } from './pastPlan';
import type { PTCAgentProposalState } from '@/types/chat';
import type { ContentSegmentRecord, ToolCallProcessRecord } from './types';
import type { ActivityItem, ToolActivityItem, LiveState, ToolCallData, ToolCallResultData } from './activityTypes';

export const MAX_IN_PROGRESS_MS = 15000; // max time a tool call can stay in-progress in live view before archiving (independent of MIN_LIVE_EXPOSURE_MS)
/** Tools that should stay in the live zone for their entire duration (no MAX_IN_PROGRESS_MS cap) */
export const ALWAYS_LIVE_TOOLS = new Set(['TaskOutput', 'WebFetch']);
/** Tools the transcript never draws a card for; the spinner gate skips them too. */
export const HIDDEN_TOOL_CALL_NAMES = new Set(['TodoWrite', 'task', 'Task', 'AskUserQuestion', 'manage_workspaces', 'ptc_agent', 'agent_output', 'manage_threads', 'ShowWidget', 'delegate_to_analyst']);

/** Render block types for the inline activity grouping */
export interface ActivityRenderBlock {
  type: 'activity';
  key: string;
  items: ActivityItem[];
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
  /** The call's artifact, which its card draws. */
  artifact: Record<string, unknown>;
  /** Null for an artifact let through by its tool's name that no card of
   *  this build draws: the block draws nothing. */
  card: ArtifactCard | null;
}

/** The card a compact artifact is drawn as, decided once where its block is built. */
export interface ArtifactCard {
  /** Its entry in `INLINE_ARTIFACT_MAP`. */
  type: string;
  /** `pill`: a one-line pill, in a wrapping row with the pills beside it.
   *  `call`: its call's own card on a line of its own, which takes the call's
   *  row over as the call lands (CardArrivals).
   *  `chart`: a chart annotation's card, which every draw on the chart feeds. */
  kind: 'pill' | 'call' | 'chart';
}

function cardOf(artifact: Record<string, unknown>): ArtifactCard | null {
  const { type } = artifact;
  if (typeof type !== 'string' || !INLINE_ARTIFACT_MAP[type]) return null;
  return { type, kind: isPillArtifactType(type) ? 'pill' : type === 'chart_annotation' ? 'chart' : 'call' };
}
export interface SubagentTaskRenderBlock {
  type: 'subagent_task';
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
  /** Set for a workspace created without asking; otherwise the card is the
   *  proposal under `segment.proposalId`. */
  proposal?: Record<string, unknown>;
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
  /** Set for a hand-off made without asking; otherwise the card is the
   *  proposal under `segment.proposalId`. */
  proposal?: PTCAgentProposalState;
}
export interface SecretaryActionRenderBlock {
  type: 'delete_workspace' | 'stop_workspace' | 'delete_thread';
  key: string;
  segment: ContentSegmentRecord;
}
export interface CreditPauseRenderBlock {
  type: 'credit_pause';
  key: string;
  segment: ContentSegmentRecord;
}
export interface ToolApprovalRenderBlock {
  type: 'tool_approval';
  key: string;
  segment: ContentSegmentRecord;
}
/** A plan from an older thread, read off its `SubmitPlan` call. */
export interface PastPlanRenderBlock {
  type: 'past_plan';
  key: string;
  description: string;
  outcome: PastPlanOutcome;
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
  | UserQuestionRenderBlock
  | CreateWorkspaceRenderBlock
  | StartQuestionRenderBlock
  | PTCAgentRenderBlock
  | SecretaryActionRenderBlock
  | CreditPauseRenderBlock
  | ToolApprovalRenderBlock
  | PastPlanRenderBlock
  | NotificationRenderBlock
  | HtmlWidgetRenderBlock;

/** Sort segments by `order` and merge consecutive text segments into one group.
 *  This runs on every chunk, so a run is joined once when it closes rather than
 *  rebuilt per segment. */
export function groupSegments(segments: ContentSegmentRecord[]): ContentSegmentRecord[] {
    const groups: ContentSegmentRecord[] = [];
    let run: ContentSegmentRecord[] = [];
    const closeRun = () => {
      if (!run.length) return;
      const first = run[0];
      groups.push({
        type: 'text',
        content: run.length === 1 ? first.content : run.map((s) => s.content || '').join(''),
        order: first.order,
      });
      run = [];
    };
    for (const segment of inChunkOrder(segments)) {
      if (segment.type === 'text') {
        run.push(segment);
      } else {
        closeRun();
        groups.push(segment);
      }
    }
    closeRun();
    return groups;
}

function toolActivity(
  proc: ToolCallProcessRecord,
  id: string,
  state: LiveState,
  root: string | null | undefined,
  extra: Partial<ToolActivityItem> = {},
): ToolActivityItem {
  return {
    ...proc,
    type: 'tool_call', id, toolCallId: id,
    toolName: typeof proc.toolName === 'string' ? proc.toolName : '',
    toolCall: (proc.toolCall ?? undefined) as ToolCallData | undefined,
    toolCallResult: (proc.toolCallResult ?? undefined) as ToolCallResultData | undefined,
    isComplete: proc.isComplete === true,
    isFailed: proc.isFailed === true,
    _liveState: state,
    root,
    ...extra,
  };
}

/** The transcript reducer: folds grouped segments into render blocks (live
 * activity zone vs archived accordion) and reports the next live→completed
 * expiry so the caller can schedule a recompute timer. */
export function buildRenderBlocks(
  groupedSegments: ContentSegmentRecord[],
  {
    reasoningProcesses,
    toolCallProcesses,
    isStreaming,
    isSubagentView,
    preparing,
    now = Date.now(),
    root,
  }: {
    reasoningProcesses: Record<string, Record<string, unknown>>;
    toolCallProcesses: Record<string, ToolCallProcessRecord>;
    isStreaming?: boolean;
    isSubagentView?: boolean;
    /** A tool call is being written. Its row belongs to the block the call
     *  will land in: the tail activity block, or a new one after trailing
     *  prose, under the key that block will have, so the row and the call
     *  are one element. */
    preparing?: boolean;
    /** The clock live exposure is measured against. A component passes its
     *  own, so a timer that advances it is visibly an input of the build. */
    now?: number;
    /** The computer's own root, so a README read under it is still hidden. */
    root?: string | null;
  },
): { blocks: RenderBlock[]; nextExpiry: number | null; pinnedLive: boolean; pinnedSettledAt: number | null } {
    // A hidden secretary call the user approved in advance has no proposal
    // segment, so it draws its card where the call sits.
    const preapproved = new Map<string, PreapprovedCard>();
    const filtered = groupedSegments.filter((s) => {
        if (s.type === 'text' || s.type === 'reasoning') return true;
        if (s.type === 'notification') return true;
        if (s.type === 'subagent_task') return true;
        if (s.type === 'user_question') return true;
        if (s.type === 'create_workspace') return true;
        if (s.type === 'start_question') return true;
        if (s.type === 'ptc_agent') return true;
        if (s.type === 'delete_workspace') return true;
        if (s.type === 'stop_workspace') return true;
        if (s.type === 'delete_thread') return true;
        if (s.type === 'credit_pause') return true;
        if (s.type === 'tool_approval') return true;
        if (s.type === 'html_widget') return true;
        if (s.type === 'tool_call') {
          const proc = toolCallProcesses[s.toolCallId!];
          const toolName = proc?.toolName as string | undefined;
          const args = (proc?.toolCall as ToolCallData | undefined)?.args;
          if (HIDDEN_TOOL_CALL_NAMES.has(toolName || '')) {
            const card = preapprovedCardOf(proc);
            if (card) preapproved.set(s.toolCallId!, card);
            return card !== null;
          }
          const path = args?.file_path || args?.filePath || args?.path || args?.filename;
          if (toolName === 'Read' && typeof path === 'string' && isUserDataReadmePath(path, root)) return false;
          return true;
        }
        return false;
      });

      // One card per chart instance, pinned at the first draw and fed the
      // freshest cumulative artifact (so it grows in place); every other draw
      // folds into the timeline as an ordinary row.
      const chartCardPlan = planChartAnnotationCards(filtered, toolCallProcesses);

      const blocks: RenderBlock[] = [];
      let pendingItems: ActivityItem[] = [];
      let activityCounter = 0;
      let computedNextExpiry: number | null = null;
      // An always-live tool still in flight, which is the one kind of work that
      // outlives the stream that started it. The turn it belongs to is still
      // working, so the fold has to keep its row on screen.
      let pinnedLive = false;
      let pinnedSettledAt: number | null = null;

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
              id: seg.reasoningId!,
              content: reasoningContent,
              reasoningStartedAt: proc._startedAt as number | undefined,
              _liveState: 'active',
            });
          } else {
            const completedAt = proc._completedAt as number | undefined;
            const completedAge = completedAt ? now - completedAt : Infinity;

            if (!streamEnded && completedAge < MIN_LIVE_EXPOSURE_MS) {
              pendingItems.push({
                type: 'reasoning',
                id: seg.reasoningId!,
                  content: reasoningContent,
                reasoningElapsedMs: proc.elapsedMs as number | undefined,
                _liveState: 'completing',
              });
              const expiry = completedAt! + MIN_LIVE_EXPOSURE_MS;
              if (computedNextExpiry === null || expiry < computedNextExpiry) {
                computedNextExpiry = expiry;
              }
            } else {
              pendingItems.push({
                type: 'reasoning',
                id: seg.reasoningId!,
                  content: reasoningContent,
                reasoningElapsedMs: proc.elapsedMs as number | undefined,
                _liveState: 'completed',
              });
            }
          }
        } else if (seg.type === 'tool_call') {
          const proc = toolCallProcesses[seg.toolCallId!];
          if (!proc) continue;
          const card = preapproved.get(seg.toolCallId!);
          if (card) {
            flushActivity();
            blocks.push(card.type === 'ptc_agent'
              ? { type: 'ptc_agent', key: `ptc-agent-${seg.toolCallId}`, segment: seg, proposal: card.proposal }
              : { type: 'create_workspace', key: `workspace-${seg.toolCallId}`, segment: seg, proposal: card.proposal });
            continue;
          }

          if (isPlanTool(proc.toolName)) {
            const pastPlan = readPastPlan(proc);
            if (pastPlan) {
              flushActivity();
              blocks.push({ type: 'past_plan', key: `plan-${seg.toolCallId}`, ...pastPlan });
            }
            continue;
          }

          const createdAt = proc._createdAt as number | undefined;
          const age = createdAt ? now - createdAt : Infinity;

          const artifactResult = proc.toolCallResult?.artifact;
          const isArtifactReady = isInlineArtifactReady(proc.toolName, artifactResult);

          const isAlwaysLive = ALWAYS_LIVE_TOOLS.has(proc.toolName as string);

          // Always-live tools (TaskOutput / WebFetch) stay pinned in the live zone
          // for their entire in-progress duration — including after the main stream
          // ends (isStreaming false) while a background subagent keeps running, so
          // the "waiting on a subagent" indicator never disappears. Safe on history/
          // replay: reconstructed tool calls are always isInProgress=false, so this
          // branch can't fire there. Regular in-progress tools still require a live
          // stream and fold once age passes MAX_IN_PROGRESS_MS.
          // The instant the last pinned call stopped, which is the instant the
          // turn stopped: an always-live call outlives the stream, so the
          // stream-close stamp is from before this work was done.
          if (isAlwaysLive && !(proc.isInProgress as boolean)) {
            const settled = proc._settledAt as number | undefined;
            if (settled && (pinnedSettledAt === null || settled > pinnedSettledAt)) pinnedSettledAt = settled;
          }

          if ((proc.isInProgress as boolean) && (isAlwaysLive || (isStreaming && age < MAX_IN_PROGRESS_MS))) {
            pendingItems.push(toolActivity(proc, seg.toolCallId!, 'active', root));
            if (isAlwaysLive) pinnedLive = true;
            if (!isAlwaysLive) {
              const expiry = createdAt! + MAX_IN_PROGRESS_MS;
              if (computedNextExpiry === null || expiry < computedNextExpiry) {
                computedNextExpiry = expiry;
              }
            }
          } else if (artifactResult && isArtifactReady) {
            const isChartAnnotation = artifactResult.type === 'chart_annotation';
            const plan = isChartAnnotation
              ? chartCardPlan.get(chartInstanceKey(artifactResult))
              : undefined;
            if (isChartAnnotation && plan && plan.anchorCallId !== seg.toolCallId) {
              // A later draw on a chart whose card is already pinned at its first
              // draw: render as an ordinary completed row (its content shows in
              // the pinned card above). `_annotationStep` stops ActivityBlock
              // (see its partition guard) from re-promoting it into a card.
              pendingItems.push(toolActivity(proc, seg.toolCallId!, 'completed', root, { _annotationStep: true }));
            } else if (isChartAnnotation && plan) {
              // The anchor (first) draw: pin the card here but feed it the
              // freshest cumulative artifact so it grows in place. Key is the
              // chart instance, not the tool-call id, so the element persists
              // across draws (no remount).
              const freshestProc = toolCallProcesses[plan.freshestCallId] ?? proc;
              // The plan picked the freshest draw for its artifact.
              const artifact = freshestProc.toolCallResult?.artifact ?? artifactResult;
              flushActivity();
              blocks.push({
                type: 'compact_artifact',
                key: `chart-${chartInstanceKey(artifactResult)}`,
                toolCallId: plan.freshestCallId,
                proc: freshestProc,
                artifact,
                card: cardOf(artifact),
              });
            } else {
              flushActivity();
              blocks.push({
                type: 'compact_artifact',
                key: `compact-${seg.toolCallId}`,
                toolCallId: seg.toolCallId!,
                proc,
                artifact: artifactResult,
                card: cardOf(artifactResult),
              });
            }
          } else if (!streamEnded && age < MIN_LIVE_EXPOSURE_MS && !INLINE_ARTIFACT_TOOLS.has(proc.toolName as string)) {
            pendingItems.push(toolActivity(proc, seg.toolCallId!, proc.isFailed ? 'failed' : 'completing', root, { _recentlyCompleted: true }));
            const expiry = createdAt! + MIN_LIVE_EXPOSURE_MS;
            if (computedNextExpiry === null || expiry < computedNextExpiry) {
              computedNextExpiry = expiry;
            }
          } else {
            pendingItems.push(toolActivity(proc, seg.toolCallId!, 'completed', root));
          }
        } else if (seg.type === 'subagent_task') {
          flushActivity();
          blocks.push({ type: 'subagent_task', key: `subagent-${seg.subagentId}`, segment: seg });
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
        } else if (seg.type === 'credit_pause') {
          flushActivity();
          blocks.push({ type: 'credit_pause', key: `credit-pause-${seg.proposalId}`, segment: seg });
        } else if (seg.type === 'tool_approval') {
          flushActivity();
          blocks.push({ type: 'tool_approval', key: `tool-approval-${seg.proposalId}`, segment: seg });
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
      if (preparing && blocks[blocks.length - 1]?.type !== 'activity') {
        blocks.push({ type: 'activity', key: `activity-${activityCounter++}`, items: [] });
      }

      // Per chart instance, only the anchor (first) draw became a
      // `compact_artifact` block — fed the freshest cumulative proc via
      // `chartCardPlan` (see `planChartAnnotationCards` above); every later draw
      // was forced to an ordinary `_annotationStep` row, so no post-pass dedup
      // is needed.
      return { blocks, nextExpiry: computedNextExpiry, pinnedLive, pinnedSettledAt };
}
