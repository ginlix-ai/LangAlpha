/**
 * History replay event handlers
 * Handles events from history replay (SSE stream of past conversations)
 */

import { isToolResultFailure, toolNameOf } from '../subagents/subagentStatus';
import { ownerOfToolCall } from '../toolCallOwner';
import { isTaskAgentId } from '../../utils/agentId';
import { opensTurn } from '../../components/messageList/messagePredicates';
import { deriveTaskSegment, applyTaskSegment, applyLaunchReply } from '../subagents/taskSegmentBuilder';
import { reportRow, senderProvenance, type SenderMetadata } from '../provenance';
import { deliveredEntryRow, steeringDeliveredEntries } from '../stream/steeringDelivery';
import type { ReportSource } from '@/types/api';
import { isDeliveredNotice, isDeliveredSteer, type DeliveredSteer } from '@/types/sse';
import type { SubagentTaskRecord, TextSegment } from '@/types/chat';
import type { MessageRecord, SetMessages, ToolCallRecord, ToolCallResultRecord, TodoPayload, HtmlWidgetData } from '../../hooks/utils/types';
import type { PairState } from '../types';
import type { StructuredError } from '@/utils/rateLimitError';
import { wakeTurnOpener } from '../wakeupEvents';


/** Shape of an SSE history event. */
interface HistoryEvent {
  agent?: string;
  content?: string;
  timestamp?: string | number;
  /** On a report-back's `user_message`, the run its turn opened, ended or not. */
  report_run_id?: string;
  /** On a `user_message`, its query row's metadata. */
  metadata?: SenderMetadata & {
    /** On a report-back only: what it reports on. */
    report_source?: ReportSource;
    [key: string]: unknown;
  };
  messages?: Array<Record<string, unknown>>;
  [key: string]: unknown;
}

/** Refs passed to history user message handler. */
interface HistoryUserMessageRefs {
  recentlySentTracker: { isRecentlySent: (content: string) => boolean };
  currentMessageRef: { current: string | null };
  newMessagesStartIndexRef: { current: number };
  [key: string]: unknown;
}

/** Refs passed to history steering delivered handler. */
interface HistorySteeringRefs {
  newMessagesStartIndexRef: { current: number };
  [key: string]: unknown;
}

/**
 * Returns true if the event is from a subagent (agent field starts with "task:").
 */
export function isSubagentHistoryEvent(event: HistoryEvent | null | undefined): boolean {
  return isTaskAgentId(event?.agent);
}

/**
 * Takes out the copy of a replayed turn that a stream left on screen without
 * marking it the replay's to replace: the reconnect was refused outright, or a
 * newer turn took the session first. With no stream writing the turn, the
 * replay's copy is the one to keep, and a run still going is streamed again
 * from its start onto the replay's empty bubble, which is then the tail.
 *
 * The copy is the turn's opener with the replayed text and turn, and the
 * replies and delivered steers under it, all of which the replay redraws. A
 * send refused before admission is no copy: it keeps the count the next send
 * takes too, so the turn's own opener is the later one, and the refused
 * question and its error stay where they are. Nor is a steer refused or not
 * yet delivered: the server never stored the one, and may have handed the
 * other back unread.
 */
function dropLiveCopy(
  prev: MessageRecord[],
  content: string,
  pairIndex: number,
  insertIndex: number,
): { rest: MessageRecord[]; removedAbove: number } {
  let opener = -1;
  for (let i = prev.length - 1; i >= 0; i--) {
    const m = prev[i];
    if (
      m.role === 'user' && !m.isHistory && !m.queued && opensTurn(m)
      && m.turnIndex === pairIndex && String(m.content ?? '').trim() === content
    ) {
      opener = i;
      break;
    }
  }
  return dropTurnFrom(prev, opener, pairIndex, insertIndex);
}

/**
 * The same for a turn that opened with no message of its own (a hidden
 * opener), whose replayed query is empty and draws no row: its copy starts at
 * the reply its send flagged. A reply the server refused before admission is
 * no copy, for the reason a refused send is none.
 */
function dropHiddenOpenerCopy(
  prev: MessageRecord[],
  pairIndex: number,
  insertIndex: number,
): { rest: MessageRecord[]; removedAbove: number } {
  const opener = prev.findLastIndex((m) => (
    m.role === 'assistant' && m.hiddenOpener === true && !m.isHistory && !m.unadmitted && m.turnIndex === pairIndex
  ));
  return dropTurnFrom(prev, opener, pairIndex, insertIndex);
}

/** Drops the live copy of turn `pairIndex` that opens at `opener` (-1 for
 *  none): the opener, and the replies and delivered steers under it. */
function dropTurnFrom(
  prev: MessageRecord[],
  opener: number,
  pairIndex: number,
  insertIndex: number,
): { rest: MessageRecord[]; removedAbove: number } {
  if (opener === -1) return { rest: prev, removedAbove: 0 };
  const drop = new Set<number>([opener]);
  for (let i = opener + 1; i < prev.length; i++) {
    const m = prev[i];
    if (m.role === 'user' && opensTurn(m)) break;
    if ((m.role === 'assistant' || m.steeringDelivered) && !m.isHistory && m.turnIndex === pairIndex) drop.add(i);
  }
  let removedAbove = 0;
  for (const i of drop) if (i < insertIndex) removedAbove++;
  return { rest: prev.filter((_, i) => !drop.has(i)), removedAbove };
}

/** A steer refused inside a turn before this one, which the reload kept. */
function refusedBefore(m: MessageRecord, turn: number): boolean {
  return m.role === 'user' && !m.isHistory && !!m.queueError && typeof m.turnIndex === 'number' && m.turnIndex < turn;
}

/**
 * The row a replayed turn opens with, or null for empty content and HITL
 * resume pairs. A system query, a report-back's invocation, is written for
 * the model: it shows only as the label naming what it reports, and stays
 * hidden when the row names nothing (an older row, or a share's replay). A
 * price alert or a wakeup that woke the agent is written for the model too,
 * and heads its turn as its row, which still draws when it names nothing.
 */
function turnOpenerRow(event: HistoryEvent, pairIndex: number, text: string): MessageRecord | null {
  // Deterministic id keyed only on pairIndex (the server-side turn_index,
  // unique per turn). Earlier code suffixed `Date.now()` here as
  // speculative collision defense, but that made the same logical bubble
  // look new on every replay: with a non-deduping setMessages reducer, a
  // second replay would silently double the visible history.
  const id = `history-user-${pairIndex}`;
  const timestamp = event.timestamp ? new Date(event.timestamp) : new Date();
  const wake = event.query_type === 'market_target' || event.query_type === 'wakeup'
    ? wakeTurnOpener(pairIndex, event.metadata, timestamp, event.query_type)
    : null;
  if (wake) return { ...wake, isHistory: true };
  if (event.query_type === 'system') {
    const source = event.metadata?.report_source;
    return source ? reportRow(source, {
      id, timestamp, isHistory: true, turnIndex: pairIndex,
      ...(event.report_run_id ? { runId: event.report_run_id } : {}),
    }) : null;
  }
  if (!text) return null;
  const row: MessageRecord = {
    id, role: 'user', content: event.content, contentType: 'text', timestamp,
    isStreaming: false, isHistory: true, turnIndex: pairIndex,
  };
  const provenance = senderProvenance(event.metadata);
  if (provenance) row.provenance = provenance;
  return row;
}

/** Handles user_message events from history replay. */
export function handleHistoryUserMessage({
  event,
  pairIndex,
  assistantMessagesByPair,
  pairStateByPair,
  refs,
  setMessages,
}: {
  event: HistoryEvent;
  pairIndex: number;
  assistantMessagesByPair: Map<number, string>;
  pairStateByPair: Map<number, PairState>;
  refs: HistoryUserMessageRefs;
  setMessages: SetMessages;
}): boolean {
  const { recentlySentTracker, currentMessageRef, newMessagesStartIndexRef } = refs;

  // Check if this is a new pair (not already processed)
  if (assistantMessagesByPair.has(pairIndex)) {
    return false;
  }

  // A steer sent before its run named the turn was stamped from this view's
  // count, which may have been behind. The replay names the run's turn here,
  // ahead of the delivery that finds the steer by it.
  const runId = typeof event.run_id === 'string' ? event.run_id : null;
  if (runId) {
    setMessages((prev: MessageRecord[]) => (
      prev.some((m) => !m.isHistory && m.steeredRunId === runId && m.turnIndex !== pairIndex)
        ? prev.map((m) => (!m.isHistory && m.steeredRunId === runId ? { ...m, turnIndex: pairIndex } : m))
        : prev
    ));
  }

  // The replay draws every turn ahead of the bubbles it kept, so a steer
  // refused in an earlier turn, which only this view holds, would sink below
  // the newest. Stepping past it leaves it under the turn it was sent in.
  setMessages((prev: MessageRecord[]) => {
    let at = newMessagesStartIndexRef.current;
    while (at < prev.length && refusedBefore(prev[at], pairIndex)) at++;
    newMessagesStartIndexRef.current = at;
    return prev;
  });

  const messageContent = (event.content || '').trim();
  // A user's turn with no words whose stored context names the `onboarding`
  // skill opened hidden: the welcome flow's kickoff, the one send of that
  // hidden skill. Its words were the skill's instruction, which no row keeps,
  // so its reply carries the flag the live one did and the turn still offers
  // no Regenerate. Other skills ride turns the user sent: attachments alone
  // with Watch on name `market-watch`, and keep their Regenerate, since their
  // input checkpoint holds the files.
  const storedContext = event.metadata?.additional_context;
  const opensHidden = !messageContent && !event.query_type && Array.isArray(storedContext)
    && storedContext.some((ctx) => {
      const entry = ctx as { type?: unknown; name?: unknown } | null;
      return entry?.type === 'skills' && entry.name === 'onboarding';
    });

  // Check if this message was recently sent (to avoid duplicates)
  const isDuplicate = messageContent && recentlySentTracker.isRecentlySent(messageContent);

  // A turn a stream is still writing keeps its live bubbles, and the replay
  // maps onto them. Any other copy on screen gives way to the replay's below.
  if (isDuplicate && currentMessageRef.current) {
    // Initialize pair state for history replay to work correctly
    if (!pairStateByPair.has(pairIndex)) {
      pairStateByPair.set(pairIndex, {
        contentOrderCounter: 0,
        reasoningId: null,
        toolCallId: null,
        steeringBatches: 0,
      });
    }
    // Map turn_index to the streaming assistant message ID
    const liveId = currentMessageRef.current;
    assistantMessagesByPair.set(pairIndex, liveId);
    // The live turn was counted on the client; the replay names it. Every
    // live bubble that count stamped moves with it: the reply, its user
    // bubble (the turn's only one, as the replay's is dropped) and any
    // steering stamped from the reply.
    setMessages((prev: MessageRecord[]) => {
      const counted = prev.find((m) => m.id === liveId)?.turnIndex;
      return prev.map((m) =>
        m.id === liveId || (!m.isHistory && counted !== undefined && m.turnIndex === counted)
          ? { ...m, turnIndex: pairIndex }
          : m,
      );
    });
    return true;
  } else {
    // Initialize state for this pair
    pairStateByPair.set(pairIndex, {
      contentOrderCounter: 0,
      reasoningId: null,
      toolCallId: null,
      steeringBatches: 0,
    });

    const userMessage = turnOpenerRow(event, pairIndex, messageContent);
    if (userMessage) {
      // Restore attachment metadata from persisted query metadata
      const attachments = event.metadata?.attachments as unknown[] | undefined;
      if (attachments && attachments.length > 0) {
        userMessage.attachments = event.metadata!.attachments;
      }

      // Restore widget snapshot metadata so the chip deck re-renders below
      // this user message on history replay. Backend persists `text` so the
      // preview modal can show what the agent saw; `image_jpeg_data_url` is
      // live-only (image rides the multimodal channel and isn't persisted).
      const widgetContexts = event.metadata?.widget_contexts as unknown[] | undefined;
      if (widgetContexts && widgetContexts.length > 0) {
        userMessage.widgetSnapshots = event.metadata!.widget_contexts;
      }

      // Restore chart-selection summaries so the selection cards re-render
      // below this user message on replay. Backend persists them already in
      // the camelCase snapshot shape the card reads.
      const chartSelections = event.metadata?.chart_selections as unknown[] | undefined;
      if (chartSelections && chartSelections.length > 0) {
        userMessage.chartSelections = event.metadata!.chart_selections;
      }

      // A live attach that drew this turn's row before a reload keeps it
      // through the reload, which keeps live rows; this row, which knows its
      // turn, replaces it. A report's row is found by its run, and a wake's
      // alert row by its id, which the live one shares. Live rows sit past the
      // insert point, so it holds.
      const runId = userMessage.runId;
      const rowId = userMessage.id;
      // A stream writing now may be writing this turn, with the tracker past
      // its window: its bubbles are not the replay's to take.
      const streaming = !!currentMessageRef.current;
      setMessages((prev: MessageRecord[]) => {
        const kept = prev.filter((m) => m.isHistory || (m.id !== rowId && !(runId && m.runId === runId)));
        const { rest, removedAbove } = streaming
          ? { rest: kept, removedAbove: 0 }
          : dropLiveCopy(kept, messageContent, pairIndex, newMessagesStartIndexRef.current);
        const insertIndex = newMessagesStartIndexRef.current - removedAbove;
        const newMessages = [
          ...rest.slice(0, insertIndex),
          userMessage,
          ...rest.slice(insertIndex),
        ];
        newMessagesStartIndexRef.current = insertIndex + 1;
        return newMessages;
      });
    } else if (!messageContent && event.query_type !== 'system' && !currentMessageRef.current) {
      // No row, so nothing above took the turn's live copy away, and the
      // placeholder below would draw the reply a second time beside it.
      setMessages((prev: MessageRecord[]) => {
        const { rest, removedAbove } = dropHiddenOpenerCopy(prev, pairIndex, newMessagesStartIndexRef.current);
        newMessagesStartIndexRef.current -= removedAbove;
        return rest;
      });
    }
  }

  // Always create assistant message placeholder for this pair
  if (!assistantMessagesByPair.has(pairIndex)) {
    // Initialize state for this pair if not already done
    if (!pairStateByPair.has(pairIndex)) {
      pairStateByPair.set(pairIndex, {
        contentOrderCounter: 0,
        reasoningId: null,
        toolCallId: null,
        steeringBatches: 0,
      });
    }

    // Create assistant message placeholder. Deterministic id keyed only on
    // pairIndex — see the matching note at history-user-${pairIndex} above.
    const currentAssistantMessageId = `history-assistant-${pairIndex}`;
    assistantMessagesByPair.set(pairIndex, currentAssistantMessageId);

    const assistantMessage: MessageRecord = {
      id: currentAssistantMessageId,
      role: 'assistant',
      content: '',
      contentType: 'text',
      timestamp: event.timestamp ? new Date(event.timestamp as string | number) : new Date(),
      isStreaming: false,
      isHistory: true,
      turnIndex: pairIndex,
      contentSegments: [],
      reasoningProcesses: {},
      toolCallProcesses: {},
      ...(opensHidden ? { hiddenOpener: true } : {}),
    };

    setMessages((prev: MessageRecord[]) => {
      const insertIndex = newMessagesStartIndexRef.current;
      const newMessages = [
        ...prev.slice(0, insertIndex),
        assistantMessage,
        ...prev.slice(insertIndex),
      ];
      newMessagesStartIndexRef.current = insertIndex + 1;
      return newMessages;
    });

    return true;
  }

  return false;
}

/** Handles reasoning signal events ('start' | 'complete') in history replay. */
export function handleHistoryReasoningSignal({ assistantMessageId, signalContent, pairIndex, pairState, setMessages, eventId, elapsedMs }: {
  assistantMessageId: string;
  signalContent: string;
  pairIndex: number;
  pairState: PairState;
  setMessages: SetMessages;
  eventId?: number | null;
  elapsedMs?: number;
}): boolean {
  if (signalContent === 'start') {
    const reasoningId = `history-reasoning-${pairIndex}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    pairState.reasoningId = reasoningId;
    const currentOrder = eventId != null ? eventId : ++pairState.contentOrderCounter;

    setMessages((prev: MessageRecord[]) =>
      prev.map((msg: MessageRecord) => {
        if (msg.id !== assistantMessageId) return msg;

        const newSegments = [
          ...((msg.contentSegments as unknown[]) || []),
          {
            type: 'reasoning',
            reasoningId,
            order: currentOrder,
          },
        ];

        const newReasoningProcesses = {
          ...((msg.reasoningProcesses as Record<string, unknown>) || {}),
          [reasoningId]: {
            content: '',
            isReasoning: false, // History: already complete
            reasoningComplete: true,
            order: currentOrder,
            _completedAt: 1,
          },
        };

        return {
          ...msg,
          contentSegments: newSegments,
          reasoningProcesses: newReasoningProcesses,
        };
      })
    );
    return true;
  } else if (signalContent === 'complete') {
    if (pairState.reasoningId) {
      const reasoningId = pairState.reasoningId;
      setMessages((prev: MessageRecord[]) =>
        prev.map((msg: MessageRecord) => {
          if (msg.id !== assistantMessageId) return msg;

          const reasoningProcesses = { ...((msg.reasoningProcesses as Record<string, Record<string, unknown>>) || {}) };
          if (reasoningProcesses[reasoningId]) {
            reasoningProcesses[reasoningId] = {
              ...reasoningProcesses[reasoningId],
              isReasoning: false,
              reasoningComplete: true,
              _completedAt: 1,
              elapsedMs,
            };
          }

          return {
            ...msg,
            reasoningProcesses,
          };
        })
      );
      pairState.reasoningId = null;
    }
    return true;
  }
  return false;
}

/** Handles reasoning content chunks in history replay. */
export function handleHistoryReasoningContent({ assistantMessageId, content, pairState, setMessages }: {
  assistantMessageId: string;
  content: string;
  pairState: PairState;
  setMessages: SetMessages;
}): boolean {
  if (content && pairState.reasoningId) {
    const reasoningId = pairState.reasoningId;
    setMessages((prev: MessageRecord[]) =>
      prev.map((msg: MessageRecord) => {
        if (msg.id !== assistantMessageId) return msg;

        const reasoningProcesses = { ...((msg.reasoningProcesses as Record<string, Record<string, unknown>>) || {}) };
        if (reasoningProcesses[reasoningId]) {
          reasoningProcesses[reasoningId] = {
            ...reasoningProcesses[reasoningId],
            content: ((reasoningProcesses[reasoningId].content as string) || '') + content,
            isReasoning: false,
            reasoningComplete: true,
            _completedAt: 1,
          };
        }

        return {
          ...msg,
          reasoningProcesses,
        };
      })
    );
    return true;
  }
  return false;
}

/** Handles text content chunks and finish_reason in history replay. */
export function handleHistoryTextContent({ assistantMessageId, content, finishReason, pairState, setMessages, eventId, phase }: {
  assistantMessageId: string;
  content: string;
  finishReason: string | undefined;
  pairState: PairState;
  setMessages: SetMessages;
  eventId?: number | null;
  phase?: TextSegment['phase'];
}): boolean {
  if (content) {
    const currentOrder = eventId != null ? eventId : ++pairState.contentOrderCounter;

    setMessages((prev: MessageRecord[]) =>
      prev.map((msg: MessageRecord) => {
        if (msg.id !== assistantMessageId) return msg;

        const newSegments = [
          ...((msg.contentSegments as unknown[]) || []),
          {
            type: 'text',
            content,
            order: currentOrder,
            ...(phase ? { phase } : {}),
          },
        ];

        const accumulatedText = ((msg.content as string) || '') + content;

        return {
          ...msg,
          contentSegments: newSegments,
          content: accumulatedText,
          contentType: 'text',
        };
      })
    );
    return true;
  } else if (finishReason) {
    // A stopped turn persists a synthetic finish_reason:"stopped" — stamp the
    // message so the per-message "⏹ Stopped" chip renders on reload, matching
    // the live finalize. Any other finish_reason just closes the message.
    const isStopped = finishReason === 'stopped';
    setMessages((prev: MessageRecord[]) =>
      prev.map((msg: MessageRecord) =>
        msg.id === assistantMessageId
          ? { ...msg, isStreaming: false, ...(isStopped ? { stopped: true } : {}) }
          : msg
      )
    );
    return true;
  }
  return false;
}

/**
 * A failed turn replays its error the way the live failure left the reply:
 * marked failed, with what it streamed still its reply and the failure kept
 * apart for the bubble's one card. Without the mark a reload shows an empty
 * reply with no Retry, though the turn's status says it failed.
 */
export function handleHistoryError({ assistantMessageId, failure, unloaded = false, setMessages }: {
  assistantMessageId: string;
  failure: StructuredError;
  unloaded?: boolean;
  setMessages: SetMessages;
}): void {
  setMessages((prev: MessageRecord[]) =>
    prev.map((msg: MessageRecord) =>
      msg.id === assistantMessageId
        ? { ...msg, isStreaming: false, error: true, structuredError: failure, ...(unloaded ? { unloaded } : {}) }
        : msg
    )
  );
}

/** Handles tool_calls events in history replay. */
export function handleHistoryToolCalls({ assistantMessageId, toolCalls, pairState, setMessages, eventId }: {
  assistantMessageId: string;
  toolCalls: ToolCallRecord[];
  pairState: PairState;
  setMessages: SetMessages;
  eventId?: number | null;
}): boolean {
  if (!toolCalls || !Array.isArray(toolCalls)) {
    return false;
  }

  toolCalls.forEach((toolCall: ToolCallRecord, toolIndex: number) => {
    const toolCallId = toolCall.id;

    if (toolCallId) {
      const currentOrder = eventId != null ? eventId + toolIndex * 0.01 : ++pairState.contentOrderCounter;

      setMessages((prev: MessageRecord[]) =>
        prev.map((msg: MessageRecord) => {
          if (msg.id !== assistantMessageId) return msg;

          const toolCallProcesses = { ...((msg.toolCallProcesses as Record<string, Record<string, unknown>>) || {}) };
          const contentSegments = [...((msg.contentSegments as Record<string, unknown>[]) || [])];
          const subagentTasks = { ...((msg.subagentTasks as Record<string, SubagentTaskRecord>) || {}) };

          // Standard tool_call segment/process
          if (!toolCallProcesses[toolCallId]) {
            contentSegments.push({
              type: 'tool_call',
              toolCallId,
              order: currentOrder,
            });

            toolCallProcesses[toolCallId] = {
              toolName: toolCall.name,
              toolCall: toolCall,
              toolCallResult: null,
              isInProgress: false, // History: already complete
              isComplete: false,
              order: currentOrder,
            };
          } else {
            toolCallProcesses[toolCallId] = {
              ...toolCallProcesses[toolCallId],
              toolName: toolCall.name,
              toolCall: toolCall,
            };
          }

          // Task spawns / resumes and RunWorkflow launches also render an
          // inline card — same derivation the live stream uses.
          const derived = deriveTaskSegment(toolCall, toolCallId, currentOrder);
          if (derived) {
            applyTaskSegment(derived, toolCallId, contentSegments, subagentTasks);
          }

          return {
            ...msg,
            contentSegments,
            toolCallProcesses,
            subagentTasks,
          };
        })
      );
    }
  });

  return true;
}

/** Handles tool_call_result events in history replay. */
export function handleHistoryToolCallResult({ assistantMessageId, toolCallId, result, pairState: _pairState, setMessages }: {
  assistantMessageId: string;
  toolCallId: string;
  result: ToolCallResultRecord;
  pairState: PairState;
  setMessages: SetMessages;
}): boolean {
  if (!toolCallId) {
    return false;
  }

  setMessages((prev: MessageRecord[]) => {
    // The message that made the call, which is an earlier one whenever a gate
    // stopped it: the resume answers it in the next turn.
    const targetId = ownerOfToolCall(prev, toolCallId) ?? assistantMessageId;
    return prev.map((msg: MessageRecord) => {
      if (msg.id !== targetId) return msg;

      const toolCallProcesses = { ...((msg.toolCallProcesses as Record<string, Record<string, unknown>>) || {}) };
      const subagentTasks = { ...((msg.subagentTasks as Record<string, SubagentTaskRecord>) || {}) };

      const isFailed = isToolResultFailure({ ...result, toolName: toolNameOf(toolCallProcesses, toolCallId) });

      if (toolCallProcesses[toolCallId]) {
        toolCallProcesses[toolCallId] = {
          ...toolCallProcesses[toolCallId],
          toolCallResult: {
            content: result.content,
            content_type: result.content_type,
            tool_call_id: result.tool_call_id,
            artifact: result.artifact,
          },
          isInProgress: false,
          isComplete: true,
          isFailed: isFailed, // Track if tool call failed
        };
      } else {
        // Orphaned tool_call_result without matching tool_calls (e.g., the result
        // of a tool that interrupted, replayed in the HITL resume pair while its
        // call sits on the bubble before). Skip silently.
        return msg;
      }

      if (subagentTasks[toolCallId]) {
        subagentTasks[toolCallId] = applyLaunchReply(subagentTasks[toolCallId], result);
      }

      return {
        ...msg,
        toolCallProcesses,
        subagentTasks,
      };
    });
  });

  return true;
}

/**
 * The copies on screen of a steer a replayed delivery names: one still
 * pending when its stream died, else one shown as failed because its POST
 * lost its answer after the server took it, as on the live stream. An entry
 * with its request key names every copy sent under it, in whatever turn it
 * waits: a failed steer sent again with its text reuses its key, and the
 * replay's row stands for that one send. One without (an older checkpoint)
 * is matched by its text within the turn: the run's queue delivers steers in
 * the order it took them, so of two copies this is the older, and a refused
 * copy never counts, the server refusing a steer before it queues it.
 */
function lostSteerCopies(prev: MessageRecord[], entry: DeliveredSteer, pairIndex: number): number[] {
  const key = entry.request_key;
  if (key) {
    return prev.flatMap((m, i) => (m.role === 'user' && !m.isHistory && !m.steeringDelivered
      && (m as { requestKey?: string }).requestKey === key ? [i] : []));
  }
  const text = entry.content.trim();
  const copy = (m: MessageRecord) => m.role === 'user' && !m.isHistory
    && m.turnIndex === pairIndex && String(m.content ?? '').trim() === text;
  const pending = prev.findIndex((m) => copy(m) && !!m.steering);
  const at = pending !== -1 ? pending : prev.findIndex((m) => copy(m) && !!m.queueError && !m.queueRefused);
  return at === -1 ? [] : [at];
}

/**
 * Handles steering_delivered events in history replay. Creates user bubble(s)
 * for each steering message, a report's caption row for each report and a
 * row for each price alert or wakeup the turn took in, then a new assistant
 * placeholder so subsequent events render in a fresh assistant bubble.
 */
export function handleHistorySteeringDelivered({
  event,
  pairIndex,
  assistantMessagesByPair,
  pairStateByPair,
  refs,
  setMessages,
  onCopyDelivered,
}: {
  event: HistoryEvent;
  pairIndex: number;
  assistantMessagesByPair: Map<number, string>;
  pairStateByPair: Map<number, PairState>;
  refs: HistorySteeringRefs;
  setMessages: SetMessages;
  /** Told the request key of each copy of this view's that the replay
   *  delivers, read off the copy still on screen. */
  onCopyDelivered?: (requestKey: string) => void;
}): void {
  const { newMessagesStartIndexRef } = refs;
  const entries = steeringDeliveredEntries(event);

  // Create user message bubble(s) for each steering message. The batch is
  // numbered within its pair, never from a process-wide counter: ids that
  // change on every replay make the same bubbles look new to every id-keyed
  // consumer (scroll observers, the minimap) each time history reattaches.
  const batchId = (pairStateByPair.get(pairIndex)?.steeringBatches ?? 0) + 1;
  for (let sIdx = 0; sIdx < entries.length; sIdx++) {
    const entry = entries[sIdx];
    const row = deliveredEntryRow(entry, (kind) => `history-steering-${kind}-${pairIndex}-${batchId}-${sIdx}`, pairIndex);
    if (!row) continue;
    const userMessage: MessageRecord = { ...row, isHistory: true };
    let delivered: string | null = null;
    setMessages((prev: MessageRecord[]) => {
      // An alert's or a wakeup's row has its notice's id live too, so a copy
      // a stream left on screen gives way to this one, in its place in the turn.
      const lost = isDeliveredSteer(entry) ? lostSteerCopies(prev, entry, pairIndex)
        : isDeliveredNotice(entry)
          ? prev.flatMap((m, i) => (m.id === row.id ? [i] : []))
          : [];
      if (lost.length > 0) delivered = (prev[lost[0]] as { requestKey?: string }).requestKey ?? null;
      const rest = lost.length === 0 ? prev : prev.filter((_, i) => !lost.includes(i));
      const idx = newMessagesStartIndexRef.current - lost.filter((i) => i < newMessagesStartIndexRef.current).length;
      const next = [...rest.slice(0, idx), userMessage, ...rest.slice(idx)];
      newMessagesStartIndexRef.current = idx + 1;
      return next;
    });
    if (delivered !== null) onCopyDelivered?.(delivered);
  }

  // Create new assistant message placeholder
  const newAssistantId = `history-assistant-steering-${pairIndex}-${batchId}`;
  assistantMessagesByPair.set(pairIndex, newAssistantId);

  // Reset pair state for the new assistant message
  pairStateByPair.set(pairIndex, {
    contentOrderCounter: 0,
    reasoningId: null,
    toolCallId: null,
    steeringBatches: batchId,
  });

  // The reply after a steer or an alert starts when the run took it in
  // (epoch seconds), not when this replay ran: the tray ages a hand-off made
  // here by it.
  const deliveredAt = typeof event.timestamp === 'number'
    ? event.timestamp
    : entries.reduce<number | null>((at, m) => (typeof m.timestamp === 'number' ? m.timestamp : at), null);
  const assistantMessage: MessageRecord = {
    id: newAssistantId,
    role: 'assistant',
    content: '',
    contentType: 'text',
    timestamp: deliveredAt != null ? new Date(deliveredAt * 1000) : new Date(),
    isStreaming: false,
    isHistory: true,
    isSteering: true,
    turnIndex: pairIndex,
    contentSegments: [],
    reasoningProcesses: {},
    toolCallProcesses: {},
  };
  setMessages((prev: MessageRecord[]) => {
    const idx = newMessagesStartIndexRef.current;
    const next = [...prev.slice(0, idx), assistantMessage, ...prev.slice(idx)];
    newMessagesStartIndexRef.current = idx + 1;
    return next;
  });
}

/** Task-artifact statuses the backend stamps onto replayed events. */
const REPLAY_TASK_STATUSES = new Set(['running', 'completed', 'cancelled', 'error']);

/**
 * Patches an inline subagent card's status from a replayed task-artifact's
 * stamped `payload.status`. The backend re-derives each task's real status at
 * replay time (init cards are otherwise reborn as "running"); this stamps that
 * truth onto the matching card.
 *
 * Cards live in the message's `subagentTasks` map keyed by the spawning Task
 * tool call id, so we match on `toolCallId` — one card per turn, so a resumed
 * task's per-turn cards are each patched by their own artifact. `taskId` is only
 * a fallback for the (contract-wise absent) case of a missing tool_call_id, and
 * then only patches resume cards whose `resumeTargetId` names the task.
 *
 * Ignores absent/unknown statuses. tool_calls is always replayed before its
 * task artifact, so the card already exists when this runs.
 */
export function handleHistoryTaskArtifactStatus({ toolCallId, taskId, status, setMessages }: {
  toolCallId: string | undefined;
  taskId: string | undefined;
  status: unknown;
  setMessages: SetMessages;
}): boolean {
  if (typeof status !== 'string' || !REPLAY_TASK_STATUSES.has(status)) {
    return false;
  }

  setMessages((prev: MessageRecord[]) =>
    prev.map((msg: MessageRecord) => {
      const subagentTasks = msg.subagentTasks as Record<string, SubagentTaskRecord> | undefined;
      if (!subagentTasks) return msg;

      // Primary: exact match on the card's own Task tool_call_id.
      if (toolCallId && subagentTasks[toolCallId]) {
        if (subagentTasks[toolCallId].status === status) return msg;
        return {
          ...msg,
          subagentTasks: {
            ...subagentTasks,
            [toolCallId]: { ...subagentTasks[toolCallId], status },
          },
        };
      }

      // Fallback: no tool_call_id — patch resume cards targeting this task.
      if (!toolCallId && taskId) {
        const agentId = `task:${taskId}`;
        let changed = false;
        const next = { ...subagentTasks };
        for (const [key, task] of Object.entries(subagentTasks)) {
          if (task.resumeTargetId === agentId && task.status !== status) {
            next[key] = { ...task, status };
            changed = true;
          }
        }
        return changed ? { ...msg, subagentTasks: next } : msg;
      }

      return msg;
    })
  );

  return true;
}

/** Handles artifact events with artifact_type "todo_update" in history replay. */
export function handleHistoryTodoUpdate({ assistantMessageId, artifactType, artifactId, payload, pairState, setMessages, eventId }: {
  assistantMessageId: string;
  artifactType: string;
  artifactId: string;
  payload: TodoPayload | null;
  pairState: PairState;
  setMessages: SetMessages;
  eventId?: number | null;
}): boolean {
  // Only handle todo_update artifacts
  if (artifactType !== 'todo_update' || !payload) {
    return false;
  }

  const { total, completed, in_progress, pending } = payload;
  const todos = Array.isArray(payload.todos) ? payload.todos : [];

  // Use artifactId as the base todoListId to track updates to the same logical todo list
  // But create a unique segmentId for each event to preserve chronological order
  const baseTodoListId = artifactId || `history-todo-list-base-${Date.now()}`;
  // Create a unique segment ID that includes timestamp to ensure chronological ordering
  const segmentId = `${baseTodoListId}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

  // Use backend event ID when available for consistent ordering across live/reconnect/replay
  const currentOrder = eventId != null ? eventId : ++pairState.contentOrderCounter;

  setMessages((prev: MessageRecord[]) => {
    const updated = prev.map((msg: MessageRecord) => {
      if (msg.id !== assistantMessageId) return msg;

      const todoListProcesses = { ...((msg.todoListProcesses as Record<string, unknown>) || {}) };
      const contentSegments = [...((msg.contentSegments as Record<string, unknown>[]) || [])];

      // Check if this segment already exists (prevent duplicates from React batching)
      const segmentExists = contentSegments.some((s: Record<string, unknown>) => s.todoListId === segmentId);
      if (segmentExists) return msg;

      // Add new segment at the current chronological position
      contentSegments.push({
        type: 'todo_list',
        todoListId: segmentId, // Use unique segmentId for this specific event
        order: currentOrder, // Use the captured order value
      });

      // Store the todo list data with the segmentId
      // If this is an update to an existing logical todo list (same artifactId),
      // we still create a new segment but can reference the base ID for data updates
      todoListProcesses[segmentId] = {
        todos,
        total: total || 0,
        completed: completed || 0,
        in_progress: in_progress || 0,
        pending: pending || 0,
        order: currentOrder,
        baseTodoListId: baseTodoListId, // Keep reference to base ID for potential future use
      };

      return {
        ...msg,
        contentSegments,
        todoListProcesses,
      };
    });

    return updated;
  });

  return true;
}

/**
 * Handles artifact events with artifact_type: "html_widget" in history replay.
 * Creates a content segment for inline rendering of interactive HTML widgets.
 */
export function handleHistoryHtmlWidget({ assistantMessageId, artifactType, artifactId, payload, pairState, setMessages, eventId }: {
  assistantMessageId: string;
  artifactType: string;
  artifactId: string;
  payload: HtmlWidgetData | null;
  pairState: PairState;
  setMessages: SetMessages;
  eventId?: number | null;
}): boolean {
  if (artifactType !== 'html_widget' || !payload) {
    return false;
  }

  const { html, title } = payload;
  const segmentId = `widget-${artifactId}`;
  const currentOrder = eventId != null ? eventId : ++pairState.contentOrderCounter;

  setMessages((prev: MessageRecord[]) => {
    const updated = prev.map((msg: MessageRecord) => {
      if (msg.id !== assistantMessageId) return msg;

      const htmlWidgetProcesses = { ...((msg.htmlWidgetProcesses as Record<string, HtmlWidgetData>) || {}) };
      const contentSegments = [...((msg.contentSegments as Record<string, unknown>[]) || [])];

      // Prevent duplicates
      const segmentExists = contentSegments.some((s: Record<string, unknown>) => s.widgetId === segmentId);
      if (segmentExists) return msg;

      contentSegments.push({
        type: 'html_widget',
        widgetId: segmentId,
        order: currentOrder,
      });

      const widgetEntry: HtmlWidgetData = {
        html: html || '',
        title: title || '',
      };
      if (payload.data) {
        widgetEntry.data = payload.data;
      }
      htmlWidgetProcesses[segmentId] = widgetEntry;

      return {
        ...msg,
        contentSegments,
        htmlWidgetProcesses,
      };
    });

    return updated;
  });

  return true;
}
