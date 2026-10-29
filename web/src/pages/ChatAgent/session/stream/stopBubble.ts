import type { StructuredError } from '@/utils/rateLimitError';
import { failAssistantMessage, finalizeAssistantMessage } from './finalizeMessage';
import { continuationChain, holdsContent } from './continuedBubbles';
import { withoutPending } from './toolCallChunks';
import type { MessageRecord, SetMessages } from '../../hooks/utils/types';
import type { StreamRefs } from '../streamRefs';

// Always-live tools (TaskOutput, WebFetch) spin off `isInProgress` whatever
// `isStreaming` says, so their rows fold here too.
function closeToolRows(msg: MessageRecord): Record<string, MessageRecord> {
  const tp: Record<string, MessageRecord> = {};
  for (const [id, val] of Object.entries((msg.toolCallProcesses as Record<string, MessageRecord>) || {})) {
    tp[id] = val.isInProgress ? { ...val, isInProgress: false, isComplete: true } : val;
  }
  return tp;
}

// Closes every open thought as handleReasoningSignal closes the current one,
// since a reconnect starts with no current thought and so cannot name one the
// dropped stream left open.
function closeThoughts(msg: MessageRecord, completedAt: number, fromBacklog: boolean): Record<string, MessageRecord> {
  const rp: Record<string, MessageRecord> = {};
  for (const [id, val] of Object.entries((msg.reasoningProcesses as Record<string, MessageRecord>) || {})) {
    if (!val.isReasoning) {
      rp[id] = val;
      continue;
    }
    const startedAt = val._startedAt as number | undefined;
    rp[id] = {
      ...val,
      isReasoning: false,
      reasoningComplete: true,
      _completedAt: completedAt,
      elapsedMs: val.elapsedMs ?? (startedAt && !fromBacklog ? completedAt - startedAt : undefined),
    };
  }
  return rp;
}

/**
 * Settles the bubbles of a turn whose run ended where its stream could not
 * close what it left open (continuedBubbles.ts). The end lands on the bubble
 * that holds the turn's content: the stream's own, or when a reconnect's
 * bubble holds nothing yet, the nearest bubble it continues that does, and the
 * first of them when none does. A stop or a failure cuts the model call off
 * with none of the closes a finished one sends, so the rows it left open close
 * on every bubble of the chain, thoughts included, which after a drop no
 * stream can close any more; the partial tool call it was writing is dropped,
 * since it never ran.
 */
function settleTurnEnd(
  { assistantMessageId, refs, setMessages }: { assistantMessageId: string; refs: StreamRefs; setMessages: SetMessages },
  settle: (target: MessageRecord) => MessageRecord,
): void {
  // Read on arrival, as handleReasoningSignal does: an end the reconnect's
  // backlog carried folds its thoughts rather than replaying their close.
  const fromBacklog = !!refs.isReconnect;
  const closedAt = fromBacklog ? 1 : Date.now();
  refs.currentReasoningIdRef.current = null;
  setMessages((prev: MessageRecord[]) => {
    const byId = new Map(prev.map((msg) => [msg.id as string, msg]));
    const chain = continuationChain((id) => byId.get(id), assistantMessageId);
    if (chain.length === 0) return prev;
    const target = chain.find(holdsContent) ?? chain[chain.length - 1];
    const settled = new Map(chain.map((msg): [string, MessageRecord] => {
      const closed = withoutPending({
        ...msg,
        toolCallProcesses: closeToolRows(msg),
        reasoningProcesses: closeThoughts(msg, closedAt, fromBacklog),
      });
      if (msg !== target) return [msg.id as string, msg.isStreaming ? finalizeAssistantMessage(closed, 'disconnected') : closed];
      return [msg.id as string, settle(closed)];
    }));
    return prev.map((msg) => settled.get(msg.id as string) ?? msg);
  });
}

/**
 * Settles a turn whose run was stopped, wherever the stop came from: this
 * tab's Stop, or a cancel from elsewhere that the stream reports as its
 * `run_end`, which after a drop reaches a reconnect's bubble that may hold
 * none of the reply.
 */
export function settleStoppedBubble(at: { assistantMessageId: string; refs: StreamRefs; setMessages: SetMessages }): void {
  settleTurnEnd(at, (msg) => finalizeAssistantMessage(msg, 'stopped'));
}

/**
 * Settles a turn whose run failed. `failure` is the card the bubble shows,
 * none when the failure is shown in the banner. An `internal` failure on a
 * turn that streamed nothing settles it without `error`, so the orphan skip
 * hides it and the banner is the whole report. A hidden opener's reply is the
 * exception: it is all its turn shows, and hidden it would leave the banner
 * with no Retry and no message to send again, so it fails with the card a
 * reload of the failed turn draws.
 */
export function settleFailedBubble({ failure, internal = false, ...at }: {
  assistantMessageId: string;
  refs: StreamRefs;
  setMessages: SetMessages;
  failure?: StructuredError;
  internal?: boolean;
}): void {
  settleTurnEnd(at, (msg) => {
    if (!internal || holdsContent(msg)) return failAssistantMessage(msg, failure);
    if (msg.hiddenOpener === true) return failAssistantMessage(msg, { message: '', kind: 'internal' });
    return finalizeAssistantMessage(msg, 'disconnected');
  });
}
