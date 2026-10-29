/**
 * A `steering_delivered` event read into the transcript: the steers it names
 * marked delivered, and a row for each entry no bubble stands for. Pure, so
 * the live stream and history replay draw an entry as the same row, and the
 * live fold is pinned without a stream.
 */
import type { ChatMessage, NotificationMessage, UserMessage } from '@/types/chat';
import {
  isDeliveredNotice, isDeliveredReport, isDeliveredSteer,
  type DeliveredReport, type DeliveredSteer, type SteeringDeliveredEntry,
} from '@/types/sse';
import { deliveredNoticeRow } from '../wakeupEvents';
import { reportRow, senderProvenance } from '../provenance';

/** A delivery's entries, typed once where the loose event shapes are read. */
export function steeringDeliveredEntries(event: { messages?: readonly unknown[] }): readonly SteeringDeliveredEntry[] {
  return (event.messages ?? []) as readonly SteeringDeliveredEntry[];
}

/** Which kind of row an id is minted for: each id scheme names it. */
export type DeliveredRowKind = 'report' | 'user';

/** A row a delivery draws: a steer's words or a report's caption, or a price
 *  alert's or a wakeup's row. */
export type DeliveredRow = UserMessage | NotificationMessage;

/**
 * The row an entry is drawn as: a report's caption, a price alert's or a
 * wakeup's row, or the steer's words labelled with the agent that sent them
 * or the chat app they came from. Delivered steering, so it opens no turn and
 * offers no edit. Null for an entry with neither, which is what a share
 * leaves of a report. An alert's or a wakeup's id is its notice's, not
 * minted: the server delivers a notice once, so the live stream and every
 * replay draw it under one id.
 */
export function deliveredEntryRow(
  entry: DeliveredSteer | DeliveredReport,
  rowId: (kind: DeliveredRowKind) => string,
  turnIndex: number | undefined,
): UserMessage | null;
export function deliveredEntryRow(
  entry: SteeringDeliveredEntry,
  rowId: (kind: DeliveredRowKind) => string,
  turnIndex: number | undefined,
): DeliveredRow | null;
export function deliveredEntryRow(
  entry: SteeringDeliveredEntry,
  rowId: (kind: DeliveredRowKind) => string,
  turnIndex: number | undefined,
): DeliveredRow | null {
  const timestamp = entry.timestamp ? new Date(entry.timestamp * 1000) : new Date();
  if (isDeliveredNotice(entry)) return deliveredNoticeRow(entry, timestamp);
  if (isDeliveredReport(entry)) {
    return reportRow(entry.report_source, {
      id: rowId('report'),
      timestamp,
      steeringDelivered: true,
      ...(turnIndex !== undefined ? { turnIndex } : {}),
    });
  }
  if (!entry.content) return null;
  const provenance = senderProvenance({ origin: entry.origin, platform: entry.platform });
  return {
    id: rowId('user'),
    role: 'user',
    content: entry.content,
    contentType: 'text',
    timestamp,
    isStreaming: false,
    steeringDelivered: true,
    turnIndex,
    ...(provenance ? { provenance } : {}),
    ...(entry.request_key ? { requestKey: entry.request_key } : {}),
  };
}

/**
 * Marks delivered the steers a delivery names. An entry that carries its
 * request key names the one send made under it, wherever it waits: in the
 * turn it was sent to, or under an earlier turn whose run ended before
 * reading it, which a later turn takes in. A steer shown failed and sent
 * again with its text reuses its key, so the key can stand on two bubbles of
 * one send: the delivery claims the one still waiting, else one failed with
 * no refusal, and the others go. The claimed bubble joins the delivering
 * turn, where replay draws it. An entry with no key (an older checkpoint)
 * is matched by its text within the steered turn: it claims the oldest
 * steer still waiting with it, then the oldest failed with no refusal, since
 * the server may have taken that one and lost only the POST's answer. A
 * refused steer was never queued, so no keyless delivery is its. Returns the
 * entries no steer claimed too: one batch can carry steers sent from another
 * tab or a channel beside this view's own.
 */
function claimDeliveredSteers(
  messages: ChatMessage[],
  delivered: readonly DeliveredSteer[],
  turn: number | undefined,
): { messages: ChatMessage[]; claimed: { prior: ChatMessage; at: number }[]; dropped: ChatMessage[]; unclaimed: DeliveredSteer[] } {
  const claimedAt: number[] = [];
  const claimedEntries = new Set<DeliveredSteer>();
  const dropped = new Set<number>();
  let next = messages;
  const claim = (i: number, entry: DeliveredSteer, joinTurn: boolean) => {
    const m = next[i] as UserMessage & { queueError?: unknown };
    const claimed: typeof m = { ...m, steering: false, steeringDelivered: true };
    delete claimed.queueError;
    if (joinTurn && turn !== undefined) claimed.turnIndex = turn;
    if (next === messages) next = [...messages];
    next[i] = claimed;
    claimedAt.push(i);
    claimedEntries.add(entry);
  };
  const keyless: DeliveredSteer[] = [];
  for (const entry of delivered) {
    if (!entry.request_key) {
      keyless.push(entry);
      continue;
    }
    const copies: number[] = [];
    next.forEach((m, i) => {
      if (m.role === 'user' && !m.steeringDelivered && !dropped.has(i)
        && (m as { requestKey?: string }).requestKey === entry.request_key) copies.push(i);
    });
    if (copies.length === 0) continue;
    const copy = (i: number) => next[i] as UserMessage & { queueError?: unknown };
    const at = copies.find((i) => !!copy(i).steering)
      ?? copies.find((i) => !!copy(i).queueError && !copy(i).queueRefused)
      ?? copies[0];
    for (const i of copies) if (i !== at) dropped.add(i);
    claim(at, entry, true);
  }
  const texts = keyless.map((q) => q.content.trim());
  for (const waiting of [true, false]) {
    for (let i = 0; i < next.length && texts.length > 0; i++) {
      const m = next[i] as ChatMessage & { queueError?: unknown };
      if (m.role !== 'user' || dropped.has(i) || (turn !== undefined && m.turnIndex !== turn)) continue;
      if (waiting ? !m.steering : (m.isHistory || !m.queueError || m.queueRefused || m.steeringDelivered)) continue;
      const at = texts.indexOf(String(m.content ?? '').trim());
      if (at === -1) continue;
      texts.splice(at, 1);
      claim(i, keyless.splice(at, 1)[0], false);
    }
  }
  const below = (i: number) => [...dropped].filter((d) => d < i).length;
  return {
    messages: dropped.size > 0 ? next.filter((_, i) => !dropped.has(i)) : next,
    claimed: claimedAt.map((i) => ({ prior: messages[i], at: i - below(i) })),
    dropped: [...dropped].map((i) => messages[i]),
    unclaimed: delivered.filter((e) => !claimedEntries.has(e)),
  };
}

/** The first steer still waiting in the turn, or -1. */
function firstWaitingSteer(messages: readonly ChatMessage[], turnIndex: number | undefined): number {
  return messages.findIndex((m) =>
    m.role === 'user' && !!m.steering && (turnIndex === undefined || m.turnIndex === turnIndex));
}

/**
 * Adds the bubble a delivery opens for the model's next reply. It goes above
 * a steer still waiting in the turn: the model has yet to read that steer, and
 * replay draws it after this reply.
 */
export function withContinuation(
  messages: ChatMessage[],
  bubble: ChatMessage,
  turnIndex: number | undefined,
): ChatMessage[] {
  const at = firstWaitingSteer(messages, turnIndex);
  return at === -1 ? [...messages, bubble] : [...messages.slice(0, at), bubble, ...messages.slice(at)];
}

export interface SteeringDelivery {
  messages: ChatMessage[];
  /** The banners the failed steers it delivered raised, which no failed steer holds now. */
  clearedErrors: unknown[];
  /** The request keys of this view's steers it delivered: the server holds
   *  each of them now, so a later send of its text is new. */
  settledKeys: string[];
}

/**
 * Folds a live delivery into the transcript. A steer of this view's it names
 * gets no copy; the rest, which this view never held (sent elsewhere) or lost
 * on a reconnect, go after what it delivers, above any steer still waiting in
 * the turn, which the model has yet to read. Reports, alerts and wakeups go
 * ahead of the user's words, as the model reads them: before the first steer
 * this delivers, else before the first still waiting in the turn (sent after
 * the run took its queue, and replay draws the report above it too), else at
 * the end. An alert or a wakeup already on screen is not drawn again: its id
 * is its notice's, so a stream re-delivering what a replay drew finds it.
 */
export function foldSteeringDelivery(
  prev: ChatMessage[],
  entries: readonly SteeringDeliveredEntry[],
  turnIndex: number | undefined,
  rowId: (kind: DeliveredRowKind) => string,
): SteeringDelivery {
  const steers = entries.filter(isDeliveredSteer);
  const { messages, claimed, dropped, unclaimed } = claimDeliveredSteers(prev, steers, turnIndex);
  const errorOf = (m: ChatMessage) => (m as { queueError?: unknown }).queueError;
  const clearedErrors = [...claimed.map((c) => c.prior), ...dropped].map(errorOf)
    .filter((e) => e !== undefined && !messages.some((m) => errorOf(m) === e));
  const settledKeys = claimed.map((c) => (c.prior as { requestKey?: string }).requestKey)
    .filter((key): key is string => !!key);
  const rows = (list: readonly SteeringDeliveredEntry[]) => list
    .map((e) => deliveredEntryRow(e, rowId, turnIndex))
    .filter((m): m is DeliveredRow => m !== null && !messages.some((shown) => shown.id === m.id));
  const ahead = rows(entries.filter((e) => !isDeliveredSteer(e)));
  const added = rows(unclaimed);
  if (ahead.length === 0 && added.length === 0) return { messages, clearedErrors, settledKeys };
  const waiting = firstWaitingSteer(messages, turnIndex);
  // A steer claimed from under an earlier turn stays where it was drawn, so
  // it places nothing.
  const inTurn = claimed
    .filter((c) => turnIndex === undefined || (c.prior as UserMessage).turnIndex === turnIndex)
    .map((c) => c.at);
  const at = inTurn.length > 0 ? Math.min(...inTurn) : waiting === -1 ? messages.length : waiting;
  const end = waiting === -1 ? messages.length : Math.max(waiting, at);
  return {
    messages: [...messages.slice(0, at), ...ahead, ...messages.slice(at, end), ...added, ...messages.slice(end)],
    clearedErrors,
    settledKeys,
  };
}
