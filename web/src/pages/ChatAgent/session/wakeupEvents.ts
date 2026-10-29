/**
 * Wakeups reaching the transcript: the row a wake turn opens with, the row a
 * notice delivered into a running turn draws, and whether a wake turn ended
 * quietly. Pure, so the live stream and history replay draw a wakeup as the
 * same row, and the fold reads a turn the same way live and on replay. A
 * price target set before wakeups reaches the thread in its own shape, read
 * here into the notice a price wakeup's is, so one row draws either.
 */
import type { ChatMessage, NotificationMessage } from '@/types/chat';
import type { DeliveredNotice } from '@/types/sse';
import type { OrderOutcome, WakeNotice, WakeupKind, WakeupOrder, WakeupPrice } from '@/types/wakeups';
import { createWakeupMessage } from '../hooks/utils/messageHelpers';
import { quietArg, wakeupArgListed } from './wakeupArgs';

export const SCHEDULE_WAKEUP = 'schedule_wakeup';

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function asNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function asString(value: unknown): string | null {
  return typeof value === 'string' && value ? value : null;
}

const KINDS: readonly WakeupKind[] = ['at', 'cron', 'price', 'order'];
const PRICE_KINDS: readonly WakeupPrice['kind'][] = ['above', 'below', 'pct_up', 'pct_down'];
const OUTCOMES: readonly OrderOutcome[] = [
  'filled',
  'partial',
  'partial_cancelled',
  'cancelled',
  'rejected',
  'failed',
  'partial_expired',
  'expired',
  'confirmed',
  'handed_off',
];

function coercePrice(raw: unknown): WakeupPrice | null {
  if (!isRecord(raw)) return null;
  const symbol = asString(raw.symbol);
  const kind = PRICE_KINDS.find((k) => k === raw.kind);
  const value = asNumber(raw.value);
  if (!symbol || !kind || value === null) return null;
  return {
    symbol,
    kind,
    value,
    pct_from: raw.pct_from === 'previous_close' || raw.pct_from === 'now' ? raw.pct_from : null,
    armed_price: asNumber(raw.armed_price),
    fired_price: asNumber(raw.fired_price),
    expires_at: asString(raw.expires_at),
  };
}

function coerceOrder(raw: unknown): WakeupOrder | null {
  if (!isRecord(raw)) return null;
  return {
    vendor_order_id: asString(raw.vendor_order_id) ?? '',
    summary: asString(raw.summary) ?? '',
    outcome: OUTCOMES.find((o) => o === raw.outcome) ?? null,
    filled_qty: asNumber(raw.filled_qty),
    qty: asNumber(raw.qty),
    avg_fill_price: asNumber(raw.avg_fill_price),
    currency: asString(raw.currency),
    reason: asString(raw.reason),
  };
}

/** One untrusted notice (query metadata or a delivered entry), or null when
 *  it names no wakeup. */
export function coerceWakeNotice(raw: unknown): WakeNotice | null {
  if (!isRecord(raw)) return null;
  const wakeupId = asString(raw.wakeup_id);
  const kind = KINDS.find((k) => k === raw.kind);
  if (!wakeupId || !kind) return null;
  const seq = asNumber(raw.seq) ?? 1;
  return {
    wakeup_id: wakeupId,
    short_id: asString(raw.short_id) ?? '',
    label: asString(raw.label),
    kind,
    notice: raw.notice === 'lapse' ? 'lapse' : 'fire',
    seq,
    total: asNumber(raw.total) ?? 1,
    last: raw.last === true,
    skipped: asNumber(raw.skipped) ?? 0,
    fired_at: asString(raw.fired_at) ?? '',
    timezone: asString(raw.timezone),
    cron: asString(raw.cron),
    price: coercePrice(raw.price),
    order: coerceOrder(raw.order),
  };
}

export function coerceWakeNotices(value: unknown): WakeNotice[] {
  if (!Array.isArray(value)) return [];
  return value.map(coerceWakeNotice).filter((n): n is WakeNotice => n !== null);
}

/** The notices a wake turn's query names, from its metadata's `wakeup`. The
 *  query text is written for the model and is never parsed for these. */
export function wakeMetaNotices(metadata: unknown): WakeNotice[] {
  if (!isRecord(metadata) || !isRecord(metadata.wakeup)) return [];
  return coerceWakeNotices(metadata.wakeup.notices);
}

/**
 * A price target's notice, as a target set before wakeups names it in its
 * wake turn's metadata and in a delivered entry: it hit, or it is about to
 * expire unhit. Every display field is optional, since an old wake turn may
 * name only the target.
 */
export interface MarketTargetNotice {
  target_id: string;
  notice?: 'hit' | 'expiring';
  symbol?: string;
  kind?: WakeupPrice['kind'];
  value?: number;
  pct_from?: WakeupPrice['pct_from'];
  fired_price?: number | null;
  fired_at?: string | null;
  expires_at?: string | null;
}

/** A target's notice as the notice a price wakeup's is. Its condition sits
 *  on the notice itself, and one too old to name it keeps no price. */
function targetNotice(raw: unknown): WakeNotice | null {
  if (!isRecord(raw)) return null;
  const targetId = asString(raw.target_id);
  if (!targetId) return null;
  return {
    wakeup_id: targetId,
    short_id: '',
    label: null,
    kind: 'price',
    notice: raw.notice === 'expiring' ? 'lapse' : 'fire',
    seq: 1,
    total: 1,
    last: true,
    skipped: 0,
    fired_at: asString(raw.fired_at) ?? '',
    timezone: null,
    cron: null,
    price: coercePrice(raw),
    order: null,
  };
}

/** The notices a target's wake turn names, from its metadata's
 *  `market_target`: `{notices: [...]}` or a single notice. */
function targetWakeNotices(metadata: unknown): WakeNotice[] {
  if (!isRecord(metadata) || !isRecord(metadata.market_target)) return [];
  const entry = metadata.market_target;
  const raws = Array.isArray(entry.notices) ? entry.notices : [entry];
  return raws.map(targetNotice).filter((n): n is WakeNotice => n !== null);
}

/** One id per delivered notice, the same live and on replay: the server
 *  delivers a wakeup's notice for a slot once. A target's keeps the id its
 *  row always had. */
function deliveredNoticeId(notice: WakeNotice, legacy: boolean): string {
  return legacy
    ? `market-alert-${notice.wakeup_id}:${notice.notice === 'lapse' ? 'expiring' : 'hit'}`
    : `wakeup-${notice.wakeup_id}:${notice.notice}:${notice.seq}`;
}

/** The row a notice delivered into a running turn draws, or null for an
 *  entry that names no wakeup or target. */
export function deliveredNoticeRow(entry: DeliveredNotice, timestamp: Date): NotificationMessage | null {
  const legacy = entry.wakeup === undefined;
  const notice = legacy ? targetNotice(entry.market_alert) : coerceWakeNotice(entry.wakeup);
  if (!notice) return null;
  return createWakeupMessage(
    deliveredNoticeId(notice, legacy),
    legacy ? { source: 'delivered', notices: [notice], legacy } : { source: 'delivered', notices: [notice] },
    timestamp,
  );
}

/** What woke a turn, as its query names it on replay and its first frame
 *  does on a live attach. */
export type WakeQuery = 'wakeup' | 'market_target';

function wakeQueryOf(metadata: unknown): WakeQuery | null {
  if (!isRecord(metadata)) return null;
  if (isRecord(metadata.wakeup)) return 'wakeup';
  if (isRecord(metadata.market_target)) return 'market_target';
  return null;
}

/**
 * The row a wake turn opens with, from its query row's metadata on replay,
 * whose kind the query names, or from the run's first frame on a live attach,
 * which names only what woke it. Keyed on the turn alone, which both name, so
 * the two draw one row (`withLiveWakeRow` keeps the first). Null when nothing
 * woke the turn.
 */
export function wakeTurnOpener(
  turnIndex: number,
  metadata: unknown,
  timestamp: Date,
  query: WakeQuery | null = wakeQueryOf(metadata),
): NotificationMessage | null {
  if (query === 'wakeup') {
    return createWakeupMessage(
      `history-wakeup-wake-${turnIndex}`,
      { source: 'wake', notices: wakeMetaNotices(metadata), turnIndex },
      timestamp,
    );
  }
  if (query === 'market_target') {
    return createWakeupMessage(
      `history-market-alert-wake-${turnIndex}`,
      { source: 'wake', notices: targetWakeNotices(metadata), turnIndex, legacy: true },
      timestamp,
    );
  }
  return null;
}

/**
 * A wake turn attached live: no query row comes with its stream, so its
 * opener row goes ahead of the bubble the attach opened. A row already drawn
 * for the turn stays where it is: a replay drew it while the run was still
 * going, or an earlier attach replayed the stream from its start.
 */
export function withLiveWakeRow(messages: ChatMessage[], row: ChatMessage, beforeId: string | null): ChatMessage[] {
  if (messages.some((m) => m.id === row.id)) return messages;
  const found = beforeId ? messages.findIndex((m) => m.id === beforeId) : -1;
  const at = found === -1 ? messages.length : found;
  return [...messages.slice(0, at), row, ...messages.slice(at)];
}

/** Whether a `schedule_wakeup` call asked to set or cancel any wakeup. Read
 *  from the call, not from what it achieved, by the rule the server marks a
 *  quiet turn changed by: a refused entry still counts. */
export function callChangesWakeups(args: unknown): boolean {
  return wakeupArgListed(args, 'triggers') || wakeupArgListed(args, 'cancel');
}

/** The fields of a tool call the quiet rule reads, structural so the stream's
 *  loose records and the typed messages both fit. */
interface CallLike {
  toolName?: unknown;
  toolCall?: { name?: unknown; args?: unknown } | null;
  toolCallResult?: unknown;
  order?: unknown;
}

/** A turn's bubble as the quiet rule reads it. */
interface BubbleLike {
  toolCallProcesses?: unknown;
  contentSegments?: unknown;
}

function callsOf(bubble: BubbleLike): CallLike[] {
  const calls = bubble.toolCallProcesses;
  if (!isRecord(calls)) return [];
  return Object.values(calls).filter(isRecord) as CallLike[];
}

function callName(call: CallLike): unknown {
  return call.toolName ?? call.toolCall?.name;
}

/** Whether the call's result ended the turn: the tool accepted `quiet`,
 *  which a refusal or a call it could not parse does not. */
function quietAccepted(call: CallLike): boolean {
  const result = call.toolCallResult;
  return isRecord(result) && isRecord(result.artifact) && result.artifact.quiet === true;
}

/** Whether the bubble has words in it after `order`. */
function spokeAfter(bubble: BubbleLike, order: number): boolean {
  const segments = bubble.contentSegments;
  if (!Array.isArray(segments)) return false;
  return segments.some((s) => isRecord(s) && s.type === 'text'
    && typeof s.content === 'string' && s.content.trim() !== ''
    && (typeof s.order === 'number' ? s.order : Infinity) > order);
}

/**
 * What a wake turn's agent said it checked, when the turn ended quietly and
 * changed no wakeup; null otherwise. Quiet is the turn's last tool call being
 * a `schedule_wakeup` whose result accepted `quiet`, with nothing said after
 * it: a model call after it (a background task's report re-invoking the
 * agent) means the turn spoke after all, as the server reads it. A turn that
 * set or cancelled a wakeup on the way is changed, and shows as any turn
 * does. The bubbles are the turn's own, in transcript order.
 */
export function quietTurnStatus(bubbles: readonly BubbleLike[]): string | null {
  let last: CallLike | null = null;
  let lastAt = -1;
  for (let at = 0; at < bubbles.length; at++) {
    let lastHere: CallLike | null = null;
    for (const call of callsOf(bubbles[at])) {
      if (callName(call) === SCHEDULE_WAKEUP && callChangesWakeups(call.toolCall?.args)) return null;
      const order = typeof call.order === 'number' ? call.order : -Infinity;
      const best = typeof lastHere?.order === 'number' ? lastHere.order : -Infinity;
      if (!lastHere || order >= best) lastHere = call;
    }
    if (lastHere) {
      last = lastHere;
      lastAt = at;
    }
  }
  if (!last || callName(last) !== SCHEDULE_WAKEUP || !quietAccepted(last)) return null;
  const order = typeof last.order === 'number' ? last.order : Infinity;
  for (let at = lastAt; at < bubbles.length; at++) {
    if (spokeAfter(bubbles[at], at === lastAt ? order : -Infinity)) return null;
  }
  return quietArg(last.toolCall?.args) || null;
}
