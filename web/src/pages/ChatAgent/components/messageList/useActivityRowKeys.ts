import { useState } from 'react';
import type { ActivityItem, PreparingCall } from './activityTypes';

interface RowKeys {
  items: ActivityItem[];
  calls: readonly PreparingCall[];
  /** The row key of each call still streaming its arguments, by pending key. */
  callKeys: Map<string, string>;
  /** Landed calls that carry on a preparing row under that row's key. */
  byId: Map<string, string>;
  nextSlot: number;
}

const itemKey = (item: ActivityItem) => `${item.type}-${item.id}`;

function sameCalls(a: readonly PreparingCall[], b: readonly PreparingCall[]): boolean {
  return a.length === b.length && a.every((call, i) => call.pendingKey === b[i].pendingKey && call.toolCallId === b[i].toolCallId);
}

function advance(prev: RowKeys, items: ActivityItem[], calls: readonly PreparingCall[]): RowKeys {
  const known = new Set(prev.items.map((item) => item.id));
  const streaming = new Set(calls.map((call) => call.pendingKey));
  const byId = new Map(prev.byId);
  // Calls that stopped streaming, in the order they were written. Each one
  // that landed hands its row to its item: by id when the producer named the
  // call, else by order.
  const ended = prev.calls.filter((call) => !streaming.has(call.pendingKey));
  for (const item of items) {
    if (item.type !== 'tool_call' || known.has(item.id)) continue;
    const own = ended.findIndex((call) => call.toolCallId === item.id);
    const at = own !== -1 ? own : ended.findIndex((call) => !call.toolCallId);
    if (at === -1) continue;
    const key = prev.callKeys.get(ended[at].pendingKey);
    if (key && key !== itemKey(item)) byId.set(item.id, key);
    ended.splice(at, 1);
  }
  let nextSlot = prev.nextSlot;
  const callKeys = new Map<string, string>();
  for (const call of calls) {
    // A named call takes the key its item will have, so the row carries on by
    // itself; an anonymous one holds a slot until the hand-off above.
    const key = prev.callKeys.get(call.pendingKey)
      ?? (call.toolCallId ? `tool_call-${call.toolCallId}` : `preparing-${nextSlot++}`);
    callKeys.set(call.pendingKey, key);
  }
  return { items, calls, callKeys, byId, nextSlot };
}

const NO_CALLS: readonly PreparingCall[] = [];

/** Row keys that let a call's row carry on from its first argument chunk to
 *  its result, one row per call even when several stream at once, instead of
 *  exiting a placeholder over the row that replaces it. */
export function useActivityRowKeys(items: ActivityItem[], calls: readonly PreparingCall[] = NO_CALLS) {
  const [previous, setPrevious] = useState<RowKeys>(() =>
    advance({ items: [], calls: NO_CALLS, callKeys: new Map(), byId: new Map(), nextSlot: 0 }, items, calls));
  let current = previous;
  const identitiesChanged = items.length !== previous.items.length
    || items.some((item, index) => item.id !== previous.items[index].id);
  if (identitiesChanged || !sameCalls(calls, previous.calls)) {
    current = advance(previous, items, calls);
    setPrevious(current);
  }
  return {
    forCall: (call: PreparingCall) => current.callKeys.get(call.pendingKey) ?? `preparing-${call.pendingKey}`,
    forItem: (item: ActivityItem) => current.byId.get(item.id) ?? itemKey(item),
  };
}
