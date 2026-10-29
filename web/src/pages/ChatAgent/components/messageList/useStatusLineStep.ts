import { useEffect, useRef, useState } from 'react';
import { type ActivityItem, type PreparingCall, isRunning } from './activityTypes';
import { isOpenThought, type LineStep, type Step } from './activityLayout';
import { STATUS_HOLD_MS } from './liveZoneTiming';

/**
 * Which step the status line names: the newest step in flight, held on the
 * line once it lands until a newer one takes its place. A newer step waits out
 * STATUS_HOLD_MS before it does, so a call that returns at once never flashes
 * past. Only steps a block has to keep track of count as in flight; an open
 * thought keeps its own row.
 */
export function useStatusLineStep(
  timeline: ActivityItem[],
  calls: readonly PreparingCall[],
  keys: { forItem: (item: ActivityItem) => string; forCall: (call: PreparingCall) => string },
  liveReasoningOpen: boolean,
  enabled: boolean,
): { line: LineStep | null; inFlight: boolean } {
  const steps = new Map<string, Step>();
  const inFlight: string[] = [];
  for (const item of timeline) {
    const key = keys.forItem(item);
    steps.set(key, { kind: 'item', item });
    if (isRunning(item) && !isOpenThought(item, liveReasoningOpen)) inFlight.push(key);
  }
  for (const call of calls) {
    const key = keys.forCall(call);
    steps.set(key, { kind: 'call', call });
    inFlight.push(key);
  }
  const candidate = inFlight.at(-1) ?? null;
  const [shown, setShown] = useState<string | null>(null);
  const lineKey = shown !== null && steps.has(shown) ? shown : candidate;
  // Keep the step the line shows, so it stays there once it stops running.
  if (enabled && lineKey !== shown) setShown(lineKey);
  const swapTo = enabled && candidate !== null && candidate !== lineKey ? candidate : null;
  // When each step in flight was first seen. The hold is counted from there,
  // so a call that has been running all along takes the line the moment the
  // one on it lands, and only a call newer than the hold waits.
  const flightSince = useRef(new Map<string, number>());
  const inFlightKeys = inFlight.join('\n');
  useEffect(() => {
    if (!enabled) return;
    const now = performance.now();
    const since = flightSince.current;
    const flying = new Set(inFlightKeys ? inFlightKeys.split('\n') : []);
    for (const key of since.keys()) if (!flying.has(key)) since.delete(key);
    for (const key of flying) if (!since.has(key)) since.set(key, now);
    if (swapTo === null) return;
    const timer = setTimeout(() => setShown(swapTo), Math.max(0, STATUS_HOLD_MS - (now - (since.get(swapTo) ?? now))));
    return () => clearTimeout(timer);
  }, [enabled, inFlightKeys, swapTo]);
  const step = lineKey !== null ? steps.get(lineKey) : undefined;
  return {
    line: lineKey !== null && step ? { key: lineKey, step, running: inFlight.includes(lineKey) } : null,
    inFlight: inFlight.length > 0,
  };
}
