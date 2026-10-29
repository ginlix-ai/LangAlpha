import type { AutomationExecution } from '@/types/automation';

export const IDLE_POLL_MS = 30_000;
/** A run takes seconds to minutes, and its row has to move from the loader
 *  to the result without a reload. */
export const LIVE_POLL_MS = 5_000;

/** How long after a run ends its delivery record can still change. The
 *  server asks the messaging service again to end a run while it gets no
 *  answer or rows still being posted: up to three asks 2 and 5 seconds
 *  apart, or five on an outbox job's backoff, each allowed 15 seconds, which
 *  comes to about two minutes at most. Past this the record stands as it
 *  reads. */
export const DELIVERY_SETTLE_MS = 3 * 60_000;

/** How often a list refetches. Only something starting or running polls
 *  fast (`isRunLive`, `isAutomationRunning`), or a run whose delivery may
 *  still change (`isDeliverySettling`): a wait can last half an hour, and the
 *  user feed already announces both its start and its end. */
export function pollMs(live: boolean): number {
  return live ? LIVE_POLL_MS : IDLE_POLL_MS;
}

/**
 * Whether a run that has ended may still get a later delivery record, which
 * nothing announces, so the list showing it has to poll for it. The record is
 * written after the run ends and may be replaced: for a while the run reads
 * finished with no record yet, or with rows that read failed ("Delivery
 * couldn't be confirmed.") until a later answer says where they landed. A
 * record whose every row landed is final, and so is any record once
 * `DELIVERY_SETTLE_MS` has passed. A run with no record waits for one only
 * when its automation has somewhere to deliver (`awaitsRecord`).
 */
export function isDeliverySettling(run: AutomationExecution, awaitsRecord: boolean, now = Date.now()): boolean {
  if (!run.completed_at) return false;
  // NaN, from a timestamp that doesn't parse, settles nothing.
  if (!(now - Date.parse(run.completed_at) < DELIVERY_SETTLE_MS)) return false;
  const attempts = run.delivery_result;
  return attempts ? attempts.some((d) => !d.success) : awaitsRecord;
}
