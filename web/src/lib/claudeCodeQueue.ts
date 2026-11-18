/**
 * A Claude Code turn waiting for one of its computer's CLIs: each computer
 * runs at most `cap` at once, turns and subagents together, and a turn that
 * finds them all taken waits for one (`claude_code_queue`). Read here so the
 * live line and the transcript's notice word the same frame.
 */
import type { ClaudeCodeQueueEvent } from '@/types/sse';

/** A wait under way: the computer it waits on and why. */
export interface CliQueueWait {
  /** Names this wait: a turn that starts a second CLI can wait twice. */
  waitId: string;
  /** The computer it waits on, which the way to run more opens at. */
  computerId: string;
  /** The computer's display name, null when it has none. */
  name: string | null;
  /** How many of its CLIs were running when it found them full. */
  running: number;
  /** The most the computer runs at once. */
  cap: number;
  /** The user sets `cap` (their own computer); a cloud computer's follows its size. */
  settable: boolean;
  /** The most a user may set `cap` to. */
  max: number;
}

/** A wait long enough to note, kept on the turn's reply where it happened. */
export interface CliWait extends Omit<CliQueueWait, 'running'> {
  /** Whole seconds the turn waited. */
  waitedS: number;
}

export type CliQueueSignal =
  | { signal: 'waiting'; waitId: string; wait: CliQueueWait }
  | { signal: 'admitted'; waitId: string; wait: CliWait }
  /** Let in too soon to note: the line goes and nothing is kept. */
  | { signal: 'cleared'; waitId: string };

/** A frame as it arrives: the stream's events are not checked on the way in. */
export type CliQueueFrame = { readonly [K in keyof ClaudeCodeQueueEvent]?: unknown };

const count = (v: unknown): number | null => (typeof v === 'number' && Number.isFinite(v) && v >= 0 ? v : null);

/** A `claude_code_queue` frame, or null for one this client cannot read. */
export function readCliQueue(event: CliQueueFrame): CliQueueSignal | null {
  const { signal, wait_id: waitId } = event;
  if (typeof waitId !== 'string' || !waitId) return null;
  if (signal === 'cleared') return { signal, waitId };
  if (signal !== 'waiting' && signal !== 'admitted') return null;
  const cap = count(event.cap);
  const max = count(event.max);
  if (typeof event.computer_id !== 'string' || cap === null || max === null) return null;
  const base = {
    waitId,
    computerId: event.computer_id,
    name: typeof event.name === 'string' && event.name ? event.name : null,
    cap,
    settable: event.settable === true,
    max,
  };
  // A turn waits only on a computer already running its most, so a frame
  // that leaves out how many run is running `cap`.
  if (signal === 'waiting') return { signal, waitId, wait: { ...base, running: count(event.running) ?? cap } };
  const waitedS = count(event.waited_s);
  return waitedS === null ? null : { signal, waitId, wait: { ...base, waitedS } };
}

/** Whether the user can let the computer run more at once from settings. */
export const canRunMore = (wait: Pick<CliQueueWait, 'settable' | 'cap' | 'max'>): boolean =>
  wait.settable && wait.cap < wait.max;
