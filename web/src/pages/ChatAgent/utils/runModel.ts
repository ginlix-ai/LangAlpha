import type { RunModel } from '@/types/chat';

/** A model as the wire carries it (`{id, provider, effort?}`) on a subagent
 *  run or a hand-off, or undefined for anything else. A run from before the
 *  server sent it has none, and an effort that is not a string is dropped
 *  rather than the model. */
export function parseRunModel(value: unknown): RunModel | undefined {
  if (!value || typeof value !== 'object') return undefined;
  const { id, provider, effort } = value as { id?: unknown; provider?: unknown; effort?: unknown };
  if (typeof id !== 'string' || !id || typeof provider !== 'string' || !provider) return undefined;
  return typeof effort === 'string' && effort ? { id, provider, effort } : { id, provider };
}
