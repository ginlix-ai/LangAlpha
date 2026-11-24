/** A compaction under way: the session's summary or offload, or the one a
 *  send waits behind (`turn_held` with reason `compact`). */
export interface Compaction {
  kind: 'summarize' | 'offload' | 'held';
  /** About how many tokens the summary has written, while it writes. */
  tokens: number | null;
}

/** The compaction the thread is in, from the session's two signals of one. */
export function compactionOf(isCompacting: string | false, tokens: number | null, turnHeld: string | null): Compaction | null {
  if (isCompacting === 'offload') return { kind: 'offload', tokens: null };
  if (isCompacting) return { kind: 'summarize', tokens };
  return turnHeld === 'compact' ? { kind: 'held', tokens: null } : null;
}
