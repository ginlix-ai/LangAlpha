/** The forms an answer with substance can take, which
 *  `agent_preference.output_format` ranks, most preferred first. A report is
 *  the visual answer's long form: the agent picks between it and a widget by
 *  the answer, so the two are one rank. */
export const OUTPUT_FORMATS = ['visual', 'markdown', 'text'] as const;
export type OutputFormat = (typeof OUTPUT_FORMATS)[number];

const isOutputFormat = (value: unknown): value is OutputFormat =>
  (OUTPUT_FORMATS as readonly unknown[]).includes(value);

/** A first choice with the rest after it, each in the order the server's
 *  migration 096 wrote for that choice: the neighbours on the effort curve
 *  first. */
export function rankingFrom(first: OutputFormat): OutputFormat[] {
  if (first === 'visual') return ['visual', 'markdown', 'text'];
  if (first === 'text') return ['text', 'markdown', 'visual'];
  return ['markdown', 'text', 'visual'];
}

/**
 * The ranking an `agent_preference` holds, or null when the agent decides.
 * A row written before migration 096 (a cached read, a draft) holds the two
 * keys the ranking replaced, read here as 096 reads them: `output_format:
 * "html"` or `deliverable: "visual"` ranks the visual answer first, and
 * `deliverable: "text"` the chat.
 */
export function readOutputFormats(agentPreference: unknown): OutputFormat[] | null {
  const prefs = (typeof agentPreference === 'object' && agentPreference !== null ? agentPreference : {}) as Record<string, unknown>;
  const stored = prefs.output_format;
  // A list with anything else in it is the user's own words, read as such.
  if (Array.isArray(stored)) return stored.length > 0 && stored.every(isOutputFormat) ? [...new Set(stored)] : null;
  // So is any other value.
  if (stored != null && stored !== 'html' && stored !== 'markdown') return null;
  if (stored === 'html' || prefs.deliverable === 'visual') return rankingFrom('visual');
  if (prefs.deliverable === 'text') return rankingFrom('text');
  return null;
}
