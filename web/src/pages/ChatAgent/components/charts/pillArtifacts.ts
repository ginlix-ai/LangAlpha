/**
 * Artifact types drawn as a one-line pill rather than a full-width card.
 * Consecutive pills share a wrapping row; every other card keeps its own line.
 * A new pill-style card opts in by adding its type here.
 */
export const PILL_ARTIFACT_TYPES: ReadonlySet<string> = new Set(['message_delivery']);

export function isPillArtifactType(type: unknown): boolean {
  return typeof type === 'string' && PILL_ARTIFACT_TYPES.has(type);
}

/** The row consecutive pills sit in; it wraps on narrow screens. */
export const PILL_ROW_CLASS = 'flex flex-wrap gap-3';
