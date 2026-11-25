/**
 * A payload's record kind, on its own so the user feed, which the app entry
 * loads, reads it without pulling in the path and chart helpers.
 */
import type { ArtifactKind } from '@/types/artifacts';

/** A payload's kind, read once as it arrives; an older server sends none, and every record it knew was a file. */
export function kindOf(x: { kind?: unknown }): ArtifactKind {
  return x.kind === 'chart' ? 'chart' : 'file';
}
