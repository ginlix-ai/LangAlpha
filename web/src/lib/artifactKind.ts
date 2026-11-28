/**
 * What a record kind settles about how its target is named, on its own so the
 * user feed, which the app entry loads, reads it without pulling in the path
 * and chart helpers.
 */
import type { ArtifactKind } from '@/types/artifacts';

/**
 * Each kind's name for its target on the records route (`?path=`, `?chart=`,
 * `?drive=`), which its query keys use too.
 */
export const ARTIFACT_KINDS = {
  file: { param: 'path' },
  chart: { param: 'chart' },
  drive: { param: 'drive' },
} as const satisfies Record<ArtifactKind, { param: string }>;

export type TargetParamName = (typeof ARTIFACT_KINDS)[ArtifactKind]['param'];

/** A payload's kind, read once as it arrives; an older server sends none, and every record it knew was a file. */
export function kindOf(x: { kind?: unknown }): ArtifactKind {
  return typeof x.kind === 'string' && Object.hasOwn(ARTIFACT_KINDS, x.kind) ? (x.kind as ArtifactKind) : 'file';
}

/**
 * A target as a key names it beside others of its workspace: a file by its
 * bare path, as keys always have, and any other kind by an object, so no
 * file's path can spell the same key.
 */
export function targetKeyPart(target: { kind: ArtifactKind; path: string }): string | Partial<Record<TargetParamName, string>> {
  return target.kind === 'file' ? target.path : { [ARTIFACT_KINDS[target.kind].param]: target.path };
}
