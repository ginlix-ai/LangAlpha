/**
 * What a comment record is on, read off any payload that names one: a
 * workspace file, or a market chart by its id `SYMBOL:timeframe`, which is
 * what its chart layers are keyed by too.
 */
import { chartParts } from '@/lib/bars/chartId';
import { INTERVAL_LABEL } from '@/lib/bars/chartConstants';
import { queryKeys } from '@/lib/queryKeys';
import { workspaceRelativePath } from '@/pages/ChatAgent/utils/agentPaths';
import type { ArtifactKind, ArtifactTarget } from '@/types/artifacts';

export { kindOf } from '@/lib/artifactKind';

/**
 * A target as its record is read: a file's path in the spelling the file
 * routes answer for, so two spellings share one record; a chart's id as given.
 */
export function targetFor(kind: ArtifactKind, path: string, root?: string | null): ArtifactTarget {
  return { kind, path: kind === 'file' ? workspaceRelativePath(path, root) : path };
}

/** The read of a target's record by its workspace, which the user feed's events reach. */
export function targetQueryKey(workspaceId: string, target: ArtifactTarget) {
  return target.kind === 'chart'
    ? queryKeys.artifacts.byChart(workspaceId, target.path)
    : queryKeys.artifacts.byPath(workspaceId, target.path);
}

export function ensuredQueryKey(workspaceId: string, target: ArtifactTarget) {
  return target.kind === 'chart'
    ? queryKeys.artifacts.ensuredChart(workspaceId, target.path)
    : queryKeys.artifacts.ensured(workspaceId, target.path);
}

/** A chart as a line names it: "NVDA · 1D". */
export function chartTitle(chartId: string): string {
  const parts = chartParts(chartId);
  if (!parts) return chartId;
  return `${parts.symbol} · ${INTERVAL_LABEL[parts.timeframe] ?? parts.timeframe}`;
}
