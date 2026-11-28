/**
 * What a comment record is on, read off any payload that names one: a
 * workspace file, a market chart by its id `SYMBOL:timeframe`, which is what
 * its chart layers are keyed by too, or a Drive file by its entry id.
 */
import { chartParts } from '@/lib/bars/chartId';
import { INTERVAL_LABEL } from '@/lib/bars/chartConstants';
import { isAgentNotesPath, parseAgentPath, workspaceRelativePath } from '@/pages/ChatAgent/utils/agentPaths';
import { AGENT_SYSTEM_DIRS, INTERNAL_DIR } from '@/pages/ChatAgent/utils/agentPaths.generated';
import type { ArtifactKind, ArtifactTarget } from '@/types/artifacts';

export { kindOf } from '@/lib/artifactKind';

/**
 * A target as its record is read: a file's path in the spelling the file
 * routes answer for, so two spellings share one record; a chart's id and a
 * Drive file's entry id as given.
 */
export function targetFor(kind: ArtifactKind, path: string, root?: string | null): ArtifactTarget {
  return { kind, path: kind === 'file' ? workspaceRelativePath(path, root) : path };
}

/** The agent's own folders, where the server keeps no record. */
const MACHINERY: ReadonlySet<string> = new Set([...AGENT_SYSTEM_DIRS, INTERNAL_DIR]);

/**
 * Whether a target may carry comments. The server keeps no record for a file
 * in the agent's own folders or the workspace's notes file (`may_be_artifact`
 * in `served_paths.py`, read from the same layout) and answers 400, so a
 * panel that offered comments there only showed them failing to load.
 */
export function commentable(target: ArtifactTarget, root?: string | null): boolean {
  if (target.kind !== 'file') return true;
  const parts = parseAgentPath(target.path, root);
  const first = parts.path.replace(/^\/+/, '').split('/', 1)[0];
  return !MACHINERY.has(first) && !isAgentNotesPath(parts);
}

/** A chart as a line names it: "NVDA · 1D". */
export function chartTitle(chartId: string): string {
  const parts = chartParts(chartId);
  if (!parts) return chartId;
  return `${parts.symbol} · ${INTERVAL_LABEL[parts.timeframe] ?? parts.timeframe}`;
}
