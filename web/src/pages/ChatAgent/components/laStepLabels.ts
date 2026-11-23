/**
 * The words an `la` step reads as: a past-tense title and the object's name
 * once it succeeded, a present-tense phrase while it runs, in the vocabulary
 * the path-aware tools use. The object is named only where its name is known
 * for that exact id, never guessed from what the command printed.
 */
import type { TFn } from '@/lib/elapsed';
import { workspaceLabel } from '@/hooks/useWorkspaceLabel';
import { laLookupId, type LaLookup, type LaStep, type LaStepKind } from '../utils/laCommand';

/** Names the app already holds, under `laNameKey`. A miss is absent, never a guess. */
export type LaNames = ReadonlyMap<string, string>;

export function laNameKey({ type, id }: LaLookup): string {
  return `${type}:${id}`;
}

/** English, for callers with no `t` (tests, logs); the catalogs hold the same. */
const EN: Record<LaStepKind, readonly [done: string, active: string]> = {
  listWorkspaces: ['Listed workspaces', 'listing workspaces...'],
  createWorkspace: ['Created workspace', 'creating workspace...'],
  archiveComputer: ["Archived workspace's computer", "archiving workspace's computer..."],
  deleteWorkspace: ['Deleted workspace', 'deleting workspace...'],
  listThreads: ['Listed threads', 'listing threads...'],
  showThread: ['Checked thread', 'checking thread...'],
  readThread: ['Read thread', 'reading thread...'],
  startThread: ['Started thread', 'starting thread...'],
  sendToThread: ['Sent to thread', 'sending to thread...'],
  followThread: ['Followed thread', 'following thread...'],
  retryThread: ["Retried thread's turn", "retrying thread's turn..."],
  cancelThread: ["Cancelled thread's turn", "cancelling thread's turn..."],
  archiveThread: ['Archived thread', 'archiving thread...'],
  deleteThread: ['Deleted thread', 'deleting thread...'],
  listComputers: ['Listed computers', 'listing computers...'],
  listModels: ['Listed models', 'listing models...'],
  listAutomations: ['Listed automations', 'listing automations...'],
};

export function laCompletedTitle(step: LaStep, t?: TFn): string {
  return t ? t(`toolArtifact:completed.la.${step.kind}`) : EN[step.kind][0];
}

export function laActiveLabel(step: LaStep, t?: TFn): string {
  return t ? t(`toolArtifact:inProgress.la.${step.kind}`) : EN[step.kind][1];
}

/**
 * The step's object for its pill: a name the command gave (a new workspace's,
 * or `-w` by name), Home under the name the app gives it, or the name the
 * app holds for the id it gave. Null otherwise, and the title stands alone.
 */
export function laStepObject(step: LaStep, t?: TFn, names?: LaNames): string | null {
  const ref = step.target?.type === 'workspace' ? step.target.ref : null;
  if (ref && 'home' in ref) return t ? workspaceLabel(t, { is_home: true }) : 'Chief of Staff';
  if (ref && 'name' in ref) return ref.name;
  const lookup = laLookupId(step);
  return (lookup && names?.get(laNameKey(lookup))) || null;
}
