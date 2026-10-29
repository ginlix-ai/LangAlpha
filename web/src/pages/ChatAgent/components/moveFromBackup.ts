/**
 * When a move that could not copy off a machine the user brought is offered
 * again from the workspace's backup.
 *
 * A machine that does not answer fails the copy with `source_unavailable`. If
 * it is gone for good, the backup is the only copy left, and a move
 * `from_backup` restores it onto the target without contacting the machine.
 * Only a machine the user brought is offered this: a hosted one that cannot
 * start is the service's to bring back, and its workspace is not lost.
 */
import type { Computer } from '@/types/api';

import { takesWorkspaces } from '../utils/computerStatus';

/** A move the user asked for and how it went: refused up front, or failed later. */
export interface MoveAttempt {
  /** The refusal's or the failure's code; null when it carried none. */
  code: string | null;
  sourceId: string | null;
  targetId: string | null;
  fromBackup: boolean;
  /** Ended before this dialog opened: by now the machine may answer again. */
  earlier?: boolean;
}

export interface BackupRestoreOffer {
  sourceId: string;
  targetId: string;
}

/**
 * The restore to offer after `attempt`, onto the same target, or null. The
 * workspace must still be on the machine that did not answer, and the target
 * able to take it now. A restore from the backup never contacts the source,
 * so its own failure is never answered with another. An earlier failure is
 * offered only while the machine is still not running: one that has come back
 * is moved off the ordinary way, with nothing lost.
 */
export function backupRestoreOffer(
  attempt: MoveAttempt | null,
  computers: readonly Pick<Computer, 'computer_id' | 'kind' | 'status' | 'provider_ref'>[],
  currentId: string | null,
): BackupRestoreOffer | null {
  if (!attempt || attempt.code !== 'source_unavailable' || attempt.fromBackup) return null;
  const { sourceId, targetId } = attempt;
  if (!sourceId || !targetId || sourceId !== currentId) return null;
  const source = computers.find((c) => c.computer_id === sourceId);
  const target = computers.find((c) => c.computer_id === targetId);
  if (source?.kind !== 'ssh' || !target || !takesWorkspaces(target)) return null;
  if (attempt.earlier && source.status === 'running') return null;
  return { sourceId, targetId };
}
