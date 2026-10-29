/**
 * Tells how each move this tab asked for ended, as a toast, unless a dialog
 * is showing it. Mount it where it stays mounted; several mounts are fine,
 * since each ending is handed out once.
 */
import { useEffect } from 'react';
import { useTranslation } from 'react-i18next';

import { toast } from '@/components/ui/use-toast';
import { useLocale } from '@/hooks/useLocale';

import { useComputers } from '../hooks/useComputers';
import {
  isMoveActive,
  takeWatchedMove,
  useMoveShowing,
  useWatchedMoves,
  useWorkspaceMove,
  type WatchedMove,
} from '../hooks/useWorkspaceMove';
import { specErrorFiles } from './specErrors';

export function WorkspaceMoveOutcomes() {
  const moves = useWatchedMoves();
  return (
    <>
      {Object.entries(moves).map(([workspaceId, entry]) => (
        <MoveOutcome key={workspaceId} workspaceId={workspaceId} entry={entry} />
      ))}
    </>
  );
}

function MoveOutcome({ workspaceId, entry }: { workspaceId: string; entry: WatchedMove }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const showing = useMoveShowing(workspaceId);
  const { data: move, isSuccess } = useWorkspaceMove(workspaceId);
  const { data: computerData } = useComputers();
  const computers = computerData?.computers;

  useEffect(() => {
    if (!isSuccess || showing || isMoveActive(move)) return;
    if (!takeWatchedMove(workspaceId, entry.moveId)) return;
    // Replaced by a newer move, or cancelled: there is nothing to tell.
    if (!move || move.move_id !== entry.moveId || move.phase === 'cancelled') return;
    const files = specErrorFiles(move.error?.files, t, locale);
    const reason = move.error?.message;
    const description = reason && files.length > 0 ? `${reason} ${files.join(', ')}` : reason;
    if (move.make_primary) {
      toast(
        move.phase === 'done'
          ? { title: t('computer.makePrimary.done', '{{name}} is now your primary computer', { name: entry.label }) }
          : {
              variant: 'destructive',
              title: t('computer.makePrimary.failedTitle', "Couldn't make {{name}} primary", { name: entry.label }),
              description: description ?? t('computer.makePrimary.failed', "Couldn't make this computer primary. Try again."),
            },
      );
      return;
    }
    const target = computers?.find((c) => c.computer_id === move.target_computer_id)?.name ?? '';
    toast(
      move.phase === 'done'
        ? { title: t('workspace.move.done', 'Moved to {{computer}}', { computer: target }), description: entry.label }
        : {
            variant: 'destructive',
            title: t('workspace.move.failedTitle', "Couldn't move {{name}}", { name: entry.label }),
            description: description ?? t('workspace.move.failed', "Couldn't move this workspace. Try again."),
          },
    );
  }, [isSuccess, showing, move, workspaceId, entry, computers, t, locale]);

  return null;
}
