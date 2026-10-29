/**
 * Move a workspace to another of the user's computers. How a move ended when
 * nobody is looking at it is told by `WorkspaceMoveOutcomes`, kept apart so the
 * hosts that always mount it do not load this dialog, and framer with it,
 * before it is opened.
 *
 * The move runs on the server once accepted, so the dialog follows the move
 * itself rather than its request: closing stops nothing, and reopening shows
 * the same move still going. The panel is mounted per opening, inside
 * `AnimatePresence`, so it leaves with the workspace it showed.
 */
import { useEffect, useId, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { CheckCircle2 } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Loader } from '@/components/ui/loader';
import { ModalShell } from '@/components/ui/ModalShell';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { useWorkspace } from '@/hooks/useWorkspace';
import { AnimatePresence } from '@/lib/framer';
import type { Computer, WorkspaceMove, WorkspaceMoveError } from '@/types/api';

import { apiErrorReason, moveRefusal } from '../utils/api';
import { PREPARING_UI, computerStatusUi, isComputerPreparing, liveComputers, takesWorkspaces } from '../utils/computerStatus';
import { useComputers } from '../hooks/useComputers';
import { useComputerSource } from '../hooks/useComputerSource';
import {
  isMoveActive,
  isMoveCancellable,
  takeWatchedMove,
  useCancelWorkspaceMove,
  useMoveWorkspace,
  useShowMove,
  useWatchedMoves,
  useWorkspaceMove,
  watchMove,
} from '../hooks/useWorkspaceMove';
import { backupAge } from './backupAge';
import { backupRestoreOffer, type MoveAttempt } from './moveFromBackup';
import { RadioCardGroup, type RadioCardOption } from './radioCardGroup';
import { effectiveTier } from './specChangeUi';
import { specErrorFiles } from './specErrors';
import { tierLabel } from './tierUi';
import { providerManagesSpec } from './providerUi';

type Translate = ReturnType<typeof useTranslation>['t'];

/** What the dialog reads off a workspace row: the gallery's and the nav tree's both fit. */
export interface MoveWorkspaceTarget {
  workspace_id: string;
  name?: string;
  computer_id?: unknown;
}

interface MoveWorkspaceDialogProps {
  target: MoveWorkspaceTarget | null;
  onClose: () => void;
}

function MoveWorkspaceDialog({ target, onClose }: MoveWorkspaceDialogProps) {
  return (
    <AnimatePresence>
      {target && <MovePanel key={target.workspace_id} workspace={target} onClose={onClose} />}
    </AnimatePresence>
  );
}

function MovePanel({ workspace, onClose }: { workspace: MoveWorkspaceTarget; onClose: () => void }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const titleId = useId();
  const workspaceId = workspace.workspace_id;
  const name = workspace.name ?? '';
  useShowMove(workspaceId);

  const { data: computerData } = useComputers();
  const computers = liveComputers(computerData?.computers);
  const { data: move, isPending: moveLoading } = useWorkspaceMove(workspaceId);
  const moveRequest = useMoveWorkspace();
  const cancelRequest = useCancelWorkspaceMove();
  const [picked, setPicked] = useState<string | null>(null);

  // The move this tab is following for the workspace: one asked for here, or
  // one seen running here. Its ending is shown; an older one's is not, since
  // the user has come back to move the workspace again. Kept once seen: Close
  // hands the ending over before the panel has finished leaving, and the
  // panel should still show that ending, not the picker, as it goes.
  const watchedId = useWatchedMoves()[workspaceId]?.moveId ?? null;
  const [followedId, setFollowedId] = useState(watchedId);
  if (watchedId && watchedId !== followedId) setFollowedId(watchedId);
  const active = isMoveActive(move) ? move : null;
  const ended = !active && move && move.move_id === followedId ? move : null;
  useEffect(() => {
    // A move started elsewhere, or before a reload, is followed from here on,
    // so its ending is told even once this dialog is closed.
    if (active && active.move_id !== watchedId) watchMove(active, name);
  }, [active, watchedId, name]);

  // The latest move knows where the workspace is better than the row this
  // dialog was opened from, which can predate it.
  const currentId =
    move?.phase === 'done'
      ? move.target_computer_id
      : typeof workspace.computer_id === 'string'
        ? workspace.computer_id
        : move?.source_computer_id ?? null;
  const nameOf = (computerId: string | null | undefined) =>
    computers.find((c) => c.computer_id === computerId)?.name ?? '';
  const sourceOf = useComputerSource(computers, { enabled: true });
  const options = computers
    .filter((c) => c.computer_id !== currentId)
    .map((c) => computerOption(t, c, sourceOf(c)));
  const selectable = options.filter((o) => !o.disabled);
  const selected = picked && selectable.some((o) => o.value === picked) ? picked : selectable[0]?.value ?? null;

  const busy = moveRequest.isPending || cancelRequest.isPending;
  // The refusal of the last request outranks the failure it was retrying.
  const refusal = moveRequest.error ? moveRefusal(moveRequest.error) : null;
  const formError: WorkspaceMoveError | null = moveRequest.isPending
    ? null
    : moveRequest.error
      ? refusal ?? { code: 'request', message: requestErrorMessage(moveRequest.error, t) }
      : ended?.phase === 'failed'
        ? ended.error ?? { code: 'unknown', message: t('workspace.move.failed', "Couldn't move this workspace. Try again.") }
        : null;
  // The last try, for the offer of a restore from the backup: this dialog's
  // request, the move it followed, or else a move that failed before it opened.
  const failedMove = active || moveRequest.isPending ? null : ended ?? (move?.phase === 'failed' ? move : null);
  const attempt: MoveAttempt | null = moveRequest.isPending
    ? null
    : moveRequest.error
      ? {
        code: refusal?.code ?? null,
        sourceId: currentId,
        targetId: moveRequest.variables?.computerId ?? null,
        fromBackup: !!moveRequest.variables?.fromBackup,
      }
      : failedMove && {
        code: failedMove.error?.code ?? null,
        sourceId: failedMove.source_computer_id,
        targetId: failedMove.target_computer_id,
        fromBackup: !!failedMove.from_backup,
        earlier: failedMove !== ended,
      };
  const restoreOffer = backupRestoreOffer(attempt, computers, currentId);
  // The restore the user is confirming, held until its request settles so the
  // confirmation stays up while it is sent.
  const [restoreTargetId, setRestoreTargetId] = useState<string | null>(null);
  const restoring = restoreTargetId ?? restoreOffer?.targetId ?? null;
  // Read only to say how old the backup is, so only while a restore is offered.
  const { data: workspaceRow } = useWorkspace(restoring ? workspaceId : null);
  const now = useNow(60_000, !!restoring);
  const backedUpAt = workspaceRow?.files_backed_up_at;
  const backupTime = backupAge(backedUpAt, locale, now);
  const cancelError = cancelRequest.error
    ? moveRefusal(cancelRequest.error)?.message
      ?? apiErrorReason(cancelRequest.error)
      ?? t('workspace.move.cancelFailed', "Couldn't cancel the move. Try again.")
    : null;

  const close = () => {
    // Seen here, so no toast repeats it.
    if (ended) takeWatchedMove(workspaceId, ended.move_id);
    onClose();
  };

  const submit = () => {
    if (!selected || busy) return;
    cancelRequest.reset();
    moveRequest.mutate({ workspaceId, computerId: selected, name });
  };

  const cancelMove = () => {
    if (busy) return;
    moveRequest.reset();
    cancelRequest.mutate({ workspaceId });
  };

  const restoreFromBackup = () => {
    if (!restoreTargetId || busy) return;
    cancelRequest.reset();
    moveRequest.mutate(
      { workspaceId, computerId: restoreTargetId, name, fromBackup: true },
      // Accepted, the move's progress takes over; refused, the form says why.
      { onSettled: () => setRestoreTargetId(null) },
    );
  };

  const view = moveLoading
    ? 'loading'
    : active
      ? 'progress'
      : ended?.phase === 'done'
        ? 'done'
        : restoreTargetId
          ? 'confirmRestore'
          : 'form';

  const footer = (
    <div className="flex items-center justify-end gap-2">
      {view === 'confirmRestore' ? (
        <>
          <Button type="button" variant="ghost" onClick={() => setRestoreTargetId(null)} disabled={busy}>
            {t('common.back', 'Back')}
          </Button>
          <Button type="button" onClick={restoreFromBackup} disabled={busy} aria-busy={moveRequest.isPending}>
            {t('workspace.move.fromBackup.button', 'Restore from backup')}
          </Button>
        </>
      ) : view === 'form' ? (
        <>
          <Button type="button" variant="ghost" onClick={close} disabled={busy}>
            {t('common.cancel')}
          </Button>
          <Button type="button" onClick={submit} disabled={busy || !selected} aria-busy={moveRequest.isPending}>
            {t('workspace.move.submit', 'Move')}
          </Button>
        </>
      ) : (
        <>
          {view === 'progress' && isMoveCancellable(active) && (
            <Button type="button" variant="ghost" onClick={cancelMove} disabled={busy} aria-busy={cancelRequest.isPending}>
              {t('workspace.move.cancel', 'Cancel move')}
            </Button>
          )}
          <Button type="button" variant={view === 'progress' ? 'ghost' : 'default'} onClick={close} disabled={busy}>
            {t('common.close', 'Close')}
          </Button>
        </>
      )}
    </div>
  );

  return (
    <ModalShell
      labelId={titleId}
      title={t('workspace.move.title', 'Move workspace')}
      subtitle={t('workspace.move.intro', {
        name,
        defaultValue: 'Move {{name}} to another of your computers. Its files, threads and settings go with it.',
      })}
      onClose={close}
      closeDisabled={busy}
      width="standard"
      density="form"
      footer={footer}
    >
      {view === 'loading' && (
        <div className="flex items-center justify-center py-4">
          <Loader size={14} className="text-(--color-accent-primary)" />
        </div>
      )}
      {view === 'progress' && active && (
        <>
          <div
            className="flex items-start gap-2 rounded-md px-3 py-2.5 text-sm"
            role="status"
            aria-live="polite"
            style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-text-primary)' }}
          >
            <Loader size={14} className="mt-0.5 text-(--color-accent-primary)" />
            <div className="flex flex-col gap-0.5">
              <span className="font-medium">{moveProgressLabel(t, active)}</span>
              <span className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
                {t('workspace.move.progressHint', 'You can close this dialog. The move keeps running.')}
              </span>
            </div>
          </div>
          {cancelError && !cancelRequest.isPending && (
            <p className="text-sm" role="alert" style={{ color: 'var(--color-icon-danger)' }}>
              {cancelError}
            </p>
          )}
        </>
      )}
      {view === 'done' && ended && (
        <div
          className="flex items-center gap-2 rounded-md px-3 py-2.5 text-sm font-medium"
          role="status"
          style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-text-primary)' }}
        >
          <CheckCircle2 className="h-4 w-4 shrink-0" style={{ color: 'var(--color-success)' }} aria-hidden="true" />
          {t('workspace.move.done', 'Moved to {{computer}}', { computer: nameOf(ended.target_computer_id) })}
        </div>
      )}
      {view === 'confirmRestore' && restoreTargetId && (
        <div className="flex flex-col gap-2 text-sm" style={{ color: 'var(--color-text-primary)' }}>
          <p>
            {backupTime
              ? t('workspace.move.fromBackup.confirm', {
                target: nameOf(restoreTargetId),
                time: backupTime,
                defaultValue: 'Restore this workspace on {{target}} from its backup taken {{time}}?',
              })
              : t('workspace.move.fromBackup.confirmUndated', {
                target: nameOf(restoreTargetId),
                defaultValue: 'Restore this workspace on {{target}} from its backup?',
              })}
          </p>
          <p className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
            {t('workspace.move.fromBackup.caveats', {
              computer: nameOf(currentId),
              defaultValue: "Files changed since that backup aren't included. The folder on {{computer}} is kept, moved to _internal/leftovers once it answers again.",
            })}
          </p>
        </div>
      )}
      {view === 'form' && (
        <>
          <RadioCardGroup
            options={options}
            value={selected}
            onChange={setPicked}
            label={t('computer.computerLabel', 'Computer')}
          />
          <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            {/* Leftovers on a machine the user brought are never removed. */}
            {computers.find((c) => c.computer_id === currentId)?.kind === 'ssh'
              ? t('workspace.move.notCopiedKept', {
                  source: nameOf(currentId),
                  defaultValue:
                    "Installed dependencies, caches and .git history aren't copied. They stay on {{source}}, in _internal/leftovers, and aren't removed.",
                })
              : t('workspace.move.notCopied', {
                  source: nameOf(currentId),
                  defaultValue:
                    "Installed dependencies, caches and .git history aren't copied. They stay on {{source}} for 7 days, then are removed.",
                })}
          </p>
          {restoreOffer ? (
            // Says what failed itself, so it stands in for the failure's sentence.
            <div
              role="alert"
              className="flex flex-col items-start gap-2 rounded-md px-3 py-2.5 text-sm"
              style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-text-primary)' }}
            >
              <p>
                {backupTime
                  ? t('workspace.move.fromBackup.offer', {
                    computer: nameOf(restoreOffer.sourceId),
                    target: nameOf(restoreOffer.targetId),
                    time: backupTime,
                    defaultValue:
                      "{{computer}} didn't answer, so the files couldn't be copied from it. If it's gone for good, you can restore this workspace on {{target}} from its backup taken {{time}}.",
                  })
                  : t('workspace.move.fromBackup.offerUndated', {
                    computer: nameOf(restoreOffer.sourceId),
                    target: nameOf(restoreOffer.targetId),
                    defaultValue:
                      "{{computer}} didn't answer, so the files couldn't be copied from it. If it's gone for good, you can restore this workspace on {{target}} from its backup.",
                  })}
              </p>
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="h-7 px-2.5 text-xs"
                onClick={() => setRestoreTargetId(restoreOffer.targetId)}
                disabled={busy}
              >
                {t('workspace.move.fromBackup.button', 'Restore from backup')}
              </Button>
            </div>
          ) : formError && (
            <div
              role="alert"
              className="rounded-md px-3 py-2 text-sm"
              style={{ backgroundColor: 'var(--color-loss-soft)', color: 'var(--color-text-primary)' }}
            >
              <p>{formError.message}</p>
              <UnsavedFileList error={formError} t={t} locale={locale} />
            </div>
          )}
        </>
      )}
    </ModalShell>
  );
}

/**
 * A picker card for one computer, described as the Computers dialog describes
 * it: `source` where that tells it apart, a tier only where the app sizes it.
 * Disabled while it cannot take a workspace.
 */
function computerOption(t: Translate, computer: Computer, source: string | null): RadioCardOption<string> {
  const status = isComputerPreparing(computer) ? PREPARING_UI : computerStatusUi(computer.status);
  return {
    value: computer.computer_id,
    title: computer.name,
    facts: [
      t(status.labelKey, status.fallback),
      ...(source ? [source] : []),
      ...(providerManagesSpec(computer.kind) ? [tierLabel(t, effectiveTier(computer))] : []),
      ...(typeof computer.workspace_count === 'number'
        ? [t('computer.workspaceCount', { count: computer.workspace_count })]
        : []),
    ],
    note: computer.is_primary ? t('computer.primaryBadge', 'Primary') : null,
    disabled: !takesWorkspaces(computer),
  };
}

/** Copying runs until the folder is set aside; past that the move is finishing on its own. */
function moveProgressLabel(t: Translate, move: WorkspaceMove): string {
  if (!isMoveCancellable(move)) return t('workspace.move.finishing', 'Finishing up…');
  return move.from_backup
    ? t('workspace.move.fromBackup.restoring', 'Restoring files from backup…')
    : t('workspace.move.copying', 'Copying files…');
}

/** A refused or failed request, in the server's words when it gave some. */
function requestErrorMessage(err: unknown, t: Translate): string {
  return moveRefusal(err)?.message ?? apiErrorReason(err) ?? t('workspace.move.failed', "Couldn't move this workspace. Try again.");
}

/** The files a backup could not save, one per line, the way a spec change lists them. */
function UnsavedFileList({ error, t, locale }: { error: WorkspaceMoveError; t: Translate; locale: string }) {
  const lines = specErrorFiles(error.files, t, locale);
  if (lines.length === 0) return null;
  return (
    <ul className="mt-1.5 list-disc pl-5 text-xs font-mono break-all" style={{ color: 'var(--color-text-secondary)' }}>
      {lines.map((line) => <li key={line}>{line}</li>)}
    </ul>
  );
}

export default MoveWorkspaceDialog;
