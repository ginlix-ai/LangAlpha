/**
 * Confirm removing a computer. Removal deletes a sandbox and its disk, but
 * leaves the files on a machine the user brings, so the dialog says which,
 * once, plainly, and the button names the act. Workspaces on it go too, each
 * named here, so the user empties and removes it in one step rather than
 * leaving to delete them one by one; only the ones named are sent.
 *
 * A primary the user brought takes Home with it, so removing one first asks
 * which computer Home moves to; that computer becomes primary, and the
 * removal follows once Home is there. The user's only computer can't be
 * removed, so the dialog offers to add another instead. Files Home's
 * Chief of Staff wrote on it while Home's machine was away and that never
 * reached Home go with it, so the dialog names them.
 *
 * Opened over the Computers dialog, which owns the request and the refusal;
 * mounted only while open.
 */
import { useId, useState } from 'react';
import { useTranslation } from 'react-i18next';

import { Button } from '@/components/ui/button';
import { CheckboxChoice } from '@/components/ui/choice';
import { Loader } from '@/components/ui/loader';
import { ModalShell } from '@/components/ui/ModalShell';
import { useAllWorkspaces } from '@/hooks/useAllWorkspaces';
import { useLocale } from '@/hooks/useLocale';
import type { Computer } from '@/types/api';

import { machineName, namedFiles } from '../hooks/useHomePlacement';
import { PREPARING_UI, computerStatusUi, isComputerPreparing, takesWorkspaces } from '../utils/computerStatus';
import { providerIsOwnMachine, providerLabel } from './providerUi';
import { RadioCardGroup, type RadioCardOption } from './radioCardGroup';

type Translate = ReturnType<typeof useTranslation>['t'];

/** Workspaces named one per line before the rest are counted. */
const LISTED_WORKSPACES = 5;

interface RemoveComputerDialogProps {
  computerId: string;
  name: string;
  /** The row's count: whether there are workspaces to name, before the list has answered. */
  workspaceCount: number;
  /** Where its files stay after the removal, on a machine the user brings; null when they go with it. */
  filesKeptIn: string | null;
  /** Our key sits in the machine's authorized_keys, so the user may choose to take it out. */
  holdsOurKey: boolean;
  /** The computer is this Mac, whose remote control goes off with it. */
  turnsOffRemoteControl?: boolean;
  /**
   * The user's other computers, when this one is the primary and Home has to
   * move off it first; null otherwise. Empty: it is their only computer.
   */
  homeTargets?: readonly Computer[] | null;
  /** This computer can't be reached, so Home is restored from its backup. */
  homeFromBackup?: boolean;
  /**
   * Files Home's stand-in holds here that a hand-back left and Home lacks,
   * named by the computer Home lives on; null when there are none.
   */
  homeFilesLeft?: { failed: boolean; paths: string[]; homeComputer: string | null } | null;
  /** Home is moving to the computer picked, ahead of the removal. */
  movingHome?: boolean;
  /** Opens New computer, for a user whose only computer this is. */
  onAddComputer: () => void;
  onClose: () => void;
  /**
   * `removeKey` is the box's state, and false where the box is not shown.
   * `workspaceIds` are the workspaces the dialog named, to delete first.
   * `deleteFiles` takes LangAlpha's files off a machine the user brought.
   * `homeTo` is the computer Home moves to first, for a primary.
   */
  onConfirm: (removeKey: boolean, workspaceIds: string[], deleteFiles: boolean, homeTo: string | null) => void;
  /** The request is in flight. */
  busy: boolean;
  /** The last refusal, already worded for the user. */
  error: string | null;
  /** The refusal was for its being the user's only computer. */
  onlyComputer?: boolean;
}

function RemoveComputerDialog({
  computerId,
  name,
  workspaceCount,
  filesKeptIn,
  holdsOurKey,
  turnsOffRemoteControl = false,
  homeTargets = null,
  homeFromBackup = false,
  homeFilesLeft = null,
  movingHome = false,
  onAddComputer,
  onClose,
  onConfirm,
  busy,
  error,
  onlyComputer = false,
}: RemoveComputerDialogProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const titleId = useId();
  // On by default: a login left behind on a machine we no longer use is the
  // surprise, taking it out is not. Mounted only while open, so each open starts here.
  const [removeKey, setRemoveKey] = useState(true);
  // Off by default: the files are on the user's own machine, and a folder
  // there may hold work kept nowhere else.
  const [deleteFiles, setDeleteFiles] = useState(false);
  // The full list, the same the gallery reads, filtered to this machine. Home
  // is never on one that can be removed, so it is left out. Until the list
  // answers the button waits: it would otherwise delete workspaces unnamed.
  const hasWorkspaces = workspaceCount > 0;
  const { data, isPending, isError } = useAllWorkspaces({ enabled: hasWorkspaces });
  const workspaces = hasWorkspaces ? (data?.workspaces ?? []).filter((w) => w.computer_id === computerId) : [];
  const listWaiting = hasWorkspaces && isPending;
  const count = workspaces.length;
  // Deleting workspaces is the part a reflex click would regret, so the
  // button waits for the user to say they read the list. The tick holds for
  // the list they read: one that changes under it (a workspace made in
  // another tab) asks again, rather than sending one they never saw.
  const listed = workspaces.map((w) => w.workspace_id).join(',');
  const [acknowledgedList, setAcknowledgedList] = useState<string | null>(null);
  const acknowledged = acknowledgedList === listed;
  // Where Home goes: the first computer that can take it, until one is picked.
  const homeOptions = (homeTargets ?? []).map((c) => homeTargetOption(t, c));
  const [pickedHome, setPickedHome] = useState<string | null>(null);
  const homeTo = homeOptions.find((o) => o.value === pickedHome && !o.disabled)?.value
    ?? homeOptions.find((o) => !o.disabled)?.value
    ?? null;
  const homeName = homeTargets?.find((c) => c.computer_id === homeTo)?.name ?? '';
  const isOnly = onlyComputer || homeTargets?.length === 0;
  const homeHeld = !!homeTargets && !homeTo;

  const footer = (
    <div className="flex items-center justify-end gap-2">
      <Button type="button" variant="ghost" onClick={onClose} disabled={busy}>
        {t('common.cancel')}
      </Button>
      <Button
        type="button"
        variant="destructive"
        onClick={() => onConfirm(holdsOurKey && removeKey, workspaces.map((w) => w.workspace_id), !!filesKeptIn && deleteFiles, homeTargets ? homeTo : null)}
        disabled={busy || isOnly || homeHeld || listWaiting || (hasWorkspaces && isError) || (count > 0 && !acknowledged)}
        aria-busy={busy}
      >
        {movingHome
          ? t('computer.makePrimary.movingHome', 'Moving Home…')
          : busy
            ? t('computer.removing', 'Removing...')
            : count > 0
              ? t('computer.removeWithWorkspaces', { count })
              : t('computer.removeConfirm', 'Remove computer')}
      </Button>
    </div>
  );

  return (
    <ModalShell
      labelId={titleId}
      title={t('computer.removeTitle', 'Remove {{name}}?', { name })}
      onClose={onClose}
      closeDisabled={busy}
      width="narrow"
      density="form"
      stacked
      footer={footer}
    >
      <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>
        {filesKeptIn
          ? deleteFiles
            ? t('computer.removeBodyOwnMachineDelete', 'We stop using this machine and delete what LangAlpha put there.')
            : t(
                'computer.removeBodyOwnMachine',
                'We stop using this machine and remove what we set up there for it. Your files in {{folder}} stay.',
                { folder: filesKeptIn },
              )
          : t('computer.removeBody', "Its sandbox and everything on its disk are deleted. This can't be undone.")}
      </p>
      {homeFilesLeft && (
        <p data-testid="remove-computer-home-files" className="text-sm" style={{ color: 'var(--color-text-primary)' }}>
          {homeFilesLeft.failed || homeFilesLeft.paths.length === 0
            ? t('computer.removeHomeFiles.failed', { mac: machineName(t, homeFilesLeft.homeComputer) })
            : t('computer.removeHomeFiles.left', { files: namedFiles(t, homeFilesLeft.paths, locale) })}
        </p>
      )}
      {homeTargets && homeTargets.length > 0 && (
        <div className="flex flex-col gap-1.5" data-testid="remove-computer-home">
          <RadioCardGroup
            options={homeOptions}
            value={homeTo}
            onChange={setPickedHome}
            label={t('computer.removeHome.label', 'Home moves to')}
          />
          <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            {homeFromBackup && homeName
              ? t(
                'computer.makePrimary.fromBackup',
                "{{from}} can't be reached, so Home is restored on {{name}} from its last backup.",
                { from: name, name: homeName },
              )
              : t('computer.removeHome.becomesPrimary', 'The computer you pick becomes your primary computer.')}
          </p>
        </div>
      )}
      {isOnly && (
        <div
          role={onlyComputer ? 'alert' : undefined}
          className="flex flex-col items-start gap-2 rounded-md px-3 py-2.5 text-sm"
          style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-text-primary)' }}
        >
          <p>{t('computer.removeRefused.onlyComputer', "This is your only computer, so it can't be removed. Add another computer first.")}</p>
          <Button type="button" variant="outline" size="sm" onClick={onAddComputer} disabled={busy}>
            {t('computer.card.add', 'Add a computer')}
          </Button>
        </div>
      )}
      {listWaiting && (
        <p className="flex items-center gap-2 text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
          <Loader size={12} style={{ color: 'var(--color-accent-primary)' }} />
          {t('computer.removeWorkspacesLoading', 'Finding the workspaces on it...')}
        </p>
      )}
      {hasWorkspaces && isError && (
        <p className="text-sm" role="alert" style={{ color: 'var(--color-icon-danger)' }}>
          {t('computer.removeWorkspacesFailed', "Couldn't read the workspaces on it. Close this and try again.")}
        </p>
      )}
      {count > 0 && (
        <div className="flex flex-col gap-1.5" data-testid="remove-computer-workspaces">
          <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>
            {t('computer.removeWorkspaces', { count })}
          </p>
          <ul
            className="flex flex-col gap-0.5 rounded-md border px-3 py-2 text-sm"
            style={{ borderColor: 'var(--color-border-muted)', color: 'var(--color-text-primary)' }}
          >
            {workspaces.slice(0, LISTED_WORKSPACES).map((w) => (
              <li key={w.workspace_id} className="truncate">{w.name}</li>
            ))}
            {count > LISTED_WORKSPACES && (
              <li style={{ color: 'var(--color-text-tertiary)' }}>
                {t('computer.removeWorkspacesMore', { count: count - LISTED_WORKSPACES })}
              </li>
            )}
          </ul>
        </div>
      )}
      {filesKeptIn && (
        <CheckboxChoice
          checked={deleteFiles}
          onCheckedChange={setDeleteFiles}
          disabled={busy}
          markClassName="mt-0.5"
          className="items-start gap-2 rounded-md text-sm"
          style={{ color: 'var(--color-text-primary)' }}
        >
          <span className="flex min-w-0 flex-col gap-0.5 break-words">
            <span>{t('computer.alsoDeleteFiles', "Also delete LangAlpha's files from this machine")}</span>
            <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              {t(
                'computer.alsoDeleteFilesHint',
                "Deletes {{folder}}, its workspaces' folders included, and LangAlpha's runtime in ~/.langalpha (about 3 GB) unless another computer uses it. Files you put in {{folder}} yourself stay. The machine has to be connected.",
                { folder: filesKeptIn },
              )}
            </span>
          </span>
        </CheckboxChoice>
      )}
      {turnsOffRemoteControl && (
        <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>
          {t('computer.remoteControl.removeTurnsOff', 'Remote control on this Mac turns off too.')}
        </p>
      )}
      {holdsOurKey && (
        <CheckboxChoice
          checked={removeKey}
          onCheckedChange={setRemoveKey}
          disabled={busy}
          className="items-center gap-2 rounded-md text-sm"
          style={{ color: 'var(--color-text-primary)' }}
        >
          <span>{t('computer.removeKey', 'Remove our key from ~/.ssh/authorized_keys')}</span>
        </CheckboxChoice>
      )}
      {count > 0 && (
        <CheckboxChoice
          checked={acknowledged}
          onCheckedChange={(on) => setAcknowledgedList(on ? listed : null)}
          disabled={busy}
          size="md"
          markClassName="mt-0.5"
          className="items-start gap-2 rounded-md text-sm"
          style={{ color: 'var(--color-text-primary)' }}
        >
          <span>
            {count === 1
              ? t('computer.removeWorkspacesAcknowledgeOne', 'I understand {{name}} and its threads are deleted for good', { name: workspaces[0].name })
              : t('computer.removeWorkspacesAcknowledge', { count })}
          </span>
        </CheckboxChoice>
      )}
      {error && !busy && !onlyComputer && (
        <p className="text-sm" role="alert" style={{ color: 'var(--color-icon-danger)' }}>
          {error}
        </p>
      )}
    </ModalShell>
  );
}

/**
 * A computer Home could move to. Held where it can't take workspaces now,
 * where it is a machine the user brings that can't be reached (Home's files
 * would have nowhere to land), or one with its file backup off, which can't
 * hold Home.
 */
function homeTargetOption(t: Translate, computer: Computer): RadioCardOption<string> {
  const status = isComputerPreparing(computer) ? PREPARING_UI : computerStatusUi(computer.status);
  const unreachable = computer.ssh?.tunnel_connected === false;
  const backupOff = providerIsOwnMachine(computer.kind) && computer.files_backup === false;
  return {
    value: computer.computer_id,
    title: computer.name,
    facts: [t(status.labelKey, status.fallback), providerLabel(computer.kind, t)],
    note: unreachable
      ? t('computer.removeHome.unreachable', "Can't be reached")
      : backupOff ? t('computer.removeHome.backupOff', 'Backup off') : null,
    disabled: !takesWorkspaces(computer) || unreachable || backupOff,
  };
}

export default RemoveComputerDialog;
