/**
 * Make another computer the primary: the one new workspaces and Home land on.
 *
 * Home moves with the primary, so the confirmation says when it will, and a
 * row whose computer is taking Home shows that move instead of the button.
 * On a machine the user brings it also says what Home's turns do while that
 * machine can't be reached, and with its file backup off it first asks to
 * turn backup on, since Home would otherwise live only on that disk. Opened
 * over the Computers dialog, which owns the request and the refusal; mounted
 * only while open.
 */
import { useId } from 'react';
import { useTranslation } from 'react-i18next';
import { House } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Loader } from '@/components/ui/loader';
import { ModalShell } from '@/components/ui/ModalShell';
import { useHomeWorkspaceId } from '@/hooks/useHomeWorkspace';
import { useWorkspace } from '@/hooks/useWorkspace';
import type { Computer } from '@/types/api';

import { apiErrorReason, moveRefusal, type HomePlacement } from '../utils/api';
import { homeEligibility } from '../utils/computerStatus';
import { providerIsOwnMachine } from './providerUi';

type Translate = ReturnType<typeof useTranslation>['t'];

export interface MakePrimaryChoice {
  /** Turn the computer's file backup on first. */
  turnOnBackup: boolean;
  /** Lay Home on it from Home's last backup: Home's computer can't be reached. */
  fromBackup: boolean;
}

interface MakePrimaryDialogProps {
  computer: Pick<Computer, 'computer_id' | 'name' | 'kind' | 'files_backup' | 'status' | 'provider_ref' | 'ssh'>;
  /** Where Home stands; undefined while unread, or where it can't be read. */
  placement: HomePlacement | undefined;
  onClose: () => void;
  onConfirm: (choice: MakePrimaryChoice) => void;
  /** The request is in flight. */
  busy: boolean;
  /** The last refusal, as the request rejected it. */
  error: unknown;
}

/** Which sentence the confirmation says, by the target and where Home stands. */
type MakePrimaryCopy = 'homeStays' | 'homeMoves' | 'fromBackup' | 'ownMachineStandIn' | 'ownMachineWaits';

function makePrimaryCopy({
  computerId,
  ownMachine,
  homeStays,
  placement,
}: {
  computerId: string;
  ownMachine: boolean;
  homeStays: boolean;
  placement: HomePlacement | undefined;
}): MakePrimaryCopy {
  if (homeStays) return 'homeStays';
  if (placement?.unreachable && placement.computer_id !== computerId) return 'fromBackup';
  // Until Home's placement is read, the stand-in is unknown, so nothing is promised.
  if (ownMachine && placement) return placement.stand_in ? 'ownMachineStandIn' : 'ownMachineWaits';
  return 'homeMoves';
}

function MakePrimaryDialog({ computer, placement, onClose, onConfirm, busy, error }: MakePrimaryDialogProps) {
  const { t } = useTranslation();
  const titleId = useId();
  const { computer_id: computerId, name } = computer;
  // Home's own query has it as of its first read, before a start put it on a
  // computer, so the detail decides. Until it lands, the copy assumes the
  // usual case: Home is on the current primary and moves.
  const { data: home, isPlaceholderData } = useWorkspace(useHomeWorkspaceId());
  const homeStays =
    !!home && !isPlaceholderData && (!home.computer_id || home.status === 'unbound' || home.computer_id === computerId);
  const copy = makePrimaryCopy({ computerId, ownMachine: providerIsOwnMachine(computer.kind), homeStays, placement });
  const fromBackup = copy === 'fromBackup';
  // Asked up front when the row says backup is off, and again if the server
  // refuses for it, the row having been read before someone turned it off.
  const refusal = moveRefusal(error);
  const asksBackup = homeEligibility(computer).backupOff || refusal?.code === 'not_backed_up';

  const footer = (
    <div className="flex items-center justify-end gap-2">
      <Button type="button" variant="ghost" onClick={onClose} disabled={busy}>
        {t('common.cancel')}
      </Button>
      <Button
        type="button"
        onClick={() => onConfirm({ turnOnBackup: asksBackup, fromBackup })}
        disabled={busy}
        aria-busy={busy}
      >
        {asksBackup
          ? t('computer.makePrimary.backupConfirm', 'Turn on and make primary')
          : t('computer.makePrimary.button', 'Make primary')}
      </Button>
    </div>
  );

  return (
    <ModalShell
      labelId={titleId}
      title={asksBackup
        ? t('computer.makePrimary.backupTitle', 'Turn on backup for {{name}}?', { name })
        : t('computer.makePrimary.title', 'Make {{name}} primary?', { name })}
      onClose={onClose}
      closeDisabled={busy}
      width="narrow"
      density="form"
      stacked
      footer={footer}
    >
      {asksBackup && (
        <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>
          {t(
            'computer.makePrimary.backupBody',
            "Home will live on this computer. With backup on, Home can be restored if it's lost.",
          )}
        </p>
      )}
      <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>
        {copyText(copy, { name, placement }, t)}
      </p>
      {error != null && !busy && !(asksBackup && refusal?.code === 'not_backed_up') && (
        <p className="text-sm" role="alert" style={{ color: 'var(--color-icon-danger)' }}>
          {refusalMessage(error, t)}
        </p>
      )}
    </ModalShell>
  );
}

function copyText(copy: MakePrimaryCopy, { name, placement }: { name: string; placement: HomePlacement | undefined }, t: Translate) {
  switch (copy) {
    case 'homeStays':
      return t('computer.makePrimary.body', 'New workspaces will be created on {{name}}.', { name });
    case 'fromBackup':
      return t(
        'computer.makePrimary.fromBackup',
        "{{from}} can't be reached, so Home is restored on {{name}} from its last backup.",
        { name, from: placement?.computer_name ?? '' },
      );
    case 'ownMachineStandIn':
      return t(
        'computer.makePrimary.ownMachineStandIn',
        "New workspaces will be created on {{name}}, and Home moves there too. While {{name}} can't be reached, the Chief of Staff works on {{cloud}}, starting it if it's stopped, and copies what it writes back into Home when {{name}} returns.",
        { name, cloud: placement?.stand_in?.name ?? '' },
      );
    case 'ownMachineWaits':
      return t(
        'computer.makePrimary.ownMachineWaits',
        "New workspaces will be created on {{name}}, and Home moves there too. While {{name}} can't be reached, the Chief of Staff waits for it.",
        { name },
      );
    default:
      return t(
        'computer.makePrimary.bodyHomeMoves',
        'New workspaces will be created on {{name}}, and Home, where the Chief of Staff keeps its files, moves there too.',
        { name },
      );
  }
}

/** A refused make-primary, in the server's words when it gave some. */
function refusalMessage(err: unknown, t: Translate): string {
  return (
    moveRefusal(err)?.message
    ?? apiErrorReason(err)
    ?? t('computer.makePrimary.failed', "Couldn't make this computer primary. Try again.")
  );
}

/**
 * The row's control: the button, or Home's move to this computer while it
 * runs, which is the request the button made still going.
 */
export function MakePrimaryButton({
  movingHome,
  disabled,
  onClick,
  describedBy,
}: {
  movingHome: boolean;
  disabled: boolean;
  onClick: () => void;
  /** The line under the buttons that says why it is held. */
  describedBy?: string;
}) {
  const { t } = useTranslation();
  if (movingHome) {
    return (
      <span
        role="status"
        aria-live="polite"
        className="inline-flex items-center gap-1.5 px-1 text-xs"
        style={{ color: 'var(--color-text-secondary)' }}
      >
        <span aria-hidden="true" className="inline-flex">
          <Loader size={12} className="text-(--color-accent-primary)" />
        </span>
        {t('computer.makePrimary.movingHome', 'Moving Home…')}
      </span>
    );
  }
  return (
    <Button variant="ghost" size="sm" className="gap-1.5" onClick={onClick} disabled={disabled} aria-describedby={describedBy}>
      <House className="h-3.5 w-3.5" />
      {t('computer.makePrimary.button', 'Make primary')}
    </Button>
  );
}

export default MakePrimaryDialog;
