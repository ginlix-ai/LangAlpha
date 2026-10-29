/**
 * Pair an SSH computer's machine again after a start that could not reach it
 * or log in: the same one-line command a new machine runs, minted for this
 * computer, or on a Mac the desktop app answers for, remote control again.
 * The computer keeps its name, folder and workspaces; only how it is reached
 * changes, and it starts once the server accepts the new connection.
 *
 * A Mac's helper can only be paired from the app on a Mac, so anywhere else
 * this says where to go instead. Opened over the Computers dialog, which owns
 * the request; mounted only while open.
 */
import { useId, useState } from 'react';
import { useTranslation } from 'react-i18next';

import { Button } from '@/components/ui/button';
import { ModalShell } from '@/components/ui/ModalShell';
import { canRemoteControl } from '@/lib/desktop';
import type { Computer, ComputerReconnectTarget } from '@/types/api';

import type { ReconnectComputerVariables } from '../hooks/useComputerActions';
import { useRemoteControl } from '../hooks/useRemoteControl';
import { StartFailureNote } from './computerParts';
import { RemoteControlActionButton, RemoteControlSetup } from './remoteControlSetup';
import { useRemoteControlSetup } from './remoteControlSetupState';
import { SshSetup } from './sshSetup';
import { useSshSetup } from './sshSetupState';
import { THIS_MAC_OPTION } from './providerUi';
import { formatSshAddress, sshReconnectTarget } from './sshTarget';
import { reconnectWay, restartRefusal, sameReconnectTarget, shownStartFailure } from './startFailure';

interface ReconnectDialogProps {
  computer: Computer;
  /** The name of another computer that is this Mac, in the desktop app on one. */
  thisMacIs: string | null;
  onClose: () => void;
  onSubmit: (vars: Omit<ReconnectComputerVariables, 'computerId'>) => void;
  /** The request is in flight. */
  busy: boolean;
  /** The last refusal, as the request rejected. */
  error: unknown;
  /**
   * Add the machine reached as a computer of its own, with *pick* the Add
   * dialog opens on, in place of moving this computer there.
   */
  onAddInstead?: (pick: string) => void;
}

function ReconnectDialog({ computer, thisMacIs, onClose, onSubmit, busy, error, onAddInstead }: ReconnectDialogProps) {
  const { t } = useTranslation();
  const titleId = useId();
  const computerId = computer.computer_id;
  const way = reconnectWay(computer, thisMacIs, canRemoteControl());
  // A tunnel's host is only its name, which no one can dial by hand.
  const address = computer.ssh && !computer.ssh.tunnel ? formatSshAddress(computer.ssh) : '';
  const ssh = useSshSetup(way === 'ssh', { computerId, address });
  const mac = useRemoteControlSetup(way === 'thisMac', { computerId });
  const { data: rcStatus } = useRemoteControl({ enabled: way === 'thisMac', poll: false });
  const failure = shownStartFailure(computer);
  // Off, a move restores nothing unasked (only an offer of the last backup).
  const backupOn = computer.files_backup !== false;
  // The take-back a refusal offered, sent with the target it was refused for.
  const [sent, setSent] = useState<ComputerReconnectTarget | null>(null);

  const ready = way === 'ssh' ? ssh.ready : way === 'thisMac' ? mac.ready : null;
  const target = sshReconnectTarget(ready);

  const send = (next: ComputerReconnectTarget) => {
    if (!ready || busy) return;
    setSent(next);
    onSubmit({ ssh: next, fingerprint: ready.probe.fingerprint, helper: ready.probe.helper });
  };

  const refusal = error && !busy ? restartRefusal(error, t, 'reconnect') : null;
  // Only against the login it was refused for: a new pairing is asked afresh.
  const refusedHere = !!target && !!sent && sameReconnectTarget(sent, target);
  // From what was sent, which matched the target: a move already confirmed stays.
  const takeBack = refusal?.takeOver && refusedHere && sent ? { ...sent, take_over: refusal.takeOver } : null;
  // A machine that holds nothing of the computer: moved there only once the
  // user has read that its files are not there.
  const moveTo = refusal?.moveTo && refusedHere && sent ? { ...sent, move: true } : null;

  const usable = way === 'ssh' || way === 'thisMac';
  const footer = (
    <div className="flex items-center justify-end gap-2">
      <Button type="button" variant="ghost" onClick={onClose} disabled={busy}>
        {usable ? t('common.cancel') : t('common.close', 'Close')}
      </Button>
      {way === 'thisMac' && mac.action ? (
        <RemoteControlActionButton state={mac} busy={busy} />
      ) : moveTo ? (
        <Button type="button" onClick={() => send(moveTo)} disabled={busy} aria-busy={busy}>
          {way === 'thisMac'
            ? t('computer.reconnect.moveToMac', 'Move to this Mac')
            : t('computer.reconnect.moveToMachine', 'Move to this machine')}
        </Button>
      ) : usable ? (
        <Button type="button" onClick={() => target && send(target)} disabled={busy || !target} aria-busy={busy}>
          {busy ? t('computer.reconnect.submitting', 'Reconnecting...') : t('computer.reconnect.submit', 'Reconnect')}
        </Button>
      ) : null}
    </div>
  );

  const reconnecting = { username: computer.ssh?.username ?? '', folder: computer.root_dir, moving: !!moveTo };

  return (
    <ModalShell
      labelId={titleId}
      title={t('computer.reconnect.title', 'Reconnect {{name}}', { name: computer.name })}
      subtitle={
        moveTo
          ? t('computer.reconnect.moveDesc', 'Move this computer to the machine below. It starts there once you confirm.')
          : t(
              'computer.reconnect.desc',
              'Connect this computer to its machine again. Its folder and workspaces stay as they are.',
            )
      }
      onClose={onClose}
      closeDisabled={busy}
      width="standard"
      density="form"
      stacked
      footer={footer}
    >
      <div className="flex flex-col gap-4">
        {failure && <StartFailureNote failure={failure} className="text-sm" />}
        {way === 'ssh' && <SshSetup state={ssh} busy={busy} reconnecting={reconnecting} />}
        {way === 'thisMac' && (
          <RemoteControlSetup
            state={mac}
            account={rcStatus?.account ?? ''}
            busy={busy}
            keeps={computer.root_dir}
            moving={!!moveTo}
          />
        )}
        {way === 'elsewhere' && (
          <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }} data-testid="reconnect-elsewhere">
            {t(
              'computer.reconnect.onThatMac',
              '{{name}} is connected by the LangAlpha app on its Mac. Open LangAlpha on that Mac and choose Reconnect there.',
              { name: computer.name },
            )}
          </p>
        )}
        {way === 'otherMac' && (
          <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }} data-testid="reconnect-other-mac">
            {t(
              'computer.reconnect.otherMac',
              'This Mac is already {{other}}. To reconnect {{name}}, open LangAlpha on the Mac it runs on and choose Reconnect there.',
              { other: thisMacIs, name: computer.name },
            )}
          </p>
        )}
        {refusal && (
          <div
            role="alert"
            className="flex flex-col gap-1.5 rounded-md px-3 py-2 text-sm"
            style={{ backgroundColor: 'var(--color-loss-soft)', color: 'var(--color-text-primary)' }}
          >
            <p>
              {!moveTo
                ? refusal.message
                : backupOn
                  ? way === 'thisMac'
                    ? t(
                        'computer.reconnect.notThisMac',
                        "This Mac isn't the one {{name}} was on: its folder and files aren't here. Workspace files come back from their backup; anything never backed up stays on the old Mac.",
                        { name: computer.name },
                      )
                    : t(
                        'computer.reconnect.notThisMachine',
                        "This isn't the machine {{name}} was on: its folder and files aren't here. Workspace files come back from their backup; anything never backed up stays on the old machine.",
                        { name: computer.name },
                      )
                  : way === 'thisMac'
                    ? t(
                        'computer.reconnect.notThisMacNoBackup',
                        "This Mac isn't the one {{name}} was on: its folder and files aren't here. Backup is off for this computer, so workspace files stay on the old Mac; a workspace backed up before then offers to restore that backup.",
                        { name: computer.name },
                      )
                    : t(
                        'computer.reconnect.notThisMachineNoBackup',
                        "This isn't the machine {{name}} was on: its folder and files aren't here. Backup is off for this computer, so workspace files stay on the old machine; a workspace backed up before then offers to restore that backup.",
                        { name: computer.name },
                      )}
            </p>
            {moveTo && onAddInstead && (
              <p className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
                {way === 'thisMac'
                  ? t(
                      'computer.reconnect.keepBothMacNote',
                      'To keep both, add this Mac as a computer of its own, and reconnect {{name}} from the Mac it was on.',
                      { name: computer.name },
                    )
                  : t(
                      'computer.reconnect.keepBothNote',
                      'To keep both, add this machine as a computer of its own, and reconnect {{name}} to the machine it was on.',
                      { name: computer.name },
                    )}{' '}
                <button
                  type="button"
                  className="underline font-medium"
                  style={{ color: 'var(--color-text-primary)' }}
                  onClick={() => onAddInstead(way === 'thisMac' ? THIS_MAC_OPTION : 'ssh')}
                >
                  {way === 'thisMac'
                    ? t('computer.reconnect.addThisMac', 'Add this Mac as a new computer')
                    : t('computer.reconnect.addThisMachine', 'Add this machine as a new computer')}
                </button>
              </p>
            )}
            {takeBack && (
              <p className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
                {t(
                  'computer.reconnect.takeBackNote',
                  '{{folder}} is this computer\'s folder. Taking it back stops the other computer working there. The files in it stay.',
                  { folder: computer.root_dir },
                )}{' '}
                <button
                  type="button"
                  className="underline font-medium"
                  style={{ color: 'var(--color-text-primary)' }}
                  onClick={() => send(takeBack)}
                >
                  {t('computer.reinstall.takeBack', 'Take the folder back')}
                </button>
              </p>
            )}
          </div>
        )}
      </div>
    </ModalShell>
  );
}

export default ReconnectDialog;
