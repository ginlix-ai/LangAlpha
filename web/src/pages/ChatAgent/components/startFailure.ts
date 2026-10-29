/**
 * A failed start and its fixes, as the computer surfaces read them: whether
 * a row shows the failure, which fix it offers beside Start, how this client
 * can reconnect a machine, and a refused reinstall or reconnect in words.
 */
import type { useTranslation } from 'react-i18next';

import type { Computer, ComputerReconnectTarget, ComputerStartFailure } from '@/types/api';

import { apiErrorDetailMessage, apiErrorObject, apiErrorReason, apiErrorStatus } from '@/api/errors';
import { folderMarkedOwner } from '../utils/sshLimit';
import { activeSpecChange } from './specChangeUi';

type Translate = ReturnType<typeof useTranslation>['t'];

/**
 * Why the last start failed, while the machine is at rest after it. A start
 * under way, or one that made it, is newer news than the failure, which the
 * server clears only once a start succeeds.
 */
export function shownStartFailure(computer: Computer): ComputerStartFailure | null {
  const failure = computer.start_failure;
  if (!failure || (computer.status !== 'stopped' && computer.status !== 'error')) return null;
  return activeSpecChange(computer) ? null : failure;
}

/**
 * The fix a failed start offers beside Start, or null where Start alone is
 * it. Reinstall and Reconnect are SSH computers' own, and a computer in error
 * is past repair: the routes take only a stopped one.
 */
export function startFailureRemedy(computer: Computer): 'reinstall' | 'reconnect' | null {
  if (computer.status !== 'stopped') return null;
  const remedy = shownStartFailure(computer)?.remedy;
  return computer.kind === 'ssh' && (remedy === 'reinstall' || remedy === 'reconnect') ? remedy : null;
}

/**
 * Whether *b* is the login *a* named, key and all: a refusal answers for the
 * target it was sent with, and a new one on the same host (another user or
 * port) is asked afresh rather than moved to on the old one's answer.
 */
export function sameReconnectTarget(a: ComputerReconnectTarget, b: ComputerReconnectTarget): boolean {
  return (
    a.host === b.host
    && a.port === b.port
    && a.username === b.username
    && (a.tunnel_id ?? null) === (b.tunnel_id ?? null)
    && a.host_key === b.host_key
  );
}

/** The folder a reinstall offers in place of `root`: beside it, with a suffix. */
export function suggestedNewFolder(root: string): string {
  const trimmed = root.replace(/\/+$/, '');
  return trimmed ? `${trimmed}-2` : '';
}

/**
 * How this client can reconnect the computer: by the SSH command, by remote
 * control of the Mac it runs on, or not here (`elsewhere`: a helper's Mac
 * from where the helper cannot be paired; `otherMac`: this Mac is already
 * another computer, whose helper pairing it here would take away).
 */
export type ReconnectWay = 'ssh' | 'thisMac' | 'elsewhere' | 'otherMac';

export function reconnectWay(computer: Pick<Computer, 'ssh'>, thisMacIs: string | null, remoteControl: boolean): ReconnectWay {
  if (!computer.ssh?.helper) return 'ssh';
  if (!remoteControl) return 'elsewhere';
  return thisMacIs ? 'otherMac' : 'thisMac';
}

/**
 * A refused reinstall or reconnect, in this app's words for the refusals the
 * routes name, else the server's own sentence. A folder another computer
 * holds also says whose, so the dialog can offer to take it.
 */
export interface RestartRefusal {
  message: string;
  /** On `folder_marked`: the computer whose folder it is, which a retry may take it from. */
  takeOver: string | null;
  /** On `different_machine`: the machine holds nothing of the computer, which a retry may move there. */
  moveTo?: boolean;
}

/** `action` names what was refused, for the line a refusal with no words of its own gets. */
export function restartRefusal(err: unknown, t: Translate, action: 'reinstall' | 'reconnect'): RestartRefusal {
  const takeOver = folderMarkedOwner(err);
  if (takeOver) {
    return {
      takeOver,
      message: t(
        'computer.restartRefused.folderMarked',
        'That folder is another computer\'s, possibly one from another LangAlpha account or server.',
      ),
    };
  }
  const server = apiErrorDetailMessage(err);
  switch (apiErrorObject(err)?.code) {
    case 'folder_owned':
      // The server's sentence names the computer, which only it knows.
      return {
        takeOver: null,
        message: server ?? t('computer.restartRefused.folderOwned', 'Another of your computers works in that folder. Remove it to free the folder.'),
      };
    case 'not_stopped':
      return { takeOver: null, message: t('computer.restartRefused.notStopped', 'This computer is starting or running. Try again once it has stopped.') };
    case 'spec_in_progress':
      return { takeOver: null, message: t('computer.restartRefused.specInProgress', 'A spec change is running on this computer. Try again once it finishes.') };
    case 'different_machine':
      // The dialog words it, since it names the computer and the machine.
      return { takeOver: null, moveTo: true, message: server ?? '' };
  }
  if (server) return { takeOver: null, message: server };
  if (apiErrorStatus(err) === 503) {
    return { takeOver: null, message: t('computer.restartRefused.unanswered', "The machine didn't answer. Check that it's on and connected, then try again.") };
  }
  const reason = apiErrorReason(err);
  if (reason) return { takeOver: null, message: reason };
  return {
    takeOver: null,
    message: action === 'reinstall'
      ? t('computer.restartRefused.reinstallFailed', "Couldn't reinstall this computer. Try again.")
      : t('computer.restartRefused.reconnectFailed', "Couldn't reconnect this computer. Try again."),
  };
}
