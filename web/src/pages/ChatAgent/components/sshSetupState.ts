/**
 * The state behind adding a machine over SSH: which way in the user is on,
 * the pairing and its polling, the address typed by hand, and the machine
 * both end on. The view is `sshSetup.tsx`; the dialog owns this hook, so the
 * create body and the name are read from it there.
 */
import { useEffect, useRef, useState } from 'react';
import { flushSync } from 'react-dom';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';

import { queryKeys } from '@/lib/queryKeys';
import type { SshErrorCode, SshPairingMachine, SshTarget } from '@/types/api';

import { useSshKey, useSshPairingMint, useSshPairingStatus } from '../hooks/useComputers';
import { connectSshPairing, probeSsh } from '../utils/api';
import { sshLimitMessage } from '../utils/sshLimit';
import {
  apiErrorDetail,
  apiErrorDetailMessage,
  apiErrorObject,
  apiErrorStatus,
  apiValidationErrors,
} from '@/api/errors';
import {
  formatSshHostPort,
  pairingReady,
  parseSshAddress,
  sshCreateBody,
  sshDefaultName,
  type SshAddressError,
  type SshReady,
} from './sshTarget';

type Translate = ReturnType<typeof useTranslation>['t'];

export type SshMode = 'pairing' | 'manual';

/** A refusal as the server coded it, with what we know about where it happened. */
export interface SshFailure {
  code: SshErrorCode | null;
  message: string;
  target: SshTarget | null;
  username: string | null;
  /** The machine's `uname -s`, when it reported one. */
  os: string | null;
  /** Seconds until the dial budget renews, on `rate_limited`. */
  retryAfter?: number;
}

/** The server's sentence when it gave one, else ours. */
function requestErrorText(err: unknown, t: Translate): string {
  const invalid = apiValidationErrors(err).map((e) => e.message).join(' ');
  if (invalid) return invalid;
  const raw = apiErrorDetail(err);
  if (typeof raw === 'string' && raw) return raw;
  return apiErrorDetailMessage(err) ?? t('computer.ssh.error.generic', 'Could not connect. Try again.');
}

/**
 * One sentence per code, the same for both ways in. A code this build does
 * not know is shown in the server's own words.
 */
export function failureText(failure: SshFailure, t: Translate): string {
  switch (failure.code) {
    case 'auth_failed':
      if (failure.username === 'root') {
        return t('computer.ssh.error.authFailedRoot', 'The machine did not accept our key for root, most likely because it does not allow root logins. Run the command as the user the agent should work as, or allow root logins by key (PermitRootLogin prohibit-password).');
      }
      if (failure.username) {
        return t('computer.ssh.error.authFailed', 'The machine did not accept our key for {{username}}.', { username: failure.username });
      }
      break;
    case 'unreachable':
      return failure.target
        ? t('computer.ssh.error.unreachable', 'We could not reach {{address}}. It must accept SSH from the internet.', {
          address: formatSshHostPort(failure.target.host, failure.target.port),
        })
        : failure.os === 'Darwin'
          ? t('computer.ssh.error.unreachableMac', 'We could not reach your Mac over SSH. Turn on Remote Login in System Settings, General, Sharing, then run the command again.')
          : t('computer.ssh.error.unreachableMachine', 'We could not reach your machine over SSH. It must accept SSH from the internet.');
    case 'timeout':
      return t('computer.ssh.error.timeout', 'The connection timed out. Check that SSH is running and its port is open.');
    case 'host_key_mismatch':
      return t('computer.ssh.error.hostKeyMismatch', 'Something other than your machine answered at that address.');
    case 'private_address':
      return t('computer.ssh.error.privateAddress', 'The machine is on a private network, which we cannot reach. Use an address that is reachable from the internet.');
    case 'unsupported':
      // The server's sentence names what is missing, such as a bash that
      // would not run; a machine it could probe lists that on the summary.
      return failure.message || t('computer.ssh.error.unsupported', 'This machine is not supported.');
    case 'expired':
      return t('computer.ssh.error.expired', 'This command expired. Run the new one.');
    case 'rate_limited':
      if (failure.retryAfter) {
        return t('computer.ssh.error.rateLimited', {
          count: Math.max(1, Math.ceil(failure.retryAfter / 60)),
          defaultValue_one: 'This account has tried too many SSH connections in the last hour. Try again in {{count}} minute.',
          defaultValue_other: 'This account has tried too many SSH connections in the last hour. Try again in {{count}} minutes.',
        });
      }
      break;
  }
  return failure.message || t('computer.ssh.error.generic', 'Could not connect. Try again.');
}

function addressErrorText(error: SshAddressError, t: Translate): string {
  return error === 'port'
    ? t('computer.ssh.portInvalid', 'Enter a port from 1 to 65535.')
    : t('computer.ssh.addressInvalid', 'Enter an address like ubuntu@203.0.113.10.');
}

/** The address the machine is most likely reached at, for the user to correct. */
function machineAddress(machine: SshPairingMachine | null): string {
  if (!machine) return '';
  return `${machine.username}@${machine.addresses[0] ?? ''}`;
}

/** What the ready step shows and sets; `startOver` is offered only where another machine can be picked. */
export interface SshReadyStep {
  folder: string;
  setFolder: (folder: string) => void;
  folderOpen: boolean;
  openFolder: () => void;
  filesBackup: boolean;
  setFilesBackup: (on: boolean) => void;
  startOver?: () => void;
}

/**
 * The machine's folder and backup choice on the ready step, and the create
 * body they make: shared by every way a machine comes in, so each files it
 * the same way. The name is the dialog's own field, the same for every
 * provider; a machine left unnamed there goes by `defaultName`.
 */
export function useSshReadyFields(ready: SshReady | null) {
  // The folder the user typed, or null while it shows our prefill, so a later
  // probe can replace a prefill but never what was typed.
  const [typedFolder, setTypedFolder] = useState<string | null>(null);
  const [folderOpen, setFolderOpen] = useState(false);
  // On unless the user says otherwise: a folder lost on their machine can then
  // be restored.
  const [filesBackup, setFilesBackup] = useState(true);

  const folder = typedFolder ?? ready?.probe.suggested_root_dir ?? '';
  // The machine's part of the create body, or null until a supported one is in.
  const body = sshCreateBody(ready, folder);
  const create = body ? { ssh: body, files_backup: filesBackup } : null;

  const step: SshReadyStep = {
    folder,
    setFolder: setTypedFolder,
    folderOpen,
    openFolder: () => setFolderOpen(true),
    filesBackup,
    setFilesBackup,
  };

  return {
    create,
    step,
    /** Its hostname, once a machine is in. */
    defaultName: ready ? sshDefaultName(ready) : null,
    /** Back to the prefill, for another machine. */
    reset: () => {
      setTypedFolder(null);
      setFolderOpen(false);
    },
  };
}

/**
 * `computerId` makes the pairing one that reconnects that SSH computer: its
 * command is minted for it, and cached apart from a new machine's. `address`
 * starts the form by hand at the address the computer was reached at.
 */
export function useSshSetup(
  active: boolean,
  { computerId = null, address: knownAddress = '' }: { computerId?: string | null; address?: string } = {},
) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const [mode, setMode] = useState<SshMode>('pairing');
  // Where focus lands when the user moves between views: the address field by
  // hand, the pairing section otherwise. Moved only on their click, never on a
  // poll, so a status change never pulls focus away from what they are doing.
  const entryRef = useRef<HTMLElement | null>(null);
  const setEntry = (el: HTMLElement | null) => {
    entryRef.current = el;
  };

  // --- Pairing ---
  const mint = useSshPairingMint({ enabled: active && mode === 'pairing', computerId });
  // A pairing that cannot be had leaves the form by hand, with a note saying why.
  const view: SshMode = mode === 'pairing' && !mint.isError ? 'pairing' : 'manual';
  // A token the user moved on from ("use a different machine") while its
  // replacement is minted: its command and status are no longer shown.
  const [dismissedToken, setDismissedToken] = useState<string | null>(null);
  const token = mint.data && mint.data.token !== dismissedToken ? mint.data.token : null;
  const connect = useMutation({
    mutationFn: (args: { token: string; host: string; port: number }) =>
      connectSshPairing(args.token, { host: args.host, port: args.port }),
  });
  const status = useSshPairingStatus(token, { enabled: active && view === 'pairing', paused: connect.isPending });
  const pairing = token ? status.data : undefined;
  const machine = pairing?.machine ?? null;
  // Both belong to the pairing they were typed against, so a replacement
  // pairing starts from the address its own machine reports.
  const [pairAddress, setPairAddress] = useState<{ token: string; value: string } | null>(null);
  const [pairError, setPairError] = useState<{ token: string; message: string } | null>(null);
  const setConnectError = (message: string | null) => setPairError(token && message ? { token, message } : null);

  // --- By hand ---
  const [address, setAddress] = useState(knownAddress);
  const [addressError, setAddressError] = useState<string | null>(null);
  const [probed, setProbed] = useState<SshReady | null>(null);
  const [probeFailure, setProbeFailure] = useState<SshFailure | null>(null);
  const probe = useMutation({ mutationFn: (target: SshTarget) => probeSsh(target, { computerId }) });
  // Only needed once a connection by hand is refused, but fetched with the
  // form so the key step has it the moment it opens.
  const key = useSshKey({ enabled: active && view === 'manual' });

  const ready = view === 'pairing' ? pairingReady(pairing) : probed;
  const fields = useSshReadyFields(ready);

  const remint = () => {
    void mint.refetch({ cancelRefetch: false });
  };

  const focusEntry = () => entryRef.current?.focus();

  const switchTo = (next: SshMode) => {
    const retry = next === 'pairing' && mint.isError;
    flushSync(() => setMode(next));
    // A pairing that failed to mint is asked for again; the view follows once
    // it is pending, so there is nothing to focus yet.
    if (retry) void queryClient.resetQueries({ queryKey: queryKeys.sshPairings.mint(computerId), exact: true });
    else focusEntry();
  };

  const startOver = () => {
    flushSync(() => {
      fields.reset();
      if (view === 'pairing') {
        setDismissedToken(token);
        setPairAddress(null);
        setPairError(null);
        connect.reset();
      } else {
        setProbed(null);
        setProbeFailure(null);
        probe.reset();
      }
    });
    if (view === 'pairing') remint();
    focusEntry();
  };

  const editAddress = (value: string) => {
    setAddress(value);
    setAddressError(null);
    setProbeFailure(null);
  };

  const connectByHand = () => {
    if (probe.isPending) return;
    const parsed = parseSshAddress(address);
    if (!parsed.ok) {
      setAddressError(parsed.error === 'empty' ? null : addressErrorText(parsed.error, t));
      return;
    }
    if (!parsed.username) {
      setAddressError(t('computer.ssh.addressNoUser', 'Include the user, like ubuntu@203.0.113.10.'));
      return;
    }
    const target: SshTarget = { host: parsed.host, port: parsed.port, username: parsed.username };
    setAddressError(null);
    setProbeFailure(null);
    probe.mutate(target, {
      onSuccess: (result) => setProbed({ target, probe: result }),
      onError: (err) => {
        const detail = apiErrorObject(err);
        const limit = sshLimitMessage(err, t);
        const code = !limit && typeof detail?.code === 'string' ? detail.code : null;
        const message = limit ?? (code ? (apiErrorDetailMessage(err) ?? '') : requestErrorText(err, t));
        const retryAfter = typeof detail?.retry_after === 'number' ? detail.retry_after : undefined;
        setProbeFailure({ code, message, target, username: target.username, os: null, retryAfter });
      },
    });
  };

  const editPairAddress = (value: string) => {
    if (token) setPairAddress({ token, value });
    setConnectError(null);
  };

  const pairAddressValue = pairAddress && pairAddress.token === token ? pairAddress.value : machineAddress(machine);
  const connectError = pairError && pairError.token === token ? pairError.message : null;

  const connectPairing = () => {
    if (!token || !machine || connect.isPending) return;
    const parsed = parseSshAddress(pairAddressValue);
    if (!parsed.ok) {
      setConnectError(addressErrorText(parsed.error, t));
      return;
    }
    // The login is the one the command ran as, and the server dials only that.
    if (parsed.username && parsed.username !== machine.username) {
      setConnectError(t('computer.ssh.pairOtherUser', 'The command ran as {{username}}. To use another login, run it as that user.', { username: machine.username }));
      return;
    }
    setConnectError(null);
    const statusKey = queryKeys.sshPairings.status(token);
    void queryClient.cancelQueries({ queryKey: statusKey, exact: true });
    connect.mutate({ token, host: parsed.host, port: parsed.port }, {
      // The answer is the pairing's new state, which the server also keeps.
      onSuccess: (result) => queryClient.setQueryData(statusKey, result),
      onError: (err) => {
        if (apiErrorStatus(err) === 404) remint();
        else setConnectError(requestErrorText(err, t));
      },
    });
  };

  // A pairing lives 15 minutes. Once it is gone a new one is minted in its
  // place, quietly: the old command tells the user so in their own terminal.
  const expired = !!token && (
    (status.isError && apiErrorStatus(status.error) === 404) || pairing?.error?.code === 'expired'
  );
  const expiredToken = expired ? token : null;
  const { refetch: refetchMint } = mint;
  useEffect(() => {
    if (expiredToken) void refetchMint({ cancelRefetch: false });
  }, [expiredToken, refetchMint]);

  return {
    view,
    setEntry,
    switchTo,
    ready,
    readyStep: { ...fields.step, startOver },
    create: fields.create,
    defaultName: fields.defaultName,
    pairing: {
      unavailable: mint.isError,
      // Why the one-line command was refused, when the reason is the cap.
      limitMessage: mint.isError ? sshLimitMessage(mint.error, t) : null,
      command: token ? mint.data?.command ?? null : null,
      localOnly: mint.data?.local_only ?? false,
      scriptUrl: token ? mint.data?.script_url ?? null : null,
      status: pairing,
      expired,
      machine,
      address: pairAddressValue,
      editAddress: editPairAddress,
      connect: connectPairing,
      connecting: connect.isPending,
      connectError,
    },
    manual: {
      address,
      editAddress,
      addressError,
      connect: connectByHand,
      connecting: probe.isPending,
      failure: probeFailure,
      key,
    },
  };
}

export type SshSetupState = ReturnType<typeof useSshSetup>;
