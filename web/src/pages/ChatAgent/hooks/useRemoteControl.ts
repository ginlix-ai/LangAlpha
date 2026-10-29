/**
 * Remote control of this Mac, as the computer surfaces read it: whether the
 * desktop app can offer it here, which computer row is this Mac, and turning
 * it off once no computer uses it.
 *
 * The status is the shell's answer, not the server's: what the helper on this
 * Mac says about itself. A computer row is this Mac when its pinned host key
 * is the helper's, so the two are matched by fingerprint. Whether a row is
 * answered by a helper at all is the server's fact (`ssh.helper`), the same on
 * every device; only being this Mac needs the shell.
 */
import { useEffect } from 'react';
import { useQuery, useQueryClient, type QueryClient } from '@tanstack/react-query';

import {
  canRemoteControl,
  disableRemoteControl,
  getRemoteControl,
  type RemoteControlStatus,
} from '@/lib/desktop';
import { queryKeys } from '@/lib/queryKeys';
import type { Computer, ComputerProvidersResponse } from '@/types/api';

import {
  CREATE_COMPUTER_MUTATION_KEY,
  fetchComputerList,
  RECONNECT_COMPUTER_MUTATION_KEY,
  useComputerProviders,
  useComputers,
} from './useComputers';

/**
 * How often the status is read again while a computer surface shows it.
 * Pausing from the app menu moves no focus to the page, so a refocus would
 * never notice it.
 */
export const REMOTE_CONTROL_POLL_MS = 10_000;

interface RemoteControlReader {
  enabled: boolean;
  poll?: boolean;
  reread?: boolean;
}

/**
 * The helper's status, read only in a shell that can turn it on and off; null
 * when the shell gave no answer. `poll: false` for a reader that needs no
 * pause noticed (it still sees what a polling reader of the same key reads).
 * `reread: false` for a reader opened on an answer just read, which a read of
 * its own as it mounts could change under it.
 */
export function useRemoteControl(options: RemoteControlReader) {
  return useQuery({
    queryKey: queryKeys.desktop.remoteControl(),
    queryFn: getRemoteControl,
    enabled: options.enabled && canRemoteControl(),
    staleTime: 0,
    retry: false,
    refetchOnMount: options.reread !== false,
    refetchInterval: options.poll === false ? false : REMOTE_CONTROL_POLL_MS,
  });
}

export function setRemoteControlStatus(queryClient: QueryClient, status: RemoteControlStatus | null | undefined) {
  if (status) queryClient.setQueryData(queryKeys.desktop.remoteControl(), status);
}

/** A row that is this Mac, and whether its remote control is paused from the app menu. */
export interface ThisMac {
  paused: boolean;
}

/** Whether a computer row is this Mac: its pinned host key is the helper's. */
export function isThisMac(computer: Pick<Computer, 'ssh'>, fingerprint: string | null | undefined): boolean {
  return !!fingerprint && computer.ssh?.fingerprint === fingerprint;
}

/**
 * The helper's status, and which row it makes this Mac: `of` answers null for
 * every other computer, and for every row outside the desktop app.
 */
export function useThisMac(options: { enabled: boolean }) {
  const { data: status } = useRemoteControl(options);
  const fingerprint = status?.fingerprint;
  const paused = status?.service === 'off';
  const of = (computer: Pick<Computer, 'ssh'>): ThisMac | null => (isThisMac(computer, fingerprint) ? { paused } : null);
  return { status, of };
}

/**
 * Whether this Mac is offered as a computer: the helper can run here, and
 * this server takes SSH computers and runs the tunnel gateway the helper
 * connects out to, since it is never dialed.
 */
export function remoteControlOffered(
  status: RemoteControlStatus | null | undefined,
  providers: ComputerProvidersResponse | undefined,
): boolean {
  return !!status?.supported && serverTakesRemoteControl(providers);
}

/** The server's half: it takes SSH computers, and runs the tunnel gateway a helper connects out to. */
export function serverTakesRemoteControl(providers: ComputerProvidersResponse | undefined): boolean {
  const ssh = providers?.providers.find((p) => p.kind === 'ssh');
  return !!ssh && ssh.allowed !== false && providers?.tunnels === true;
}

/**
 * Whether this Mac is offered as a computer here, and whether a computer
 * already is it: undefined while the list that says so is still loading.
 * Read only while `enabled`, and each read waits on the one before, so a
 * browser, or a Mac the helper cannot run on, asks the server nothing.
 * `poll` and `reread` as for `useRemoteControl`. `settled` once every read it
 * waits on has answered or failed, for a surface laid out by the answer: the
 * shell answers apart from the server, and can land after it.
 */
export function useThisMacAvailability(options: RemoteControlReader) {
  const remoteControl = useRemoteControl(options);
  const { data: status, isLoading: statusLoading } = remoteControl;
  const { data: providers, isLoading: providersLoading } = useComputerProviders({
    enabled: options.enabled && !!status?.supported,
  });
  const offered = options.enabled && remoteControlOffered(status, providers);
  const { data: list, isLoading: listLoading } = useComputers({ enabled: offered });
  const added = !offered ? false : list?.computers.some((c) => isThisMac(c, status?.fingerprint));
  // A read that failed has answered too, or one refused list would hold the
  // surface shut; `added` then stays undefined, and the card stays out.
  // A null kept from a channel that threw is no answer, so the read this mount
  // starts is waited for; the polls after it are not, or a form open on the
  // answer would close with each one.
  const statusRetrying = status === null && remoteControl.isFetching && !remoteControl.isFetchedAfterMount;
  const settled = !statusLoading && !statusRetrying && !providersLoading && !listLoading;
  return { offered, added, status, settled };
}

/**
 * Resolves once no computer is being created or reconnected. Either stops
 * counting only after its own onSuccess has updated the list, so what is read
 * after this holds a computer a setup just added, or the new host key of one
 * it just reconnected.
 */
function createsLanded(queryClient: QueryClient): Promise<void> {
  const creating = () =>
    queryClient.isMutating({ mutationKey: CREATE_COMPUTER_MUTATION_KEY }) > 0 ||
    queryClient.isMutating({ mutationKey: RECONNECT_COMPUTER_MUTATION_KEY }) > 0;
  return new Promise((resolve) => {
    if (!creating()) {
      resolve();
      return;
    }
    const unsubscribe = queryClient.getMutationCache().subscribe(() => {
      if (creating()) return;
      unsubscribe();
      resolve();
    });
  });
}

/** Whether any computer still logs in to this host key. Rejects when the list cannot be read. */
async function inUse(queryClient: QueryClient, fingerprint: string | null): Promise<boolean> {
  if (!fingerprint) return false;
  const { computers } = await fetchComputerList(queryClient);
  // A removed row can stay on the list for a moment on its way out.
  return computers.some((c) => c.status !== 'deleted' && isThisMac(c, fingerprint));
}

// One login item and one folder on this Mac, so turning it on and off take
// turns: a disable owed by a dialog that closed mid-enable cannot land inside
// the enable a reopened one starts. It holds no user data, so sign-out has
// nothing to reset here.
let bridgeQueue: Promise<unknown> = Promise.resolve();

/** Run `job` once every earlier one has settled. Never call it from inside a job: that job would wait on itself. */
export function withRemoteControl<T>(job: () => Promise<T>): Promise<T> {
  const run = bridgeQueue.then(job);
  bridgeQueue = run.catch(() => {});
  return run;
}

/**
 * Turn remote control off unless a computer still is this Mac.
 *
 * `onlyFor` turns it off only while the helper is still the Mac that host key
 * named, for a removed row. `when` is asked as the job starts, behind every
 * earlier one, so a caller can decide from how a try still in flight ended.
 * Never rejects: a list that cannot be read leaves it on, since turning off a
 * Mac in use costs more than leaving an unused one on, and the app menu can
 * still turn that one off.
 */
export function releaseRemoteControl(
  queryClient: QueryClient,
  { onlyFor, when }: { onlyFor?: string; when?: () => boolean } = {},
): Promise<void> {
  return withRemoteControl(async () => {
    if (when && !when()) return;
    const status = await getRemoteControl();
    if (!status?.supported) return;
    if (onlyFor !== undefined && status.fingerprint !== onlyFor) return;
    // A setup closes on the computer it has just created or reconnected.
    await createsLanded(queryClient);
    if (await inUse(queryClient, status.fingerprint)) return;
    setRemoteControlStatus(queryClient, await disableRemoteControl());
  }).catch(() => {});
}

/**
 * Turn off a helper the deployment refuses, unless a computer of this account
 * is still this Mac. The gateway refuses a tunnel only once it is gone, and one
 * goes only when no computer uses it: this Mac was removed somewhere other than
 * this app, or set up and never added. A helper that merely cannot reach the
 * gateway says connecting instead.
 */
export function useReleaseRefusedRemoteControl(status: RemoteControlStatus | null | undefined): void {
  const queryClient = useQueryClient();
  const refused = status?.paired && status.helper === 'refused' ? status.fingerprint : null;
  useEffect(() => {
    if (refused) void releaseRemoteControl(queryClient, { onlyFor: refused });
  }, [queryClient, refused]);
}
