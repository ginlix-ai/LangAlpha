/**
 * A workspace's computer that did not answer, in this app's words.
 *
 * The server names the connect code wherever it reports one: on a failed
 * turn's `error` frame (`error_code`), on a file listing or read it answered
 * from the backup (`unreachable`), and on a 503 with no backup to stand in
 * (the `X-Computer-Unreachable` header). Its own sentence is English and
 * says nothing the machine's owner can act on, so the code is what is read,
 * and the machine is called what its owner calls it: "your Mac" for a Mac,
 * its name for any other.
 */
import type { QueryClient } from '@tanstack/react-query';
import type { useTranslation } from 'react-i18next';

import { queryKeys } from '@/lib/queryKeys';
import type { Computer, ComputersResponse } from '@/types/api';

import { machineArtOf } from '../components/machineArt/machine';

type Translate = ReturnType<typeof useTranslation>['t'];

/** Read lower-cased: the API client keys every response header that way. */
const UNREACHABLE_HEADER = 'x-computer-unreachable';
/** The service's own tunnel gateway, which nothing done to the machine fixes. */
const SERVICE_CODE = 'gateway_unreachable';

/** How often a view showing a machine that went away asks again, so it returns to live by itself. */
export const AWAY_REFETCH_MS = 15_000;

export interface AwayMachine {
  /** A Mac the user brings, worded as "your Mac". */
  mac: boolean;
  /** Paired through the desktop app's Remote control, which answers only while the app is open. */
  remoteControl: boolean;
  /** Its name, for any other machine; null when not known. */
  name: string | null;
}

const UNKNOWN_MACHINE: AwayMachine = { mac: false, remoteControl: false, name: null };

export function awayMachineOf(computer: Pick<Computer, 'kind' | 'name' | 'ssh'> | null | undefined): AwayMachine {
  if (!computer) return UNKNOWN_MACHINE;
  const art = machineArtOf(computer);
  const mac = art !== 'cloud' && art !== 'linux';
  return { mac, remoteControl: mac && !!computer.ssh?.helper, name: computer.name?.trim() || null };
}

/** The suffix of the key worded for this machine: `Mac`, `Named`, or the unnamed one. */
function variant(machine: AwayMachine): 'Mac' | 'Named' | '' {
  if (machine.mac) return 'Mac';
  return machine.name ? 'Named' : '';
}

/** The connect code of a 503 refused because the computer did not answer; null for any other failure. */
export function unreachableCode(err: unknown): string | null {
  const response = (err as { response?: { status?: number; headers?: Record<string, unknown> } } | null)?.response;
  if (response?.status !== 503) return null;
  const code = response.headers?.[UNREACHABLE_HEADER];
  return typeof code === 'string' && code ? code : null;
}

/** Whether the code is the machine's own silence rather than the service's gateway. */
export function isMachineCode(code: string | null | undefined): boolean {
  return !!code && code !== SERVICE_CODE;
}

/**
 * The connect code of a listing or read the server answered from the backup
 * because the computer did not answer; null for a live one, and for a stopped
 * workspace's backup, which carries no code.
 */
export function backupUnreachable(data: unknown): string | null {
  const answer = data as { source?: unknown; unreachable?: unknown } | null | undefined;
  return answer?.source === 'database' && typeof answer.unreachable === 'string' && answer.unreachable
    ? answer.unreachable
    : null;
}

/** What to tell someone whose request the computer could not take. */
export function awayRetryText(t: Translate, code: string, machine: AwayMachine): string {
  if (code === SERVICE_CODE) return t('filePanel.away.service');
  return t(`filePanel.away.retry${variant(machine)}`, { name: machine.name });
}

/** A request refused because the computer did not answer, in words; null for any other failure. */
export function awayRetryMessage(err: unknown, t: Translate, machine: AwayMachine): string | null {
  const code = unreachableCode(err);
  return code ? awayRetryText(t, code, machine) : null;
}

/** The notice over files the panel shows from the backup while the computer is away. */
export function backupShownText(t: Translate, code: string, machine: AwayMachine): string {
  if (code === SERVICE_CODE) return t('filePanel.away.backupService');
  return t(`filePanel.away.backup${variant(machine)}`, { name: machine.name });
}

/**
 * A turn that ended because its computer did not answer: a headline naming
 * the machine and what brings it back. A Mac under Remote control answers
 * only while the desktop app is open on it, so that is what it is told.
 */
export function unreachableTurnCopy(
  t: Translate, code: string | undefined, machine: AwayMachine,
): { title: string; detail: string } {
  if (code === SERVICE_CODE) {
    return { title: t('chat.computerUnreachable.titleService'), detail: t('chat.computerUnreachable.service') };
  }
  const title = t(`chat.computerUnreachable.title${variant(machine)}`, { name: machine.name });
  if (machine.remoteControl) return { title, detail: t('chat.computerUnreachable.remoteControl') };
  return { title, detail: t(`chat.computerUnreachable.check${variant(machine)}`, { name: machine.name }) };
}

/**
 * The computer as the lists already read hold it, or null. Read from the
 * cache when a request fails rather than subscribed to: a surface that never
 * fails pays no read of the computer list for it.
 */
export function cachedComputer(queryClient: QueryClient, computerId: string | null | undefined): Computer | null {
  if (!computerId) return null;
  for (const [, data] of queryClient.getQueriesData<ComputersResponse>({ queryKey: queryKeys.computers.lists() })) {
    const row = data?.computers?.find((c) => c.computer_id === computerId);
    if (row) return row;
  }
  return null;
}
