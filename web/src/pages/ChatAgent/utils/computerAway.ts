/**
 * A workspace's computer that did not answer, in this app's words.
 *
 * The server names the connect code wherever it reports one: on a failed
 * turn's `error` frame (`error_code`), on a file listing or read it answered
 * from the backup (`ReadFrom`), and on a 503 with no backup to stand in
 * (the `X-Computer-Unreachable` header). Its own sentence is English and
 * says nothing the machine's owner can act on, so the code is what is read,
 * and the machine is called what its owner calls it: "your Mac" for a Mac,
 * its name for any other.
 */
import type { useTranslation } from 'react-i18next';

import { isClientError } from '@/api/errors';
import type { Computer } from '@/types/api';

import { machineArtOf } from '../components/machineArt/machine';
import type { ReadFrom } from './api/readFrom';

type Translate = ReturnType<typeof useTranslation>['t'];

/** Read lower-cased: the API client keys every response header that way. */
const UNREACHABLE_HEADER = 'x-computer-unreachable';
/** The service's own tunnel gateway. */
export const SERVICE_CODE = 'gateway_unreachable';

/** How often a view showing a machine that went away asks again, so it returns to live by itself. */
const AWAY_REFETCH_MS = 15_000;

/** The queries polling for a computer. Held weakly, so an entry goes with its query. */
const polling = new WeakSet<object>();

/**
 * A query's refetch interval: asked again while it shows a computer that did
 * not answer, by its connect code. A poll that fails without a code (the
 * network, the edge, a 5xx) says nothing about the machine, so it keeps the
 * polling going; only an answer, a read or a 4xx, ends it.
 */
export function awayRefetch(
  query: { state: { error: unknown; data?: unknown } },
  code: string | null | undefined,
): number | false {
  const { error, data } = query.state;
  // Checked before the code, which a backup body kept from an earlier read
  // still carries: a 4xx is the server's answer about this file.
  if (error && isClientError(error)) {
    polling.delete(query);
    return false;
  }
  if (code) {
    polling.add(query);
    return AWAY_REFETCH_MS;
  }
  if (!error && data !== undefined) polling.delete(query);
  return polling.has(query) ? AWAY_REFETCH_MS : false;
}

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

/** The suffix of the key worded for this code and machine: `Service` for the
 *  service's own gateway, which nothing done to the machine fixes, else `Mac`,
 *  `Named`, or the unnamed one. */
function variant(code: string | undefined, machine: AwayMachine): 'Service' | 'Mac' | 'Named' | '' {
  if (code === SERVICE_CODE) return 'Service';
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

/** The connect code of a read the backup answered because the computer did
 *  not; null for a live read, and for a stopped workspace's backup. */
export function backupCode(from: ReadFrom | undefined): string | null {
  return from?.kind === 'backup' ? from.unreachable : null;
}

/** What to tell someone whose request the computer could not take. */
export function awayRetryText(t: Translate, code: string, machine: AwayMachine): string {
  return t(`filePanel.away.retry${variant(code, machine)}`, { name: machine.name });
}

/** A request refused because the computer did not answer, in words; null for any other failure. */
export function awayRetryMessage(err: unknown, t: Translate, machine: AwayMachine): string | null {
  const code = unreachableCode(err);
  return code ? awayRetryText(t, code, machine) : null;
}

/** The notice over files the panel shows from the backup while the computer is away. */
export function backupShownText(t: Translate, code: string, machine: AwayMachine): string {
  return t(`filePanel.away.backup${variant(code, machine)}`, { name: machine.name });
}

/**
 * A turn that ended because its computer did not answer: a headline naming
 * the machine and what brings it back. A Mac under Remote control answers
 * only while the desktop app is open on it, so that is what it is told.
 */
export function unreachableTurnCopy(
  t: Translate, code: string | undefined, machine: AwayMachine,
): { title: string; detail: string } {
  const v = variant(code, machine);
  const title = t(`chat.computerUnreachable.title${v}`, { name: machine.name });
  if (v === 'Mac' && machine.remoteControl) return { title, detail: t('chat.computerUnreachable.remoteControl') };
  return { title, detail: t(`chat.computerUnreachable.check${v}`, { name: machine.name }) };
}
