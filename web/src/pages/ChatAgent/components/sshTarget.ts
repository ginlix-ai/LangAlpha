/**
 * The address of the user's own machine as they type it, and the body that
 * adds it. Pure, so the reading rules are pinned by tests rather than by the
 * form: people paste the `ssh` line they already use, so that is accepted as
 * well as a bare `user@host[:port]`.
 */
import type { ComputerCreate, ComputerReconnectTarget, SshPairingStatus, SshProbeResult, SshTarget } from '@/types/api';

export const SSH_DEFAULT_PORT = 22;

/** Why an address could not be read. `empty` is not worth a message. */
export type SshAddressError = 'empty' | 'host' | 'port';

/** An address as read; the login is null when it named none. */
export type SshAddress =
  | { ok: true; username: string | null; host: string; port: number }
  | { ok: false; error: SshAddressError };

// The ssh(1) options that take an argument, so the word after one is its
// value rather than the destination.
const OPTIONS_WITH_ARGUMENT = new Set('BbcDEeFIiJLlmOoPpQRSWw');

// Loose on purpose: the server validates the host and words its own refusal.
// This only catches what is plainly not a host, such as a URL.
const HOST_CHARS = /^[A-Za-z0-9._%:-]+$/;

function readPort(raw: string): number | null {
  if (!/^\d{1,5}$/.test(raw)) return null;
  const port = Number(raw);
  return port >= 1 && port <= 65535 ? port : null;
}

/** `user@host`, `user@host:port`, `user@[v6]:port`, a bare v6 address, or `ssh://`. */
function readDestination(word: string): { username: string | null; host: string; port: string | null } | null {
  let rest = word.replace(/^ssh:\/\//i, '').replace(/\/$/, '');
  // A URL of some other scheme, or a path: not an address.
  if (rest.includes('/')) return null;
  let username: string | null = null;
  // The last `@`, as ssh splits it: a host never holds one.
  const at = rest.lastIndexOf('@');
  if (at >= 0) {
    username = rest.slice(0, at) || null;
    rest = rest.slice(at + 1);
  }
  if (rest.startsWith('[')) {
    const close = rest.indexOf(']');
    if (close < 0) return null;
    const after = rest.slice(close + 1);
    if (after && !after.startsWith(':')) return null;
    return { username, host: rest.slice(1, close), port: after ? after.slice(1) : null };
  }
  const colons = rest.split(':').length - 1;
  if (colons === 1) {
    const [host, port] = rest.split(':');
    return { username, host, port };
  }
  // No colon, or a bare IPv6 address, which leaves no room for a port.
  return { username, host: rest, port: null };
}

/**
 * Read what the user typed or pasted. Options are read as ssh reads them,
 * before and after the destination, and clustered (`-vp 2222`); `-p` and `-l`
 * win over a port or login in the destination, as they do for ssh. Any other
 * option is skipped with its argument, and words after the destination that
 * are not options are the remote command, which is ignored.
 */
export function parseSshAddress(raw: string): SshAddress {
  const words = raw.trim().split(/\s+/).filter(Boolean);
  if (words[0] === 'ssh') words.shift();
  if (words.length === 0) return { ok: false, error: 'empty' };

  let destination: string | null = null;
  let login: string | null = null;
  let portArg: string | null = null;
  let optionsEnded = false;

  for (let i = 0; i < words.length; i++) {
    const word = words[i];
    if (!optionsEnded && word === '--') {
      optionsEnded = true;
      continue;
    }
    if (!optionsEnded && word.length > 1 && word.startsWith('-')) {
      for (let j = 1; j < word.length; j++) {
        const flag = word[j];
        if (!OPTIONS_WITH_ARGUMENT.has(flag)) continue;
        const attached = word.slice(j + 1);
        const value = attached || words[++i];
        if (value === undefined) {
          if (flag === 'p') return { ok: false, error: 'port' };
          break;
        }
        if (flag === 'p') portArg = value;
        if (flag === 'l') login = value;
        break;
      }
      continue;
    }
    if (destination !== null) break;
    destination = word;
  }

  if (destination === null) return { ok: false, error: 'host' };
  const dest = readDestination(destination);
  if (!dest || !dest.host || !HOST_CHARS.test(dest.host)) return { ok: false, error: 'host' };
  const portText = portArg ?? dest.port;
  const port = portText === null ? SSH_DEFAULT_PORT : readPort(portText);
  if (port === null) return { ok: false, error: 'port' };
  return { ok: true, username: login ?? dest.username, host: dest.host, port };
}

/** `host:port`, with an IPv6 host bracketed so the port reads apart from it. */
export function formatSshHostPort(host: string, port: number): string {
  return `${host.includes(':') ? `[${host}]` : host}:${port}`;
}

/** `user@host`, with the port only when it is not 22. */
export function formatSshAddress(target: SshTarget): string {
  if (target.port === SSH_DEFAULT_PORT) return `${target.username}@${target.host}`;
  return `${target.username}@${formatSshHostPort(target.host, target.port)}`;
}

/**
 * How a machine is reached, as the user reads it: `user@name` through a
 * tunnel, whose host is only a name and whose port is the machine's own
 * business, else {@link formatSshAddress}.
 */
export function formatSshReached(target: SshTarget, tunneled: boolean): string {
  return tunneled ? `${target.username}@${target.host}` : formatSshAddress(target);
}

/** A machine the server connected to, and what it found there. */
export interface SshReady {
  target: SshTarget;
  probe: SshProbeResult;
}

/** The machine a pairing reached, once it is ready; null until then. */
export function pairingReady(pairing: SshPairingStatus | undefined): SshReady | null {
  return pairing?.status === 'ready' && pairing.target && pairing.probe ? { target: pairing.target, probe: pairing.probe } : null;
}

/** The name a computer gets when the user leaves the prefill alone. */
export function sshDefaultName(ready: SshReady): string {
  return ready.probe.hostname?.trim() || ready.target.host;
}

/**
 * `~` and `~/x` read against the login's home, as a shell there would read
 * them: the server takes only an absolute folder. With no home reported there
 * is nothing to read them against, so the folder goes as typed and the
 * server's refusal says what it needs.
 */
function expandHome(folder: string, home: string): string {
  if (!home || (folder !== '~' && !folder.startsWith('~/'))) return folder;
  const base = home.replace(/\/+$/, '');
  return folder === '~' ? base || '/' : `${base}${folder.slice(1)}`;
}

/**
 * The create body, or null until a supported machine is in. The host key is
 * the one the server saw when it connected, so the one pinned is the one
 * that answered; the folder is sent only when set.
 */
export function sshCreateBody(ready: SshReady | null, folder: string): ComputerCreate['ssh'] | null {
  if (!ready || !ready.probe.supported) return null;
  const { host, port, username, tunnel_id: tunnelId } = ready.target;
  const rootDir = expandHome(folder.trim(), ready.probe.home);
  return { host, port, username, ...(tunnelId ? { tunnel_id: tunnelId } : {}), host_key: ready.probe.host_key, ...(rootDir ? { root_dir: rootDir } : {}) };
}

/**
 * What a reconnect sends: where a pairing bound to the computer reached the
 * machine, and the host key that answered there, or null until a supported
 * machine is in. The folder is the computer's own, so none is sent.
 */
export function sshReconnectTarget(ready: SshReady | null): ComputerReconnectTarget | null {
  if (!ready || !ready.probe.supported) return null;
  const { host, port, username, tunnel_id: tunnelId } = ready.target;
  return { host, port, username, ...(tunnelId ? { tunnel_id: tunnelId } : {}), host_key: ready.probe.host_key };
}
