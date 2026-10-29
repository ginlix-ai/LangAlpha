/**
 * Computer endpoints: the machine a workspace runs on.
 *
 * `POST /computers` is the only route here that allocates anything, so it is
 * the only one that can be refused for capacity; every other call is about a
 * machine the user already has.
 */
import { api } from '@/api/client';
import type {
  Computer,
  ComputerActionResponse,
  ComputerCreate,
  ComputerProvidersResponse,
  ComputerReconnectTarget,
  ComputerReinstall,
  ComputerRemoveResponse,
  ComputerStorage,
  ComputersResponse,
  MakePrimaryResponse,
  SshKeyResponse,
  SshPairing,
  SshPairingStatus,
  SshProbeResult,
  SshTarget,
  ResourceTier,
  UnsavedFile,
} from '@/types/api';

import { apiErrorDetail, apiErrorStatus } from '@/api/errors';
import { streamStatusEvents, type StatusFrame } from './statusStream';

// Matches the workspace module's bounds: starting a machine legitimately runs
// tens of seconds, while a read that hangs should surface as a failure.
const COMPUTER_MUTATION_TIMEOUT_MS = 120000;
const WORKSPACES_REMOVAL_TIMEOUT_MS = 600000;
// `du` across every folder on the machine. The server bounds the reading at the
// exec's 30 s plus a 5 s connect, then answers the stored reading instead of
// failing; wait past that so a slow machine shows its last reading.
const COMPUTER_STORAGE_TIMEOUT_MS = 45000;

export async function getComputers(): Promise<ComputersResponse> {
  const { data } = await api.get<ComputersResponse>('/api/v1/computers');
  return data;
}

/**
 * The providers a new computer can go on, default first, each with whether
 * the user's plan allows it. One entry means there is no choice to offer.
 */
export async function getComputerProviders(): Promise<ComputerProvidersResponse> {
  const { data } = await api.get<ComputerProvidersResponse>('/api/v1/computers/providers');
  return data;
}

/**
 * Create a computer. The tier is picked here because it is the machine's
 * property, not a workspace's. In platform mode this is the metered route: a
 * refusal arrives as a 429 whose `detail.message` is the platform's own
 * wording, which the shared API client puts on `err.rateLimitInfo`, or as a
 * 403 when the plan lacks the tier or the provider. With no name the server
 * picks one from the provider.
 */
export async function createComputer(body: ComputerCreate = {}): Promise<Computer> {
  const { data } = await api.post<Computer>(
    '/api/v1/computers',
    {
      ...(body.name ? { name: body.name } : {}),
      resource_tier: body.resource_tier ?? 'standard',
      ...(body.kind ? { kind: body.kind } : {}),
      ...(body.ssh ? { ssh: body.ssh } : {}),
      ...(body.files_backup !== undefined ? { files_backup: body.files_backup } : {}),
    },
    { timeout: COMPUTER_MUTATION_TIMEOUT_MS },
  );
  return data;
}

/**
 * Start a stopped computer. `{ lazy: true }` returns 202 immediately with
 * status 'starting' and continues in the background, which is what a UI that
 * then watches the status stream wants.
 */
export async function startComputer(
  computerId: string,
  opts: { lazy?: boolean } = {},
): Promise<ComputerActionResponse> {
  if (!computerId) throw new Error('Computer ID is required');
  const params = opts.lazy ? '?lazy=true' : '';
  const { data } = await api.post<ComputerActionResponse>(
    `/api/v1/computers/${computerId}/start${params}`,
    null,
    { timeout: COMPUTER_MUTATION_TIMEOUT_MS },
  );
  return data;
}

/**
 * Set a stopped SSH computer up again on its machine after a start that
 * failed on its folder: take the folder back from the computer whose it is
 * now, or move to a new folder. A 202 says 'starting', and the start runs in
 * the background as a lazy one does. Refusals are a coded `detail`:
 * `folder_owned` (409, the folder is another computer of this account's),
 * `folder_marked` (409, the new folder is another computer's, with its
 * `owner`), `root_dir` (422, the new folder cannot be used), `not_stopped`,
 * `spec_in_progress`, `not_ssh`.
 */
export async function reinstallComputer(computerId: string, body: ComputerReinstall): Promise<ComputerActionResponse> {
  if (!computerId) throw new Error('Computer ID is required');
  const { data } = await api.post<ComputerActionResponse>(`/api/v1/computers/${computerId}/reinstall`, body, {
    timeout: COMPUTER_MUTATION_TIMEOUT_MS,
  });
  return data;
}

/**
 * Point an SSH computer at the connection a pairing bound to it reached, and
 * start it. A 202 says 'starting'. A 409 `folder_marked` means the machine
 * answered but its folder is another computer's now; sent again with that
 * `take_over`, the folder is taken back.
 */
export async function reconnectComputer(
  computerId: string,
  ssh: ComputerReconnectTarget,
): Promise<ComputerActionResponse> {
  if (!computerId) throw new Error('Computer ID is required');
  const { data } = await api.post<ComputerActionResponse>(
    `/api/v1/computers/${computerId}/reconnect`,
    { ssh },
    { timeout: COMPUTER_MUTATION_TIMEOUT_MS },
  );
  return data;
}

/** Stop a running computer, preserving its files. Every workspace on it stops with it. */
export async function stopComputer(computerId: string): Promise<ComputerActionResponse> {
  if (!computerId) throw new Error('Computer ID is required');
  const { data } = await api.post<ComputerActionResponse>(
    `/api/v1/computers/${computerId}/stop`,
    null,
    { timeout: COMPUTER_MUTATION_TIMEOUT_MS },
  );
  return data;
}

export async function renameComputer(computerId: string, name: string): Promise<Computer> {
  if (!computerId) throw new Error('Computer ID is required');
  const { data } = await api.patch<Computer>(`/api/v1/computers/${computerId}`, { name });
  return data;
}

/** The structured `detail` of a 409 from the spec route. */
export interface SpecConflictDetail {
  code: 'turn_active' | 'spec_in_progress' | 'busy' | 'backup_incomplete';
  message: string;
  files?: UnsavedFile[];
}

/** The 409 a spec request was refused with, or null for any other failure. */
export function specConflictDetail(err: unknown): SpecConflictDetail | null {
  if (apiErrorStatus(err) !== 409) return null;
  const detail = apiErrorDetail(err);
  // The server's contract for this route; narrowed once, here.
  return detail && typeof detail === 'object' && 'code' in detail ? (detail as SpecConflictDetail) : null;
}

/**
 * Ask for a tier change. The server answers 202 once the change is accepted
 * and runs it afterwards (a backup and a recreate take minutes), so the
 * returned row carries `spec_change` in progress and its outcome is read back
 * from the computer. A 200 means the machine is already at that tier. Up-front
 * refusals arrive as 403/429 (plan and quota, platform mode), or as a 409 with
 * a `SpecConflictDetail` (a turn running, a change or other operation already
 * in progress, a backup that could not finish).
 */
export async function setComputerSpec(computerId: string, tier: ResourceTier): Promise<Computer> {
  if (!computerId) throw new Error('Computer ID is required');
  const { data } = await api.post<Computer>(
    `/api/v1/computers/${computerId}/spec`,
    { tier },
    { timeout: COMPUTER_MUTATION_TIMEOUT_MS },
  );
  return data;
}

/**
 * Remove a computer: its sandbox and everything on its disk go with it. The
 * server keeps a tombstone, which the list no longer returns, and answers what
 * became of our key line on an SSH computer's machine (`key`, null for other
 * kinds, for one a helper answers, and when `removeKey` was false).
 * `removeKey` is sent explicitly, so the choice the user saw is the one that
 * is made. `deleteWorkspaces` names the workspaces on it to delete first, the
 * ones the user was shown; one it does not name keeps the computer. They go in
 * the body, since a computer full of them would overflow a request line limit
 * in the query, and in the query too while they fit there, for a backend from
 * before the body and a proxy that drops a DELETE's body. `workspaces_deleted`
 * answers which went: one moved off it meanwhile still exists.
 * `deleteFiles`, on an SSH computer, also deletes LangAlpha's files from its
 * machine, the named workspaces' folders among them. It refuses
 * with a 409 whose `detail.code` says why: `primary`, `has_workspaces`, or
 * `busy` (moving between states, being prepared, changing spec, a workspace
 * moving, or held by another action).
 */
/** As many ids as a request line keeps well under the 8 KB proxies allow. */
const QUERY_WORKSPACES_MAX = 30;

export async function deleteComputer(
  computerId: string,
  {
    removeKey,
    deleteWorkspaces = [],
    deleteFiles = false,
  }: { removeKey: boolean; deleteWorkspaces?: readonly string[]; deleteFiles?: boolean },
): Promise<ComputerRemoveResponse> {
  if (!computerId) throw new Error('Computer ID is required');
  const params = new URLSearchParams({ remove_key: String(removeKey) });
  if (deleteFiles) params.set('delete_files', 'true');
  if (deleteWorkspaces.length <= QUERY_WORKSPACES_MAX) {
    for (const id of deleteWorkspaces) params.append('delete_workspaces', id);
  }
  const { data } = await api.delete<ComputerRemoveResponse>(`/api/v1/computers/${computerId}`, {
    params,
    data: { delete_workspaces: deleteWorkspaces },
    // Each workspace takes a last backup on its way out.
    timeout: deleteWorkspaces.length ? WORKSPACES_REMOVAL_TIMEOUT_MS : COMPUTER_MUTATION_TIMEOUT_MS,
  });
  return data;
}

/**
 * Make this the computer new workspaces and Home land on. A 200 means the
 * flip landed now (Home was on no computer yet, or already on this one). A
 * 202 means Home's files move first: `move` is that move, read back like any
 * other, and the flip lands when it commits. Refusals are a 409 in the move's
 * error shape (`moveRefusal`).
 */
export async function makeComputerPrimary(computerId: string): Promise<MakePrimaryResponse> {
  if (!computerId) throw new Error('Computer ID is required');
  const { data } = await api.post<MakePrimaryResponse>(`/api/v1/computers/${computerId}/make-primary`, null, {
    timeout: COMPUTER_MUTATION_TIMEOUT_MS,
  });
  return data;
}

/** Keep the machine running (idle auto-stop off). Enabling on a stopped machine starts it. */
export async function setComputerAlwaysOn(computerId: string, enabled: boolean): Promise<Computer> {
  if (!computerId) throw new Error('Computer ID is required');
  const { data } = await api.post<Computer>(
    `/api/v1/computers/${computerId}/always-on`,
    { enabled },
    { timeout: COMPUTER_MUTATION_TIMEOUT_MS },
  );
  return data;
}

/**
 * Turn the file backup of a machine the user brings on or off. Off keeps each
 * workspace's last backup but stops updating it. The server refuses a hosted
 * machine, which always backs up, with a 409 whose `detail` is its sentence.
 */
export async function setComputerFilesBackup(computerId: string, enabled: boolean): Promise<Computer> {
  if (!computerId) throw new Error('Computer ID is required');
  const { data } = await api.post<Computer>(
    `/api/v1/computers/${computerId}/files-backup`,
    { enabled },
    { timeout: COMPUTER_MUTATION_TIMEOUT_MS },
  );
  return data;
}

/**
 * Disk reading plus a per-workspace breakdown. On a running machine this
 * measures now (and the server stores the reading on the row); otherwise it
 * returns the last stored reading with no breakdown.
 */
export async function getComputerStorage(computerId: string): Promise<ComputerStorage> {
  if (!computerId) throw new Error('Computer ID is required');
  const { data } = await api.get<ComputerStorage>(`/api/v1/computers/${computerId}/storage`, {
    timeout: COMPUTER_STORAGE_TIMEOUT_MS,
  });
  return data;
}

/**
 * Subscribe to a computer's lifecycle status via SSE. Same frames, keepalive
 * and 600 s cap as the workspace channel, and the same best-effort contract:
 * resolves on a terminal status, the server's timeout, or an abort, and never
 * throws on a network error.
 *
 * This is the authoritative channel for every workspace on the machine: one
 * subscription answers for all of them, where the workspace channel would open
 * one connection per workspace to report the same transition. While a new
 * sandbox is built, frames also name its `stage`; a failed background start
 * ends on an `error` frame that carries the reason.
 */
export async function streamComputerEvents(
  computerId: string,
  onFrame: (frame: StatusFrame) => void,
  signal: AbortSignal,
): Promise<void> {
  if (!computerId) return;
  await streamStatusEvents(`/api/v1/computers/${computerId}/events`, onFrame, signal);
}

/** The key a user authorizes on their own machine. Minted on the first call. */
export async function getSshKey(): Promise<SshKeyResponse> {
  const { data } = await api.get<SshKeyResponse>('/api/v1/computers/ssh/key');
  return data;
}

// The server bounds its own dial; this only keeps a hung request from lingering.
const SSH_PROBE_TIMEOUT_MS = 60000;

/**
 * Connect to the machine once and report what is on it, without creating
 * anything. With `computerId`, one of the user's SSH computers reached again
 * by hand, the computer limit is not asked.
 */
export async function probeSsh(target: SshTarget, { computerId = null }: { computerId?: string | null } = {}): Promise<SshProbeResult> {
  const body = computerId ? { ...target, computer_id: computerId } : target;
  const { data } = await api.post<SshProbeResult>('/api/v1/computers/ssh/probe', body, {
    timeout: SSH_PROBE_TIMEOUT_MS,
  });
  return data;
}

/**
 * Start pairing a machine: the one-line command the user runs on it. With a
 * `computerId` the pairing reconnects that SSH computer rather than adding
 * one: it is not counted against the SSH limit, and the machine it reaches
 * keeps the computer's folder.
 */
export async function createSshPairing({ computerId }: { computerId?: string | null } = {}): Promise<SshPairing> {
  const url = '/api/v1/computers/ssh/pairings';
  const { data } = computerId
    ? await api.post<SshPairing>(url, { computer_id: computerId })
    : await api.post<SshPairing>(url);
  return data;
}

/** Where a pairing stands. A 404 means it expired, or never was this user's. */
export async function getSshPairing(token: string): Promise<SshPairingStatus> {
  const { data } = await api.get<SshPairingStatus>(
    `/api/v1/computers/ssh/pairings/${encodeURIComponent(token)}`,
  );
  return data;
}

/**
 * Try an address the user gave for a machine that checked in but could not be
 * reached on its own. The login and host keys are the ones the machine reported.
 */
export async function connectSshPairing(
  token: string,
  address: Pick<SshTarget, 'host' | 'port'>,
): Promise<SshPairingStatus> {
  const { data } = await api.post<SshPairingStatus>(
    `/api/v1/computers/ssh/pairings/${encodeURIComponent(token)}/connect`,
    address,
    { timeout: SSH_PROBE_TIMEOUT_MS },
  );
  return data;
}
