/**
 * A self-hosted server's own setup: where it runs agent code and the vendor
 * keys its tools use. Every route 404s on a hosted server, and a hosted build
 * asks none of them (`setupStatusQuery`).
 */
import { api } from '@/api/client';

export type SandboxKind = 'local' | 'docker' | 'daytona' | 'e2b';

export interface InstallGroup {
  id: string;
  required: boolean;
  selected: boolean;
  /** Measured sizes; absent for a group not yet measured. */
  download_mb?: number | null;
  disk_mb?: number | null;
}

export interface SandboxOption {
  kind: SandboxKind;
  available: boolean;
  /** Why it is not: `not_offered` (local, docker), `daemon_unreachable` (docker). */
  detail: string | null;
  install_groups?: InstallGroup[];
  key_set?: boolean;
}

export interface SetupKey {
  name: string;
  group: 'data' | 'search' | 'fetch' | 'sandbox';
  provider: string;
  /** `environment` is set outside the app, so it is read-only here. */
  state: 'unset' | 'saved' | 'environment';
  masked?: string;
}

export interface SetupStatus {
  completed: boolean;
  needs_setup: boolean;
  sandbox: {
    /** The one in effect, not necessarily the saved choice. */
    provider: string;
    provider_source: 'environment' | 'saved' | 'config' | 'detected';
    options: SandboxOption[];
  };
  keys: SetupKey[];
  /** The server runs inside the desktop app, so only while this computer is awake and the app is open. */
  desktop_app?: boolean;
}

/** The server's verdict on a key; `error` is its own English sentence. */
export type KeyTestResult = { ok: true } | { ok: false; error: string };

/** The gate waits on this before the app opens, so a stalled server cannot hold it for long. */
const STATUS_TIMEOUT_MS = 8000;

export async function getSetupStatus(): Promise<SetupStatus> {
  const { data } = await api.get<SetupStatus>('/api/v1/setup/status', { timeout: STATUS_TIMEOUT_MS });
  return data;
}

export async function saveSetupKeys(keys: Record<string, string | null>): Promise<SetupStatus> {
  const { data } = await api.put<SetupStatus>('/api/v1/setup/keys', { keys });
  return data;
}

export async function testSetupKey(name: string, value: string): Promise<KeyTestResult> {
  const { data } = await api.post<KeyTestResult>('/api/v1/setup/keys/test', { name, value });
  return data;
}

export async function saveSetupSandbox(provider: SandboxKind, installGroups?: string[]): Promise<SetupStatus> {
  const { data } = await api.put<SetupStatus>('/api/v1/setup/sandbox', {
    provider,
    ...(installGroups ? { install_groups: installGroups } : {}),
  });
  return data;
}

export async function completeSetup(): Promise<SetupStatus> {
  const { data } = await api.post<SetupStatus>('/api/v1/setup/complete');
  return data;
}
