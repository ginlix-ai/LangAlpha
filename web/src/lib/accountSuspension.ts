/**
 * A suspended account, as any answer reveals it. Once an account's access
 * through its organization ends, every route but `/users/me` answers 403
 * `account_suspended`, so the first refusal the app meets (a list, a send, a
 * stream frame) is where it learns. Each reader reports here instead of
 * showing its own error, and the shell turns the whole app into one state.
 *
 * Module state, since the API client and the stream transport report it from
 * outside React; cleared on sign-out and account switch.
 */
import { useSyncExternalStore } from 'react';
import { registerAuthReset } from '@/lib/authResets';

/** The `type` of the refusal every route gives a suspended account. */
export const ACCOUNT_SUSPENDED = 'account_suspended';

export interface SuspensionNotice {
  /** The organization's name when the refusal carried it. */
  organization: string | null;
}

let current: SuspensionNotice | null = null;
const listeners = new Set<() => void>();

function emit(): void {
  for (const fn of listeners) fn();
}

/** Record a suspension the server reported. The first name it gives is kept:
 *  a later refusal that names none does not erase it. */
export function noteAccountSuspended(organization: string | null): void {
  if (current && (current.organization || !organization)) return;
  current = { organization: current?.organization ?? organization };
  emit();
}

/** The suspension `detail`, a refusal's structured body or a stream frame,
 *  reports, or null when it is no suspended account's. */
export function readSuspension(detail: unknown): SuspensionNotice | null {
  if (!detail || typeof detail !== 'object') return null;
  const d = detail as Record<string, unknown>;
  if (d.type !== ACCOUNT_SUSPENDED && d.error_type !== ACCOUNT_SUSPENDED) return null;
  return { organization: typeof d.organization === 'string' && d.organization ? d.organization : null };
}

/** Whether `detail` is the suspended account's; noted here when it is. */
export function noteIfSuspended(detail: unknown): boolean {
  const suspension = readSuspension(detail);
  if (suspension) noteAccountSuspended(suspension.organization);
  return suspension !== null;
}

function subscribe(fn: () => void): () => void {
  listeners.add(fn);
  return () => listeners.delete(fn);
}

export function accountSuspendedNotice(): SuspensionNotice | null {
  return current;
}

export function useAccountSuspendedNotice(): SuspensionNotice | null {
  return useSyncExternalStore(subscribe, accountSuspendedNotice, accountSuspendedNotice);
}

export function resetAccountSuspended(): void {
  if (current === null) return;
  current = null;
  emit();
}

registerAuthReset(resetAccountSuspended);
