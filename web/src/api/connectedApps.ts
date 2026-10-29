/**
 * The outside apps connected to the account, held by the platform service.
 */
import { api } from '@/api/client';
import { isPlatformMode } from '@/config/hostMode';

/**
 * Signs every connected app out of the account. Fire and forget: the caller
 * has already done what it set out to do, and this must neither delay nor
 * fail it. After a password change the platform service also signs them out
 * itself, so this call only gets there first. A self-hosted build has no
 * platform service to ask.
 */
export function revokeAllConnectedApps(): void {
  if (!isPlatformMode) return;
  api.post('/api/auth/connected-apps/revoke-all').catch((err: unknown) => {
    console.warn('[auth] could not sign connected apps out:', err);
  });
}
