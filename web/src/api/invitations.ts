import { api } from '@/api/client';

/** Redeems an invitation code on the platform service (`/api/auth/`, not `/api/v1/`). */
export async function redeemInvitation(code: string): Promise<void> {
  await api.post('/api/auth/invitations/redeem', { code });
}
