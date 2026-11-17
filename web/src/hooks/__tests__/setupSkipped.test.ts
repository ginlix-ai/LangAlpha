/**
 * "Exit setup" holds for the account that pressed it, read under that
 * account: another one signed in on the tab, with no auth reset between (the
 * hosted console switches accounts and reloads the app), has its own setup.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

vi.mock('@/config/hostMode', () => ({ isPlatformMode: true, HOST_MODE: 'platform', APP_ENTRY_PATH: '/app' }));
vi.mock('@/config/localUser', () => ({ LOCAL_USER_ID: 'local-dev-user', hasAuthEnv: true }));
vi.mock('../useUser', () => ({ useUser: vi.fn() }));
vi.mock('../useClaudeCode', () => ({ useClaudeCodeSetup: vi.fn() }));

import { setStorageUser } from '@/lib/userStorage';
import { setupSkipped, skipSetup } from '../useSetupGate';

describe('setupSkipped', () => {
  beforeEach(() => {
    sessionStorage.clear();
    setStorageUser(null);
  });

  it('holds only for the account that pressed it', () => {
    setStorageUser('u1');
    skipSetup();
    expect(setupSkipped()).toBe(true);
    setStorageUser('u2');
    expect(setupSkipped()).toBe(false);
    setStorageUser('u1');
    expect(setupSkipped()).toBe(true);
  });

  it('is not adopted from the unscoped flag an older app wrote', () => {
    sessionStorage.setItem('ginlix_setup_skipped', '1');
    setStorageUser('u2');
    expect(setupSkipped()).toBe(false);
  });
});
