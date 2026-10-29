import { describe, expect, it, vi } from 'vitest';
import { renderHookWithProviders } from '@/test/utils';
import { catalogServer, httpCatalogServer } from '@/test/factories';
import type { CatalogServer } from '@/pages/ChatAgent/utils/api';
import { useShellData } from '../useShellData';

let servers: CatalogServer[] = [];
vi.mock('@/hooks/useMcpServers', () => ({
  useMcpCatalog: () => ({ data: { servers } }),
  useBuiltinMcpServers: () => ({ data: { servers: [] } }),
  useBrokerages: () => ({ data: [{ name: 'ibkr' }, { name: 'robinhood' }] }),
}));
vi.mock('@/hooks/usePlugins', () => ({ usePlugins: () => ({ data: undefined }) }));
vi.mock('@/hooks/useSkills', () => ({ useSkills: () => ({ data: undefined }) }));
vi.mock('@/hooks/useUserVault', () => ({ useUserVaultSecrets: () => ({ data: undefined }) }));

/**
 * The rail counts a broker the way its card reads it: only an http row signs
 * in, so a row the user pointed at a local command is not waiting on one.
 */
describe('the rail counts brokerages', () => {
  it('counts a signed-out http row as a sign-in and a local one as connected', () => {
    servers = [
      httpCatalogServer({ name: 'ibkr', oauth_status: null }),
      catalogServer({ name: 'robinhood', oauth_status: null }),
    ];
    const { result } = renderHookWithProviders(() => useShellData());
    expect(result.current.brokerages).toEqual({ connected: 1, signIn: 1 });
  });
});
