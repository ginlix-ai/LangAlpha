/** The server's own settings exist only on a self-hosted build: a hosted one's
 *  keys and sandbox are its operator's, so no link or search may reach them. */
import { describe, expect, it, vi } from 'vitest';

vi.mock('@/config/hostMode', () => ({ isPlatformMode: true, HOST_MODE: 'platform', APP_ENTRY_PATH: '/app' }));

import { readSection, searchableSettings, sectionShown } from '../sections';

describe('the server section on a hosted build', () => {
  it('is not drawn, not linked to, and not found by search', () => {
    expect(sectionShown('server')).toBe(false);
    expect(readSection(new URLSearchParams('tab=server'), '')).toBe('account');
    expect(readSection(new URLSearchParams(), '#agent-location')).toBe('account');
    expect(searchableSettings(false).some((entry) => entry.section === 'server')).toBe(false);
  });
});
