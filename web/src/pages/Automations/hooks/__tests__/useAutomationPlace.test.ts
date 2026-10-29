import { describe, it, expect, vi } from 'vitest';
import { renderHookWithProviders } from '@/test/utils';

vi.mock('@/hooks/useHomeWorkspace', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useHomeWorkspace')>()),
  useHomeWorkspaceId: () => 'home-1',
}));

import { useAutomationPlace } from '../useAutomationPlace';

const place = () => renderHookWithProviders(() => useAutomationPlace()).result.current;

describe('useAutomationPlace', () => {
  it('reads a row naming Home as Chief of Staff', () => {
    const { label, runsInHome } = place();
    expect(label({ workspace_id: 'home-1' })).toBe('Chief of Staff');
    expect(label({ workspace_id: 'home-1' }, 'Home')).toBe('Chief of Staff');
    expect(runsInHome({ workspace_id: 'home-1' })).toBe(true);
  });

  it('reads any other row as its workspace, by name when given', () => {
    const { label, runsInHome } = place();
    expect(label({ workspace_id: 'ws-nvda' }, 'NVDA deep dive')).toBe('In NVDA deep dive');
    expect(label({ workspace_id: 'ws-nvda' })).toBe('Workspace');
    expect(runsInHome({ workspace_id: 'ws-nvda' })).toBe(false);
  });
});
