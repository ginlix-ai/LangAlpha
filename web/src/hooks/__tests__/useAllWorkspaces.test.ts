import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Mock } from 'vitest';

vi.mock('../../pages/ChatAgent/utils/api', () => ({
  getWorkspaces: vi.fn(),
}));

import { getWorkspaces } from '../../pages/ChatAgent/utils/api';
import { getAllWorkspaces } from '../useAllWorkspaces';

const mockGetWorkspaces = getWorkspaces as Mock;

function workspace(index: number) {
  return {
    workspace_id: `workspace-${index}`,
    name: `Workspace ${index}`,
    status: 'stopped',
    updated_at: '2026-09-20T00:00:00Z',
  };
}

describe('getAllWorkspaces', () => {
  beforeEach(() => vi.resetAllMocks());

  it('loads every page in the order asked for, Home included when asked', async () => {
    const firstPage = Array.from({ length: 100 }, (_, index) => workspace(index));
    const secondPage = [workspace(100)];
    mockGetWorkspaces
      .mockResolvedValueOnce({ workspaces: firstPage, total: 101 })
      .mockResolvedValueOnce({ workspaces: secondPage, total: 101 });

    const rows = await getAllWorkspaces('name', true);

    expect(rows).toHaveLength(101);
    expect(mockGetWorkspaces).toHaveBeenCalledTimes(2);
    expect(mockGetWorkspaces).toHaveBeenNthCalledWith(1, 100, 0, 'name', true);
    expect(mockGetWorkspaces).toHaveBeenNthCalledWith(2, 100, 100, 'name', true);
  });

  it('reads again from the top when a row moved up past the read', async () => {
    const firstPage = Array.from({ length: 100 }, (_, index) => workspace(index));
    mockGetWorkspaces
      // Workspace 100 moved up into the first page after it was read, pushing 99 down.
      .mockResolvedValueOnce({ workspaces: firstPage, total: 101 })
      .mockResolvedValueOnce({ workspaces: [workspace(99)], total: 101 })
      .mockResolvedValueOnce({ workspaces: [workspace(100), ...firstPage.slice(0, 99)], total: 101 })
      .mockResolvedValueOnce({ workspaces: [workspace(99)], total: 101 });

    const rows = await getAllWorkspaces('custom', false);

    expect(rows).toHaveLength(101);
    expect(new Set(rows.map((ws) => ws.workspace_id)).size).toBe(101);
    expect(mockGetWorkspaces).toHaveBeenCalledTimes(4);
    expect(mockGetWorkspaces).toHaveBeenNthCalledWith(3, 100, 0, 'custom', false);
  });

  it('settles for a short read after three tries rather than read forever', async () => {
    const firstPage = Array.from({ length: 100 }, (_, index) => workspace(index));
    mockGetWorkspaces.mockImplementation(async (_limit: number, offset: number) =>
      offset === 0 ? { workspaces: firstPage, total: 101 } : { workspaces: [workspace(99)], total: 101 });

    const rows = await getAllWorkspaces('custom', false);

    expect(rows).toHaveLength(100);
    expect(mockGetWorkspaces).toHaveBeenCalledTimes(6);
  });
});
