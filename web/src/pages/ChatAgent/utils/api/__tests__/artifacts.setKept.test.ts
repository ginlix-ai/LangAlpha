import { beforeEach, describe, expect, it, vi } from 'vitest';

const mockPut = vi.hoisted(() => vi.fn());
vi.mock('@/api/client', () => ({ api: { put: mockPut, defaults: { baseURL: '' } } }));
vi.mock('../messages', () => ({ sendChatMessageStream: vi.fn() }));

import { setArtifactKept } from '../artifacts';

describe('setArtifactKept', () => {
  beforeEach(() => {
    mockPut.mockReset();
    mockPut.mockResolvedValue({ data: { artifact_id: 'a1' } });
  });

  it('carries what the open page loaded, which a path built in code is named by alone', async () => {
    await setArtifactKept('a1', true, { now: ['data/b.csv'], earlier: ['data/a.csv'] });
    expect(mockPut).toHaveBeenCalledWith(expect.stringMatching(/\/kept$/), {
      kept: true,
      loaded: ['data/b.csv'],
      earlier: ['data/a.csv'],
    });
  });

  it('sends no report to stop keeping, or with none to send', async () => {
    await setArtifactKept('a1', false, { now: ['data/b.csv'], earlier: [] });
    await setArtifactKept('a1', true);
    expect(mockPut.mock.calls.map((call) => call[1])).toEqual([{ kept: false }, { kept: true }]);
  });
});
