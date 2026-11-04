import { describe, it, expect, vi } from 'vitest';
import type { Mock } from 'vitest';

vi.mock('@/api/client', () => ({ api: { get: vi.fn() } }));

import { api } from '@/api/client';
import { getClaudeCodeComputers } from '../model';

const failing = (status: number) => Object.assign(new Error(`HTTP ${status}`), { response: { status } });

describe('getClaudeCodeComputers', () => {
  it('reads a server with no list of computers as one with none of them', async () => {
    (api.get as Mock).mockRejectedValueOnce(failing(404));
    await expect(getClaudeCodeComputers()).resolves.toEqual({ computers: [] });
  });

  it('passes any other failure on, for the read to try again', async () => {
    (api.get as Mock).mockRejectedValueOnce(failing(503));
    await expect(getClaudeCodeComputers()).rejects.toThrow('HTTP 503');
  });
});
