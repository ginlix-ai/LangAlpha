/**
 * A refused create re-reads the computers, whoever opened the form: a refusal
 * for one still being prepared means the list the form chose from is behind.
 */
import { describe, it, expect, vi } from 'vitest';
import type { Mock } from 'vitest';

import { renderHookWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';

vi.mock('../../utils/api', () => ({ createWorkspace: vi.fn() }));

import { createWorkspace } from '../../utils/api';
import { useCreateWorkspace } from '../useCreateWorkspace';

describe('useCreateWorkspace', () => {
  it('re-reads the computers on a refusal, and passes the refusal on', async () => {
    const refusal = Object.assign(new Error('Request failed with status code 409'), {
      response: {
        status: 409,
        data: { detail: { code: 'computer_preparing', message: 'Computer is still being prepared' } },
      },
    });
    (createWorkspace as Mock).mockRejectedValue(refusal);
    const { result, queryClient } = renderHookWithProviders(() => useCreateWorkspace());
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');

    await expect(result.current({ name: 'Raced', description: '' })).rejects.toBe(refusal);
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.computers.lists() });
  });
});
