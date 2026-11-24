import { act, waitFor } from '@testing-library/react';
import { useQuery } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { getEntry, moveEntry, trashEntry } from '@/api/drive';
import { queryKeys } from '@/lib/queryKeys';
import { renderHookWithProviders } from '@/test/utils';
import { useDriveMutations } from '../useDrive';

vi.mock('@/api/drive', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/drive')>()),
  getEntry: vi.fn(),
  moveEntry: vi.fn(),
  trashEntry: vi.fn(),
}));

/** The open file's entry, observed as the page observes it, and the space's mutations. */
function openOn(path: string) {
  vi.mocked(getEntry).mockResolvedValue({ id: 'e1', path } as never);
  return renderHookWithProviders(() => ({
    entry: useQuery({ queryKey: queryKeys.drive.entry('personal', path), queryFn: () => getEntry('personal', path) }),
    m: useDriveMutations('personal'),
  }));
}

beforeEach(() => vi.resetAllMocks());

describe('useDriveMutations: the entry at a path that is no longer there', () => {
  it('is not read again after a move, which would only answer 404', async () => {
    const { result } = openOn('a/x.pdf');
    await waitFor(() => expect(result.current.entry.isSuccess).toBe(true));
    vi.mocked(moveEntry).mockResolvedValue({ id: 'e1', path: 'a/x.xlsx' } as never);
    await act(() => result.current.m.move.mutateAsync({ from: 'a/x.pdf', to: 'a/x.xlsx' }));
    await new Promise((r) => setTimeout(r, 0));
    expect(getEntry).toHaveBeenCalledTimes(1);
  });

  it('is not read again after it goes to the trash', async () => {
    const { result } = openOn('a/x.pdf');
    await waitFor(() => expect(result.current.entry.isSuccess).toBe(true));
    vi.mocked(trashEntry).mockResolvedValue('t1');
    await act(() => result.current.m.trash.mutateAsync('a/x.pdf'));
    await new Promise((r) => setTimeout(r, 0));
    expect(getEntry).toHaveBeenCalledTimes(1);
  });
});
