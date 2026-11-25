import { act, fireEvent, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '@/api/client';
import { restoreTrash } from '@/api/drive';
import { toast } from '@/components/ui/use-toast';
import { renderHookWithProviders, renderWithProviders } from '@/test/utils';
import type { DriveEntry, DriveTrashItem, DriveVersion } from '@/types/drive';
import { DriveHistory } from '../components/DriveHistory';
import { useDriveActions } from '../useDriveActions';

vi.mock('@/api/drive', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/drive')>()),
  restoreTrash: vi.fn(),
}));
vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

const MB = 1024 * 1024;

function version(seq: number, expires_at: string | null): DriveVersion {
  return {
    seq, size: 300_000, sha256: String(seq).repeat(64).slice(0, 64), created_at: '2026-11-20T00:00:00Z',
    actor: { kind: 'person', user_id: 'u1', agent: null, via: 'app', thread_id: null, run_id: null, workspace_id: null },
    note: null, restored_from: null, expires_at,
  };
}

function history(props: Partial<Parameters<typeof DriveHistory>[0]>) {
  return renderWithProviders(
    <DriveHistory
      name="model.bin" versions={[]} historyBytes={0} tooLarge={false} clearing={false}
      loading={false} failed={false} onRetry={() => {}} viewing={null} onView={() => {}} onClose={() => {}}
      {...props}
    />,
  );
}

describe('DriveHistory', () => {
  it('says when each earlier version goes, what they take, and confirms a clear naming the file and size', async () => {
    const onClear = vi.fn().mockResolvedValue(true);
    history({
      versions: [version(3, null), version(2, '2026-12-24T00:00:00Z'), version(1, null)],
      historyBytes: 600 * 1024,
      onClear,
    });
    expect(screen.getByText(/kept until Dec 2[34]/)).toBeInTheDocument();
    expect(screen.getByText('kept')).toBeInTheDocument();
    expect(screen.getByText('Earlier versions take 600 KB')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Clear history' }));
    expect(screen.getByText('Delete the 2 earlier versions of model.bin for good? This frees 600 KB and cannot be undone.')).toBeInTheDocument();
    expect(onClear).not.toHaveBeenCalled();
    await act(async () => {
      fireEvent.click(screen.getAllByRole('button', { name: 'Clear history' }).at(-1)!);
    });
    expect(onClear).toHaveBeenCalledOnce();
  });

  it('says a file too large for history keeps none, with no list and no clear', () => {
    history({ versions: [version(1, null)], tooLarge: true, onClear: vi.fn() });
    expect(screen.getByText('Files this large keep no earlier versions: saving over model.bin replaces it.')).toBeInTheDocument();
    expect(screen.queryByRole('list')).toBeNull();
    expect(screen.queryByRole('button', { name: 'Clear history' })).toBeNull();
  });
});

describe('useDriveActions restore', () => {
  beforeEach(() => vi.mocked(toast).mockClear());

  it('says a restore refused for room as a sentence with the numbers', async () => {
    vi.mocked(restoreTrash).mockRejectedValue(new ApiError('Request failed with status code 507', 'ERR_BAD_RESPONSE', {
      data: { detail: { type: 'over_quota', message: 'Over quota.' } }, status: 507, statusText: '', headers: {},
    }));
    const usage = { bytes: 5_020 * MB, entries: 10, max_bytes: 5_120 * MB, trash_bytes: 300 * MB, max_entries: 100_000, max_file_bytes: 50 * MB };
    const { result } = renderHookWithProviders(() => useDriveActions('space-1', usage, () => {}));
    const entry = { id: 'e1', path: 'big.bin', name: 'big.bin', kind: 'file', size: 200 * MB } as DriveEntry;
    const item = { trash_id: 't1', entry, entries: 1, deleted_at: '2026-11-20T00:00:00Z' } as DriveTrashItem;

    await act(async () => {
      expect(await result.current.restoreFromTrash(item)).toBeNull();
    });
    expect(toast).toHaveBeenCalledWith(expect.objectContaining({
      variant: 'destructive',
      title: 'big.bin needs 200 MB, and only 100 MB of your 5 GB is free. Delete files to make room.',
    }));
  });
});
