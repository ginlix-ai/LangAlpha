import type { ReactNode } from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import { getBackupStatus } from '../../../utils/api';
import { STATUS_EVERY_MS, useFileBackup } from '../useFileBackup';

vi.mock('../../../utils/api', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getBackupStatus: vi.fn(),
  backupWorkspaceFiles: vi.fn(),
}));

const asked = vi.mocked(getBackupStatus);

function mount(filesBackup: boolean, files: { backed_up?: string[]; modified?: string[] } = {}) {
  asked.mockResolvedValue({ backed_up: [], modified: [], ...files, files_backup: filesBackup } as never);
  return render();
}

function render() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );
  return renderHook(({ files, workspaceId = 'ws' }: { files: string[]; workspaceId?: string }) => useFileBackup({ workspaceId, files }), {
    wrapper,
    initialProps: { files: ['a.md'] } as { files: string[]; workspaceId?: string },
  });
}

beforeEach(() => {
  vi.useFakeTimers();
  asked.mockReset();
});

afterEach(() => {
  vi.useRealTimers();
});

describe('backup status as the file list changes', () => {
  it('asks at once, then once at the end of the window however often the list changes', async () => {
    const { rerender } = mount(true);
    await act(async () => {});
    expect(asked).toHaveBeenCalledTimes(1);

    rerender({ files: ['a.md', 'b.md'] });
    rerender({ files: ['a.md', 'b.md', 'c.md'] });
    await act(async () => {});
    expect(asked).toHaveBeenCalledTimes(1);

    await act(async () => { vi.advanceTimersByTime(STATUS_EVERY_MS); });
    expect(asked).toHaveBeenCalledTimes(2);
  });

  it('asks about another workspace at once, inside the last one\'s window', async () => {
    const { rerender } = mount(true);
    await act(async () => {});
    expect(asked).toHaveBeenCalledTimes(1);

    rerender({ files: ['a.md'], workspaceId: 'ws-2' });
    await act(async () => {});
    expect(asked).toHaveBeenCalledTimes(2);
    expect(asked).toHaveBeenLastCalledWith('ws-2');
  });

  it('stops asking once the computer does not back up', async () => {
    const { rerender, result } = mount(false);
    await act(async () => {});
    expect(result.current.backupOff).toBe(true);

    rerender({ files: ['a.md', 'b.md'] });
    await act(async () => { vi.advanceTimersByTime(STATUS_EVERY_MS * 3); });
    expect(asked).toHaveBeenCalledTimes(1);
  });

  it('keeps one workspace\'s switch from another, which it then asks about', async () => {
    const { rerender, result } = mount(false);
    await act(async () => {});
    expect(result.current.backupOff).toBe(true);

    asked.mockResolvedValue({ backed_up: [], modified: [], files_backup: true } as never);
    rerender({ files: ['a.md'], workspaceId: 'ws-2' });
    expect(result.current.backupOff).toBe(false);
    await act(async () => {});
    expect(asked).toHaveBeenLastCalledWith('ws-2');
  });

  it('ignores an answer that lands after its workspace is no longer shown', async () => {
    let answerFirst: (status: unknown) => void = () => {};
    asked.mockImplementation((id: string) => (id === 'ws'
      ? new Promise((resolve) => { answerFirst = resolve; })
      : Promise.resolve({ backed_up: ['b.md'], modified: [], files_backup: true })) as never);
    const { rerender, result } = render();
    rerender({ files: ['a.md'], workspaceId: 'ws-2' });
    await act(async () => {});
    expect(result.current.backupOff).toBe(false);
    expect([...result.current.backedUpSet]).toEqual(['b.md']);

    // The late answer would turn the switch off and the marks over to its own.
    await act(async () => { answerFirst({ backed_up: ['a.md'], modified: ['a.md'], files_backup: false }); });
    expect(result.current.backupOff).toBe(false);
    expect([...result.current.backedUpSet]).toEqual(['b.md']);
    expect(result.current.modifiedSet.size).toBe(0);
  });

  it('marks no file while the computer does not back up, whatever an answer held', async () => {
    const { result } = mount(false, { backed_up: ['a.md'], modified: ['b.md'] });
    await act(async () => {});

    expect(result.current.backupOff).toBe(true);
    expect(result.current.backedUpSet.size).toBe(0);
    expect(result.current.modifiedSet.size).toBe(0);
  });
});
