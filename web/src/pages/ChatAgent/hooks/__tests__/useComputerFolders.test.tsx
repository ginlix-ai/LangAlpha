/**
 * Markdown rebuilds every rendered block when the folders it reads change
 * identity, so the hook keeps one value until a folder actually changes,
 * whatever else a refetched workspace row carries.
 */
import { describe, it, expect, vi } from 'vitest';
import { renderHook } from '@testing-library/react';
import { useComputerFolders } from '../useComputerFolders';
import { getCompletedRowTitle } from '../../components/toolDisplayConfig';

const mocks = vi.hoisted(() => ({
  viewed: null as Record<string, unknown> | null,
  rows: [] as Record<string, unknown>[],
  computers: [] as Record<string, unknown>[],
}));

vi.mock('@/hooks/useWorkspace', () => ({
  useWorkspace: () => ({ data: mocks.viewed }),
}));

vi.mock('@/hooks/useAllWorkspaces', () => ({
  useAllWorkspaces: () => ({ data: { workspaces: mocks.rows } }),
}));

vi.mock('../useComputers', () => ({
  useComputerRoots: () => mocks.computers.map((c) => ({ computer_id: c.computer_id, root_dir: c.root_dir })),
}));

const viewed = (extra: Record<string, unknown> = {}) => ({
  workspace_id: 'ws-1', computer_id: 'c-1', dir_name: 'NVDA', previous_dir_names: [], status: 'running', ...extra,
});
const sibling = (extra: Record<string, unknown> = {}) => ({
  workspace_id: 'ws-2', computer_id: 'c-1', dir_name: 'AMD', previous_dir_names: [], status: 'running', ...extra,
});

describe('useComputerFolders', () => {
  it('keeps its value across a refetch that changed no folder', () => {
    mocks.viewed = viewed({ last_activity_at: '2026-10-05T10:00:00Z' });
    mocks.rows = [mocks.viewed, sibling()];
    const { result, rerender } = renderHook(() => useComputerFolders('ws-1'));
    const first = result.current;
    expect(first?.siblings.map((s) => s.dirName)).toEqual(['AMD']);

    mocks.viewed = viewed({ last_activity_at: '2026-10-05T10:05:00Z' });
    mocks.rows = [mocks.viewed, sibling({ status: 'stopped' })];
    rerender();
    expect(result.current).toBe(first);
  });

  it("carries the root of the workspace's own computer", () => {
    // A computer whose user cannot create the stock root keeps its folders
    // under one of its own, and the agent's absolute paths start there.
    mocks.viewed = viewed();
    mocks.rows = [mocks.viewed];
    mocks.computers = [
      { computer_id: 'c-0', root_dir: '/home/workspace' },
      { computer_id: 'c-1', root_dir: '/home/someone/langalpha' },
    ];
    const { result } = renderHook(() => useComputerFolders('ws-1'));
    expect(result.current?.rootDir).toBe('/home/someone/langalpha');
    mocks.computers = [];
  });

  it('names a tool by its path before the computer list answers', () => {
    // A tool title reads its path against the computer's root. Taken from the
    // computer list alone, which is asked for only once the row is in, a
    // memory read under a root of its own showed "Read" and then turned into
    // "Read memory" as the list answered.
    const ROOT = '/home/someone/langalpha';
    const read = { args: { file_path: `${ROOT}/.agents/user/memory/notes.md` } };
    mocks.viewed = viewed({ root_dir: ROOT });
    mocks.rows = [mocks.viewed];
    mocks.computers = [];
    const { result, rerender } = renderHook(() =>
      getCompletedRowTitle('Read', read, undefined, { root: useComputerFolders('ws-1')?.rootDir }));
    const pending = result.current;

    mocks.computers = [{ computer_id: 'c-1', root_dir: ROOT }];
    rerender();
    expect([pending, result.current]).toEqual(['Read memory', 'Read memory']);
    mocks.computers = [];
  });

  it("places the other computers' workspaces under their roots, by the list when a row has none", () => {
    // The Chief of Staff links a file on another computer at its whole path there.
    mocks.viewed = viewed({ workspace_id: 'ws-home', dir_name: 'Home', root_dir: '/home/workspace' });
    mocks.rows = [
      mocks.viewed,
      sibling(),
      { workspace_id: 'ws-ssh', computer_id: 'c-2', dir_name: 'ssh-e2e', previous_dir_names: [], status: 'running', root_dir: null },
    ];
    mocks.computers = [
      { computer_id: 'c-1', root_dir: '/home/workspace' },
      { computer_id: 'c-2', root_dir: '/home/nosudo/langalpha-2/' },
    ];
    const { result } = renderHook(() => useComputerFolders('ws-home'));
    expect(result.current?.siblings.map((s) => s.dirName)).toEqual(['AMD']);
    expect(result.current?.otherComputers).toEqual([
      { root: '/home/nosudo/langalpha-2', workspaces: [{ workspaceId: 'ws-ssh', dirName: 'ssh-e2e', previousDirNames: [] }] },
    ]);
    mocks.computers = [];
  });

  it('changes when a folder does', () => {
    mocks.viewed = viewed();
    mocks.rows = [mocks.viewed, sibling()];
    const { result, rerender } = renderHook(() => useComputerFolders('ws-1'));
    const first = result.current;

    mocks.rows = [mocks.viewed, sibling({ dir_name: 'AMD-2', previous_dir_names: ['AMD'] })];
    rerender();
    expect(result.current).not.toBe(first);
    expect(result.current?.siblings[0]).toMatchObject({ dirName: 'AMD-2', previousDirNames: ['AMD'] });
  });
});
