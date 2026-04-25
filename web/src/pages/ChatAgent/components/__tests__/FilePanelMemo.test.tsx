import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

vi.mock('@/pages/ChatAgent/utils/api', () => ({
  listUserMemos: vi.fn(),
}));

import { listUserMemos, type MemoEntry } from '@/pages/ChatAgent/utils/api';
import {
  memoMimeForName,
  useWorkspaceMemoIndex,
} from '../FilePanelMemo';

const mockList = listUserMemos as Mock;

beforeEach(() => {
  vi.clearAllMocks();
});

describe('memoMimeForName', () => {
  it('maps known extensions to canonical mimes', () => {
    expect(memoMimeForName('notes.md')).toBe('text/markdown');
    expect(memoMimeForName('readme.txt')).toBe('text/plain');
    expect(memoMimeForName('data.csv')).toBe('text/csv');
    expect(memoMimeForName('config.json')).toBe('application/json');
    expect(memoMimeForName('paper.pdf')).toBe('application/pdf');
  });

  it('is case-insensitive on extension', () => {
    expect(memoMimeForName('notes.MD')).toBe('text/markdown');
    expect(memoMimeForName('PAPER.PDF')).toBe('application/pdf');
  });

  it('returns null for unsupported types', () => {
    expect(memoMimeForName('image.png')).toBeNull();
    expect(memoMimeForName('archive.zip')).toBeNull();
  });

  it('returns null when there is no extension', () => {
    expect(memoMimeForName('Makefile')).toBeNull();
  });
});

function makeEntry(overrides: Partial<MemoEntry>): MemoEntry {
  return {
    key: 'k',
    original_filename: null,
    mime_type: null,
    size_bytes: 0,
    description: null,
    metadata_status: 'ready',
    created_at: null,
    modified_at: null,
    source_kind: null,
    source_workspace_id: null,
    source_path: null,
    sha256: null,
    ...overrides,
  };
}

describe('useWorkspaceMemoIndex', () => {
  it('returns an empty map while the list is loading', () => {
    // Pending promise → query never resolves
    mockList.mockReturnValue(new Promise(() => {}));
    const { result } = renderHookWithProviders(() =>
      useWorkspaceMemoIndex('ws-1'),
    );
    expect(result.current.size).toBe(0);
  });

  it('indexes memos that are sandbox-sourced from this workspace by source_path', async () => {
    const entries: MemoEntry[] = [
      makeEntry({
        key: 'k1',
        source_kind: 'sandbox',
        source_workspace_id: 'ws-1',
        source_path: 'work/notes/a.md',
      }),
      makeEntry({
        key: 'k2',
        source_kind: 'sandbox',
        source_workspace_id: 'ws-1',
        source_path: 'work/results/b.md',
      }),
    ];
    mockList.mockResolvedValue({ entries, truncated: false });

    const { result } = renderHookWithProviders(() =>
      useWorkspaceMemoIndex('ws-1'),
    );

    await waitFor(() => expect(result.current.size).toBe(2));
    expect(result.current.get('work/notes/a.md')?.key).toBe('k1');
    expect(result.current.get('work/results/b.md')?.key).toBe('k2');
  });

  it('excludes entries from other workspaces', async () => {
    const entries: MemoEntry[] = [
      makeEntry({
        key: 'k1',
        source_kind: 'sandbox',
        source_workspace_id: 'ws-other',
        source_path: 'work/a.md',
      }),
      makeEntry({
        key: 'k2',
        source_kind: 'sandbox',
        source_workspace_id: 'ws-1',
        source_path: 'work/b.md',
      }),
    ];
    mockList.mockResolvedValue({ entries, truncated: false });

    const { result } = renderHookWithProviders(() =>
      useWorkspaceMemoIndex('ws-1'),
    );

    await waitFor(() => expect(result.current.size).toBe(1));
    expect(result.current.has('work/a.md')).toBe(false);
    expect(result.current.get('work/b.md')?.key).toBe('k2');
  });

  it('excludes upload-sourced memos and entries without source_path', async () => {
    const entries: MemoEntry[] = [
      makeEntry({
        key: 'upload-only',
        source_kind: 'upload',
        source_workspace_id: 'ws-1',
        source_path: 'work/abc.md',
      }),
      makeEntry({
        key: 'no-path',
        source_kind: 'sandbox',
        source_workspace_id: 'ws-1',
        source_path: null,
      }),
      makeEntry({
        key: 'good',
        source_kind: 'sandbox',
        source_workspace_id: 'ws-1',
        source_path: 'work/good.md',
      }),
    ];
    mockList.mockResolvedValue({ entries, truncated: false });

    const { result } = renderHookWithProviders(() =>
      useWorkspaceMemoIndex('ws-1'),
    );

    await waitFor(() => expect(result.current.size).toBe(1));
    expect(result.current.get('work/good.md')?.key).toBe('good');
  });
});
