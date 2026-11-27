import { QueryClient } from '@tanstack/react-query';
import { act, fireEvent, screen } from '@testing-library/react';
import { useLocation } from 'react-router';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { getChanges, getEntry, getSpaces, listEntries, listMemos, listVersions } from '@/api/drive';
import { runAuthResets } from '@/lib/authResets';
import { queryKeys } from '@/lib/queryKeys';
import { keepDraft } from '../driveDrafts';
import { renderWithProviders } from '@/test/utils';
import Drive from '../Drive';

vi.mock('@/hooks/useIsMobile', () => ({ useIsMobile: () => true }));
vi.mock('@/pages/ChatAgent/components/viewers/CodeEditor', () => ({ default: () => <div data-testid="editor" /> }));
// Kept drafts live in memory by entry id: one test's draft is no other's.
afterEach(() => runAuthResets());
vi.mock('@/api/drive', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/drive')>()),
  getSpaces: vi.fn(),
  listEntries: vi.fn(),
  getChanges: vi.fn(),
  getEntry: vi.fn(),
  listMemos: vi.fn(),
  listVersions: vi.fn(),
}));

function Where() {
  const loc = useLocation();
  return <output data-testid="where">{loc.pathname + loc.search}</output>;
}

describe('Drive page on a phone', () => {
  it('shows the spaces failing to load, with its retry, when opened on a file link', async () => {
    vi.mocked(getSpaces).mockRejectedValue(new Error('Network Error'));
    const { container } = renderWithProviders(<Drive />, { route: '/drive?file=notes%2Fq3.md' });
    expect(await screen.findByText('Could not load this. Check your connection and try again.')).toBeInTheDocument();
    // Not the file layout, which hides the folder side on a phone.
    expect(container.querySelector('.drive-shell.has-file')).toBeNull();
  });
});

describe('Drive page on a space no longer there', () => {
  it('goes to the personal space instead of loading for ever', async () => {
    vi.mocked(getSpaces).mockResolvedValue([{
      id: 'sp-personal', kind: 'personal', created_at: '2026-11-01T00:00:00Z', change_seq: 0, trash_days: 14,
      usage: { bytes: 0, entries: 0, max_bytes: 1, max_entries: 1, max_file_bytes: 1 },
    }] as never);
    vi.mocked(listEntries).mockResolvedValue([]);
    vi.mocked(getChanges).mockResolvedValue({ cursor: 0, entries: [], resync: false } as never);
    renderWithProviders(<><Drive /><Where /></>, { route: '/drive?space=sp-gone&dir=a&file=a%2Fx.md' });
    await vi.waitFor(() => expect(screen.getByTestId('where')).toHaveTextContent(/^\/drive$/));
  });
});

describe('Drive page: back from an open file, on a phone', () => {
  const space = {
    id: 'sp-personal', kind: 'personal', created_at: '2026-11-01T00:00:00Z', change_seq: 0, trash_days: 14,
    usage: { bytes: 0, entries: 0, max_bytes: 1, max_entries: 1, max_file_bytes: 1 },
  };
  const file = {
    id: 'e1', path: 'research/aapl.md', name: 'aapl.md', kind: 'file', size: 0, sha256: null, content_type: null, is_text: false,
    version: 1, created_at: '2026-11-01T00:00:00Z', modified_at: '2026-11-01T00:00:00Z', modified_by: null, deleted_at: null, note: null, undo_until: null,
    memo: { marked: true, status: 'draft', tickers: [], final_seq: null, final_at: null },
  };
  const version = (seq: number) => ({
    seq, size: 0, sha256: String(seq).repeat(64), created_at: '2026-11-01T00:00:00Z', note: null, change_id: null, restored_from: null,
    is_text: true, expires_at: null, text: 'text' as const,
    actor: { kind: 'person' as const, user_id: 'u', agent: null, via: 'app', thread_id: null, thread_title: null, run_id: null, workspace_id: null },
  });
  const setup = (route: string) => {
    vi.mocked(getSpaces).mockResolvedValue([space] as never);
    vi.mocked(listEntries).mockResolvedValue([]);
    vi.mocked(listMemos).mockResolvedValue({ memos: [], next: null });
    vi.mocked(getEntry).mockResolvedValue(file as never);
    vi.mocked(getChanges).mockResolvedValue({ cursor: 0, entries: [], resync: false } as never);
    vi.mocked(listVersions).mockResolvedValue({ versions: [3, 2, 1].map(version), history_bytes: 0, too_large_for_history: false, clearable: 2 });
    renderWithProviders(<><Drive /><Where /></>, { route });
  };

  it('returns to the memo list a file was opened from', async () => {
    setup('/drive?memos=1&file=research%2Faapl.md&tab=record');
    fireEvent.click(await screen.findByRole('button', { name: 'Back to memos' }));
    expect(screen.getByTestId('where')).toHaveTextContent(/^\/drive\?memos=1$/);
  });

  it('returns to the folder a file was opened from', async () => {
    setup('/drive?file=research%2Faapl.md');
    fireEvent.click(await screen.findByRole('button', { name: 'Back to folder' }));
    expect(screen.getByTestId('where')).toHaveTextContent(/^\/drive\?dir=research$/);
  });

  it('keeps the memo list in the address while the file moves between its views', async () => {
    setup('/drive?memos=1&file=research%2Faapl.md');
    fireEvent.click(await screen.findByRole('tab', { name: 'Record' }));
    expect(screen.getByTestId('where')).toHaveTextContent('/drive?memos=1&file=research%2Faapl.md&tab=record');
  });

  it('writes the address as the file reads it: an old history link as the record, the head as no version', async () => {
    setup('/drive?file=research%2Faapl.md&history=1&v=1');
    await vi.waitFor(() => expect(screen.getByTestId('where')).toHaveTextContent(/^\/drive\?file=research%2Faapl\.md&tab=record$/));
  });

  it('writes an old history link as the record even when nothing else in it differs', async () => {
    setup('/drive?file=research%2Faapl.md&history=1');
    await vi.waitFor(() => expect(screen.getByTestId('where')).toHaveTextContent(/^\/drive\?file=research%2Faapl\.md&tab=record$/));
  });

  it('drops a comparison with a version no longer kept from the address', async () => {
    vi.mocked(getEntry).mockResolvedValue({ ...file, version: 3 } as never);
    setup('/drive?file=research%2Faapl.md&tab=changes&v=2&vs=9');
    vi.mocked(getEntry).mockResolvedValue({ ...file, version: 3 } as never);
    await vi.waitFor(() => expect(screen.getByTestId('where')).toHaveTextContent(/^\/drive\?file=research%2Faapl\.md&tab=changes&v=2$/));
  });

  it('keeps a comparison with a version saved since the versions were last read, until they are read again', async () => {
    vi.mocked(getSpaces).mockResolvedValue([space] as never);
    vi.mocked(listEntries).mockResolvedValue([]);
    vi.mocked(listMemos).mockResolvedValue({ memos: [], next: null });
    vi.mocked(getEntry).mockResolvedValue({ ...file, version: 3 } as never);
    vi.mocked(getChanges).mockResolvedValue({ cursor: 0, entries: [], resync: false } as never);
    const answer = (seqs: number[]) => ({ versions: seqs.map(version), history_bytes: 0, too_large_for_history: false, clearable: 1 });
    let arrive!: (value: ReturnType<typeof answer>) => void;
    vi.mocked(listVersions).mockReturnValue(new Promise((resolve) => { arrive = resolve; }) as never);
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    queryClient.setQueryData(queryKeys.drive.versions('personal', file.id), answer([2, 1]), { updatedAt: 1 });
    renderWithProviders(<><Drive /><Where /></>, { route: '/drive?file=research%2Faapl.md&tab=changes&v=2&vs=3', queryClient });
    expect(await screen.findByRole('tab', { name: 'Changes' })).toHaveAttribute('aria-selected', 'true');
    await vi.waitFor(() => expect(listVersions).toHaveBeenCalled());
    expect(screen.getByTestId('where')).toHaveTextContent('tab=changes&v=2&vs=3');
    await act(async () => { arrive(answer([3, 2, 1])); });
    expect(screen.getByTestId('where')).toHaveTextContent('tab=changes&v=2&vs=3');
  });

  it('keeps that comparison when reading the versions again fails', async () => {
    vi.mocked(getSpaces).mockResolvedValue([space] as never);
    vi.mocked(listEntries).mockResolvedValue([]);
    vi.mocked(listMemos).mockResolvedValue({ memos: [], next: null });
    vi.mocked(getEntry).mockResolvedValue({ ...file, version: 3 } as never);
    vi.mocked(getChanges).mockResolvedValue({ cursor: 0, entries: [], resync: false } as never);
    let fail!: (reason: Error) => void;
    vi.mocked(listVersions).mockReturnValue(new Promise((_, reject) => { fail = reject; }) as never);
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const stale = { versions: [2, 1].map(version), history_bytes: 0, too_large_for_history: false, clearable: 1 };
    queryClient.setQueryData(queryKeys.drive.versions('personal', file.id), stale, { updatedAt: 1 });
    renderWithProviders(<><Drive /><Where /></>, { route: '/drive?file=research%2Faapl.md&tab=changes&v=2&vs=3', queryClient });
    expect(await screen.findByRole('tab', { name: 'Changes' })).toHaveAttribute('aria-selected', 'true');
    await vi.waitFor(() => expect(listVersions).toHaveBeenCalled());
    await act(async () => { fail(new Error('Network Error')); });
    await vi.waitFor(() => expect(queryClient.getQueryState(queryKeys.drive.versions('personal', file.id))?.status).toBe('error'));
    expect(screen.getByTestId('where')).toHaveTextContent('tab=changes&v=2&vs=3');
  });

  it("keeps a memo's other view over its kept draft, and opens the draft at the document", async () => {
    keepDraft(file.id, { base: { sha: 'a'.repeat(64), content: 'base' }, draft: 'mine', note: '' });
    setup('/drive?file=research%2Faapl.md&tab=changes');
    expect(await screen.findByRole('tab', { name: 'Changes' })).toHaveAttribute('aria-selected', 'true');
    expect(screen.queryByRole('button', { name: 'Save' })).toBeNull();
    expect(screen.getByTestId('where')).toHaveTextContent('tab=changes');
    fireEvent.click(screen.getByRole('tab', { name: 'Document' }));
    expect(await screen.findByRole('button', { name: 'Save' })).toBeInTheDocument();
    expect(await screen.findByTestId('editor')).toBeInTheDocument();
  });

  it('drops a version and a tab from the address of a plain file', async () => {
    vi.mocked(getEntry).mockResolvedValue({ ...file, memo: null } as never);
    setup('/drive?file=research%2Faapl.md&tab=changes&v=3&vs=1');
    vi.mocked(getEntry).mockResolvedValue({ ...file, memo: null } as never);
    await vi.waitFor(() => expect(screen.getByTestId('where')).toHaveTextContent(/^\/drive\?file=research%2Faapl\.md$/));
  });
});
