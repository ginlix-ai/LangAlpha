import { act, fireEvent, screen, waitFor, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '@/api/client';
import { clearHistory, getRecord, listVersions, restoreTrash, undoSave } from '@/api/drive';
import { toast } from '@/components/ui/use-toast';
import { queryKeys } from '@/lib/queryKeys';
import { CHANGE_READ_MAX, recordEnd } from '@/hooks/useDrive';
import { renderHookWithProviders, renderWithProviders } from '@/test/utils';
import type { DriveEntry, DriveRecordEvent, DriveRecordVersion, DriveTrashItem } from '@/types/drive';
import { MemoRecord } from '../components/MemoRecord';
import { DriveContext, type DriveContextValue } from '../driveContext';
import { useDriveActions, type DriveActions } from '../useDriveActions';

vi.mock('@/api/drive', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/drive')>()),
  restoreTrash: vi.fn(),
  undoSave: vi.fn(),
  getRecord: vi.fn(),
  listVersions: vi.fn(),
  clearHistory: vi.fn(),
}));
vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

const MB = 1024 * 1024;
const person = { kind: 'person' as const, user_id: 'u1', agent: null, via: 'app', thread_id: null, thread_title: null, run_id: null, workspace_id: null };
const agent = { ...person, kind: 'agent' as const, agent: 'main', via: 'tool', thread_id: 't1', thread_title: 'Q3 research' };

const memo: DriveEntry = {
  id: 'e1', path: 'memos/aapl.md', name: 'aapl.md', kind: 'file', size: 10, sha256: 'a'.repeat(64),
  content_type: 'text/markdown', is_text: true, version: 7,
  created_at: '2026-11-01T00:00:00Z', modified_at: '2026-11-25T14:02:00Z', modified_by: agent, deleted_at: null,
  memo: { marked: true, status: 'final', tickers: ['AAPL'], final_seq: 7, final_at: null }, undo_until: null,
};

function version(seq: number, over: Partial<DriveRecordVersion> = {}): DriveRecordVersion {
  return {
    type: 'version', seq, size: 10, sha256: String(seq).repeat(64).slice(0, 64), is_text: true,
    created_at: '2026-11-25T14:02:00Z', actor: agent, note: null, change_id: null, restored_from: null, expires_at: null,
    added: null, removed: null, text: 'text', ...over,
  };
}

const eventBase = { type: 'event' as const, created_at: '2026-11-25T14:05:00Z', actor: person };
const finalEvent: DriveRecordEvent = {
  ...eventBase, id: 3, kind: 'update', note: 'Signed off at the IC',
  status_before: 'draft', status_after: 'final', tickers_before: ['AAPL'], tickers_after: ['AAPL'], final_seq: 7,
};

function renderRecord(onPick = vi.fn(), entry: DriveEntry = memo) {
  const actions = {
    explain: vi.fn().mockResolvedValue(true), clearHistory: vi.fn().mockResolvedValue(true), pending: { explain: false, clearHistory: false },
  } as unknown as DriveActions;
  const ctx = {
    space: 'personal', actions, go: vi.fn(), open: vi.fn(), trash: vi.fn(), leaveEditor: vi.fn(), back: vi.fn(), backTo: 'folder' as const,
    ask: { rename: vi.fn(), move: vi.fn(), newFolder: vi.fn(), memo: vi.fn() },
  } as DriveContextValue;
  const { queryClient, container, rerender } = renderWithProviders(<DriveContext value={ctx}><MemoRecord entry={entry} onPick={onPick} /></DriveContext>);
  return { actions, onPick, queryClient, container, rerender, ctx };
}

describe('MemoRecord', () => {
  beforeEach(() => {
    vi.mocked(getRecord).mockReset();
    vi.mocked(listVersions).mockReset();
    vi.mocked(listVersions).mockResolvedValue({ versions: [], history_bytes: 0, too_large_for_history: false, clearable: 0 });
  });

  it('lists versions and mark changes newest first: who, where, why and the lines they changed', async () => {
    vi.mocked(getRecord).mockResolvedValue({
      items: [finalEvent, version(7, { note: 'Raised the target on services', added: 6, removed: 2 }), version(6, { actor: person, text: 'none' })],
      next: null,
    });
    renderRecord();
    const rows = await screen.findAllByRole('listitem');
    expect(rows).toHaveLength(3);
    expect(rows[0]).toHaveTextContent('You set status Draft → Final, pinning v7');
    expect(rows[0]).toHaveTextContent('Signed off at the IC');
    expect(rows[0]).toHaveTextContent('status');
    expect(rows[1]).toHaveTextContent(/^v7 · /);
    expect(rows[1]).toHaveTextContent('Raised the target on services');
    expect(rows[1]).toHaveTextContent('+6−2');
    expect(await screen.findByRole('link', { name: 'Q3 research' })).toHaveAttribute('href', '/chat/t/t1');
    expect(rows[1]).toHaveTextContent('Agent in Q3 research');
    // A version with no counts shows none, rather than a zero it does not know.
    expect(rows[2]).not.toHaveTextContent('+');
  });

  it('hands a version picked from the record to the pane, as its row has it', async () => {
    vi.mocked(getRecord).mockResolvedValue({ items: [version(7, { text: 'none' })], next: null });
    const { onPick } = renderRecord();
    fireEvent.click(await screen.findByRole('button', { name: /^v7 · / }));
    expect(onPick).toHaveBeenCalledWith(expect.objectContaining({ seq: 7, text: 'none' }));
  });

  it('reads older items from where the last page ended', async () => {
    vi.mocked(getRecord)
      .mockResolvedValueOnce({ items: [version(7)], next: 'cursor-1' })
      .mockResolvedValueOnce({ items: [version(6)], next: null });
    renderRecord();
    fireEvent.click(await screen.findByRole('button', { name: 'Show older' }));
    expect(await screen.findByRole('button', { name: /^v6 · / })).toBeInTheDocument();
    expect(getRecord).toHaveBeenLastCalledWith('personal', 'e1', 'cursor-1');
    expect(screen.queryByRole('button', { name: 'Show older' })).toBeNull();
  });

  it("lets the person say why they made a version they left unexplained, and only theirs", async () => {
    vi.mocked(getRecord).mockResolvedValue({ items: [version(7), version(6, { actor: person })], next: null });
    const { actions } = renderRecord();
    const add = await screen.findAllByRole('button', { name: 'Add why' });
    expect(add).toHaveLength(1);
    fireEvent.click(add[0]);
    fireEvent.change(screen.getByRole('textbox', { name: 'Add why' }), { target: { value: 'Fixed the share count' } });
    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: 'Save' }));
    });
    expect(actions.explain).toHaveBeenCalledWith(memo, 6, 'Fixed the share count');
    await waitFor(() => expect(screen.queryByRole('textbox', { name: 'Add why' })).toBeNull());
  });

  it('shows an agent\'s change of several versions as one row that opens to its versions', async () => {
    // Local calendar days, so the day range reads the same in any time zone.
    const change = { actor: agent, note: 'Raised the services estimate', change_id: 'c1' };
    vi.mocked(getRecord).mockResolvedValue({
      items: [
        version(7, { ...change, created_at: new Date(2026, 10, 25, 12).toISOString(), added: 3, removed: 1 }),
        version(6, { ...change, created_at: new Date(2026, 10, 24, 10).toISOString(), added: 2, removed: 0 }),
        version(5, { ...change, created_at: new Date(2026, 10, 24, 9).toISOString(), added: 1, removed: 4 }),
        version(4, { actor: person }),
      ],
      next: null,
    });
    const { onPick } = renderRecord();
    const rows = await screen.findAllByRole('listitem');
    expect(rows).toHaveLength(2);
    expect(rows[0]).toHaveTextContent(/^v5 to v7 · Nov 24 to 25/);
    expect(rows[0]).toHaveTextContent('Current');
    expect(rows[0]).toHaveTextContent('Final');
    expect(rows[0]).toHaveTextContent('+6−5');
    expect(rows[0]).toHaveTextContent('Agent in Q3 research');
    expect(screen.getAllByText('Raised the services estimate')).toHaveLength(1);
    const toggle = screen.getByRole('button', { name: '3 versions in this change' });
    expect(toggle).toHaveAttribute('aria-expanded', 'false');
    fireEvent.click(toggle);
    expect(toggle).toHaveAttribute('aria-expanded', 'true');
    const inner = within(screen.getByRole('list', { name: '3 versions in this change' })).getAllByRole('listitem');
    expect(inner.map((li) => li.textContent?.slice(0, 2))).toEqual(['v7', 'v6', 'v5']);
    // A change always carries its reason, so nothing in it asks for one.
    expect(within(rows[0]).queryByRole('button', { name: 'Add why' })).toBeNull();
    fireEvent.click(within(inner[1]).getByRole('button', { name: /^v6 · / }));
    expect(onPick).toHaveBeenCalledWith(expect.objectContaining({ seq: 6 }));
  });

  it('reads on through a change the first page ends inside, and shows it whole', async () => {
    const change = { note: 'Trimmed the risks', change_id: 'c2' };
    vi.mocked(getRecord)
      .mockResolvedValueOnce({
        items: [version(8, { actor: person }), version(7, { ...change, added: 1, removed: 0 }), version(6, { ...change, added: 2, removed: 1 })],
        next: 'cursor-1',
      })
      .mockResolvedValueOnce({
        items: [version(5, { ...change, added: 3, removed: 2 }), version(4, { ...change, added: 4, removed: 3 }), version(3, { actor: person })],
        next: 'cursor-2',
      });
    renderRecord();
    expect(await screen.findByRole('button', { name: '4 versions in this change' })).toBeInTheDocument();
    const rows = screen.getAllByRole('listitem');
    expect(rows).toHaveLength(3);
    expect(rows[1]).toHaveTextContent(/^v4 to v7 · /);
    expect(rows[1]).toHaveTextContent('+10−6');
    expect(getRecord).toHaveBeenCalledTimes(2);
    expect(getRecord).toHaveBeenLastCalledWith('personal', 'e1', 'cursor-1');
    // The change is whole, so nothing older is read until asked.
    await new Promise((done) => setTimeout(done, 100));
    expect(getRecord).toHaveBeenCalledTimes(2);
    expect(screen.getByRole('button', { name: 'Show older' })).toBeInTheDocument();
  });

  it('reads on through a change that spans several older pages', async () => {
    const change = { note: 'Trimmed the risks', change_id: 'c2' };
    vi.mocked(getRecord)
      .mockResolvedValueOnce({ items: [version(8, { actor: person }), version(7, change)], next: 'cursor-1' })
      .mockResolvedValueOnce({ items: [version(6, change)], next: 'cursor-2' })
      .mockResolvedValueOnce({ items: [version(5, change)], next: 'cursor-3' })
      .mockResolvedValueOnce({ items: [version(4, { actor: person })], next: 'cursor-4' });
    const { container } = renderRecord();
    expect(await screen.findByRole('button', { name: '3 versions in this change' })).toBeInTheDocument();
    await new Promise((done) => setTimeout(done, 100));
    expect(getRecord).toHaveBeenCalledTimes(4);
    expect(container.querySelector('[aria-busy="true"]')).toBeNull();
  });

  it('never shows a change cut at the page boundary while the rest of it is read', async () => {
    const change = { note: 'Trimmed the risks', change_id: 'c2' };
    let answer!: (page: { items: DriveRecordVersion[]; next: string | null }) => void;
    vi.mocked(getRecord)
      .mockResolvedValueOnce({ items: [version(8, { actor: person }), version(7, change), version(6, change)], next: 'cursor-1' })
      .mockReturnValueOnce(new Promise((done) => { answer = done; }));
    renderRecord();
    await waitFor(() => expect(getRecord).toHaveBeenCalledTimes(2));
    expect(screen.getAllByRole('listitem')).toHaveLength(1);
    expect(screen.queryByRole('button', { name: /in this change/ })).toBeNull();
    await act(async () => answer({ items: [version(5, change), version(4, { actor: person })], next: null }));
    expect(await screen.findByRole('button', { name: '3 versions in this change' })).toBeInTheDocument();
  });

  it('reads one page more when a change ends at the page boundary, and no further', async () => {
    const change = { note: 'Trimmed the risks', change_id: 'c2' };
    vi.mocked(getRecord)
      .mockResolvedValueOnce({ items: [version(8, { actor: person }), version(7, change), version(6, change)], next: 'cursor-1' })
      .mockResolvedValueOnce({ items: [version(5, { actor: person })], next: 'cursor-2' });
    renderRecord();
    await waitFor(() => expect(screen.getAllByRole('listitem')).toHaveLength(3));
    const rows = screen.getAllByRole('listitem');
    expect(rows[1]).toHaveTextContent(/^v6 to v7 · /);
    expect(within(rows[1]).getByRole('button', { name: '2 versions in this change' })).toBeInTheDocument();
    expect(rows[2]).toHaveTextContent(/^v5 · /);
    await new Promise((done) => setTimeout(done, 100));
    expect(getRecord).toHaveBeenCalledTimes(2);
  });

  it('stops reading on through a change when an older page fails, and reads it when asked', async () => {
    const change = { note: 'Trimmed the risks', change_id: 'c2' };
    vi.mocked(getRecord)
      .mockResolvedValueOnce({ items: [version(8, { actor: person }), version(7, change), version(6, change)], next: 'cursor-1' })
      .mockRejectedValueOnce(new Error('offline'))
      .mockResolvedValueOnce({ items: [version(5, change), version(4, { actor: person })], next: null });
    renderRecord();
    expect(await screen.findByRole('alert')).toHaveTextContent('Could not load this');
    await new Promise((done) => setTimeout(done, 200));
    expect(getRecord).toHaveBeenCalledTimes(2);
    // The part of the change read so far is not shown as if it were all of it.
    expect(screen.queryByRole('button', { name: /in this change/ })).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Show older' }));
    expect(await screen.findByRole('button', { name: '3 versions in this change' })).toBeInTheDocument();
    expect(getRecord).toHaveBeenCalledTimes(3);
  });

  it('reads on only for the change the asked page ended inside, not for one the extra page ends inside', async () => {
    const a = { note: 'Trimmed the risks', change_id: 'ca' };
    const b = { note: 'Raised the target', change_id: 'cb' };
    vi.mocked(getRecord)
      .mockResolvedValueOnce({ items: [version(10, { actor: person }), version(9, a), version(8, a)], next: 'cursor-1' })
      // Answered a moment later, as over the network, so the read is seen under way.
      .mockReturnValueOnce(new Promise((done) => setTimeout(() => done({
        items: [version(7, a), version(6, { actor: person }), version(5, b), version(4, b)], next: 'cursor-2',
      }), 20)))
      .mockResolvedValueOnce({ items: [version(3, b), version(2, { actor: person })], next: null });
    const { container } = renderRecord();
    expect(await screen.findByRole('button', { name: '3 versions in this change' })).toBeInTheDocument();
    await new Promise((done) => setTimeout(done, 100));
    expect(getRecord).toHaveBeenCalledTimes(2);
    const rows = screen.getAllByRole('listitem');
    expect(rows).toHaveLength(3);
    expect(rows[1]).toHaveTextContent(/^v7 to v9 · /);
    expect(rows[2]).toHaveTextContent(/^v6 · /);
    // B is cut at the end and not whole, so it waits for the person to ask.
    expect(screen.queryByText(/^v4 to v5/)).toBeNull();
    expect(screen.getByRole('button', { name: 'Show older' })).toBeInTheDocument();
    expect(container.querySelector('[aria-busy="true"]')).toBeNull();
  });

  it('reads on for the change a page the person asked for ends inside', async () => {
    const a = { note: 'Trimmed the risks', change_id: 'ca' };
    const b = { note: 'Raised the target', change_id: 'cb' };
    const c = { note: 'Cut the multiple', change_id: 'cc' };
    vi.mocked(getRecord)
      .mockResolvedValueOnce({ items: [version(10, { actor: person }), version(9, a), version(8, a)], next: 'cursor-1' })
      .mockResolvedValueOnce({ items: [version(7, a), version(6, { actor: person }), version(5, b)], next: 'cursor-2' })
      .mockResolvedValueOnce({ items: [version(4, b)], next: 'cursor-3' })
      .mockResolvedValueOnce({ items: [version(3, b), version(2, c)], next: 'cursor-4' })
      .mockResolvedValueOnce({ items: [version(1, c)], next: null });
    renderRecord();
    expect(await screen.findByRole('button', { name: '3 versions in this change' })).toBeInTheDocument();
    await new Promise((done) => setTimeout(done, 100));
    expect(getRecord).toHaveBeenCalledTimes(2);
    fireEvent.click(screen.getByRole('button', { name: 'Show older' }));
    // The page asked for ends inside B, so the next is read for it; that one ends inside C, which waits.
    expect(await screen.findByText(/^v3 to v5/)).toBeInTheDocument();
    await new Promise((done) => setTimeout(done, 100));
    expect(getRecord).toHaveBeenCalledTimes(4);
    expect(getRecord).toHaveBeenLastCalledWith('personal', 'e1', 'cursor-3');
    expect(screen.queryByText(/^v2 · /)).toBeNull();
    expect(screen.getByRole('button', { name: 'Show older' })).toBeInTheDocument();
  });

  it('reads nothing older unasked when the oldest item shown is not part of a change', async () => {
    const change = { note: 'Trimmed the risks', change_id: 'c2' };
    vi.mocked(getRecord).mockResolvedValueOnce({ items: [version(8, change), version(7, change), version(6, { actor: person })], next: 'cursor-1' });
    renderRecord();
    expect(await screen.findByRole('button', { name: '2 versions in this change' })).toBeInTheDocument();
    await new Promise((done) => setTimeout(done, 100));
    expect(getRecord).toHaveBeenCalledTimes(1);
  });

  it('shows a change of one version as a version row', async () => {
    vi.mocked(getRecord).mockResolvedValue({ items: [version(7, { change_id: 'c3', note: 'One edit' })], next: null });
    renderRecord();
    expect(await screen.findByRole('button', { name: /^v7 · / })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /in this change/ })).toBeNull();
  });

  it("shows a subagent's name as text, never as markup", async () => {
    vi.mocked(getRecord).mockResolvedValue({ items: [version(7, { actor: { ...agent, agent: '<b>quant</b>' } })], next: null });
    renderRecord();
    const row = (await screen.findAllByRole('listitem'))[0];
    expect(row.querySelector('b')).toBeNull();
    expect(row).toHaveTextContent('<b>quant</b>');
  });

  it('names an untitled thread with the generic label, still linked', async () => {
    vi.mocked(getRecord).mockResolvedValue({ items: [version(7, { actor: { ...agent, thread_title: null } })], next: null });
    renderRecord();
    expect(await screen.findByRole('link', { name: 'a thread' })).toHaveAttribute('href', '/chat/t/t1');
  });

  it('marks a person and the agent with a glyph, in the same neutral text', async () => {
    vi.mocked(getRecord).mockResolvedValue({ items: [version(7), version(6, { actor: person })], next: null });
    renderRecord();
    const rows = await screen.findAllByRole('listitem');
    expect(rows[0].querySelector('.drive-who-glyph.lucide-bot')).not.toBeNull();
    expect(rows[1].querySelector('.drive-who-glyph.lucide-user')).not.toBeNull();
  });

  it('clears the earlier versions from its menu, after saying how many go and that the record keeps the clear', async () => {
    vi.mocked(listVersions).mockResolvedValue({
      versions: [7, 6, 5].map((seq) => ({ seq, size: 10, sha256: 'x', created_at: '2026-11-25T14:02:00Z', actor: person, note: null, change_id: null, restored_from: null, is_text: true, expires_at: null, text: 'text' as const })),
      history_bytes: 2048, too_large_for_history: false, clearable: 2,
    });
    vi.mocked(getRecord).mockResolvedValue({ items: [version(7)], next: null });
    const { actions } = renderRecord();
    fireEvent.pointerDown(await screen.findByRole('button', { name: 'More for the record' }), { button: 0, pointerType: 'mouse' });
    fireEvent.click(await screen.findByRole('menuitem', { name: 'Clear earlier versions…' }));
    expect(screen.getByText(/2 earlier versions go for good, freeing 2 KB\. The current version stays, and the record will show that they were cleared\./)).toBeInTheDocument();
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'Clear versions' })));
    // Bound to the head the count was made against, from the same answer.
    expect(actions.clearHistory).toHaveBeenCalledWith(memo, { head: 7, final: null });
  });

  it('keeps the pinned final out of what a clear takes, and says it stays', async () => {
    vi.mocked(listVersions).mockResolvedValue({
      versions: [7, 6, 5, 4].map((seq) => ({ seq, size: 10, sha256: 'x', created_at: '2026-11-25T14:02:00Z', actor: person, note: null, change_id: null, restored_from: null, is_text: true, expires_at: null, text: 'text' as const })),
      history_bytes: 2048, too_large_for_history: false, clearable: 2,
      memo: { marked: true, status: 'final', tickers: ['AAPL'], final_seq: 5, final_at: null },
    });
    vi.mocked(getRecord).mockResolvedValue({ items: [version(7)], next: null });
    // The entry may lag the versions answer: the dialog and the clear read only the answer.
    const { actions } = renderRecord(vi.fn(), { ...memo, version: 6, memo: { marked: true, status: 'draft', tickers: ['AAPL'], final_seq: null, final_at: null } });
    fireEvent.pointerDown(await screen.findByRole('button', { name: 'More for the record' }), { button: 0, pointerType: 'mouse' });
    fireEvent.click(await screen.findByRole('menuitem', { name: 'Clear earlier versions…' }));
    expect(screen.getByText('2 earlier versions go for good, freeing 2 KB. The current version and the final (v5) stay, and the record will show that they were cleared.')).toBeInTheDocument();
    // Two clicks before the first answers are one clear, bound to the head and the pin that answer named.
    const submit = screen.getByRole('button', { name: 'Clear versions' });
    await act(async () => {
      fireEvent.click(submit);
      fireEvent.click(submit);
    });
    expect(actions.clearHistory).toHaveBeenCalledTimes(1);
    expect(actions.clearHistory).toHaveBeenCalledWith(expect.objectContaining({ id: 'e1' }), { head: 7, final: 5 });
  });

  it('clears to the cutoff the dialog opened on, though a newer answer lands while it is open', async () => {
    const listed = (seqs: number[]) => seqs.map((seq) => ({ seq, size: 10, sha256: 'x', created_at: '2026-11-25T14:02:00Z', actor: person, note: null, change_id: null, restored_from: null, is_text: true, expires_at: null, text: 'text' as const }));
    vi.mocked(listVersions).mockResolvedValue({
      versions: listed([7, 6, 5, 4]), history_bytes: 2048, too_large_for_history: false, clearable: 2,
      memo: { marked: true, status: 'final', tickers: ['AAPL'], final_seq: 5, final_at: null },
    });
    vi.mocked(getRecord).mockResolvedValue({ items: [version(7)], next: null });
    const { actions, queryClient } = renderRecord();
    fireEvent.pointerDown(await screen.findByRole('button', { name: 'More for the record' }), { button: 0, pointerType: 'mouse' });
    fireEvent.click(await screen.findByRole('menuitem', { name: 'Clear earlier versions…' }));
    const said = '2 earlier versions go for good, freeing 2 KB. The current version and the final (v5) stay, and the record will show that they were cleared.';
    expect(screen.getByText(said)).toBeInTheDocument();
    // The agent saves v8 and v9 and the pin moves to v8, while the person reads.
    vi.mocked(listVersions).mockResolvedValue({
      versions: listed([9, 8, 7, 6, 5, 4]), history_bytes: 4096, too_large_for_history: false, clearable: 4,
      memo: { marked: true, status: 'final', tickers: ['AAPL'], final_seq: 8, final_at: null },
    });
    const calls = vi.mocked(listVersions).mock.calls.length;
    await act(async () => {
      await queryClient.refetchQueries({ queryKey: queryKeys.drive.versions('personal', 'e1') });
      // React Query tells its observers on a later tick; let that render land.
      await new Promise((r) => setTimeout(r, 0));
    });
    expect(vi.mocked(listVersions).mock.calls.length).toBeGreaterThan(calls);
    expect(screen.getByText(said)).toBeInTheDocument();
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'Clear versions' })));
    expect(actions.clearHistory).toHaveBeenCalledWith(expect.objectContaining({ id: 'e1' }), { head: 7, final: 5 });
  });

  it('drops a confirmation opened for one file when another file arrives at its path', async () => {
    vi.mocked(listVersions).mockResolvedValue({
      versions: [7, 6, 5].map((seq) => ({ seq, size: 10, sha256: 'x', created_at: '2026-11-25T14:02:00Z', actor: person, note: null, change_id: null, restored_from: null, is_text: true, expires_at: null, text: 'text' as const })),
      history_bytes: 2048, too_large_for_history: false, clearable: 2,
    });
    vi.mocked(getRecord).mockResolvedValue({ items: [version(7)], next: null });
    const { actions, rerender, ctx, queryClient } = renderRecord();
    fireEvent.pointerDown(await screen.findByRole('button', { name: 'More for the record' }), { button: 0, pointerType: 'mouse' });
    fireEvent.click(await screen.findByRole('menuitem', { name: 'Clear earlier versions…' }));
    expect(screen.getByRole('button', { name: 'Clear versions' })).toBeInTheDocument();
    // The other file's record is already read, so the record stays up through the swap.
    queryClient.setQueryData(queryKeys.drive.record('personal', 'e2'), { pages: [{ items: [version(7)], next: null }], pageParams: [null] });
    // The first file is trashed and another moved to its path before this tab refreshes.
    rerender(<DriveContext value={ctx}><MemoRecord entry={{ ...memo, id: 'e2' }} onPick={vi.fn()} /></DriveContext>);
    expect(screen.getByRole('list', { name: 'Record', hidden: true })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Clear versions' })).toBeNull();
    expect(actions.clearHistory).not.toHaveBeenCalled();
  });

  it('offers no clear while there is nothing earlier to clear', async () => {
    vi.mocked(getRecord).mockResolvedValue({ items: [version(7)], next: null });
    renderRecord();
    await screen.findAllByRole('listitem');
    expect(screen.queryByRole('button', { name: 'More for the record' })).toBeNull();
  });

  it('says what a record event did to the tickers and the history', async () => {
    vi.mocked(getRecord).mockResolvedValue({
      items: [
        { ...eventBase, id: 5, kind: 'update', note: null, status_before: 'final', status_after: 'final', final_seq: null, tickers_before: ['AAPL'], tickers_after: ['AAPL', 'MSFT'] },
        { ...eventBase, id: 4, kind: 'clear', note: null, cleared: 3 },
        { ...eventBase, id: 1, kind: 'mark', note: null, status_after: null, tickers_after: [], final_seq: null },
      ],
      next: null,
    });
    renderRecord();
    const rows = await screen.findAllByRole('listitem');
    expect(rows[0]).toHaveTextContent('You set tickers AAPL, MSFT');
    expect(rows[0]).toHaveTextContent('tickers');
    expect(rows[1]).toHaveTextContent('You cleared 3 earlier versions');
    expect(rows[2]).toHaveTextContent('You marked it as a memo');
  });

  it('reads a record again while the server is still counting its lines', async () => {
    vi.mocked(getRecord)
      .mockResolvedValueOnce({ items: [{ ...version(7), added: null, removed: null }], next: null, counting: true })
      .mockResolvedValue({ items: [{ ...version(7), added: 12, removed: 3 }], next: null });
    renderRecord();
    expect(await screen.findByText('+12', {}, { timeout: 4_000 })).toBeInTheDocument();
    expect(getRecord).toHaveBeenCalledTimes(2);
    // Counted, it is not read again.
    await new Promise((done) => setTimeout(done, 2_200));
    expect(getRecord).toHaveBeenCalledTimes(2);
  }, 10_000);

  it('says the status and tickers a file was marked with', async () => {
    const marked = { ...eventBase, kind: 'mark' as const, note: null };
    vi.mocked(getRecord).mockResolvedValue({
      items: [
        { ...marked, id: 3, status_after: 'final', tickers_after: [], final_seq: 4 },
        { ...marked, id: 2, status_after: 'draft', tickers_after: ['AAPL', 'MSFT'], final_seq: null },
        { ...marked, id: 1, status_after: null, tickers_after: ['NVDA'], final_seq: null },
      ],
      next: null,
    });
    renderRecord();
    const rows = await screen.findAllByRole('listitem');
    expect(rows[0]).toHaveTextContent('You marked it as a memo with status Final, pinning v4');
    expect(rows[1]).toHaveTextContent('You marked it as a memo with status Draft, and set tickers AAPL, MSFT');
    expect(rows[2]).toHaveTextContent('You marked it as a memo, and set tickers NVDA');
  });
});

describe('recordEnd', () => {
  const c = (seq: number, id: string | null) => version(seq, { change_id: id });
  const neither = { cut: false, readOn: false };

  it('is cut when the oldest item is a version of a change and an older page is left', () => {
    expect(recordEnd([c(3, null), c(2, 'a'), c(1, 'a')], true, 2)).toEqual({ cut: true, readOn: true });
    expect(recordEnd([c(3, 'b'), c(2, 'a')], true, 1)).toEqual({ cut: true, readOn: true });
  });

  it('reads on only for the row the asked page ended inside', () => {
    // The asked page ended inside a; the page read for it ends inside b.
    expect(recordEnd([c(4, 'a'), c(3, 'a'), c(2, 'b'), c(1, 'b')], true, 1)).toEqual({ cut: true, readOn: false });
    // The asked page ended on no change, or on an event.
    expect(recordEnd([c(2, null), c(1, 'b')], true, 0)).toEqual({ cut: true, readOn: false });
    expect(recordEnd([finalEvent, c(1, 'b')], true, 0)).toEqual({ cut: true, readOn: false });
    // Not loaded yet.
    expect(recordEnd([c(2, 'a'), c(1, 'a')], true, -1)).toEqual({ cut: true, readOn: false });
  });

  it('does not read on for a later part of the same change past an event', () => {
    // The asked page ended inside a; the page read for it closes that row at
    // an event and ends inside more of a, which is a row of its own.
    const items = [c(5, 'a'), c(4, 'a'), c(3, 'a'), finalEvent, c(2, 'a'), c(1, 'a')];
    expect(recordEnd(items, true, 1)).toEqual({ cut: true, readOn: false });
    // Asked for itself, that later part is read on.
    expect(recordEnd(items, true, 5)).toEqual({ cut: true, readOn: true });
  });

  it('is whole with nothing older, with no change, or with an event last', () => {
    expect(recordEnd([c(2, 'a'), c(1, 'a')], false, 1)).toEqual(neither);
    expect(recordEnd([c(2, 'a'), c(1, null)], true, 0)).toEqual(neither);
    expect(recordEnd([c(2, 'a'), finalEvent], true, 0)).toEqual(neither);
    expect(recordEnd([], true, -1)).toEqual(neither);
  });

  it('stops reading on CHANGE_READ_MAX items past the asked page, the row still cut, and goes on from the next ask', () => {
    const run = (n: number) => Array.from({ length: n }, (_, i) => c(n - i, 'a'));
    const asked = 49;
    expect(recordEnd(run(asked + CHANGE_READ_MAX), true, asked)).toEqual({ cut: true, readOn: true });
    expect(recordEnd(run(asked + 1 + CHANGE_READ_MAX), true, asked)).toEqual({ cut: true, readOn: false });
    // "Show older" asks from the end of what is loaded.
    expect(recordEnd(run(asked + 51 + CHANGE_READ_MAX), true, asked + 50 + CHANGE_READ_MAX)).toEqual({ cut: true, readOn: true });
  });
});

describe('useDriveActions', () => {
  it('clears the earlier versions through the clear route and says what it freed', async () => {
    vi.mocked(clearHistory).mockResolvedValue({ dropped: 2, freed_bytes: 2048 });
    const { result } = renderHookWithProviders(() => useDriveActions('space-1', null, () => {}));
    await act(async () => {
      expect(await result.current.clearHistory(memo, { head: 7, final: 7 })).toBe(true);
    });
    // Bounded to the head the person was shown: a version saved since stays.
    expect(clearHistory).toHaveBeenCalledWith('space-1', 'e1', { head: 7, final: 7 });
    expect(toast).toHaveBeenCalledWith({ description: 'Cleared 2 earlier versions of aapl.md, freeing 2 KB' });
  });

  beforeEach(() => vi.mocked(toast).mockClear());
  const usage = { bytes: 5_020 * MB, entries: 10, max_bytes: 5_120 * MB, trash_bytes: 300 * MB, max_entries: 100_000, max_file_bytes: 50 * MB };
  const refusal = (status: number, type: string) => new ApiError(`Request failed with status code ${status}`, 'ERR_BAD_RESPONSE', {
    data: { detail: { type, message: 'refused' } }, status, statusText: '', headers: {},
  });

  it('says a restore refused for room as a sentence with the numbers', async () => {
    vi.mocked(restoreTrash).mockRejectedValue(refusal(507, 'over_quota'));
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

  it('undoes the last save over the head it was offered on', async () => {
    const plain = { ...memo, memo: null, name: 'notes.md' };
    vi.mocked(undoSave).mockResolvedValue({ ...plain, version: 8 });
    const { result } = renderHookWithProviders(() => useDriveActions('space-1', usage, () => {}));
    await act(async () => {
      expect(await result.current.undoSave(plain)).toMatchObject({ version: 8 });
    });
    expect(undoSave).toHaveBeenCalledWith('space-1', 'e1', plain.sha256);
    expect(toast).toHaveBeenCalledWith({ description: 'Put back notes.md as it was before its last save' });
  });

  it('says when there is no earlier copy to go back to, and when the file moved on first', async () => {
    const plain = { ...memo, memo: null, name: 'notes.md' };
    const { result } = renderHookWithProviders(() => useDriveActions('space-1', usage, () => {}));
    vi.mocked(undoSave).mockRejectedValueOnce(refusal(404, 'not_found'));
    await act(async () => void (await result.current.undoSave(plain)));
    expect(toast).toHaveBeenLastCalledWith({ variant: 'destructive', title: 'There is no earlier copy to go back to.' });

    vi.mocked(undoSave).mockRejectedValueOnce(refusal(412, 'changed'));
    await act(async () => void (await result.current.undoSave(plain)));
    expect(toast).toHaveBeenLastCalledWith(expect.objectContaining({
      title: 'Could not undo the save',
      description: 'notes.md changed since you last looked. Look at it again, then try once more.',
    }));
  });
});
