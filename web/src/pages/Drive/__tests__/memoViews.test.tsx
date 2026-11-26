import { act, fireEvent, screen, waitFor, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '@/api/client';
import { getAttribution, getRecord, getVersionText, listMemos, listVersions } from '@/api/drive';
import { renderWithProviders } from '@/test/utils';
import type { DriveEntry, DriveMemo, DriveRecordVersion, DriveTextSource, DriveVersion } from '@/types/drive';
import { MemoAuthors } from '../components/MemoAuthors';
import { MemoChanges } from '../components/MemoChanges';
import { MemoMarkDialog } from '../components/MemoMarkDialog';
import { MemosView } from '../components/MemosView';
import { DriveContext, type DriveContextValue } from '../driveContext';
import type { DriveActions } from '../useDriveActions';

vi.mock('@/pages/ChatAgent/components/viewers/CodeEditor', () => ({
  // A diff only when asked for one, so a pane that stops asking shows as plain text.
  default: (p: { value: string; originalValue?: string; inline?: boolean; diffMode?: boolean }) => (p.diffMode
    ? <pre data-testid={p.inline ? 'inline-diff' : 'diff'}>{`${p.originalValue} | ${p.value}`}</pre>
    : <pre data-testid="plain">{p.value}</pre>
  ),
}));
vi.mock('@/api/drive', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/drive')>()),
  listVersions: vi.fn(),
  getVersionText: vi.fn(),
  getAttribution: vi.fn(),
  listMemos: vi.fn(),
  getRecord: vi.fn(),
}));

const person = { kind: 'person' as const, user_id: 'u', agent: null, via: 'app', thread_id: null, thread_title: null, run_id: null, workspace_id: null };
const agent = { ...person, kind: 'agent' as const, agent: 'main', via: 'tool' };
const sha = (n: number) => String(n).repeat(64).slice(0, 64);

const memo: DriveEntry = {
  id: 'e1', path: 'memos/aapl.md', name: 'aapl.md', kind: 'file', size: 10, sha256: sha(3), content_type: 'text/markdown',
  is_text: true, version: 3, created_at: '2026-11-01T00:00:00Z', modified_at: '2026-11-25T10:00:00Z', modified_by: agent,
  deleted_at: null, undo_until: null, memo: { marked: true, status: 'draft', tickers: ['AAPL'], final_seq: null, final_at: null },
};

const v = (seq: number, actor: DriveVersion['actor'] = person, created_at = '2026-11-23T10:00:00Z'): DriveVersion => ({
  seq, size: 10, sha256: sha(seq), created_at, actor, note: null, change_id: null, restored_from: null, is_text: true, expires_at: null, text: 'text',
});

function withContext(ui: React.ReactElement, over: Partial<DriveContextValue> = {}) {
  const ctx = {
    space: 'personal', actions: { pending: {} } as unknown as DriveActions, go: vi.fn(), open: vi.fn(), trash: vi.fn(), leaveEditor: vi.fn(), back: vi.fn(), backTo: 'folder' as const,
    ask: { rename: vi.fn(), move: vi.fn(), newFolder: vi.fn(), memo: vi.fn() },
    ...over,
  } as DriveContextValue;
  renderWithProviders(<DriveContext value={ctx}>{ui}</DriveContext>);
  return ctx;
}

/** The record's rows for these versions, each with the text kind the server reads it as. */
const recordOf = (rows: [number, DriveTextSource][]) => ({
  items: rows.map(([seq, kind]): DriveRecordVersion => ({ type: 'version', ...v(seq), is_text: kind === 'text', added: null, removed: null, text: kind })),
  next: null,
});

const text = (seq: number, body: string, over = {}) => ({ seq, sha256: sha(seq), source: 'text' as const, status: 'ok' as const, text: body, truncated: false, ...over });

beforeEach(() => {
  vi.mocked(listVersions).mockReset();
  vi.mocked(getVersionText).mockReset();
  vi.mocked(getAttribution).mockReset();
  vi.mocked(listMemos).mockReset();
  vi.mocked(getRecord).mockReset();
  vi.mocked(getRecord).mockResolvedValue(recordOf([[3, 'text'], [2, 'text'], [1, 'text']]));
});

describe('MemoChanges', () => {
  beforeEach(() => {
    vi.mocked(listVersions).mockResolvedValue({ versions: [v(3), v(2), v(1)], history_bytes: 0, too_large_for_history: false, clearable: 0 });
    vi.mocked(getVersionText).mockImplementation(async (_s, _e, seq) => text(seq, `text ${seq}`));
  });

  it('shows the newest version against the one before, inline', async () => {
    withContext(<MemoChanges entry={memo} version={null} against={null} onPick={vi.fn()} />);
    expect(await screen.findByTestId('inline-diff')).toHaveTextContent('text 2 | text 3');
    expect(screen.getByRole('button', { name: 'version 2' })).toHaveAttribute('aria-pressed', 'true');
    // Which versions have text comes with the versions list, not the record.
    expect(getRecord).not.toHaveBeenCalled();
  });

  it('compares with any version picked, and goes back to the one before', async () => {
    const onPick = vi.fn();
    withContext(<MemoChanges entry={memo} version={3} against={1} onPick={onPick} />);
    expect(await screen.findByTestId('inline-diff')).toHaveTextContent('text 1 | text 3');
    fireEvent.click(screen.getByRole('button', { name: 'version 2' }));
    expect(onPick).toHaveBeenLastCalledWith(3, null);
    fireEvent.change(screen.getByRole('combobox', { name: 'any version' }), { target: { value: '2' } });
    expect(onPick).toHaveBeenLastCalledWith(3, 2);
    fireEvent.change(screen.getByRole('combobox', { name: 'Version' }), { target: { value: '2' } });
    expect(onPick).toHaveBeenLastCalledWith(2, null);
  });

  it('says the first version is all new', async () => {
    withContext(<MemoChanges entry={memo} version={1} against={null} onPick={vi.fn()} />);
    expect(await screen.findByTestId('inline-diff')).toHaveTextContent('| text 1');
    expect(screen.getByText('Version 1 is the first: all of it is new.')).toBeInTheDocument();
  });

  it('says when no text could be read out of a version, rather than comparing nothing', async () => {
    vi.mocked(getVersionText).mockImplementation(async (_s, _e, seq) => text(seq, '', seq === 2 ? { status: 'empty', text: null, source: 'extracted' } : {}));
    withContext(<MemoChanges entry={{ ...memo, name: 'aapl.pdf' }} version={null} against={null} onPick={vi.fn()} />);
    expect(await screen.findByText('No text could be read out of version 2.')).toBeInTheDocument();
    expect(screen.queryByTestId('inline-diff')).toBeNull();
  });

  it('offers no comparison for a memo whose versions have no text', async () => {
    vi.mocked(listVersions).mockResolvedValue({ versions: [{ ...v(3), is_text: false, text: 'none' }, { ...v(2), is_text: false, text: 'none' }], history_bytes: 0, too_large_for_history: false, clearable: 0 });
    withContext(<MemoChanges entry={{ ...memo, name: 'model.xlsx', is_text: false }} version={null} against={null} onPick={vi.fn()} />);
    expect(await screen.findByText(/Changes are shown for text, Word and PDF memos/)).toBeInTheDocument();
    expect(getVersionText).not.toHaveBeenCalled();
  });

  it('compares a Word version the server reads as extracted text, whatever its bytes look like', async () => {
    vi.mocked(listVersions).mockResolvedValue({ versions: [{ ...v(3), is_text: false, text: 'extracted' }, { ...v(2), is_text: true, text: 'extracted' }, { ...v(1), text: 'extracted' }], history_bytes: 0, too_large_for_history: false, clearable: 0 });
    vi.mocked(getVersionText).mockImplementation(async (_s, _e, seq) => text(seq, `text ${seq}`, { source: 'extracted' }));
    withContext(<MemoChanges entry={{ ...memo, name: 'aapl.docx', is_text: false }} version={null} against={null} onPick={vi.fn()} />);
    expect(await screen.findByTestId('inline-diff')).toHaveTextContent('text 2 | text 3');
  });

  it('compares with the version it is given, never one the address named before resolving', async () => {
    const onPick = vi.fn();
    withContext(<MemoChanges entry={memo} version={3} against={1} onPick={onPick} />);
    expect(await screen.findByTestId('inline-diff')).toHaveTextContent('text 1 | text 3');
    // What the address says is resolved before it gets here: nothing to put right.
    expect(onPick).not.toHaveBeenCalled();
  });

  it('waits on a text from other bytes than the listing named, and compares once they agree', async () => {
    const listed = (head: string) => ({ versions: [{ ...v(3), sha256: head }, v(2), v(1)], history_bytes: 0, too_large_for_history: false, clearable: 0 });
    let caughtUp: (answer: ReturnType<typeof listed>) => void = () => {};
    vi.mocked(listVersions)
      .mockResolvedValueOnce(listed(sha(3)))
      .mockImplementationOnce(() => new Promise((resolve) => { caughtUp = resolve; }));
    // A save folded into the head after the listing: same seq, other bytes.
    vi.mocked(getVersionText).mockImplementation(async (_s, _e, seq) => (seq === 3 ? { ...text(3, 'folded'), sha256: sha(4) } : text(seq, `text ${seq}`)));
    withContext(<MemoChanges entry={memo} version={null} against={null} onPick={vi.fn()} />);
    // The mismatch asks for the listing again; until it answers, no diff.
    await waitFor(() => expect(listVersions).toHaveBeenCalledTimes(2));
    expect(screen.queryByTestId('inline-diff')).not.toBeInTheDocument();
    expect(screen.getByRole('status')).toBeInTheDocument();
    await act(async () => caughtUp(listed(sha(4))));
    expect(await screen.findByTestId('inline-diff')).toHaveTextContent('text 2 | folded');
  });

  it('reads the text again when the listing comes back naming the bytes it named before', async () => {
    // A fold and its undo: the head went to other bytes and back while the listing was asked again.
    let head = 4;
    vi.mocked(getVersionText).mockImplementation(async (_s, _e, seq) => (seq === 3 ? { ...text(3, `head ${head}`), sha256: sha(head) } : text(seq, `text ${seq}`)));
    let asked = 0;
    vi.mocked(listVersions).mockImplementation(async () => {
      if (++asked > 1) head = 3;
      return { versions: [v(3), v(2), v(1)], history_bytes: 0, too_large_for_history: false, clearable: 0 };
    });
    withContext(<MemoChanges entry={memo} version={null} against={null} onPick={vi.fn()} />);
    expect(await screen.findByTestId('inline-diff')).toHaveTextContent('text 2 | head 3');
    expect(listVersions).toHaveBeenCalledTimes(2);
  });

  it('offers a retry when the reread of a text from other bytes fails, rather than waiting on it', async () => {
    const failed = new ApiError('Request failed with status code 500', 'ERR_BAD_RESPONSE', { status: 500, data: { detail: 'boom' }, headers: {} } as never);
    let head: 'other' | 'fail' | 'same' = 'other';
    vi.mocked(getVersionText).mockImplementation(async (_s, _e, seq) => {
      if (seq !== 3) return text(seq, `text ${seq}`);
      if (head === 'other') { head = 'fail'; return { ...text(3, 'folded'), sha256: sha(4) }; }
      if (head === 'fail') throw failed;
      return text(3, 'text 3');
    });
    withContext(<MemoChanges entry={memo} version={null} against={null} onPick={vi.fn()} />);
    // The listing still names the bytes it named; the reread fails and keeps the other bytes' text.
    expect(await screen.findByText('Could not load this.')).toBeInTheDocument();
    expect(screen.queryByTestId('inline-diff')).not.toBeInTheDocument();
    head = 'same';
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
    expect(await screen.findByTestId('inline-diff')).toHaveTextContent('text 2 | text 3');
  });

  it('reads a text from other bytes again once a retried listing answers, though it names the bytes it named before', async () => {
    const failed = new ApiError('Request failed with status code 500', 'ERR_BAD_RESPONSE', { status: 500, data: { detail: 'boom' }, headers: {} } as never);
    const listed = { versions: [v(3), v(2), v(1)], history_bytes: 0, too_large_for_history: false, clearable: 0 };
    vi.mocked(listVersions).mockResolvedValueOnce(listed).mockRejectedValueOnce(failed).mockResolvedValue(listed);
    // The head is folded to other bytes, then goes back to the bytes the listing names.
    let head = 4;
    vi.mocked(getVersionText).mockImplementation(async (_s, _e, seq) => (seq === 3 ? { ...text(3, `head ${head}`), sha256: sha(head) } : text(seq, `text ${seq}`)));
    withContext(<MemoChanges entry={memo} version={null} against={null} onPick={vi.fn()} />);
    // The mismatch asks for the listing again, and that read fails.
    expect(await screen.findByText('Could not load this.')).toBeInTheDocument();
    // Any read of the text that comes of it lands while the head is still folded.
    await new Promise((resolve) => setTimeout(resolve, 50));
    head = 3;
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
    expect(await screen.findByTestId('inline-diff')).toHaveTextContent('text 2 | head 3');
  });

  it('blames the connection only when the server never answered', async () => {
    vi.mocked(getVersionText).mockRejectedValue(new ApiError('Request failed with status code 500', 'ERR_BAD_RESPONSE', {
      status: 500, data: { detail: 'boom' }, headers: {},
    } as never));
    withContext(<MemoChanges entry={memo} version={null} against={null} onPick={vi.fn()} />);
    expect(await screen.findByText('Could not load this.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Retry' })).toBeInTheDocument();
  });
});

describe('MemoAuthors', () => {
  it('labels each run of lines with who wrote it and when, and opens that version', async () => {
    vi.mocked(getAttribution).mockResolvedValue({
      seq: 3, sha256: sha(3),
      runs: [{ start: 1, count: 2, seq: 1 }, { start: 3, count: 1, seq: 3 }],
      // A local calendar day, so the label reads the same in every time zone.
      versions: [v(1, person, new Date(2026, 10, 23, 10).toISOString()), v(3, agent, new Date().toISOString())],
    });
    vi.mocked(getVersionText).mockResolvedValue(text(3, '# AAPL\nthesis\nnew risk\n'));
    const onPick = vi.fn();
    withContext(<MemoAuthors entry={memo} onPick={onPick} />);
    const you = await screen.findByRole('button', { name: 'You · Nov 23' });
    const agentRun = screen.getByRole('button', { name: 'Agent · today' });
    expect(agentRun.className).toBe(you.className);
    expect(screen.getAllByRole('cell').map((c) => c.textContent)).toEqual(['# AAPL\nthesis', 'new risk']);
    fireEvent.click(you);
    expect(onPick).toHaveBeenCalledWith(1);
  });

  it('asks again for who wrote what when a save changed the text under the same version', async () => {
    const runs = [{ start: 1, count: 1, seq: 3 }];
    const versions = [v(3, agent, new Date().toISOString())];
    vi.mocked(getAttribution)
      .mockResolvedValueOnce({ seq: 3, sha256: sha(3), runs, versions })
      .mockResolvedValue({ seq: 3, sha256: sha(4), runs: [{ start: 1, count: 2, seq: 3 }], versions });
    // The head was saved again between the two requests: same seq, other bytes.
    vi.mocked(getVersionText).mockResolvedValue({ ...text(3, 'one\ntwo\n'), sha256: sha(4) });
    withContext(<MemoAuthors entry={memo} onPick={vi.fn()} />);
    expect(await screen.findByRole('cell')).toHaveTextContent('one two');
    expect(getAttribution).toHaveBeenCalledTimes(2);
  });

  it('reads the text again when who wrote what comes back naming the bytes it named before', async () => {
    const versions = [v(3, agent, new Date().toISOString())];
    let head = 4;
    vi.mocked(getAttribution).mockImplementation(async () => ({ seq: 3, sha256: sha(3), runs: [{ start: 1, count: 1, seq: 3 }], versions }));
    vi.mocked(getVersionText).mockImplementation(async () => {
      const answer = { ...text(3, `head ${head}\n`), sha256: sha(head) };
      head = 3;
      return answer;
    });
    withContext(<MemoAuthors entry={memo} onPick={vi.fn()} />);
    expect(await screen.findByRole('cell')).toHaveTextContent('head 3');
    expect(getAttribution).toHaveBeenCalledTimes(2);
    expect(getVersionText).toHaveBeenCalledTimes(2);
  });

  it('stops asking when the text keeps answering other bytes than the source names', async () => {
    vi.mocked(getAttribution).mockResolvedValue({ seq: 3, sha256: sha(3), runs: [], versions: [] });
    vi.mocked(getVersionText).mockResolvedValue({ ...text(3, 'other\n'), sha256: sha(4) });
    withContext(<MemoAuthors entry={memo} onPick={vi.fn()} />);
    await waitFor(() => expect(getVersionText).toHaveBeenCalledTimes(2));
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(getAttribution).toHaveBeenCalledTimes(2);
    expect(getVersionText).toHaveBeenCalledTimes(2);
    expect(screen.getByRole('status')).toBeInTheDocument();
  });

  it('says who wrote what is for text memos when the server says so', async () => {
    vi.mocked(getAttribution).mockRejectedValue(new ApiError('Request failed with status code 409', 'ERR_BAD_REQUEST', {
      data: { detail: { type: 'not_text', message: 'not text' } }, status: 409, statusText: '', headers: {},
    }));
    withContext(<MemoAuthors entry={memo} onPick={vi.fn()} />);
    expect(await screen.findByText('Who wrote what is shown for text memos only.')).toBeInTheDocument();
  });
});

describe('MemoMarkDialog', () => {
  const plain = { ...memo, memo: null };

  it('marks a file with its status, tickers and why', async () => {
    const onSubmit = vi.fn().mockResolvedValue(true);
    renderWithProviders(<MemoMarkDialog ask={{ kind: 'mark', entry: plain }} onClose={vi.fn()} onSubmit={onSubmit} />);
    fireEvent.click(screen.getByRole('button', { name: 'Final' }));
    fireEvent.change(screen.getByPlaceholderText('AAPL, MSFT'), { target: { value: 'aapl msft' } });
    fireEvent.change(screen.getByPlaceholderText('Optional, one line'), { target: { value: 'IC memo' } });
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'Mark as memo' })));
    expect(onSubmit).toHaveBeenCalledWith(plain, { memo: true, status: 'final', ifMatch: plain.sha256, tickers: ['AAPL', 'MSFT'], note: 'IC memo' });
  });

  it('re-marks a former memo without sending the status it kept, so its final stays pinned', async () => {
    const onSubmit = vi.fn().mockResolvedValue(true);
    const former = { ...memo, version: 8, memo: { marked: false, status: 'final' as const, tickers: ['AAPL'], final_seq: 6, final_at: null } };
    renderWithProviders(<MemoMarkDialog ask={{ kind: 'mark', entry: former }} onClose={vi.fn()} onSubmit={onSubmit} />);
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'Mark as memo' })));
    expect(onSubmit).toHaveBeenCalledWith(former, { memo: true, note: null });
  });

  it('clears the tickers when re-marking with the field emptied', async () => {
    const onSubmit = vi.fn().mockResolvedValue(true);
    const former = { ...memo, memo: { marked: false, status: 'draft' as const, tickers: ['AAPL'], final_seq: null, final_at: null } };
    renderWithProviders(<MemoMarkDialog ask={{ kind: 'mark', entry: former }} onClose={vi.fn()} onSubmit={onSubmit} />);
    fireEvent.change(screen.getByPlaceholderText('AAPL, MSFT'), { target: { value: '' } });
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'Mark as memo' })));
    expect(onSubmit).toHaveBeenCalledWith(former, { memo: true, tickers: [], note: null });
  });

  it('sends only what changed on a memo, and nothing when nothing did', async () => {
    const onSubmit = vi.fn().mockResolvedValue(true);
    const onClose = vi.fn();
    renderWithProviders(<MemoMarkDialog ask={{ kind: 'edit', entry: memo }} onClose={onClose} onSubmit={onSubmit} />);
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'Save' })));
    expect(onSubmit).not.toHaveBeenCalled();
    expect(onClose).toHaveBeenCalled();

    fireEvent.change(screen.getByPlaceholderText('AAPL, MSFT'), { target: { value: 'AAPL, NVDA' } });
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'Save' })));
    expect(onSubmit).toHaveBeenCalledWith(memo, { note: null, tickers: ['AAPL', 'NVDA'] });
  });

  it('refuses a ticker the server would, before sending', () => {
    const onSubmit = vi.fn();
    renderWithProviders(<MemoMarkDialog ask={{ kind: 'mark', entry: plain }} onClose={vi.fn()} onSubmit={onSubmit} />);
    fireEvent.change(screen.getByPlaceholderText('AAPL, MSFT'), { target: { value: 'AAPL $X' } });
    expect(screen.getByRole('alert')).toHaveTextContent('Not a ticker: $X');
    expect(screen.getByRole('button', { name: 'Mark as memo' })).toBeDisabled();
  });

  it('takes the mark off from the details, saying the record is kept', async () => {
    const onSubmit = vi.fn().mockResolvedValue(true);
    renderWithProviders(<MemoMarkDialog ask={{ kind: 'edit', entry: memo }} onClose={vi.fn()} onSubmit={onSubmit} />);
    fireEvent.click(screen.getByRole('button', { name: 'Remove memo mark' }));
    expect(screen.getByText(/Its versions and record are kept/)).toBeInTheDocument();
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'Remove mark' })));
    expect(onSubmit).toHaveBeenCalledWith(memo, { memo: false, note: null });
  });
});

describe('MemosView', () => {
  const row = (name: string, over: Partial<DriveEntry> = {}): DriveMemo => ({ entry: { ...memo, id: name, name, path: `research/${name}`, ...over } });

  it('lists every memo with its folder, status, tickers and who revised it, and filters by a ticker picked', async () => {
    const aapl = row('aapl.md');
    const msft = row('msft.md', { path: 'msft.md', version: 7, memo: { marked: true, status: 'final', tickers: ['MSFT'], final_seq: 6, final_at: null } });
    // Answered as the server would: a ticker filter holds only that ticker's memos.
    vi.mocked(listMemos).mockImplementation(async (_space, query) => ({
      memos: [aapl, msft].filter((m) => !query.ticker || m.entry.memo?.tickers.includes(query.ticker)),
      next: null,
    }));
    const ctx = withContext(<MemosView openPath={null} compact={false} />);
    const rows = await screen.findAllByRole('row');
    // The header, then a row per memo.
    expect(rows).toHaveLength(3);
    expect(rows[1]).toHaveTextContent('aapl.md');
    expect(rows[1]).toHaveTextContent('research');
    expect(rows[1]).toHaveTextContent('Draft');
    expect(rows[1]).toHaveTextContent('Agent ·');
    expect(rows[2]).toHaveTextContent('Final v6');
    expect(rows[2]).toHaveTextContent('Drive');
    expect(listMemos).toHaveBeenLastCalledWith('personal', { sort: 'revised', order: 'desc', status: null, ticker: null }, null);

    fireEvent.click(screen.getByRole('button', { name: 'MSFT' }));
    await waitFor(() => expect(listMemos).toHaveBeenLastCalledWith('personal', expect.objectContaining({ ticker: 'MSFT' }), null));
    // The earlier rows stay up while the filtered answer is on its way.
    await waitFor(() => expect(screen.queryByText('aapl.md')).toBeNull());
    fireEvent.click(within(screen.getByRole('columnheader', { name: 'Name' })).getByRole('button', { name: 'Name' }));
    await waitFor(() => expect(listMemos).toHaveBeenLastCalledWith('personal', expect.objectContaining({ ticker: 'MSFT', sort: 'name', order: 'asc' }), null));

    fireEvent.click(screen.getByText('msft.md'));
    expect(ctx.open).toHaveBeenCalledWith(expect.objectContaining({ name: 'msft.md' }));
  });

  it('keeps the sort controls and the opener as buttons inside their table roles, the sort state on the header', async () => {
    vi.mocked(listMemos).mockResolvedValue({ memos: [row('aapl.md')], next: null });
    const ctx = withContext(<MemosView openPath={null} compact={false} />);
    await screen.findAllByRole('row');
    const revised = screen.getByRole('columnheader', { name: 'Revised' });
    expect(revised).toHaveAttribute('aria-sort', 'descending');
    const sort = within(revised).getByRole('button', { name: 'Revised' });
    expect(sort).not.toHaveAttribute('aria-sort');
    expect(screen.getByRole('columnheader', { name: 'Name' })).not.toHaveAttribute('aria-sort');
    fireEvent.click(sort);
    await waitFor(() => expect(screen.getByRole('columnheader', { name: 'Revised' })).toHaveAttribute('aria-sort', 'ascending'));

    const cell = screen.getAllByRole('cell')[0];
    const opener = within(cell).getByRole('button', { name: /aapl\.md/ });
    fireEvent.click(opener);
    expect(ctx.open).toHaveBeenCalledWith(expect.objectContaining({ name: 'aapl.md' }));
  });

  it('says how to make a memo when there are none, and offers no new one', async () => {
    vi.mocked(listMemos).mockResolvedValue({ memos: [], next: null });
    withContext(<MemosView openPath={null} compact={false} />);
    expect(await screen.findByText('No memos yet. Mark any file as a memo from its menu.')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'New memo' })).toBeNull();
  });
});
