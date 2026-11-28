import { act, fireEvent, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import i18n from '@/i18n';
import { renderWithProviders } from '@/test/utils';
import { CommentMarker } from '../components/CommentMarker';
import { DriveContext, type DriveContextValue } from '../driveContext';

const getCommentCounts = vi.hoisted(() => vi.fn());
vi.mock('@/api/drive', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/drive')>()),
  getCommentCounts,
}));

function renderMarkers(space: string, ids: string[], go = vi.fn()) {
  const ctx = { space, go } as unknown as DriveContextValue;
  return renderWithProviders(
    <DriveContext value={ctx}>
      {ids.map((id) => <CommentMarker key={id} entry={{ id, path: `notes/${id}.md` }} />)}
    </DriveContext>,
  );
}

afterEach(async () => {
  await act(() => i18n.changeLanguage('en-US'));
});

beforeEach(() => {
  getCommentCounts.mockReset();
  getCommentCounts.mockResolvedValue({
    open: { open: 2, unread: 0 },
    unread: { open: 1, unread: 1 },
    answered: { open: 0, unread: 3 },
    quiet: { open: 0, unread: 0 },
  });
});

describe('a file\'s comment marker in a Drive list', () => {
  it('counts the open comments, adds a dot for an unread answer, and says both', async () => {
    renderMarkers('personal', ['open', 'unread', 'answered', 'quiet', 'missing']);

    const open = await screen.findByTestId('drive-comment-marker-open');
    expect(open).toHaveTextContent('2');
    expect(open.title).toBe('2 open comments');
    expect(open.querySelector('.drive-comment-marker-dot')).toBeNull();

    const unread = screen.getByTestId('drive-comment-marker-unread');
    expect(unread.querySelector('.drive-comment-marker-dot')).not.toBeNull();
    expect(unread.title).toBe('1 open comment, 1 with an unread answer');

    const answered = screen.getByTestId('drive-comment-marker-answered');
    expect(answered.querySelector('.drive-comment-marker-count')).toBeNull();
    expect(answered.querySelector('.drive-comment-marker-dot')).not.toBeNull();

    expect(screen.queryByTestId('drive-comment-marker-quiet')).toBeNull();
    expect(screen.queryByTestId('drive-comment-marker-missing')).toBeNull();
    expect(getCommentCounts).toHaveBeenCalledTimes(1);
  });

  it('says both counts as one sentence of the reader\'s language', async () => {
    await act(() => i18n.changeLanguage('zh-CN'));
    renderMarkers('personal', ['unread']);
    const unread = await screen.findByTestId('drive-comment-marker-unread');
    expect(unread.title).toBe('1 条未解决的评论，1 条有未读回复');
    expect(unread.querySelector('.sr-only')).toHaveTextContent('1 条未解决的评论，1 条有未读回复');
  });

  it('is a button named by its counts that opens the file with its comment list, not the row it sits in', async () => {
    const go = vi.fn();
    const row = vi.fn();
    const ctx = { space: 'personal', go } as unknown as DriveContextValue;
    renderWithProviders(
      <DriveContext value={ctx}>
        <div onClick={row}><CommentMarker entry={{ id: 'unread', path: 'notes/a.md' }} /></div>
      </DriveContext>,
    );
    const marker = await screen.findByRole('button', { name: '1 open comment, 1 with an unread answer' });
    fireEvent.click(marker);
    expect(go).toHaveBeenCalledWith({ file: 'notes/a.md', ask: 'list' });
    expect(row).not.toHaveBeenCalled();
  });

  it('asks nothing in a space that keeps no comments', async () => {
    renderMarkers('shared', ['open']);
    await Promise.resolve();
    expect(getCommentCounts).not.toHaveBeenCalled();
    expect(screen.queryByTestId('drive-comment-marker-open')).toBeNull();
  });
});
