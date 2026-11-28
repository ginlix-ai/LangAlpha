/**
 * A card read only (an earlier version's comment) offers no move, a card the
 * file no longer shows links to where it can be seen as made, and an edit
 * hands the focus back to the menu it was asked from.
 */
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { act, fireEvent, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { renderWithProviders } from '@/test/utils';
import type { ArtifactComment } from '@/types/api';
import { CommentCard } from '../CommentCard';
import { CommentLayerContext, type CommentLayerValue } from '../commentLayerContext';
import { CommentSurfaceContext, type CommentSurface } from '../commentSurface';

vi.mock('@/lib/threadLifecycle/store', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  useThreadFlags: () => ({ isRunning: false, status: 'idle' }),
}));

const comment = {
  comment_id: 'c-1',
  artifact_id: 'a-1',
  number: 3,
  kind: 'drive',
  status: 'open',
  delivery: 'note',
  thread_id: null,
  unread: 0,
  read_through: 0,
  created_at: '2026-11-01T00:00:00Z',
  anchor: { kind: 'text', quote: 'the margin' },
  messages: [{ message_id: 'm1', seq: 1, author: 'user', body: 'Check the margin', sent_at: null, request_key: null, thread_id: null, created_at: '2026-11-01T00:00:00Z' }],
} as unknown as ArtifactComment;

function render(surface: CommentSurface = {}, outdated = false, place: 'over' | 'list' = 'over') {
  const edit = vi.fn().mockResolvedValue(undefined);
  const layer = {
    actions: { sendState: () => ({ sending: false, pending: null, refusal: null }), edit },
    besideThreadId: null,
    destinations: { targetFor: () => null },
    runningFor: () => false,
  } as unknown as CommentLayerValue;
  renderWithProviders(
    <CommentSurfaceContext value={surface}>
      <CommentLayerContext value={layer}>
        <CommentCard comment={comment} outdated={outdated} open onDeleted={vi.fn()} place={place === 'over' ? { kind: 'over' } : { kind: 'list', onToggle: vi.fn() }} />
      </CommentLayerContext>
    </CommentSurfaceContext>,
  );
  return { edit };
}

describe('a comment card', () => {
  it('read only, shows the comment with no move on it', () => {
    render({ readOnly: true });
    expect(screen.getByText('Check the margin')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'More actions' })).toBeNull();
    expect(screen.queryByRole('button', { name: 'Resolve' })).toBeNull();
    expect(screen.queryByRole('textbox')).toBeNull();
  });

  it('offers its moves on the live file', () => {
    render();
    expect(screen.getByRole('button', { name: 'More actions' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Resolve' })).toBeInTheDocument();
  });

  it('Outdated, links to the version it can be seen on, and only then', () => {
    const open = vi.fn();
    const outdatedLink = vi.fn(() => ({ label: 'See it on v3', open }));
    render({ outdatedLink }, true);
    fireEvent.click(screen.getByRole('button', { name: 'See it on v3' }));
    expect(open).toHaveBeenCalled();
    expect(outdatedLink).toHaveBeenCalledWith(comment);
  });

  it.each(['over', 'list'] as const)('Outdated, keeps the pill and its link in a box of their own beside the writer row (%s)', (place) => {
    render({ outdatedLink: () => ({ label: 'See it on v3', open: vi.fn() }) }, true, place);
    const link = screen.getByRole('button', { name: 'See it on v3' });
    const note = link.closest('.page-comment-outdated-note');
    expect(note).not.toBeNull();
    expect(note!.querySelector('.page-comment-outdated')).toHaveTextContent('Outdated');
    // Beside the row, not in it: the row's time cannot run under them.
    expect(note!.closest('.page-comment-card-toggle, .page-comment-card-who')).toBeNull();
    expect(note!.previousElementSibling).toHaveClass(place === 'list' ? 'page-comment-card-toggle' : 'page-comment-card-who');
    // Still heard as part of the toggle's name in the list.
    if (place === 'list') expect(screen.getByRole('button', { name: /Outdated/ })).toHaveAttribute('aria-expanded');
  });

  it('starts the outdated note at its least, its link under the pill, so the writer row loses nothing to it', () => {
    const css = readFileSync(resolve(__dirname, '../CommentCard.css'), 'utf8').replace(/\/\*[\s\S]*?\*\//g, '');
    const rule = (sel: string) => new RegExp(`(^|\\n)${sel.replace(/[.]/g, '\\.')}\\s*\\{([^}]*)\\}`).exec(css)?.[2] ?? '';
    const note = rule('.page-comment-outdated-note');
    expect(note).toMatch(/flex-wrap:\s*wrap/);
    // It grows into the room left, to one line at most, and never shrinks:
    // a share of a shrink took under a pixel from the writer's name and cut it.
    const [, grow, shrink, basis] = /flex:\s*(\d+)\s+(\d+)\s+([\w-]+)/.exec(note) ?? [];
    expect(Number(grow)).toBeGreaterThan(1);
    expect(Number(shrink)).toBe(0);
    expect(basis).toBe('min-content');
    expect(note).toMatch(/max-width:\s*max-content/);
    expect(rule('.page-comment-outdated')).toMatch(/white-space:\s*nowrap/);
  });

  it('with no link where the file still shows it', () => {
    const outdatedLink = vi.fn(() => ({ label: 'See it on v3', open: vi.fn() }));
    render({ outdatedLink }, false);
    expect(screen.queryByRole('button', { name: 'See it on v3' })).toBeNull();
  });

  it('hands the focus back to its menu once an edit is saved or let go', async () => {
    const { edit } = render();
    const menu = screen.getByRole('button', { name: 'More actions' });
    const startEdit = async () => {
      fireEvent.pointerDown(menu, { button: 0, pointerType: 'mouse' });
      fireEvent.click(await screen.findByRole('menuitem', { name: 'Edit' }));
      return screen.findByRole('textbox', { name: 'Edit the comment' });
    };

    await startEdit();
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(menu).toHaveFocus();

    const field = await startEdit();
    fireEvent.change(field, { target: { value: 'Check the gutter' } });
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'Save' })));
    expect(edit).toHaveBeenCalledWith(comment, 'm1', 'Check the gutter');
    expect(menu).toHaveFocus();
  });
});
