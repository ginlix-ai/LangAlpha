/**
 * The "To:" line's search for another chat, by the keys: the field takes the
 * typing, words typed go before the search does, Tab closes it onto the line,
 * and a pick hands the focus back to the comment's own field. And the line
 * when the chats could not be read.
 */
import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { renderWithProviders } from '@/test/utils';
import type { DestinationThread } from '@/types/api';
import { FieldFocus } from '../fieldFocus';
import { CommentDestination } from '../CommentDestination';
import type { DestinationChoice } from '../../../../artifactComments/destinations';

const listDestinationThreads = vi.hoisted(() => vi.fn());
vi.mock('@/pages/ChatAgent/utils/api/artifacts', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api/artifacts')>()),
  listDestinationThreads,
}));

const LONG = 'Rebuild the margin bridge from segment disclosures and reconcile to guidance';
const worked: DestinationChoice = {
  key: 'thread:th-worked', target: { threadId: 'th-worked' }, label: LONG, isDefault: true, tag: 'worked on it',
};
const found: DestinationThread = {
  thread_id: 'th-found', title: 'Q3 notes', workspace_id: 'ws-2', workspace_name: 'Research', home: false, updated_at: null,
};

function setup(props: Partial<React.ComponentProps<typeof CommentDestination>> = {}) {
  listDestinationThreads.mockResolvedValue([found]);
  const onFound = vi.fn();
  renderWithProviders(
    <FieldFocus value={() => screen.getByRole('textbox', { name: 'Comment' }).focus()}>
      <textarea aria-label="Comment" />
      <CommentDestination choices={[worked]} status={{ kind: 'ready', selected: worked.key }} onPick={vi.fn()} search={{ artifactId: 'art-1', onPick: onFound }} {...props} />
    </FieldFocus>,
  );
  return { onFound, user: userEvent.setup() };
}

async function openSearch(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByRole('button', { name: /Rebuild the margin bridge/ }));
  await user.click(within(await screen.findByRole('menu')).getByRole('menuitem', { name: /Another chat/ }));
  const field = await screen.findByRole('searchbox');
  await waitFor(() => expect(field).toHaveFocus());
  return field;
}

describe('the search for another chat, by the keys', () => {
  it('hands the focus to the comment\'s field once a chat is picked', async () => {
    const { user, onFound } = setup();
    await openSearch(user);
    await user.click(await screen.findByRole('menuitem', { name: /Q3 notes/ }));
    expect(onFound).toHaveBeenCalledWith(found);
    await waitFor(() => expect(screen.getByRole('textbox', { name: 'Comment' })).toHaveFocus());
  });

  it('clears words typed on Esc before it closes on the next', async () => {
    const { user } = setup();
    const field = await openSearch(user);
    await user.type(field, 'q3');
    await user.keyboard('{Escape}');
    expect(screen.getByRole('searchbox')).toHaveValue('');
    await user.keyboard('{Escape}');
    await waitFor(() => expect(screen.queryByRole('searchbox')).toBeNull());
  });

  it('closes onto the "To:" line on Tab, either way', async () => {
    const { user } = setup();
    await openSearch(user);
    await user.keyboard('{Tab}');
    await waitFor(() => expect(screen.queryByRole('searchbox')).toBeNull());
    await waitFor(() => expect(screen.getByRole('button', { name: /Rebuild the margin bridge/ })).toHaveFocus());
    await openSearch(user);
    await user.keyboard('{Shift>}{Tab}{/Shift}');
    await waitFor(() => expect(screen.queryByRole('searchbox')).toBeNull());
  });

  it('names a long chat in full on hover, in the menu and the search', async () => {
    const { user } = setup();
    await user.click(screen.getByRole('button', { name: /Rebuild the margin bridge/ }));
    const row = within(await screen.findByRole('menu')).getByRole('menuitemradio', { name: /Rebuild/ });
    expect(row).toHaveAttribute('title', LONG);
    await user.click(within(screen.getByRole('menu')).getByRole('menuitem', { name: /Another chat/ }));
    expect((await screen.findByRole('menuitem', { name: /Q3 notes/ })).querySelector('[title="Q3 notes"]')).not.toBeNull();
  });
});

describe('the "To:" line when the chats could not be read', () => {
  it('says so and tries again, rather than leaving sending dimmed with no reason', async () => {
    const onRetry = vi.fn();
    const user = userEvent.setup();
    renderWithProviders(<CommentDestination choices={[]} status={{ kind: 'failed', retry: onRetry }} onPick={vi.fn()} />);
    expect(screen.getByRole('status')).toHaveTextContent('Chats could not be loaded');
    await user.click(screen.getByRole('button', { name: 'Try again' }));
    expect(onRetry).toHaveBeenCalledTimes(1);
  });

  it('shows nothing while they are still being read', () => {
    const { container } = renderWithProviders(<CommentDestination choices={[]} status={{ kind: 'loading' }} onPick={vi.fn()} />);
    expect(container).toBeEmptyDOMElement();
  });
});

describe('the "To:" line while a kept chat is checked', () => {
  it('waits on a neutral bar rather than naming the default', () => {
    renderWithProviders(<CommentDestination choices={[worked]} status={{ kind: 'checking' }} onPick={vi.fn()} />);
    const line = screen.getByRole('button', { name: 'Loading...' });
    expect(line).toHaveAttribute('aria-busy', 'true');
    expect(line).toBeDisabled();
    expect(screen.queryByText(LONG)).toBeNull();
  });
});
