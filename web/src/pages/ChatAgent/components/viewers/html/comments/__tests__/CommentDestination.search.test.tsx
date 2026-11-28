import { screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { renderWithProviders } from '@/test/utils';
import type { DestinationThread } from '@/types/api';
import { CommentDestination } from '../CommentDestination';
import type { DestinationChoice } from '../../../../artifactComments/destinations';

const listDestinationThreads = vi.hoisted(() => vi.fn());
vi.mock('@/pages/ChatAgent/utils/api/artifacts', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api/artifacts')>()),
  listDestinationThreads,
}));

const worked: DestinationChoice = {
  key: 'thread:th-worked', target: { threadId: 'th-worked' }, label: 'Margin model', isDefault: true, tag: 'worked on it',
};
const chief: DestinationChoice = {
  key: 'new:ws-home', target: { newThreadIn: 'ws-home' }, label: 'New Chief of Staff chat', isDefault: false,
};
const found: DestinationThread = {
  thread_id: 'th-found', title: 'Q3 notes', workspace_id: 'ws-2', workspace_name: 'Research', home: false, updated_at: null,
};

describe('the "To:" line on the Drive', () => {
  it('tags the worked default, and picks a chat found through "Another chat…"', async () => {
    listDestinationThreads.mockResolvedValue([found]);
    const onPick = vi.fn();
    const onFound = vi.fn();
    const user = userEvent.setup();
    renderWithProviders(
      <CommentDestination
        choices={[worked, chief]}
        status={{ kind: 'ready', selected: worked.key }}
        onPick={onPick}
        search={{ artifactId: 'art-1', onPick: onFound }}
      />,
    );

    await user.click(screen.getByRole('button', { name: /Margin model/ }));
    const menu = await screen.findByRole('menu');
    expect(within(menu).getByText('worked on it')).toBeInTheDocument();
    expect(within(menu).getByText('New Chief of Staff chat')).toBeInTheDocument();

    await user.click(within(menu).getByRole('menuitem', { name: /Another chat/ }));
    const search = await screen.findByRole('searchbox');
    expect(search).toHaveFocus();
    const row = await screen.findByRole('menuitem', { name: /Q3 notes/ });
    expect(row).toHaveTextContent('Research');
    expect(listDestinationThreads).toHaveBeenCalledWith('art-1', '');

    await user.click(row);
    expect(onFound).toHaveBeenCalledWith(found);
    expect(onPick).not.toHaveBeenCalled();
  });

  it('offers no search where the surface does not', async () => {
    const user = userEvent.setup();
    renderWithProviders(<CommentDestination choices={[worked, chief]} status={{ kind: 'ready', selected: worked.key }} onPick={vi.fn()} />);
    await user.click(screen.getByRole('button', { name: /Margin model/ }));
    const menu = await screen.findByRole('menu');
    expect(within(menu).queryByRole('menuitem', { name: /Another chat/ })).toBeNull();
  });
});
