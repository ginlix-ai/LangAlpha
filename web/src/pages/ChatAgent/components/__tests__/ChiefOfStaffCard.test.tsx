/**
 * The Chief of Staff's card: its latest threads, the questions it offers
 * before it has any, and where each of its buttons goes.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { renderWithProviders } from '@/test/utils';
import type { Thread, Workspace } from '@/types/api';

const mockNavigate = vi.fn();
vi.mock('react-router', async (importOriginal) => ({
  ...(await importOriginal<typeof import('react-router')>()),
  useNavigate: () => mockNavigate,
}));

vi.mock('../../utils/api', () => ({
  getWorkspaceThreads: vi.fn(),
}));

import { getWorkspaceThreads } from '../../utils/api';
import { ChiefOfStaffCard } from '../workspaceGallery/ChiefOfStaffCard';

const mockThreads = getWorkspaceThreads as Mock;

const HOME: Workspace = {
  workspace_id: 'home-0001',
  name: 'Home',
  status: 'running',
  is_home: true,
};

const thread = (id: string, title: string | null, minutesAgo: number, extra: Partial<Thread> = {}): Thread => ({
  workspace_id: HOME.workspace_id,
  thread_id: id,
  title,
  updated_at: new Date(Date.now() - minutesAgo * 60_000).toISOString(),
  ...extra,
} as Thread);

const route = { workspaceId: HOME.workspace_id, workspaceName: 'Chief of Staff', workspaceStatus: 'running' };

function renderCard(onOpenThreads = vi.fn()) {
  const { queryClient } = renderWithProviders(<ChiefOfStaffCard workspace={HOME} now={Date.now()} onOpenThreads={onOpenThreads} />);
  return { onOpenThreads, queryClient };
}

beforeEach(() => {
  vi.resetAllMocks();
});

describe('ChiefOfStaffCard', () => {
  it('says it coordinates the Analysts, under its name', () => {
    mockThreads.mockResolvedValue({ threads: [], total: 0 });
    renderCard();

    expect(screen.getByRole('heading', { name: 'Chief of Staff' })).toBeInTheDocument();
    expect(screen.getByText(/Coordinates the Analysts in every workspace/)).toBeInTheDocument();
  });

  it('reads past a first page full of old pins to find its newest threads', async () => {
    const pins = Array.from({ length: 19 }, (_, i) => thread(`pin-${i}`, `Pinned ${i}`, 10_000 + i, { is_pinned: true }));
    mockThreads.mockImplementation(async (_ws: string, _limit: number, offset: number) =>
      offset === 0
        ? { threads: [...pins, thread('t-3', 'Third newest', 300)], total: 22 }
        : { threads: [thread('t-1', 'Newest', 5), thread('t-2', 'Second newest', 60)], total: 22 });
    renderCard();

    await waitFor(() => expect(screen.getAllByTestId('chief-of-staff-thread')[0]).toHaveTextContent('Newest'));
    expect(screen.getAllByTestId('chief-of-staff-thread').map((r) => r.textContent)).toEqual([
      expect.stringContaining('Newest'),
      expect.stringContaining('Second newest'),
      expect.stringContaining('Third newest'),
    ]);
    expect(mockThreads).toHaveBeenCalledWith(HOME.workspace_id, 20, 20, null, false);
  });

  it('keeps the first page when a page after it fails, without asking again', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const pins = Array.from({ length: 20 }, (_, i) => thread(`pin-${i}`, `Pinned ${i}`, 10_000 + i, { is_pinned: true }));
    mockThreads.mockImplementation(async (_ws: string, _limit: number, offset: number) => {
      if (offset === 0) return { threads: pins, total: 25 };
      throw new Error('offline');
    });
    renderCard();

    await waitFor(() => expect(mockThreads).toHaveBeenCalledTimes(2));
    expect(screen.getAllByTestId('chief-of-staff-thread')[0]).toHaveTextContent('Pinned 0');
    await new Promise((r) => setTimeout(r, 50));
    expect(mockThreads).toHaveBeenCalledTimes(2);
  });

  it('lists its three latest threads by time, a pinned one included, and opens one', async () => {
    const user = userEvent.setup();
    mockThreads.mockResolvedValue({
      threads: [
        thread('t-old', 'Old but pinned', 600, { is_pinned: true }),
        thread('t-1', 'NVIDIA staged IBKR limit order', 49),
        thread('t-2', null, 180),
        thread('t-3', 'Feishu test', 1440),
      ],
      total: 4,
    });
    renderCard();

    const rows = await screen.findAllByTestId('chief-of-staff-thread');
    expect(rows.map((r) => r.textContent)).toEqual([
      expect.stringContaining('NVIDIA staged IBKR limit order'),
      expect.stringContaining('Untitled'),
      expect.stringContaining('Old but pinned'),
    ]);
    expect(screen.queryByText('Try asking')).not.toBeInTheDocument();

    await user.click(rows[0]);
    expect(mockNavigate).toHaveBeenCalledWith('/chat/t/t-1', { state: route });
  });

  it('opens its thread list from All threads', async () => {
    const user = userEvent.setup();
    mockThreads.mockResolvedValue({ threads: [thread('t-1', 'One', 5)], total: 1 });
    const { onOpenThreads } = renderCard();

    await user.click(await screen.findByTestId('chief-of-staff-threads'));
    expect(onOpenThreads).toHaveBeenCalledWith(HOME.workspace_id, 'Chief of Staff', 'running');
  });

  it('starts a blank thread from New thread', async () => {
    const user = userEvent.setup();
    mockThreads.mockResolvedValue({ threads: [], total: 0 });
    renderCard();

    await user.click(screen.getByTestId('chief-of-staff-new-thread'));
    expect(mockNavigate).toHaveBeenCalledWith('/chat/t/__default__', { state: route });
  });

  it('offers questions before it has a thread, each opening a thread with it typed but not sent', async () => {
    const user = userEvent.setup();
    mockThreads.mockResolvedValue({ threads: [], total: 0 });
    renderCard();

    expect(await screen.findByText('Try asking')).toBeInTheDocument();
    expect(screen.queryByTestId('chief-of-staff-threads')).not.toBeInTheDocument();
    const starters = screen.getAllByTestId('chief-of-staff-starter');
    expect(starters).toHaveLength(3);

    await user.click(starters[0]);
    expect(mockNavigate).toHaveBeenCalledWith('/chat/t/__default__', {
      state: { ...route, draftMessage: 'What have my Analysts been working on this week?' },
    });
    // Typed, not sent: an initial message would send on arrival.
    expect(mockNavigate.mock.calls[0][1].state).not.toHaveProperty('initialMessage');
  });

  it('keeps the threads it has when a later read fails', async () => {
    mockThreads.mockResolvedValue({ threads: [thread('t-1', 'One', 5)], total: 1 });
    const { queryClient } = renderCard();
    await screen.findByTestId('chief-of-staff-thread');

    mockThreads.mockRejectedValue(new Error('offline'));
    await queryClient.refetchQueries();
    expect(screen.getByTestId('chief-of-staff-thread')).toHaveTextContent('One');
    expect(screen.queryByTestId('chief-of-staff-starter')).not.toBeInTheDocument();
    expect(screen.getByTestId('chief-of-staff-threads')).toBeInTheDocument();
  });

  it('labels nothing and offers no thread list until the threads are read', () => {
    mockThreads.mockReturnValue(new Promise(() => {}));
    renderCard();

    expect(screen.queryByText('Recent')).not.toBeInTheDocument();
    expect(screen.queryByTestId('chief-of-staff-threads')).not.toBeInTheDocument();
    expect(screen.getByTestId('chief-of-staff-new-thread')).toBeInTheDocument();
  });

  it('offers the questions, and keeps its thread list a click away, when its threads cannot be read', async () => {
    const user = userEvent.setup();
    mockThreads.mockRejectedValue(new Error('offline'));
    const { onOpenThreads } = renderCard();

    await waitFor(() => expect(screen.getAllByTestId('chief-of-staff-starter')).toHaveLength(3));
    await user.click(screen.getByTestId('chief-of-staff-threads'));
    expect(onOpenThreads).toHaveBeenCalledWith(HOME.workspace_id, 'Chief of Staff', 'running');
  });

  it('offers no thread list when it has no threads', async () => {
    mockThreads.mockResolvedValue({ threads: [], total: 0 });
    renderCard();

    await waitFor(() => expect(screen.getAllByTestId('chief-of-staff-starter')).toHaveLength(3));
    expect(screen.queryByTestId('chief-of-staff-threads')).not.toBeInTheDocument();
  });
});
