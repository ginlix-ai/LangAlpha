/**
 * Home's notices above the composer: working on the cloud stand-in while
 * Home's machine is away, waiting for it with no cloud computer, and the
 * hand-back once it is back, or its giving up, dismissed through the server.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { act, fireEvent, screen, waitFor } from '@testing-library/react';

import { queryKeys } from '@/lib/queryKeys';
import { renderWithProviders } from '@/test/utils';

import type { HomeHandback, HomePlacement } from '../../../utils/api/workspaces';

const api = vi.hoisted(() => ({
  getHomePlacement: vi.fn(),
  markHomeHandbackSeen: vi.fn(async () => {}),
}));
vi.mock('@/pages/ChatAgent/utils/api/workspaces', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getHomePlacement: api.getHomePlacement,
  markHomeHandbackSeen: api.markHomeHandbackSeen,
}));

import { HomePlacementNotice } from '../HomePlacementNotice';
import { homePlacementPollMs } from '../../../hooks/useHomePlacement';

const STAND_IN = { computer_id: 'c-cloud', name: 'Cloud One', status: 'stopped' };

function placement(over: Partial<HomePlacement> = {}): HomePlacement {
  return {
    computer_id: 'c-mac',
    computer_name: 'Office Mac',
    computer_kind: 'ssh',
    unreachable: false,
    stand_in: STAND_IN,
    handback_owed: false,
    handback: null,
    left_on_stand_in: null,
    move: null,
    ...over,
  };
}

const HANDBACK: HomeHandback = {
  handback_id: 'hb-1',
  folder: 'From the cloud/2026-11-15',
  files: 3,
  left_on_cloud: [],
  unreadable: [],
  failed: false,
  computer_name: 'Office Mac',
  cloud_name: 'Cloud One',
  created_at: '2026-11-15T10:00:00Z',
};

/** Draws the notice once the placement has landed and been drawn. */
async function show(value: HomePlacement) {
  api.getHomePlacement.mockResolvedValue(value);
  const view = renderWithProviders(<HomePlacementNotice />);
  await waitFor(() => expect(view.queryClient.getQueryState(queryKeys.workspaces.homePlacement())?.status).toBe('success'));
  // The query tells its observers on the next macrotask.
  await act(() => new Promise((resolve) => setTimeout(resolve, 0)));
  return view;
}

afterEach(() => {
  vi.clearAllMocks();
});

describe('HomePlacementNotice', () => {
  it('says the Chief of Staff works on the cloud computer while the machine is away', async () => {
    await show(placement({ unreachable: true }));
    expect(screen.getByTestId('home-on-cloud')).toHaveTextContent(
      "Office Mac can't be reached, so the Chief of Staff is working on Cloud One for now. "
        + "What it writes is copied into Home on Office Mac once it's back.",
    );
    expect(screen.queryByTestId('home-waiting')).toBeNull();
  });

  it('says it waits when there is no cloud computer to stand in', async () => {
    await show(placement({ unreachable: true, stand_in: null }));
    expect(screen.getByTestId('home-waiting')).toHaveTextContent(
      "Office Mac can't be reached, so the Chief of Staff is waiting for it. Messages sent now won't run.",
    );
    expect(screen.queryByTestId('home-on-cloud')).toBeNull();
  });

  it('says where the files written on the cloud landed, and what was left there', async () => {
    await show(placement({ handback: { ...HANDBACK, left_on_cloud: ['a.mov', 'b.zip', 'c.bin', 'd.iso'] } }));
    const line = screen.getByTestId('home-handback');
    expect(line).toHaveTextContent('3 files written on Cloud One are now in Home/From the cloud/2026-11-15 on Office Mac.');
    expect(line).toHaveTextContent('Too large to copy, so left on Cloud One: a.mov, b.zip, c.bin, and 1 more.');
  });

  it('words a single file in the singular, and leaves out the left-behind line when nothing was', async () => {
    await show(placement({ handback: { ...HANDBACK, files: 1 } }));
    const line = screen.getByTestId('home-handback');
    expect(line).toHaveTextContent('1 file written on Cloud One is now in Home/From the cloud/2026-11-15 on Office Mac.');
    expect(line).not.toHaveTextContent('Too large');
  });

  it('says when the copy was given up and the files are still on the cloud', async () => {
    await show(placement({ handback: { ...HANDBACK, folder: null, files: 0, failed: true } }));
    const line = screen.getByTestId('home-handback');
    expect(line).toHaveTextContent(
      "The files written on Cloud One while Office Mac was away couldn't be copied into Home. They're still on Cloud One.",
    );
    expect(line).not.toHaveTextContent('now in Home');
  });

  it('names files that could not be read, with no copy line when none arrived', async () => {
    await show(placement({ handback: { ...HANDBACK, folder: null, files: 0, unreadable: ['locked.db'] } }));
    const line = screen.getByTestId('home-handback');
    expect(line).toHaveTextContent("Couldn't be read, so left on Cloud One: locked.db.");
    expect(line).not.toHaveTextContent('now in Home');
  });

  it('dismisses the hand-back at once and tells the server', async () => {
    await show(placement({ handback: HANDBACK }));
    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: 'Dismiss' }));
    });
    expect(api.markHomeHandbackSeen).toHaveBeenCalledWith('hb-1');
    await waitFor(() => expect(screen.queryByTestId('home-handback')).toBeNull());
  });

  it('keeps a dismissed hand-back gone when a read already on its way lands after', async () => {
    const view = await show(placement({ handback: HANDBACK }));
    let land: (value: HomePlacement) => void = () => {};
    api.getHomePlacement.mockImplementation(() => new Promise<HomePlacement>((resolve) => { land = resolve; }));
    void view.queryClient.refetchQueries({ queryKey: queryKeys.workspaces.homePlacement() });
    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: 'Dismiss' }));
    });
    await act(async () => {
      land(placement({ handback: HANDBACK }));
      await new Promise((resolve) => setTimeout(resolve, 0));
    });
    expect(screen.queryByTestId('home-handback')).toBeNull();
  });

  it('shows nothing while Home is reachable and nothing came back', async () => {
    const { container } = await show(placement());
    expect(container).toBeEmptyDOMElement();
  });
});

describe('homePlacementPollMs', () => {
  it('polls a Home on a machine the user brought, and leaves a cloud Home alone', () => {
    expect(homePlacementPollMs(placement())).toBe(15_000);
    expect(homePlacementPollMs(placement({ computer_kind: 'daytona' }))).toBe(false);
    expect(homePlacementPollMs(placement({ computer_kind: 'daytona', handback_owed: true }))).toBe(15_000);
    expect(homePlacementPollMs(undefined)).toBe(false);
  });
});
