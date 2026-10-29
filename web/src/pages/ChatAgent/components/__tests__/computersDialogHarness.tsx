/**
 * What the Computers dialog suites share: the live list they render, and the
 * steps a reader takes through the dialog. Each suite mocks the API itself
 * (computersDialogApi.ts) before importing this.
 */
import { expect } from 'vitest';
import { screen, waitFor, within } from '@testing-library/react';
import type userEvent from '@testing-library/user-event';

import { renderWithProviders } from '@/test/utils';
import type { ComputerProviderOption } from '@/types/api';

import type { ComputersPanelView } from '../../hooks/computerPanelStore';
import ComputersDialog from '../ComputersDialog';

export type User = ReturnType<typeof userEvent.setup>;

export const PRIMARY_ID = '5319ad0e-7835-4ca6-9541-b7c2961a7bf6';
export const SECOND_ID = '3b788b47-352e-4cfb-94f0-920b599ea227';
export const NEW_ID = 'b7d1f0c2-9a4e-4f3b-8c61-2e5d7a9f0b13';

// Verbatim from curl against :8060. `workspace_count` is the server's own
// count of the folders on each machine, which is the whole point: the dialog is
// handed no workspace list to count for itself.
export const LIVE_LIST = {
  computers: [
    {
      computer_id: PRIMARY_ID,
      user_id: 'wp13-final-1789525377',
      kind: 'daytona',
      name: 'Alpha Research',
      status: 'running',
      resource_tier: 'standard',
      is_always_on: false,
      is_primary: true,
      workspace_count: 3,
      root_dir: '/home/workspace',
      provider_ref: '91885905-79ed-48dc-81ac-0a3d2d608458',
      config: {},
    },
    {
      computer_id: SECOND_ID,
      user_id: 'wp13-final-1789525377',
      kind: 'daytona',
      name: 'Second machine',
      status: 'stopped',
      resource_tier: 'performance',
      is_always_on: false,
      is_primary: false,
      workspace_count: 0,
      root_dir: '/home/workspace',
      provider_ref: null,
      config: {},
    },
  ],
  total: 2,
};

// The row `POST /api/v1/computers` answers with: its sandbox is being built.
export const CREATED = {
  computer_id: NEW_ID,
  user_id: 'wp13-final-1789525377',
  kind: 'e2b',
  name: 'Research box',
  status: 'starting',
  resource_tier: 'performance',
  is_always_on: false,
  is_primary: false,
  workspace_count: 0,
  root_dir: '/home/workspace',
  provider_ref: null,
  config: {},
};

export const DAYTONA_ONLY: ComputerProviderOption[] = [{ kind: 'daytona', is_default: true, allowed: null }];
export const DAYTONA_AND_E2B: ComputerProviderOption[] = [
  { kind: 'daytona', is_default: true, allowed: null },
  { kind: 'e2b', is_default: false, allowed: true },
];

/** The list with the second machine changed, as the server would report it. */
export function withSecond(patch: Record<string, unknown>) {
  return {
    ...LIVE_LIST,
    computers: LIVE_LIST.computers.map((computer) =>
      computer.computer_id === SECOND_ID ? { ...computer, ...patch } : computer),
  };
}

/** The dialog, opened onto `panel` as the host would open it. */
export function openDialog(panel: ComputersPanelView = { view: 'list' }) {
  return renderWithProviders(<ComputersDialog panel={panel} onClose={() => {}} />);
}

/**
 * Open the named computer's details: bring its card to the middle of the rail,
 * where it takes input, and press its Details. Resolves to the way back.
 */
export async function openDetails(user: User, name: string) {
  const card = await screen.findByRole('article', { name });
  if (!card.classList.contains('is-active')) {
    await user.click(card);
    await waitFor(() => expect(card).toHaveClass('is-active'));
  }
  await user.click(within(card).getByRole('button', { name: 'Details' }));
  return screen.findByRole('button', { name: 'Computers' });
}

export async function backToRail(user: User) {
  await user.click(await screen.findByRole('button', { name: 'Computers' }));
  await screen.findAllByTestId('computer-card');
}

// Found by its title: covered, it is hidden from the tree, and a role query
// cannot name a dialog whose label sits inside the hidden part.
export const computersDialog = () =>
  screen.getByText('Computers', { selector: 'h3' }).closest<HTMLElement>('[role="dialog"]') as HTMLElement;
// The shell's backdrop is the dialog's parent; it carries the dimming.
export const backdropOf = (dialog: HTMLElement) => dialog.parentElement as HTMLElement;
