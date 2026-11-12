/**
 * Check now in Claude Code's view: in a card's header row while one of its
 * tokens can be checked, once per credential, and, where the table is too
 * narrow for that header row, in each row's More actions menu.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, fireEvent, screen, waitFor, within } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import type { ClaudeCodeComputer, ClaudeCodePlan, ClaudeCodeSetup, ClaudeCodeStatus } from '@/api/model';

const h = vi.hoisted(() => ({ width: 0 }));

vi.mock('@/hooks/useProviderCatalog', () => ({
  useProviderCatalog: () => ({ keyGroups: [], planGroups: [], localEntries: [], placeOf: () => undefined, optionFor: () => null }),
}));
vi.mock('@/hooks/useConfiguredProviders', () => ({
  useConfiguredProviders: () => ({ providers: [], withdrawn: [], configuredSet: new Set(), statusLoading: false, statusError: false }),
  useOAuthStatus: () => ({ data: undefined }),
}));
vi.mock('@/api/model', () => ({
  getClaudeCodeSetup: vi.fn(),
  getClaudeCodeStatus: vi.fn(),
  getClaudeCodeComputers: vi.fn(),
  checkClaudeCodePlan: vi.fn(),
}));
vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api')>()),
  getWorkspace: vi.fn(),
}));
vi.mock('@/config/hostMode', async (original) => ({
  ...(await original<typeof import('@/config/hostMode')>()),
  HOST_MODE: 'oss',
  isPlatformMode: false,
}));

import { checkClaudeCodePlan, getClaudeCodeComputers, getClaudeCodeSetup, getClaudeCodeStatus } from '@/api/model';
import { ModelAccessSection } from '../ModelAccessSection';

const FUTURE = '2099-01-01T00:00:00Z';
const plan = (over: Partial<ClaudeCodePlan> = {}): ClaudeCodePlan => ({
  five_hour: { used: 0.4, resets_at: FUTURE }, seven_day: { used: 0.2, resets_at: FUTURE }, limits: [], credits: null, observed_at: '2026-11-07T15:00:00Z', ...over,
});
const AT_LIMIT = plan({ limits: [{ window: 'five_hour', family: null, until: FUTURE, reset_known: true }] });

const MAC: ClaudeCodeComputer = {
  computer_id: 'cmp-mac', name: 'Studio Mac', kind: 'helper', state: 'signed_in', source: 'machine',
  email: 'user@example.com', expires_at: null, plan: plan(),
};
const BOX: ClaudeCodeComputer = {
  computer_id: 'cmp-box', name: 'Lab box', kind: 'ssh', state: 'signed_in', source: 'machine',
  email: 'user@example.com', expires_at: null, plan: plan(),
};
const NO_TOKEN: ClaudeCodeComputer = { ...BOX, computer_id: 'cmp-new', name: 'New box', state: 'no_token', source: null, email: null, plan: null };
const status = (s: Partial<ClaudeCodeStatus> = {}): ClaudeCodeStatus => ({
  enabled: true, state: 'no_token', email: null, expires_at: null, saved: false, ...s,
});
const SAVED_OK = status({ state: 'signed_in', saved: true, plan: plan() });

function render({ computers = [MAC], account = status(), setupComputers = computers }: {
  computers?: ClaudeCodeComputer[]; account?: ClaudeCodeStatus; setupComputers?: ClaudeCodeComputer[];
} = {}) {
  const setup: ClaudeCodeSetup = { state: 'connected', on: true, computers: setupComputers, saved_token_runs: true, cloud: false, too_small: [] };
  (getClaudeCodeSetup as Mock).mockResolvedValue(setup);
  (getClaudeCodeStatus as Mock).mockResolvedValue(account);
  (getClaudeCodeComputers as Mock).mockResolvedValue({ computers });
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, retryDelay: 0 } } });
  queryClient.setQueryData(queryKeys.features.list(), [{ key: 'claude_code_runtime', enabled: true, gate: 'opt_in' }]);
  return renderWithProviders(<ModelAccessSection />, { queryClient });
}

const view = async () => {
  const open = screen.queryByRole('dialog', { name: 'Claude Code', hidden: true });
  if (open) return open;
  fireEvent.click(await screen.findByRole('button', { name: 'Manage' }));
  return screen.findByRole('dialog', { name: 'Claude Code' });
};
/** A card's header row, by what its first column holds. */
const headerOf = async (first: 'Computer' | 'Token') => (await within(await view()).findByText(first, { selector: 'span' })).closest('li') as HTMLElement;
const rowOf = async (name: string) => (await within(await view()).findByText(name)).closest('li') as HTMLElement;
const checkNow = (scope: HTMLElement) => within(scope).queryByRole('button', { name: 'Check now' });
/** A row's More actions menu, opened. */
function openMore(row: HTMLElement) {
  fireEvent.keyDown(within(row).getByRole('button', { name: 'More actions' }), { key: 'Enter' });
  return within(screen.getByRole('menu'));
}

describe('Claude Code view: Check now', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    h.width = 0;
    (checkClaudeCodePlan as Mock).mockResolvedValue(undefined);
    vi.spyOn(Element.prototype, 'getBoundingClientRect').mockImplementation(() => ({
      width: h.width, height: 0, top: 0, left: 0, right: h.width, bottom: 0, x: 0, y: 0, toJSON: () => ({}),
    }));
  });
  afterEach(() => vi.restoreAllMocks());

  describe('in a card\'s header row', () => {
    it('shows for a computer whose token works', async () => {
      render({ computers: [MAC, NO_TOKEN] });
      expect(checkNow(await headerOf('Computer'))).toBeInTheDocument();
    });

    it('shows for a computer whose token is at its plan limit', async () => {
      render({ computers: [{ ...MAC, plan: AT_LIMIT }] });
      const row = await rowOf('Studio Mac');
      expect(within(row).getByText('Limit reached')).toBeInTheDocument();
      expect(checkNow(await headerOf('Computer'))).toBeInTheDocument();
    });

    // Under Stop with usage credits not known to be off, a check would be
    // billed, and the limit line already says when the plan resets.
    it.each([
      ['a computer', 'Computer' as const, { computers: [{ ...MAC, plan: AT_LIMIT, check_bills: true }] }, 'Studio Mac'],
      ['the saved token', 'Token' as const, { computers: [], account: { ...SAVED_OK, plan: AT_LIMIT, check_bills: true } }, 'Your setup token'],
    ])('is left out for %s whose check would be billed to usage credits', async (_, first, shown, name) => {
      render(shown);
      const row = await rowOf(name);
      expect(within(row).getByText('Limit reached')).toBeInTheDocument();
      expect(checkNow(await headerOf(first))).toBeNull();
    });

    it('shows for the saved token while it works', async () => {
      render({ computers: [], account: SAVED_OK });
      expect(checkNow(await headerOf('Token'))).toBeInTheDocument();
    });

    it.each([
      ['refused', { ...MAC, state: 'rejected' as const }],
      ['not added yet', NO_TOKEN],
      ['being checked', { ...MAC, state: 'checking' as const }],
      ['on a computer that is offline', { ...MAC, state: 'offline' as const }],
    ])('is left out where a computer\'s token is %s', async (_, computer) => {
      render({ computers: [computer] });
      const header = await headerOf('Computer');
      await rowOf(computer.name);
      expect(checkNow(header)).toBeNull();
    });

    it('is left out for a computer the setup read found out of reach, whatever its token line says', async () => {
      render({ computers: [MAC], setupComputers: [{ ...MAC, state: 'offline' }] });
      const row = await rowOf('Studio Mac');
      await within(row).findByText('Offline');
      expect(checkNow(await headerOf('Computer'))).toBeNull();
    });

    it('is left out for a saved token Claude refused', async () => {
      render({ computers: [], account: status({ state: 'rejected', saved: true }) });
      const header = await headerOf('Token');
      await rowOf('Your setup token');
      expect(checkNow(header)).toBeNull();
    });
  });

  describe('which credentials it checks', () => {
    it('checks the saved token once for the computers that run on it, whose plan is that token\'s', async () => {
      let finish: () => void = () => {};
      (checkClaudeCodePlan as Mock).mockReturnValue(new Promise<void>((resolve) => { finish = resolve; }));
      render({
        computers: [{ ...MAC, source: 'saved' }, { ...BOX, source: 'saved' }],
        account: SAVED_OK,
      });
      const computers = await headerOf('Computer');
      const saved = await headerOf('Token');
      fireEvent.click(checkNow(computers) as HTMLElement);
      await waitFor(() => expect(checkClaudeCodePlan).toHaveBeenCalledTimes(1));
      expect(checkClaudeCodePlan).toHaveBeenCalledWith({ kind: 'saved' });
      // The saved token's own header is the same credential, so it reads the same.
      await waitFor(() => expect(within(saved).getByRole('button', { name: 'Checking…' })).toBeInTheDocument());
      expect(within(computers).getByRole('button', { name: 'Checking…' })).toBeInTheDocument();
      await act(async () => finish());
    });

    it('checks each computer that has a token of its own', async () => {
      render({ computers: [MAC, BOX] });
      fireEvent.click(checkNow(await headerOf('Computer')) as HTMLElement);
      await waitFor(() => expect(checkClaudeCodePlan).toHaveBeenCalledTimes(2));
      expect((checkClaudeCodePlan as Mock).mock.calls.map(([credential]) => credential)).toEqual([
        { kind: 'computer', computerId: 'cmp-mac' }, { kind: 'computer', computerId: 'cmp-box' },
      ]);
    });
  });

  describe('a card with no use to show', () => {
    it('names no window, and its rows take the window columns for their text, while a card with use keeps them', async () => {
      h.width = 800;
      render({ computers: [{ ...MAC, state: 'offline' }], account: SAVED_OK });
      const row = await rowOf('Studio Mac');
      await within(row).findByText('Offline');
      const computers = await headerOf('Computer');
      expect(within(computers).queryByText('5h')).toBeNull();
      expect(within(computers).getByText('Computer')).toHaveClass('col-span-4');
      expect(row.querySelector('[data-usage-cell]')).toBeNull();
      expect(row.querySelector('[data-row-detail]')?.parentElement).toHaveClass('col-span-4');
      const token = await headerOf('Token');
      expect(within(token).getByText('5h')).toBeInTheDocument();
      expect((await rowOf('Your setup token')).querySelectorAll('[data-usage-cell]')).toHaveLength(3);
    });
  });

  describe('where the table is too narrow for its header row', () => {
    it('keeps the header row at a width with every column, and leaves Check now out of the menus', async () => {
      h.width = 800;
      render({ computers: [MAC], account: SAVED_OK });
      const header = await headerOf('Computer');
      expect(header).toHaveClass('grid');
      expect(header).not.toHaveClass('hidden');
      const menu = openMore(await rowOf('Studio Mac'));
      expect(menu.getByRole('menuitem', { name: 'Remove' })).toBeInTheDocument();
      expect(menu.queryByRole('menuitem', { name: 'Check now' })).toBeNull();
    });

    it('drops the header row and offers Check now in the More actions menu of a row whose token can be checked', async () => {
      h.width = 600;
      render({ computers: [MAC], account: SAVED_OK });
      expect(await headerOf('Computer')).toHaveClass('hidden');
      expect(await headerOf('Token')).toHaveClass('hidden');
      expect(openMore(await rowOf('Studio Mac')).getByRole('menuitem', { name: 'Check now' })).toBeInTheDocument();
    });

    it('offers it in no menu of a row whose token cannot be checked', async () => {
      h.width = 600;
      render({ computers: [{ ...BOX, state: 'rejected' }] });
      const menu = openMore(await rowOf('Lab box'));
      expect(menu.getByRole('menuitem', { name: 'Remove' })).toBeInTheDocument();
      expect(menu.queryByRole('menuitem', { name: 'Check now' })).toBeNull();
    });

    it('checks that row\'s credential from its menu item', async () => {
      h.width = 600;
      render({ computers: [MAC], account: SAVED_OK });
      fireEvent.click(openMore(await rowOf('Studio Mac')).getByRole('menuitem', { name: 'Check now' }));
      await waitFor(() => expect(checkClaudeCodePlan).toHaveBeenCalledWith({ kind: 'computer', computerId: 'cmp-mac' }));
      expect(checkClaudeCodePlan).toHaveBeenCalledTimes(1);
    });

    it('offers it for the saved token\'s row too', async () => {
      h.width = 600;
      render({ computers: [], account: SAVED_OK });
      fireEvent.click(openMore(await rowOf('Your setup token')).getByRole('menuitem', { name: 'Check now' }));
      await waitFor(() => expect(checkClaudeCodePlan).toHaveBeenCalledWith({ kind: 'saved' }));
    });
  });
});
