/**
 * Opting into a level that skips approval records consent to one version of
 * the agreement, and that has to be the version whose text the dialog showed.
 * The text ships in this bundle, so the version sent is the bundle's constant;
 * when the server asks for another one, a stale tab offers a reload instead of
 * Agree rather than accepting text it never displayed.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, screen, waitFor, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import { TRADING_AGREEMENT_VERSION } from '@/lib/tradingPermission';
import type { TradingPermission } from '@/types/api';

vi.mock('@/api/tradingPermission', () => ({
  getTradingPermission: vi.fn(),
  updateTradingPermission: vi.fn(),
}));

vi.mock('@/components/ui/use-toast', () => ({
  useToast: () => ({ toast: vi.fn() }),
}));

import { getTradingPermission, updateTradingPermission } from '@/api/tradingPermission';
import { TradingPermissionSection } from '../panels/TradingPermissionSection';

const mockGet = vi.mocked(getTradingPermission);
const mockUpdate = vi.mocked(updateTradingPermission);

const PLAN_FIRST = 'Trade without approval, plan first';
const CHECKBOX = 'I understand that orders the agent places are my responsibility.';
const AGREE = 'Agree and turn on';
const CHANGED = 'This agreement has changed since the page loaded. Reload to read the current version.';

const row = (over: Partial<TradingPermission> = {}): TradingPermission => ({
  level: 'approve_each',
  agreement_version: TRADING_AGREEMENT_VERSION,
  agreed_at: null,
  updated_at: null,
  ...over,
});

async function openAgreement() {
  renderWithProviders(<TradingPermissionSection />, { route: '/settings?tab=preferences' });
  fireEvent.click(await screen.findByRole('radio', { name: PLAN_FIRST }));
  return screen.findByRole('dialog');
}

describe('TradingPermissionSection agreement', () => {
  beforeEach(() => {
    mockGet.mockReset();
    mockUpdate.mockReset();
  });

  it('sends the version of the text this bundle shows', async () => {
    mockGet.mockResolvedValue(row());
    mockUpdate.mockResolvedValue(row({ level: 'plan_first', agreed_at: '2026-10-06T00:00:00Z' }));
    const dialog = await openAgreement();

    fireEvent.click(within(dialog).getByRole('checkbox', { name: CHECKBOX }));
    fireEvent.click(within(dialog).getByRole('button', { name: AGREE }));

    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
    expect(mockUpdate).toHaveBeenCalledTimes(1);
    expect(mockUpdate).toHaveBeenCalledWith({
      level: 'plan_first',
      agreement_version: TRADING_AGREEMENT_VERSION,
    });
    expect(screen.getByRole('radio', { name: PLAN_FIRST })).toBeChecked();
  });

  // The stale tab: the server moved to new copy after this bundle loaded. The
  // refused click must not be followed by one that accepts the new version.
  it('turns into a reload prompt when a refusal reveals a newer agreement', async () => {
    mockGet
      .mockResolvedValueOnce(row())
      .mockResolvedValue(row({ agreement_version: TRADING_AGREEMENT_VERSION + 1 }));
    mockUpdate.mockRejectedValue(new Error('Request failed with status code 422'));
    const dialog = await openAgreement();

    fireEvent.click(within(dialog).getByRole('checkbox', { name: CHECKBOX }));
    fireEvent.click(within(dialog).getByRole('button', { name: AGREE }));

    expect(await within(dialog).findByRole('alert')).toHaveTextContent(CHANGED);
    expect(within(dialog).getByRole('button', { name: 'Reload' })).toBeEnabled();
    expect(within(dialog).queryByRole('button', { name: AGREE })).toBeNull();
    expect(within(dialog).queryByRole('checkbox')).toBeNull();
    expect(within(dialog).getByRole('button', { name: 'Cancel' })).toBeEnabled();
    expect(mockUpdate).toHaveBeenCalledTimes(1);
    expect(mockUpdate).toHaveBeenCalledWith({
      level: 'plan_first',
      agreement_version: TRADING_AGREEMENT_VERSION,
    });
  });

  it('offers a reload, not Agree, when the server already asks for another version', async () => {
    mockGet.mockResolvedValue(row({ agreement_version: TRADING_AGREEMENT_VERSION + 1 }));
    const dialog = await openAgreement();

    expect(within(dialog).getByRole('alert')).toHaveTextContent(CHANGED);
    expect(within(dialog).getByRole('button', { name: 'Reload' })).toBeEnabled();
    expect(within(dialog).queryByRole('button', { name: AGREE })).toBeNull();
    expect(within(dialog).queryByRole('checkbox')).toBeNull();

    fireEvent.click(within(dialog).getByRole('button', { name: 'Cancel' }));
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
    expect(mockUpdate).not.toHaveBeenCalled();
    expect(screen.getByRole('radio', { name: 'Approve each order' })).toBeChecked();
  });

  // A failure that is not a version move keeps the question exactly as it was,
  // so the retry is one click.
  it('stays open with the tick kept when a save fails for another reason', async () => {
    mockGet.mockResolvedValue(row());
    mockUpdate.mockRejectedValue(new Error('Request failed with status code 500'));
    const dialog = await openAgreement();

    fireEvent.click(within(dialog).getByRole('checkbox', { name: CHECKBOX }));
    fireEvent.click(within(dialog).getByRole('button', { name: AGREE }));

    await waitFor(() => expect(mockGet).toHaveBeenCalledTimes(2));
    await waitFor(() =>
      expect(within(dialog).getByRole('button', { name: AGREE })).toBeEnabled(),
    );
    expect(within(dialog).getByRole('checkbox', { name: CHECKBOX })).toBeChecked();
    expect(within(dialog).queryByRole('alert')).toBeNull();
    expect(screen.getByRole('radio', { name: 'Approve each order' })).toBeChecked();
  });

  // The save may have landed with its answer lost, so the level still drawn
  // can be out of date until a read succeeds.
  it('says the level could not be read when the reread after a save fails', async () => {
    mockGet
      .mockResolvedValueOnce(row())
      .mockRejectedValue(new Error('Network Error'));
    mockUpdate.mockRejectedValue(new Error('Network Error'));
    const dialog = await openAgreement();

    fireEvent.click(within(dialog).getByRole('checkbox', { name: CHECKBOX }));
    fireEvent.click(within(dialog).getByRole('button', { name: AGREE }));

    expect(await screen.findByText("Couldn't load your trading permission.")).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Retry' })).toBeInTheDocument();
  });
});
