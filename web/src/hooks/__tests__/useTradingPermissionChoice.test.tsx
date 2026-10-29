/**
 * Each save the trading choice starts is handed to `onPreparing` as it starts,
 * for a caller that must not go on while the server still holds the level
 * before it: the welcome flow's Start. A level that asks saves on the click;
 * one that skips approval saves only once its agreement is accepted, so the
 * click that opens the agreement hands nothing on. What is handed on settles
 * once the save has, and resolves even when the save fails: the toast says the
 * failure, and the caller goes on with the level the server kept.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, fireEvent, screen, waitFor, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import { TRADING_AGREEMENT_VERSION, TRADING_LEVELS } from '@/lib/tradingPermission';
import type { TradingPermission, TradingPermissionUpdate } from '@/types/api';

const { toast } = vi.hoisted(() => ({ toast: vi.fn() }));

vi.mock('@/api/tradingPermission', () => ({
  getTradingPermission: vi.fn(),
  updateTradingPermission: vi.fn(),
}));
vi.mock('@/components/ui/use-toast', () => ({
  useToast: () => ({ toast }),
}));

import { getTradingPermission, updateTradingPermission } from '@/api/tradingPermission';
import { useTradingPermissionChoice } from '../useTradingPermissionChoice';

const mockGet = vi.mocked(getTradingPermission);
const mockUpdate = vi.mocked(updateTradingPermission);

const CHECKBOX = 'I understand that orders the agent places are my responsibility.';
const AGREE = 'Agree and turn on';
const SAVE_FAILED = "Couldn't change your trading permission";

const row = (over: Partial<TradingPermission> = {}): TradingPermission => ({
  level: 'autonomous',
  agreement_version: TRADING_AGREEMENT_VERSION,
  agreed_at: '2026-10-06T00:00:00Z',
  updated_at: null,
  ...over,
});

/** The choice drawn as a button per level, with the level shown as chosen
 *  and the agreement beside them. */
function Levels({ onPreparing }: { onPreparing: (work: Promise<void>) => void }) {
  const { permission, shown, select, dialog } = useTradingPermissionChoice({ onPreparing });
  if (permission.data === undefined) return null;
  return (
    <>
      <p data-testid="shown">{shown}</p>
      {TRADING_LEVELS.map((level) => (
        <button key={level} type="button" onClick={() => select(level)}>
          {level}
        </button>
      ))}
      {dialog}
    </>
  );
}

/** Whether `work` has settled by the time everything queued so far has run. */
async function settled(work: Promise<void>) {
  let done = false;
  void work.then(
    () => (done = true),
    () => (done = true),
  );
  await act(() => new Promise((resolve) => setTimeout(resolve, 0)));
  return done;
}

describe('useTradingPermissionChoice onPreparing', () => {
  let land: () => void;
  let refuse: (err: Error) => void;
  const onPreparing = vi.fn<(work: Promise<void>) => void>();

  beforeEach(() => {
    mockGet.mockReset();
    mockUpdate.mockReset();
    toast.mockReset();
    onPreparing.mockReset();
    // Each save is held until the test answers it.
    mockUpdate.mockImplementation(
      (body: TradingPermissionUpdate) =>
        new Promise((resolve, reject) => {
          land = () => resolve(row({ level: body.level }));
          refuse = reject;
        }),
    );
  });

  /** The work handed on by the one save started. */
  function handed() {
    expect(onPreparing).toHaveBeenCalledTimes(1);
    return onPreparing.mock.calls[0][0];
  }

  describe('a level that asks, saved on the click', () => {
    beforeEach(() => mockGet.mockResolvedValue(row()));

    async function lower() {
      renderWithProviders(<Levels onPreparing={onPreparing} />);
      fireEvent.click(await screen.findByRole('button', { name: 'approve_each' }));
      await waitFor(() => expect(mockUpdate).toHaveBeenCalledWith({ level: 'approve_each' }));
      const work = handed();
      expect(await settled(work)).toBe(false);
      // Wrapped, or the async return would wait for it.
      return { work };
    }

    it('is handed on as it starts, and settles once the save has', async () => {
      const { work } = await lower();
      await act(async () => land());
      await expect(work).resolves.toBeUndefined();
      expect(screen.getByTestId('shown')).toHaveTextContent('approve_each');
      expect(toast).not.toHaveBeenCalled();
    });

    it('resolves when the save fails, with the toast shown and the server level back', async () => {
      const { work } = await lower();
      await act(async () => refuse(new Error('Request failed with status code 500')));
      await expect(work).resolves.toBeUndefined();
      expect(toast).toHaveBeenCalledTimes(1);
      expect(toast.mock.calls[0][0]).toMatchObject({ variant: 'destructive', description: SAVE_FAILED });
      await waitFor(() => expect(screen.getByTestId('shown')).toHaveTextContent('autonomous'));
    });
  });

  describe('a level that skips approval, saved once agreed to', () => {
    beforeEach(() => mockGet.mockResolvedValue(row({ level: 'approve_each', agreed_at: null })));

    async function agree() {
      renderWithProviders(<Levels onPreparing={onPreparing} />);
      fireEvent.click(await screen.findByRole('button', { name: 'plan_first' }));
      const dialog = await screen.findByRole('dialog');
      expect(onPreparing).not.toHaveBeenCalled();
      fireEvent.click(within(dialog).getByRole('checkbox', { name: CHECKBOX }));
      fireEvent.click(within(dialog).getByRole('button', { name: AGREE }));
      await waitFor(() =>
        expect(mockUpdate).toHaveBeenCalledWith({ level: 'plan_first', agreement_version: TRADING_AGREEMENT_VERSION }),
      );
      const work = handed();
      expect(await settled(work)).toBe(false);
      return { dialog, work };
    }

    it('is handed on when the agreement is accepted, and settles once the save has', async () => {
      const { work } = await agree();
      await act(async () => land());
      await expect(work).resolves.toBeUndefined();
      await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
      expect(screen.getByTestId('shown')).toHaveTextContent('plan_first');
      expect(toast).not.toHaveBeenCalled();
    });

    it('resolves when the save fails, with the toast shown and the agreement kept open', async () => {
      const { dialog, work } = await agree();
      await act(async () => refuse(new Error('Request failed with status code 500')));
      await expect(work).resolves.toBeUndefined();
      expect(toast).toHaveBeenCalledTimes(1);
      expect(toast.mock.calls[0][0]).toMatchObject({ variant: 'destructive', description: SAVE_FAILED });
      await waitFor(() => expect(within(dialog).getByRole('button', { name: AGREE })).toBeEnabled());
      expect(screen.getByTestId('shown')).toHaveTextContent('approve_each');
    });
  });
});
