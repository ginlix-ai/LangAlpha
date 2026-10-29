/**
 * The approval card's "Always allow" button hands its answer, with the current
 * report-back choice, to `useApproveAlways` (whose save-then-answer contract
 * is tested in hooks/__tests__/useApproveAlways.test.tsx), and every action
 * waits while that save is out. A separate file from AnalystHandoff.test.tsx so
 * mocking the hook here can't perturb that file's other cases.
 */
import { beforeEach, describe, it, expect, vi } from 'vitest';
import { screen, fireEvent } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';

vi.mock('../../../utils/api', () => ({
  getHandoffLiveness: vi.fn(),
}));

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
  Trans: ({ i18nKey }: { i18nKey: string }) => i18nKey,
}));

const approveAlways = vi.hoisted(() => ({ saving: false, fn: vi.fn((then: () => void) => then()) }));
vi.mock('@/hooks/useAutoApprove', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/hooks/useAutoApprove')>();
  return {
    AUTO_APPROVE: actual.AUTO_APPROVE,
    useApproveAlways: () => ({ approveAlways: approveAlways.fn, saving: approveAlways.saving }),
  };
});

import { HandoffApproval } from '../HandoffApproval';

// A case that leaves the save out would disable every button in the next one.
beforeEach(() => {
  approveAlways.saving = false;
  approveAlways.fn.mockClear();
});

describe('HandoffApproval: always allow', () => {
  it('answers through useApproveAlways with the report-back choice, and waits while it saves', () => {
    const onApprove = vi.fn();
    const card = () => (
      <HandoffApproval
        proposal={{ status: 'pending', question: 'Compare NVDA vs AMD', report_back: true }}
        onApprove={onApprove}
        onReject={vi.fn()}
      />
    );
    const { rerender } = renderWithProviders(card());

    fireEvent.click(screen.getByRole('button', { name: 'chat.ptcCard.alwaysApprove' }));

    expect(approveAlways.fn).toHaveBeenCalledOnce();
    expect(onApprove).toHaveBeenCalledWith({ report_back: true });

    approveAlways.saving = true;
    rerender(card());

    expect(screen.getByRole('button', { name: 'chat.ptcCard.handOff' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'chat.ptcCard.alwaysApprove' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'chat.ptcCard.decline' })).toBeDisabled();
  });
});
