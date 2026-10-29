/**
 * AutoApproveSettings: the Settings-side switches that turn auto-approval
 * back to asking. A row always flips the opposite of its current `enabled`,
 * and a failed save surfaces a destructive toast instead of silently
 * reverting (the switch itself is uncontrolled by any optimistic state here).
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, fireEvent, waitFor } from '@testing-library/react';
import { render } from '@testing-library/react';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

const mockToast = vi.fn();
vi.mock('@/components/ui/use-toast', () => ({
  useToast: () => ({ toast: mockToast }),
}));

const h = vi.hoisted(() => ({
  handoffsEnabled: false,
  workspaceCreationEnabled: false,
  setHandoffs: vi.fn(),
  setWorkspaceCreation: vi.fn(),
}));

vi.mock('@/hooks/useAutoApprove', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/hooks/useAutoApprove')>();
  return {
    AUTO_APPROVE: actual.AUTO_APPROVE,
    useAutoApprove: (key: string) =>
      key === actual.AUTO_APPROVE.handoffs
        ? { enabled: h.handoffsEnabled, set: h.setHandoffs }
        : { enabled: h.workspaceCreationEnabled, set: h.setWorkspaceCreation },
  };
});

import { AutoApproveSettings } from '../AutoApproveSettings';

describe('AutoApproveSettings', () => {
  beforeEach(() => {
    h.handoffsEnabled = false;
    h.workspaceCreationEnabled = false;
    h.setHandoffs.mockReset().mockResolvedValue(undefined);
    h.setWorkspaceCreation.mockReset().mockResolvedValue(undefined);
    mockToast.mockReset();
  });

  it('turns an off toggle on', () => {
    render(<AutoApproveSettings />);
    const switches = screen.getAllByRole('switch');
    expect(switches[0]).toHaveAttribute('aria-checked', 'false');

    fireEvent.click(switches[0]);

    expect(h.setHandoffs).toHaveBeenCalledWith(true);
  });

  it('turns an on toggle off', () => {
    h.workspaceCreationEnabled = true;
    render(<AutoApproveSettings />);
    const switches = screen.getAllByRole('switch');
    expect(switches[1]).toHaveAttribute('aria-checked', 'true');

    fireEvent.click(switches[1]);

    expect(h.setWorkspaceCreation).toHaveBeenCalledWith(false);
  });

  it('shows a destructive toast when the save fails', async () => {
    h.setHandoffs.mockRejectedValue(new Error('network down'));
    render(<AutoApproveSettings />);

    fireEvent.click(screen.getAllByRole('switch')[0]);

    await waitFor(() =>
      expect(mockToast).toHaveBeenCalledWith(
        expect.objectContaining({ variant: 'destructive' }),
      ),
    );
  });
});
