/**
 * Taking a grant back answers at once on the switch, and the frame loads
 * again once the popover closes, not under the pointer.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, fireEvent, screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { DashboardAccess } from '../DashboardAccess';

const mocks = vi.hoisted(() => ({
  granted: new Set<string>(),
  settle: (() => {}) as (saved: boolean) => void,
  revoke: vi.fn(),
}));

vi.mock('@/lib/appBridge/useAppScopes', () => ({
  useAppScopes: () => ({ granted: mocks.granted, grant: vi.fn(), revoke: mocks.revoke }),
}));

beforeEach(() => {
  mocks.granted = new Set(['portfolio', 'threads']);
  mocks.revoke = vi.fn(() => new Promise<boolean>((resolve) => { mocks.settle = resolve; }));
});

function open(onRevoked = vi.fn(), onDone = vi.fn()) {
  renderWithProviders(
    <DashboardAccess grantKey="pg1" asked={['quotes', 'portfolio', 'threads']} onRevoked={onRevoked} onDone={onDone} />,
  );
  fireEvent.click(screen.getByRole('button', { name: 'Data access' }));
  return { onRevoked, onDone };
}

describe('DashboardAccess', () => {
  it('lists only what needs leave', () => {
    open();
    expect(screen.getAllByRole('switch')).toHaveLength(2);
  });

  it('turns the switch at once and reloads the frame only when closed', async () => {
    const { onRevoked, onDone } = open();
    const portfolio = screen.getByRole('switch', { name: 'Portfolio' });
    fireEvent.click(portfolio);
    expect(portfolio).toHaveAttribute('aria-checked', 'false');
    // A second press while it saves is not a second change.
    fireEvent.click(portfolio);
    expect(mocks.revoke).toHaveBeenCalledTimes(1);
    await act(async () => mocks.settle(true));
    expect(onRevoked).toHaveBeenCalledTimes(1);
    expect(onDone).not.toHaveBeenCalled();
    fireEvent.keyDown(document.activeElement ?? document.body, { key: 'Escape' });
    expect(onDone).toHaveBeenCalledTimes(1);
  });

  it('puts the switch back when the change was not saved', async () => {
    const { onRevoked, onDone } = open();
    const portfolio = screen.getByRole('switch', { name: 'Portfolio' });
    fireEvent.click(portfolio);
    await act(async () => mocks.settle(false));
    expect(portfolio).toHaveAttribute('aria-checked', 'true');
    fireEvent.keyDown(document.activeElement ?? document.body, { key: 'Escape' });
    expect(onRevoked).not.toHaveBeenCalled();
    expect(onDone).not.toHaveBeenCalled();
  });
});
