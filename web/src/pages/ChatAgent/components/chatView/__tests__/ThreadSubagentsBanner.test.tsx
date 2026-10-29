/**
 * The offer writes the thread's side as the default, and goes on its own: an
 * aside to the flip, not a question it asked. The clock stops while the
 * pointer or focus is on it, and a host re-rendering under it, as a streaming
 * turn does with every token, must not restart it.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act, fireEvent, screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { ThreadSubagentsBanner } from '../ThreadSubagentsBanner';
import { OFFER_EXPIRES_MS } from '../ThreadOfferRow';

const mocks = vi.hoisted(() => ({
  mutateAsync: vi.fn(),
  toast: vi.fn(),
}));

vi.mock('@/hooks/useUpdatePreferences', () => ({
  useUpdatePreferences: () => ({ mutateAsync: mocks.mutateAsync, isPending: false }),
}));

vi.mock('@/components/ui/use-toast', () => ({
  toast: mocks.toast,
}));

function banner(allowed: boolean, onExpire: () => void) {
  return <ThreadSubagentsBanner allowed={allowed} onDismiss={vi.fn()} onExpire={onExpire} />;
}

const tick = (ms: number) => act(() => { vi.advanceTimersByTime(ms); });

describe('ThreadSubagentsBanner', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    mocks.mutateAsync.mockReset().mockResolvedValue({});
    mocks.toast.mockReset();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("makes the thread's side the default", () => {
    renderWithProviders(banner(false, vi.fn()));
    fireEvent.click(screen.getByRole('button', { name: 'Turn off by default' }));
    expect(mocks.mutateAsync).toHaveBeenCalledWith({ other_preference: { subagents_default: false } });
  });

  it('says so when the default could not be saved', async () => {
    mocks.mutateAsync.mockRejectedValue(new Error('500'));
    renderWithProviders(banner(true, vi.fn()));
    await act(async () => { fireEvent.click(screen.getByRole('button', { name: 'Turn on by default' })); });
    expect(mocks.toast).toHaveBeenCalledWith(expect.objectContaining({ variant: 'destructive' }));
  });

  it('goes on its own, keeping its clock through the host re-rendering', () => {
    const onExpire = vi.fn();
    const { rerender } = renderWithProviders(banner(false, () => onExpire()));
    tick(OFFER_EXPIRES_MS - 1);
    rerender(banner(false, () => onExpire()));
    tick(1);
    expect(onExpire).toHaveBeenCalledTimes(1);
  });

  it('waits while the pointer is on it, and starts over when it leaves', () => {
    const onExpire = vi.fn();
    renderWithProviders(banner(false, onExpire));
    const row = screen.getByRole('status');

    fireEvent.mouseEnter(row);
    tick(OFFER_EXPIRES_MS * 2);
    expect(onExpire).not.toHaveBeenCalled();

    fireEvent.mouseLeave(row);
    tick(OFFER_EXPIRES_MS - 1);
    expect(onExpire).not.toHaveBeenCalled();
    tick(1);
    expect(onExpire).toHaveBeenCalledTimes(1);
  });

  it('waits while focus is on it, so its action stays in reach', () => {
    const onExpire = vi.fn();
    renderWithProviders(banner(false, onExpire));

    act(() => screen.getByRole('button', { name: 'Turn off by default' }).focus());
    tick(OFFER_EXPIRES_MS * 2);
    expect(onExpire).not.toHaveBeenCalled();

    act(() => (document.activeElement as HTMLElement).blur());
    tick(OFFER_EXPIRES_MS);
    expect(onExpire).toHaveBeenCalledTimes(1);
  });
});
