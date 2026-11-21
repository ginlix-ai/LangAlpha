import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import type { Computer } from '@/types/api';

let pending = false;
const mutate = vi.fn();
vi.mock('../../hooks/useComputerActions', () => ({
  useSetBrowserSignins: () => ({ mutate, isPending: pending, variables: pending ? { computerId: 'c-1' } : undefined }),
}));

import { BrowserSigninsSetting } from '../BrowserSigninsSetting';

function show(kept?: boolean) {
  const computer = {
    computer_id: 'c-1',
    kind: 'docker',
    status: 'running',
    config: kept === undefined ? {} : { browser_signins: kept },
  } as unknown as Computer;
  renderWithProviders(<BrowserSigninsSetting computer={computer} />);
  return mutate;
}

describe('BrowserSigninsSetting', () => {
  beforeEach(() => {
    pending = false;
    mutate.mockReset();
  });

  it('keeps sign-ins until someone turns it off', () => {
    show();
    expect(screen.getByRole('switch', { name: 'Keep sign-ins' })).toHaveAttribute('aria-checked', 'true');
    expect(screen.getByRole('button', { name: 'Sign out of every site' })).toBeInTheDocument();
  });

  it('offers no sign-out once off, and turns it back on', () => {
    const save = show(false);
    const toggle = screen.getByRole('switch', { name: 'Keep sign-ins' });
    expect(toggle).toHaveAttribute('aria-checked', 'false');
    expect(screen.queryByRole('button', { name: 'Sign out of every site' })).toBeNull();
    fireEvent.click(toggle);
    expect(save).toHaveBeenCalledWith({ computerId: 'c-1', keep: true });
  });

  it('signs out by saving it on again, and off by turning it off', () => {
    const save = show(true);
    fireEvent.click(screen.getByRole('button', { name: 'Sign out of every site' }));
    fireEvent.click(screen.getByRole('switch', { name: 'Keep sign-ins' }));
    expect(save.mock.calls).toEqual([
      [{ computerId: 'c-1', keep: true }],
      [{ computerId: 'c-1', keep: false }],
    ]);
  });

  it('takes nothing more while a change is on its way', () => {
    pending = true;
    show(true);
    expect(screen.getByRole('switch', { name: 'Keep sign-ins' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Sign out of every site' })).toBeDisabled();
  });
});
