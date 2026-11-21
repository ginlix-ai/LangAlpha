import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import type { Computer } from '@/types/api';

let pending = false;
const mutate = vi.fn();
vi.mock('../../hooks/useSetBrowserChoices', () => ({
  useSetBrowserChoices: () => ({ mutate, busyFor: (id: string) => pending && id === 'c-1' }),
}));

import { BrowserSigninsSetting } from '../BrowserSigninsSetting';

function show(kept?: boolean) {
  const computer = {
    computer_id: 'c-1',
    kind: 'docker',
    status: 'running',
    browser: { quality: null, signins: kept ?? true, window: false, window_available: false },
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
    expect(save).toHaveBeenCalledWith({ computerId: 'c-1', patch: { signins: true } });
  });

  it('signs out by saving it on again, and off by turning it off', () => {
    const save = show(true);
    fireEvent.click(screen.getByRole('button', { name: 'Sign out of every site' }));
    fireEvent.click(screen.getByRole('switch', { name: 'Keep sign-ins' }));
    expect(save.mock.calls).toEqual([
      [{ computerId: 'c-1', patch: { signins: true } }],
      [{ computerId: 'c-1', patch: { signins: false } }],
    ]);
  });

  it('takes nothing more while a change is on its way', () => {
    pending = true;
    show(true);
    expect(screen.getByRole('switch', { name: 'Keep sign-ins' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Sign out of every site' })).toBeDisabled();
  });
});
