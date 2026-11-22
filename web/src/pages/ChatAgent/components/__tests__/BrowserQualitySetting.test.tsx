import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import type { Computer } from '@/types/api';

let cpu = 1;
vi.mock('../../hooks/useComputers', () => ({
  useComputerProviders: () => ({
    data: { providers: [{ kind: 'docker', tiers: [{ tier: 'standard', cpu, memory_gib: 2 }] }] },
  }),
}));

let pending = false;
const mutate = vi.fn();
vi.mock('../../hooks/useSetBrowserChoices', () => ({
  useSetBrowserChoices: () => ({ mutate, busyFor: (id: string) => pending && id === 'c-1' }),
}));

import { BrowserQualitySetting } from '../BrowserQualitySetting';

const computer = (quality?: string): Computer =>
  ({
    computer_id: 'c-1',
    kind: 'docker',
    status: 'running',
    resource_tier: 'standard',
    browser: { quality: quality ?? null, signins: true, window: false, window_available: false },
  }) as unknown as Computer;

/** A machine of the user's own, which has no sizes and reports its cores. */
const ownMachine = (cores: number | null, quality?: string): Computer =>
  ({ ...computer(quality), kind: 'ssh', ssh: { cores } }) as unknown as Computer;

function show(quality?: string, target = computer(quality)) {
  renderWithProviders(<BrowserQualitySetting computer={target} />);
  return mutate;
}

describe('BrowserQualitySetting', () => {
  beforeEach(() => {
    cpu = 1;
    pending = false;
    mutate.mockReset();
  });

  it('follows the computer size until someone chooses, and says what that is', () => {
    cpu = 4;
    show();
    expect(screen.getByRole('button', { name: 'Auto' })).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByText(/Auto uses Retina on this computer/)).toBeInTheDocument();
  });

  it('saves a choice, and going back to Auto clears it', () => {
    const save = show('sharp');
    expect(screen.getByRole('button', { name: 'Sharp' })).toHaveAttribute('aria-pressed', 'true');
    fireEvent.click(screen.getByRole('button', { name: 'Light' }));
    fireEvent.click(screen.getByRole('button', { name: 'Auto' }));
    expect(save.mock.calls).toEqual([
      [{ computerId: 'c-1', patch: { quality: 'light' } }],
      [{ computerId: 'c-1', patch: { quality: null } }],
    ]);
  });

  it('holds Retina below two CPUs, where the computer uses Sharp instead', () => {
    show('retina');
    expect(screen.getByRole('button', { name: 'Retina' })).toBeDisabled();
    expect(screen.getByText(/so this computer uses Sharp/)).toBeInTheDocument();
  });

  it('offers Retina from two CPUs and says what it costs', () => {
    cpu = 2;
    show('retina');
    expect(screen.getByRole('button', { name: 'Retina' })).toBeEnabled();
    expect(screen.getByText(/about a whole CPU/)).toBeInTheDocument();
  });

  it("reads a machine of the user's own by its cores, where Auto is Standard", () => {
    show('retina', ownMachine(1, 'retina'));
    expect(screen.getByRole('button', { name: 'Retina' })).toBeDisabled();
    expect(screen.getByText(/so this computer uses Sharp/)).toBeInTheDocument();
  });

  it("says Auto is Standard on a machine of the user's own, however many cores", () => {
    show(undefined, ownMachine(16));
    expect(screen.getByText(/Auto uses Standard on this computer/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Retina' })).toBeEnabled();
  });

  it('disables the control while a change for this computer is in flight', () => {
    pending = true;
    show('sharp');
    expect(screen.getByRole('button', { name: 'Light' })).toBeDisabled();
  });
});
