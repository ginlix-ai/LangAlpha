/**
 * The SSH page adds a computer with Home placed as the answers say, so
 * nothing can be added before they are read.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { useState } from 'react';

const h = vi.hoisted(() => ({ loading: true, runsOn: [] as unknown[], userId: 'u1', mounts: 0 }));
vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: { user_id: h.userId } }) }));
vi.mock('@/pages/ChatAgent/components/sshSetupState', () => ({
  useSshSetup: () => {
    useState(() => ++h.mounts);
    return { create: { host: 'lab' }, ready: false, readyStep: { filesBackup: true }, defaultName: null };
  },
}));
vi.mock('@/pages/ChatAgent/components/sshSetup', () => ({ SshSetup: () => <p>ssh pairing</p> }));
vi.mock('../../answers/addOwnComputer', () => ({
  useAddOwnComputer: (runsOn: unknown) => {
    h.runsOn.push(runsOn);
    return { mutate: vi.fn(), isPending: false, isSuccess: false, error: null };
  },
}));
vi.mock('../../answers/flow', () => ({
  useAnswerFlow: () => ({
    isLoading: h.loading,
    answers: h.loading ? null : { models: 'claude_code', runsOn: 'cloud' },
    finish: vi.fn(),
    finishing: false,
    finishFailed: false,
  }),
}));

import SshStep from '../SshStep';

const renderStep = () => render(<MemoryRouter><SshStep /></MemoryRouter>);

describe('SshStep', () => {
  beforeEach(() => {
    h.loading = true;
    h.runsOn = [];
    h.userId = 'u1';
    h.mounts = 0;
  });

  it('starts the pairing over for another account signing in here', () => {
    h.loading = false;
    const view = render(<MemoryRouter><SshStep /></MemoryRouter>);
    expect(h.mounts).toBe(1);
    h.userId = 'u2';
    view.rerender(<MemoryRouter><SshStep /></MemoryRouter>);
    expect(h.mounts).toBe(2);
  });

  it('offers no pairing while the answers are being read', () => {
    renderStep();
    expect(screen.queryByText('ssh pairing')).toBeNull();
    expect(screen.queryByRole('button')).toBeNull();
  });

  it('offers the pairing once the answers are read, placing Home as they say', () => {
    h.loading = false;
    renderStep();
    expect(screen.getByText('ssh pairing')).toBeInTheDocument();
    expect(h.runsOn.at(-1)).toBe('cloud');
  });
});
