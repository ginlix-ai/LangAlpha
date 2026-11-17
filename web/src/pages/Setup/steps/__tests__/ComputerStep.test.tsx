/**
 * The computer step's ways in: a browser continues on the Mac where the
 * server takes Remote control, the Mac app pairs this Mac where it can (or
 * says to update an app too old to), and SSH stays inside setup.
 */
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router';

const h = vi.hoisted(() => ({
  shell: false,
  remote: false,
  platform: 'darwin',
  offered: false,
  added: undefined as boolean | undefined,
  serverTakes: true,
  own: [] as unknown[],
  loading: false,
  facts: undefined as undefined | object,
  makePrimary: vi.fn(),
  serverFailed: false,
  retryServer: vi.fn(),
}));
vi.mock('@/config/hostMode', () => ({ isPlatformMode: true, HOST_MODE: 'platform', APP_ENTRY_PATH: '/app' }));
vi.mock('@/lib/desktop', () => ({
  isDesktopShell: () => h.shell,
  canRemoteControl: () => h.remote,
  canAddThisMac: () => h.shell && h.remote && h.platform === 'darwin',
  get desktop() { return h.shell ? { platform: h.platform } : undefined; },
  openConsolePage: vi.fn(),
}));
vi.mock('@/pages/ChatAgent/hooks/useRemoteControl', () => ({
  useThisMacAvailability: () => ({ offered: h.offered, added: h.added, settled: true, serverFailed: h.serverFailed, retryServer: h.retryServer }),
  serverTakesRemoteControl: () => h.serverTakes,
}));
vi.mock('@/pages/ChatAgent/hooks/useComputers', () => ({ useComputerProviders: () => ({ data: { providers: [] } }) }));
vi.mock('@/pages/ChatAgent/hooks/useComputerActions', () => ({ useComputerActions: () => ({ makePrimary: { mutate: h.makePrimary, isPending: false, error: null } }) }));
vi.mock('@/pages/ChatAgent/hooks/useHomePlacement', () => ({ useHomePlacement: () => ({ data: undefined }) }));
vi.mock('@/pages/ChatAgent/components/MakePrimaryDialog', () => ({
  default: ({ computer, onConfirm }: { computer: { name: string }; onConfirm: (c: object) => void }) => (
    <button type="button" onClick={() => onConfirm({ turnOnBackup: false, fromBackup: false })}>{`Confirm ${computer.name}`}</button>
  ),
}));
vi.mock('../../answers/flow', () => ({
  useAnswerFlow: () => ({
    answers: { models: 'claude_code', runsOn: 'own_computer' },
    first: true,
    ownComputers: h.own,
    facts: h.facts,
    isLoading: h.loading,
    finish: vi.fn(),
    finishing: false,
    finishFailed: false,
    back: null,
  }),
  useChangeAnswers: () => vi.fn(),
}));

import ComputerStep from '../ComputerStep';

function Where() {
  return <p data-testid="where">{useLocation().pathname}</p>;
}

function renderStep() {
  render(
    <MemoryRouter initialEntries={['/setup/computer']}>
      <Routes>
        <Route path="/setup/computer" element={<ComputerStep />} />
        <Route path="*" element={<Where />} />
      </Routes>
    </MemoryRouter>,
  );
}

describe('ComputerStep', () => {
  beforeEach(() => {
    h.shell = false;
    h.remote = false;
    h.platform = 'darwin';
    h.offered = false;
    h.added = undefined;
    h.serverTakes = true;
    h.own = [];
    h.loading = false;
    h.facts = undefined;
    h.makePrimary = vi.fn();
    h.serverFailed = false;
    h.retryServer = vi.fn();
  });

  it('offers no way in while the computers already connected are being read', () => {
    h.loading = true;
    renderStep();
    expect(screen.queryByTestId('setup-computer-ssh')).toBeNull();
    expect(screen.queryByTestId('setup-computer-mac')).toBeNull();
    expect(screen.queryByRole('button')).toBeNull();
  });

  it('sends a browser on to the Mac', async () => {
    renderStep();
    expect(screen.getByTestId('setup-answers')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Continue on your Mac' }));
    expect(screen.getByTestId('where')).toHaveTextContent('/setup/mac');
  });

  it('offers no Mac in a browser where the server cannot take Remote control', () => {
    h.serverTakes = false;
    renderStep();
    expect(screen.queryByTestId('setup-computer-mac')).toBeNull();
  });

  it('pairs this Mac in the Mac app', async () => {
    h.shell = true;
    h.remote = true;
    h.offered = true;
    h.added = false;
    renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Pair this Mac' }));
    expect(screen.getByTestId('where')).toHaveTextContent('/setup/this-mac');
  });

  it('offers no Mac in an app that cannot pair here (Windows, or a server without Remote control)', () => {
    h.shell = true;
    h.remote = true;
    h.offered = false;
    renderStep();
    expect(screen.queryByTestId('setup-computer-mac')).toBeNull();
    expect(screen.getByRole('button', { name: 'Add SSH computer' })).toBeInTheDocument();
  });

  it('asks a Mac app too old to pair to update, and never sends it to the browser\'s Mac page', () => {
    h.shell = true;
    renderStep();
    expect(screen.getByTestId('setup-computer-mac')).toHaveTextContent('Update the LangAlpha app to pair this Mac.');
    expect(screen.queryByRole('button', { name: 'Continue on your Mac' })).toBeNull();
    expect(screen.queryByRole('button', { name: 'Pair this Mac' })).toBeNull();
  });

  it('offers no Mac in an older app on another system', () => {
    h.shell = true;
    h.platform = 'win32';
    renderStep();
    expect(screen.queryByTestId('setup-computer-mac')).toBeNull();
  });

  it('adds an SSH computer without leaving setup', async () => {
    renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Add SSH computer' }));
    expect(screen.getByTestId('where')).toHaveTextContent('/setup/ssh');
  });

  it('reads again where the server could not say whether this Mac can pair, instead of offering no Mac', async () => {
    h.shell = true;
    h.remote = true;
    h.serverFailed = true;
    renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Try again' }));
    expect(h.retryServer).toHaveBeenCalled();
  });

  it('goes on only once a computer is connected', () => {
    renderStep();
    expect(screen.queryByRole('button', { name: 'Continue' })).toBeNull();
  });

  it('offers to make a connected computer primary where Home is on a cloud computer', async () => {
    h.own = [{ computer_id: 'c1', name: 'lab-server', ssh: { username: 'nadia', host: 'lab.example', helper: false } }];
    h.facts = { homeOnCloud: true };
    renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Make primary' }));
    await userEvent.click(screen.getByRole('button', { name: 'Confirm lab-server' }));
    expect(h.makePrimary).toHaveBeenCalledWith({ computerId: 'c1', name: 'lab-server', fromBackup: false, turnOnBackup: false }, expect.anything());
  });

  it('lists a connected computer and goes on with it', () => {
    h.own = [{ computer_id: 'c1', name: 'lab-server', ssh: { username: 'nadia', host: 'lab.example', helper: false } }];
    renderStep();
    expect(screen.getByText('lab-server')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Continue' })).toBeEnabled();
  });
});
