/**
 * A turn whose own machine did not answer names that machine and what brings
 * it back: "your Mac" only for a Mac, the desktop app only for one under
 * Remote control, the computer's name for any other.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import React from 'react';
import type { Computer } from '@/types/api';

let computer: Partial<Computer> | null = null;

vi.mock('@/hooks/useWorkspace', () => ({
  useWorkspace: (id: string | null) => ({ data: id ? { workspace_id: id, computer_id: 'comp-1' } : undefined }),
}));
let computers: Record<string, Partial<Computer>> | null = null;

vi.mock('../../hooks/useComputers', () => ({
  useComputer: (id: string | null) => ({
    data: !id ? undefined : computers ? computers[id] ?? null : computer,
  }),
}));
vi.mock('../Markdown', () => ({ default: () => null }));

import { TurnErrorCard } from '../TextMessageContent';
import { WorkspaceProvider } from '../../contexts/WorkspaceContext';

const ssh = (over: Record<string, unknown>) => ({
  host: 'h', port: 22, username: 'u', fingerprint: null, tunnel: true, tunnel_connected: false, helper: false, ...over,
});

function renderCard(code = 'not_connected') {
  return renderWithProviders(
    <WorkspaceProvider workspaceId="ws-1" downloadFile={null}>
      <TurnErrorCard err={{ message: 'work-mac is not connected.', kind: 'computer_unreachable', unreachable: { code } }} />
    </WorkspaceProvider>,
  );
}

describe('TurnErrorCard: a computer that did not answer', () => {
  beforeEach(() => { computer = null; computers = null; });

  it('tells a Mac under Remote control to open the desktop app', () => {
    computer = { kind: 'ssh', name: 'work-mac', ssh: ssh({ helper: true }) as Computer['ssh'] };
    renderCard();
    expect(screen.getByText("Couldn't reach your Mac")).toBeInTheDocument();
    expect(screen.getByText('Wake your Mac and make sure the desktop app is open on it, then try again.')).toBeInTheDocument();
    // The provider's sentence is the server's English, not shown.
    expect(screen.queryByText('work-mac is not connected.')).not.toBeInTheDocument();
  });

  it('tells a Mac reached over SSH to wake and get online', () => {
    computer = { kind: 'ssh', name: 'work-mac', ssh: ssh({ tunnel: false, os: 'Darwin' }) as Computer['ssh'] };
    renderCard('timeout');
    expect(screen.getByText("Couldn't reach your Mac")).toBeInTheDocument();
    expect(screen.getByText("Wake your Mac and check that it's online, then try again.")).toBeInTheDocument();
  });

  it('names any other machine', () => {
    computer = { kind: 'ssh', name: 'lab-box', ssh: ssh({ tunnel: false, os: 'Linux' }) as Computer['ssh'] };
    renderCard('unreachable');
    expect(screen.getByText("Couldn't reach lab-box")).toBeInTheDocument();
    expect(screen.getByText('Check that lab-box is on and online, then try again.')).toBeInTheDocument();
  });

  it('blames the service, not the machine, when its own gateway is down', () => {
    computer = { kind: 'ssh', name: 'work-mac', ssh: ssh({ helper: true }) as Computer['ssh'] };
    renderCard('gateway_unreachable');
    expect(screen.getByText("Couldn't reach the computer")).toBeInTheDocument();
    expect(screen.queryByText(/your Mac/)).not.toBeInTheDocument();
  });

  it('names the computer the failure names, not the workspace\'s', () => {
    // A Chief of Staff's page shows Home's computer; the turn failed on another.
    computers = {
      'comp-1': { kind: 'daytona', name: 'cloud' },
      'comp-mac': { kind: 'ssh', name: 'work-mac', ssh: ssh({ helper: true }) as Computer['ssh'] },
    };
    renderWithProviders(
      <WorkspaceProvider workspaceId="ws-1" downloadFile={null}>
        <TurnErrorCard err={{ message: '', kind: 'computer_unreachable', unreachable: { code: 'not_connected', computerId: 'comp-mac' } }} />
      </WorkspaceProvider>,
    );
    expect(screen.getByText("Couldn't reach your Mac")).toBeInTheDocument();
  });

  it('names a computer the list no longer holds as the failure named it', () => {
    computers = {};
    renderWithProviders(
      <WorkspaceProvider workspaceId="ws-1" downloadFile={null}>
        <TurnErrorCard err={{ message: '', kind: 'computer_unreachable', unreachable: { code: 'timeout', computerId: 'gone', computerName: 'lab-box' } }} />
      </WorkspaceProvider>,
    );
    expect(screen.getByText("Couldn't reach lab-box")).toBeInTheDocument();
  });

  it('names no machine where the view has no workspace', () => {
    renderWithProviders(<TurnErrorCard err={{ message: '', kind: 'computer_unreachable', unreachable: { code: 'not_connected' } }} />);
    expect(screen.getByText("Couldn't reach this workspace's computer")).toBeInTheDocument();
  });
});
