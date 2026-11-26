/**
 * The desktop app's update panel: what it says before a restart while the
 * count of running work is out, when that count cannot be had, and after an
 * automatic download failed.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, waitFor } from '@testing-library/react';
import { onlineManager } from '@tanstack/react-query';
import userEvent from '@testing-library/user-event';

import { renderWithProviders } from '@/test/utils';
import type { DesktopUpdateState } from '@/lib/desktop';

const shell = vi.hoisted(() => ({ state: null as DesktopUpdateState | null }));

vi.mock('@/lib/desktop', () => ({
  canShowUpdates: () => true,
  canDesktopSettings: () => true,
  getUpdateState: vi.fn(async () => shell.state),
  onUpdateState: vi.fn(() => () => {}),
  installUpdate: vi.fn(async () => {}),
  hideUpdates: vi.fn(),
  setDesktopSettings: vi.fn(async () => null),
}));

vi.mock('@/api/runningWork', () => ({ getRunningWork: vi.fn() }));

import { getRunningWork } from '@/api/runningWork';
import { getUpdateState, hideUpdates } from '@/lib/desktop';
import AppUpdate from '../AppUpdate';

function updateState(patch: Partial<DesktopUpdateState>): DesktopUpdateState {
  return {
    status: 'ready',
    version: '0.4.1',
    currentVersion: '0.4.0',
    channel: 'stable',
    mode: 'auto',
    automatic: true,
    installsOnQuit: true,
    notesUrl: null,
    stopsServer: true,
    ...patch,
  };
}

async function openPanel(state: DesktopUpdateState) {
  shell.state = state;
  renderWithProviders(<AppUpdate shown />);
  const row = await screen.findByRole('button', { name: /Update (ready|available)/ });
  await userEvent.click(row);
}

describe('AppUpdate panel', () => {
  beforeEach(() => {
    vi.mocked(getRunningWork).mockReset();
    vi.mocked(hideUpdates).mockClear();
    vi.mocked(getUpdateState).mockClear();
  });

  it('holds Restart now while it counts what is running', async () => {
    vi.mocked(getRunningWork).mockReturnValue(new Promise(() => {}));
    await openPanel(updateState({}));
    expect(await screen.findByText("Checking what's running…")).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Restart now' })).toBeDisabled();
  });

  it('warns in general and lets the restart go ahead when the count fails', async () => {
    vi.mocked(getRunningWork).mockRejectedValue(new Error('offline'));
    await openPanel(updateState({}));
    expect(await screen.findByText('Anything running on this computer pauses or stops.')).toBeInTheDocument();
    expect(screen.queryByText("Checking what's running…")).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Restart now' })).toBeEnabled();
  });

  it('shows the counts once they are in', async () => {
    vi.mocked(getRunningWork).mockResolvedValue({ turns: 2, background_tasks: 0, local_commands: 0 });
    await openPanel(updateState({}));
    expect(await screen.findByText('2 running turns')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Restart now' })).toBeEnabled();
  });

  it('counts while the computer is offline, since the server it asks is on this computer', async () => {
    onlineManager.setOnline(false);
    try {
      vi.mocked(getRunningWork).mockResolvedValue({ turns: 1, background_tasks: 0, local_commands: 0 });
      await openPanel(updateState({}));
      expect(await screen.findByText('1 running turn')).toBeInTheDocument();
      expect(screen.getByRole('button', { name: 'Restart now' })).toBeEnabled();
    } finally {
      onlineManager.setOnline(true);
    }
  });

  it('asks for nothing where a restart leaves the server running', async () => {
    await openPanel(updateState({ stopsServer: false }));
    expect(screen.getByRole('button', { name: 'Restart now' })).toBeEnabled();
    expect(getRunningWork).not.toHaveBeenCalled();
  });

  it('offers a retry when a download failed with automatic updates on', async () => {
    await openPanel(updateState({ status: 'available', automatic: true }));
    expect(await screen.findByText("The download didn't finish. Download to try again.")).toBeInTheDocument();
    expect(screen.queryByText(/Automatic updates are off/)).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Download' })).toBeEnabled();
  });

  it('reads a shell that does not report automatic updates as on', async () => {
    await openPanel(updateState({ status: 'available', automatic: undefined }));
    expect(await screen.findByText("The download didn't finish. Download to try again.")).toBeInTheDocument();
    expect(screen.queryByText(/Automatic updates are off/)).not.toBeInTheDocument();
  });

  it('says automatic updates are off when they are', async () => {
    await openPanel(updateState({ status: 'available', automatic: false }));
    expect(await screen.findByText(/Automatic updates are off/)).toBeInTheDocument();
  });
  it('hands the update to the shell while the sidebar is hidden, and takes it back once shown', async () => {
    shell.state = updateState({});
    const { rerender } = renderWithProviders(<AppUpdate shown={false} />);
    await waitFor(() => expect(hideUpdates).toHaveBeenCalled());
    // Still drawn, for a peek at the hidden sidebar.
    expect(await screen.findByRole('button', { name: /Update ready/ })).toBeInTheDocument();
    vi.mocked(hideUpdates).mockClear();
    vi.mocked(getUpdateState).mockClear();
    rerender(<AppUpdate shown />);
    await waitFor(() => expect(getUpdateState).toHaveBeenCalledTimes(1));
    await new Promise((resolve) => setTimeout(resolve, 0));
    // The cleanup of the hidden pass took it back once; showing asks again and keeps it.
    expect(hideUpdates).toHaveBeenCalledTimes(1);
  });
});
