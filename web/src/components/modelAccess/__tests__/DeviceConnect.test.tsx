/**
 * A device sign-in whose page arrives with the code filled in asks the user to
 * compare it, not paste it, and leaves the clipboard alone; one whose plan has
 * no API access offers no new code, since signing in again cannot help.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { renderWithProviders } from '@/test/utils';

const h = vi.hoisted(() => ({
  initiateDevice: vi.fn(),
  pollDevice: vi.fn(),
}));

vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({ rawModels: {}, metadata: {}, isLoading: false }),
}));
vi.mock('@/api/oauth', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/oauth')>()),
  initiateDevice: h.initiateDevice,
  pollDevice: h.pollDevice,
}));

import { DeviceConnect } from '../DeviceConnect';

const device = (prefilled: boolean) => ({
  user_code: 'ABCD-1234',
  verification_url: 'https://accounts.example.com/device?code=ABCD-1234',
  verification_url_has_code: prefilled,
  interval: 60,
  expires_at: null,
});

async function getCode() {
  // user-event puts its own clipboard on the page, so it is watched from here.
  const user = userEvent.setup();
  const write = vi.spyOn(navigator.clipboard, 'writeText');
  renderWithProviders(<DeviceConnect provider="grok-oauth" onConnected={() => {}} onCancel={() => {}} />);
  await user.click(screen.getByRole('checkbox'));
  await user.click(screen.getByRole('button', { name: /get my code/i }));
  return { user, write };
}

afterEach(() => {
  vi.clearAllMocks();
});

describe('DeviceConnect for Grok', () => {
  it('asks to compare a code the page fills in, without copying it', async () => {
    h.initiateDevice.mockResolvedValue(device(true));
    h.pollDevice.mockResolvedValue({ status: 'pending' });
    const { write } = await getCode();

    expect(await screen.findByText("SpaceXSI's page should show this code")).toBeInTheDocument();
    expect(screen.getByText('ABCD-1234')).toBeInTheDocument();
    expect(screen.getByText('Check that SpaceXSI shows the same code')).toBeInTheDocument();
    expect(screen.queryByText('Copy the code')).toBeNull();
    expect(write).not.toHaveBeenCalled();
  });

  it('asks to copy and paste a code the page does not fill in', async () => {
    h.initiateDevice.mockResolvedValue(device(false));
    h.pollDevice.mockResolvedValue({ status: 'pending' });
    const { write } = await getCode();

    expect(await screen.findByText('Your code')).toBeInTheDocument();
    expect(screen.getByText('Open SpaceXSI and paste the code')).toBeInTheDocument();
    expect(screen.queryByText('Check that SpaceXSI shows the same code')).toBeNull();
    expect(write).toHaveBeenCalledWith('ABCD-1234');
    expect(h.initiateDevice).toHaveBeenCalledWith('grok-oauth');
  });

  it('names a refusal by the code under error, not the detail sentence', async () => {
    h.initiateDevice.mockRejectedValue({
      response: {
        data: {
          detail: 'SpaceXSI is not offering device sign-in right now.',
          error: { code: 'device_flow_unavailable', message: 'unused', retryable: false },
        },
      },
    });
    await getCode();

    expect(await screen.findByText("SpaceXSI isn't offering code sign-in right now.")).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /try again/i })).toBeInTheDocument();
  });
});
