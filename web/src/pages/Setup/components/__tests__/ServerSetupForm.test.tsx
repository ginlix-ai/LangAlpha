/**
 * Continue saves in order and stops at the first write that fails, which the
 * step says once, under the form.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, screen, waitFor, within } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import type { SetupStatus } from '@/api/setup';

const api = vi.hoisted(() => ({
  getSetupStatus: vi.fn(),
  saveSetupSandbox: vi.fn(),
  saveSetupKeys: vi.fn(),
  completeSetup: vi.fn(),
}));
vi.mock('@/api/setup', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  ...api,
}));
vi.mock('@/hooks/usePreferences', () => ({ usePreferences: () => ({ preferences: {}, isLoading: false }) }));
vi.mock('@/hooks/useUpdatePreferences', () => ({ useUpdatePreferences: () => ({ mutateAsync: vi.fn() }) }));
vi.mock('@/hooks/useModels', () => ({ useModels: () => ({ models: null }) }));

import { ServerSetupForm } from '../ServerSetupForm';

const status: SetupStatus = {
  completed: false,
  needs_setup: true,
  sandbox: {
    provider: 'docker',
    provider_source: 'detected',
    options: [{ kind: 'docker', available: true, detail: null }],
  },
  keys: [],
};

const finish = vi.fn();

function renderStep() {
  renderWithProviders(
    <ServerSetupForm
      layout="step"
      footer={(step) => (
        <button type="button" onClick={() => void step.run().then((saved) => saved && finish())}>
          Continue
        </button>
      )}
    />,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  api.getSetupStatus.mockResolvedValue(status);
  api.saveSetupSandbox.mockResolvedValue(status);
  api.saveSetupKeys.mockResolvedValue(status);
  api.completeSetup.mockResolvedValue({ ...status, completed: true, needs_setup: false });
});

describe('the server step', () => {
  it('says a failed sandbox save once, and saves nothing after it', async () => {
    api.saveSetupSandbox.mockRejectedValue(new Error('Network Error'));
    renderStep();
    fireEvent.click(await screen.findByRole('button', { name: 'Continue' }));
    expect(await screen.findByText('Could not save. Try again.')).toBeInTheDocument();
    expect(screen.getAllByText('Could not save. Try again.')).toHaveLength(1);
    expect(api.completeSetup).not.toHaveBeenCalled();
    expect(finish).not.toHaveBeenCalled();
  });

  it('saves where code runs, then marks the step answered, then goes on', async () => {
    renderStep();
    fireEvent.click(await screen.findByRole('button', { name: 'Continue' }));
    await waitFor(() => expect(finish).toHaveBeenCalled());
    expect(api.saveSetupSandbox).toHaveBeenCalledWith('docker', undefined);
    expect(api.saveSetupSandbox.mock.invocationCallOrder[0]).toBeLessThan(api.completeSetup.mock.invocationCallOrder[0]);
    // Nothing typed is no key to save.
    expect(api.saveSetupKeys).not.toHaveBeenCalled();
  });
});

describe('the Settings form', () => {
  it('gives the market data keys a Save of their own', async () => {
    renderWithProviders(<ServerSetupForm layout="panel" />);
    await screen.findByText('Market data');
    for (const anchor of ['market-data', 'web-search']) {
      const group = document.getElementById(anchor);
      expect(group, anchor).not.toBeNull();
      expect(within(group as HTMLElement).getByRole('button', { name: /save/i })).toBeInTheDocument();
    }
  });
});
