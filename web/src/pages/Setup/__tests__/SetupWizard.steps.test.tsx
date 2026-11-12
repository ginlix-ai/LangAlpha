/**
 * Where each setup route stands in the wizard: its dot on the stepper, and
 * whether the note on keys shows under it. Claude Code's step stands where a
 * sign-in's connect step does, without that note.
 */
import { describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter, Route, Routes } from 'react-router';

vi.mock('@/config/hostMode', () => ({ isPlatformMode: false, HOST_MODE: 'oss', APP_ENTRY_PATH: '/' }));
vi.mock('@/hooks/useSetupGate', () => ({ skipSetup: vi.fn() }));
vi.mock('@/hooks/useConfiguredProviders', () => ({ useConfiguredProviders: () => ({ providers: [], hasAny: false }) }));
vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: { access_tier: -1 } }) }));
vi.mock('@/contexts/ThemeContext', () => ({ useTheme: () => ({ theme: 'light' }) }));
vi.mock('../steps/MethodStep', () => ({ default: () => <p>step</p> }));
vi.mock('../steps/ProviderStep', () => ({ default: () => <p>step</p> }));
vi.mock('../steps/ConnectStep', () => ({ default: () => <p>step</p> }));
vi.mock('../steps/ClaudeCodeStep', () => ({ default: () => <p>step</p> }));
vi.mock('../steps/ModelPickStep', () => ({ default: () => <p>step</p> }));
vi.mock('../steps/DefaultsStep', () => ({ default: () => <p>step</p> }));
vi.mock('../steps/DoneStep', () => ({ default: () => <p>step</p> }));

import SetupWizard from '../SetupWizard';

function renderAt(pathname: string, state: unknown = null) {
  render(
    <QueryClientProvider client={new QueryClient()}>
      <MemoryRouter initialEntries={[{ pathname, state }]}>
        <Routes>
          <Route path="/setup/*" element={<SetupWizard />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

const NOTE = 'Your API keys are encrypted and never shared';

describe('SetupWizard steps', () => {
  it.each([
    ['/setup/method', null, 'Method', true],
    ['/setup/provider', null, 'Provider', true],
    ['/setup/connect', { method: 'api_key' }, 'Provider', true],
    ['/setup/connect', { method: 'oauth' }, 'Provider', false],
    ['/setup/claude-code', null, 'Provider', false],
    ['/setup/models', null, 'Models', false],
    ['/setup/defaults', null, 'Models', false],
    ['/setup/ready', null, 'Ready', false],
  ])('%s (%j) is on the %s dot, with the note on keys: %s', async (pathname, state, dot, note) => {
    renderAt(pathname, state);
    await screen.findByText('step');
    expect(screen.getByRole('listitem', { current: 'step' })).toHaveTextContent(dot);
    expect(screen.queryByText(NOTE) !== null).toBe(note);
  });
});
