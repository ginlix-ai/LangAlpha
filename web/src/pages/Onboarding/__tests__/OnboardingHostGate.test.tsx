import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';

let mockPhase = 'idle';
let mockVisible = false;
vi.mock('../OnboardingProvider', () => ({
  useOnboarding: () => ({ phase: mockPhase, gettingStarted: { visible: mockVisible } }),
}));

// Spy on the chunk boundary: the factory runs only if the lazy import fires.
const hostChunkLoaded = vi.hoisted(() => vi.fn());
vi.mock('../OnboardingHost', () => {
  hostChunkLoaded();
  return { OnboardingHost: () => <div data-testid="onboarding-host" /> };
});

import { OnboardingHostGate } from '../OnboardingHostGate';

describe('OnboardingHostGate', () => {
  beforeEach(() => {
    mockPhase = 'idle';
    mockVisible = false;
    hostChunkLoaded.mockClear();
  });

  it('renders nothing — and never imports the chunk — when no surface is needed', () => {
    const { container } = render(<OnboardingHostGate />);
    expect(container).toBeEmptyDOMElement();
    expect(hostChunkLoaded).not.toHaveBeenCalled();
  });

  it('mounts the host when a phase is active', async () => {
    mockPhase = 'pageIntro';
    render(<OnboardingHostGate />);
    await waitFor(() => expect(screen.getByTestId('onboarding-host')).toBeInTheDocument());
  });

  it('mounts the host when the getting-started card is visible', async () => {
    mockVisible = true;
    render(<OnboardingHostGate />);
    await waitFor(() => expect(screen.getByTestId('onboarding-host')).toBeInTheDocument());
  });

  it('latches: stays mounted after the surfaces go quiet', async () => {
    mockVisible = true;
    const { rerender } = render(<OnboardingHostGate />);
    await waitFor(() => expect(screen.getByTestId('onboarding-host')).toBeInTheDocument());

    mockVisible = false;
    rerender(<OnboardingHostGate />);
    expect(screen.getByTestId('onboarding-host')).toBeInTheDocument();
  });
});
