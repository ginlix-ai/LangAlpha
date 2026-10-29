import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';

let mockVisible = false;
vi.mock('../OnboardingProvider', () => ({
  useOnboarding: () => ({ gettingStarted: { visible: mockVisible } }),
}));

// Spy on the chunk boundary: the factory runs only if the lazy import fires.
const guideChunkLoaded = vi.hoisted(() => vi.fn());
vi.mock('../engine/GettingStarted', () => {
  guideChunkLoaded();
  return {
    GettingStartedLauncher: ({ variant }: { variant: string }) => (
      <div data-testid="guide">{variant}</div>
    ),
  };
});

import { GettingStartedGate } from '../GettingStartedGate';

describe('GettingStartedGate', () => {
  beforeEach(() => {
    mockVisible = false;
    guideChunkLoaded.mockClear();
  });

  it('renders nothing, and never imports the guide, once it is finished or hidden', () => {
    const { container } = render(<GettingStartedGate variant="row" />);
    expect(container).toBeEmptyDOMElement();
    expect(guideChunkLoaded).not.toHaveBeenCalled();
  });

  it('loads the guide while it has tasks to show', async () => {
    mockVisible = true;
    render(<GettingStartedGate variant="rail" onExpand={vi.fn()} />);
    expect(await screen.findByTestId('guide')).toHaveTextContent('rail');
  });
});
